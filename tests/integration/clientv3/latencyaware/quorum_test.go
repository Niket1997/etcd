// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package latencyaware

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// The quorum fallback: the policy never takes more than MaxUnhealthyEndpoints
// out of rotation. When refused members plus members without a connection
// exceed it, it routes plain round robin and says why.

const (
	fallbackMsg    = "latency-aware: too many members unhealthy, routing plain round robin"
	fallbackEndMsg = "latency-aware: quorum fallback ended"
)

// fallbacks counts the fallback's start logs with cause at level.
func (cli *client) fallbacks(cause string, level zapcore.Level) int {
	return cli.logs.FilterMessage(fallbackMsg).FilterField(zap.String("cause", cause)).FilterLevelExact(level).Len()
}

// capped connects to every member with mode and the cap, and settles.
func capped(t *testing.T, c *cluster, mode string, maxUnhealthy int) *client {
	t.Helper()
	cli := c.client(t, with(tuning(), func(cfg *clientv3.LatencyAwareConfig) {
		cfg.Mode, cfg.MaxUnhealthyEndpoints = mode, maxUnhealthy
	}))
	settle(t, cli, len(c.Members))
	return cli
}

func TestTheCapDecidesBetweenRefusalAndTheFallback(t *testing.T) {
	for _, tc := range []struct {
		slow, maxUnhealthy int
		mode               string
		fallback           bool
	}{
		{2, 1, clientv3.LatencyAwareEnabled, true},
		{2, 2, clientv3.LatencyAwareEnabled, false},
		{3, 2, clientv3.LatencyAwareEnabled, true},
		{2, 1, clientv3.LatencyAwareShadow, true},
		{2, 2, clientv3.LatencyAwareShadow, false},
		{3, 2, clientv3.LatencyAwareShadow, true},
	} {
		t.Run(fmt.Sprintf("%d slow of 5, cap %d, %s", tc.slow, tc.maxUnhealthy, tc.mode), func(t *testing.T) {
			c := start(t, 5, false)
			cli := capped(t, c, tc.mode, tc.maxUnhealthy)
			for m := range tc.slow {
				c.Slow(m, slow, 0)
			}

			// Within the cap every slow member is refused, in shadow on paper.
			// Over it the fallback takes over as soon as the last one is refused.
			require.Truef(t, rounds(t, cli, 30, 20, func() bool {
				if tc.fallback {
					return cli.quorum() > 0
				}
				for m := range tc.slow {
					if cli.refused(c.addr(m)) == 0 {
						return false
					}
				}
				return true
			}), "the policy never reached the expected state")
			served := round(t, cli, c)

			switch {
			case tc.fallback:
				// Routing equals off, and the cause is the refused members themselves.
				even(t, served)
				assert.Zero(t, cli.fallbacks("missing", zapcore.WarnLevel)+cli.fallbacks("missing", zapcore.InfoLevel))
			case tc.mode == clientv3.LatencyAwareEnabled:
				// The slow members are out but for their probes, and the rest carry everything.
				assert.LessOrEqualf(t, sum(served[:tc.slow]), tc.slow, "retention: %v", served)
				even(t, served[tc.slow:])
				assert.Zero(t, cli.quorum())
			default:
				even(t, served)
				assert.Zero(t, cli.quorum())
			}
			// It warns when it overrides a live decision, and says so at Info
			// when nothing was being enforced.
			assert.Equal(t, tc.fallback && tc.mode == clientv3.LatencyAwareEnabled, cli.fallbacks("unhealthy", zapcore.WarnLevel) > 0)
			assert.Equal(t, tc.fallback && tc.mode == clientv3.LatencyAwareShadow, cli.fallbacks("unhealthy", zapcore.InfoLevel) > 0)
		})
	}
}

func TestAnUnreachableMemberCountsAgainstTheCap(t *testing.T) {
	for _, tc := range []struct {
		maxUnhealthy int
		fallback     bool
	}{{1, true}, {2, false}} {
		t.Run(fmt.Sprintf("cap %d", tc.maxUnhealthy), func(t *testing.T) {
			c := start(t, 5, false)
			cli := capped(t, c, clientv3.LatencyAwareEnabled, tc.maxUnhealthy)

			// One member gone (no READY connection) plus one slow one is two out
			// of rotation, which only a cap of 2 allows.
			c.Members[4].Stop(t)
			require.Eventuallyf(t, func() bool { return cli.members("ready") == 4 }, 10*time.Second, 20*time.Millisecond, "the stopped member still counts as ready")
			c.Slow(0, slow, 0)
			require.Truef(t, rounds(t, cli, 30, 20, func() bool {
				return cli.quorum() > 0 || cli.refused(c.addr(0)) > 0
			}), "the slow member was never judged")

			if tc.fallback {
				assert.Positivef(t, cli.fallbacks("missing", zapcore.WarnLevel), "the missing member tipped it, not the refusals")
				assert.Zero(t, cli.fallbacks("unhealthy", zapcore.WarnLevel))
				assert.Zerof(t, cli.refused(c.addr(0)), "the fallback is checked before any pick is refused")
			} else {
				assert.Positive(t, cli.refused(c.addr(0)))
				assert.Zero(t, cli.quorum())
			}
		})
	}
}

func TestTheFallbackEndsAsSoonAsAMemberHeals(t *testing.T) {
	c := start(t, 5, false)
	cli := capped(t, c, clientv3.LatencyAwareEnabled, 1)
	c.Slow(0, slow, 0)
	c.Slow(1, slow, 0)
	require.Truef(t, rounds(t, cli, 30, 20, func() bool { return cli.quorum() > 0 }), "the quorum fallback never started")

	// The fallback is decided per pick, never latched. Under it member 1 keeps
	// getting calls, so once fast again its average drops under the line
	// within a few samples, one refused member fits the cap, and member 0 is
	// refused from the next pick on.
	c.Heal(1)
	refused := false
	for i := 0; i < 20 && !refused; i++ {
		refused = round(t, cli, c)[0] == 0
	}
	require.Truef(t, refused, "member 0 was never refused again")
	quorum := cli.quorum()
	assert.LessOrEqualf(t, round(t, cli, c)[0], 1, "only a probe reaches member 0")
	assert.Equalf(t, quorum, cli.quorum(), "the fallback stays off")
	assert.Positive(t, cli.refused(c.addr(0)))
	assert.Positive(t, cli.logged(fallbackEndMsg))
}

func TestASlowdownEverywhereNeverStopsTraffic(t *testing.T) {
	c := start(t, 3, false)
	cli := capped(t, c, clientv3.LatencyAwareEnabled, 2)

	// Every member turns slow at once. Until the cluster minimum follows them
	// every member is refused, three with a cap of two is the fallback's case,
	// and after the rescan nobody is over twice the new minimum: either way no
	// call is refused and nothing fails.
	for m := range 3 {
		c.Slow(m, slow, 0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rs := run(ctx, loadConfig{rate: 200, maxInFlight: 8, timeout: time.Second, req: get(cli)})
	assert.Zero(t, rs.failed())
	assert.Greaterf(t, len(rs), 40, "eight in flight at 100 ms is about 80 a second")
	assert.Lenf(t, rs.byMember(), 3, "every member kept serving")

	// One member heals: it is the minimum again, the other two are over the
	// line, two refused fit the cap, and it carries the traffic alone.
	c.Heal(0)
	require.Truef(t, rounds(t, cli, 30, 20, func() bool {
		return cli.refused(c.addr(1)) > 0 && cli.refused(c.addr(2)) > 0
	}), "the slow pair was never refused")
	served := during(t, cli, c, 300*time.Millisecond)
	assert.GreaterOrEqualf(t, served[0], sum(served)*9/10, "the healed member carries the calls: %v", served)
}
