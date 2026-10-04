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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Installing the policy, changing it at runtime, and what it reports.
// Dashboards and alerts key on the metric names and labels, so the exact set is pinned.

// steadyMetrics is everything a healthy client reports, enabled or in shadow.
var steadyMetrics = map[string][]string{
	"picks":            {"etcd.endpoint", "etcd.outcome", "grpc.target"},
	"rpcs":             {"etcd.endpoint", "etcd.result", "grpc.status", "grpc.target"},
	"endpoints":        {"etcd.state", "grpc.target"},
	"cluster_min":      {"grpc.target"},
	"endpoint_latency": {"etcd.endpoint", "grpc.target"},
}

func TestNoPolicyMeansPlainRoundRobinAndNoMetrics(t *testing.T) {
	c := start(t, 3, false)
	cli := c.client(t, nil)
	warm(t, cli, 3)

	// Round robin gives each member 10 of 30; 8 leaves room for a connection blip.
	for m, n := range round(t, cli, c) {
		assert.GreaterOrEqualf(t, n, 8, "member %d served %d of 30", m, n)
	}
	assert.Empty(t, cli.metrics.contract())
}

func TestAPlainClientKeepsRoundRobinWhileItsSiblingRoutesAround(t *testing.T) {
	c := start(t, 3, false)
	sibling := enabled(t, c)
	plain := c.client(t, nil)
	warm(t, plain, 3)
	c.Slow(0, slow, 0)

	// Only the sibling sends now, so member 0's count is the sibling's share.
	refused := false
	for i := 0; i < 20 && !refused; i++ {
		refused = round(t, sibling, c)[0] == 0
	}
	require.Truef(t, refused, "the sibling kept picking the slow member")

	// The plain client still round-robins, slow member included, and pays for it.
	began := time.Now()
	served := round(t, plain, c)
	assert.GreaterOrEqualf(t, served[0], 8, "round robin sends a third of the calls to the slow member: %v", served)
	assert.GreaterOrEqualf(t, time.Since(began), 800*time.Millisecond, "each of those calls waited 100 ms")
	assert.Empty(t, plain.metrics.contract())
}

func TestEachClientLearnsFromItsOwnCalls(t *testing.T) {
	// State is per client: a member another client refused is still fast
	// to this one until its own calls show otherwise.
	c := start(t, 3, false)
	teacher, learner := enabled(t, c), enabled(t, c)
	c.Slow(0, slow, 0)
	refuse(t, teacher, c.addr(0))

	served := round(t, learner, c)
	assert.Positivef(t, served[0], "the learner skipped a member only the teacher refused: %v", served)
	refuse(t, learner, c.addr(0))
}

func TestAModeChangeTakesEffectOnTheNextPick(t *testing.T) {
	c := start(t, 3, false)
	mode := func(m string) clientv3.LatencyAwareConfig {
		return *with(tuning(), func(cfg *clientv3.LatencyAwareConfig) { cfg.Mode = m })
	}
	off := mode(clientv3.LatencyAwareOff)
	cli := c.client(t, &off)
	warm(t, cli, 3)
	c.Slow(0, slow, 0)

	// Off: plain round robin, and nothing learned or counted.
	assert.GreaterOrEqual(t, round(t, cli, c)[0], 8)
	assert.Zero(t, cli.outcomes())

	// Shadow judges and reports as if it enforced; routing stays round robin.
	require.NoError(t, cli.SetLatencyAwareConfig(mode(clientv3.LatencyAwareShadow)))
	refuse(t, cli, c.addr(0))
	assert.GreaterOrEqualf(t, round(t, cli, c)[0], 8, "shadow still routes to the slow member")
	assert.Equalf(t, cli.refused(""), cli.refused(c.addr(0)), "only member 0 was judged")

	// Enabled enforces shadow's verdict from the next pick. A fast member's
	// stall can briefly make two look slow, which the quorum fallback routes
	// round robin, so a clean round is awaited.
	require.NoError(t, cli.SetLatencyAwareConfig(mode(clientv3.LatencyAwareEnabled)))
	refused := false
	for i := 0; i < 5 && !refused; i++ {
		refused = round(t, cli, c)[0] == 0
	}
	assert.Truef(t, refused, "enabled never refused the slow member for a whole round")

	// Off again: the share is back within the round, and counting stops.
	require.NoError(t, cli.SetLatencyAwareConfig(off))
	seen := cli.outcomes()
	assert.GreaterOrEqual(t, round(t, cli, c)[0], 8)
	assert.Equal(t, seen, cli.outcomes())
}

func TestAHealthyClientReportsExactlyTheSteadyMetrics(t *testing.T) {
	for _, mode := range []string{clientv3.LatencyAwareEnabled, clientv3.LatencyAwareShadow} {
		t.Run(mode, func(t *testing.T) {
			c := start(t, 3, false)
			cli := c.client(t, with(steady(), func(cfg *clientv3.LatencyAwareConfig) { cfg.Mode = mode }))
			settle(t, cli, 3)

			assert.Equal(t, steadyMetrics, cli.metrics.contract())
			picks, _ := cli.metrics.sum("name", "picks")
			assert.Equalf(t, picks, cli.picks("allowed", ""), "every pick allowed")
			assert.Equalf(t, cli.outcomes(), cli.rpcs("etcd.result", "success"), "every call a success")
			for m := range 3 {
				assert.Positivef(t, cli.picks("allowed", c.addr(m)), "member %d was picked", m)
				avg, _ := cli.gauge("endpoint_latency", "etcd.endpoint", c.addr(m))
				assert.Positivef(t, avg, "member %d's average", m)
			}
			for state, want := range map[string]int64{"total": 3, "ready": 3, "refused": 0} {
				got, ok := cli.gauge("endpoints", "etcd.state", state)
				assert.Truef(t, ok, "no %s gauge", state)
				assert.Equalf(t, want, got, "%s members", state)
			}
			minimum, _ := cli.gauge("cluster_min")
			assert.EqualValuesf(t, 50000, minimum, "fast members sit on the floor")
		})
	}
}

func TestFaultsAreClassifiedByCode(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)

	// An error the policy does not act on is counted under its code, once per failed call.
	c.Fail(0, status.Error(codes.Internal, "boom"))
	failed := sequential(30, time.Second, get(cli)).failed()
	c.Heal(0)
	assert.Equal(t, 10, failed)
	assert.EqualValues(t, failed, cli.rpcs("etcd.endpoint", c.addr(0), "etcd.result", "ignored", "grpc.status", "Internal"))
	assert.Zero(t, cli.rpcs("etcd.result", "failure"))
	assert.Zero(t, cli.refused(""))

	// A hung member's timeouts are failures under DeadlineExceeded. With a slow
	// member already refused that is two refused, over the cap: the fallback
	// names the refusals as its cause.
	c.Slow(0, slow, 0)
	refuse(t, cli, c.addr(0))
	assert.Zero(t, cli.refused(c.addr(1)))
	c.Hang(1)
	timedOut := 0
	for deadline := time.Now().Add(10 * time.Second); cli.fallbacks("unhealthy", zapcore.WarnLevel) == 0; {
		require.Truef(t, time.Now().Before(deadline), "the quorum fallback never started")
		timedOut += sequential(30, 100*time.Millisecond, get(cli)).failed()
	}
	assert.Positive(t, timedOut)
	assert.GreaterOrEqual(t, cli.rpcs("etcd.result", "failure", "grpc.status", "DeadlineExceeded"), int64(timedOut))
	assert.Positive(t, cli.rpcs("etcd.endpoint", c.addr(1), "etcd.result", "failure"))
	assert.Zerof(t, cli.fallbacks("missing", zapcore.WarnLevel), "every member was connected")
}

func TestAStoppedMemberShowsInTheEndpointGauges(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)

	// Down, the member is resolved but not READY; back, it is READY again. It
	// never leaves the total.
	c.Members[2].Stop(t)
	require.Eventuallyf(t, func() bool { return cli.members("ready") == 2 }, 10*time.Second, 20*time.Millisecond, "the stopped member still counts as ready")
	gets(t, cli, 30)
	assert.EqualValues(t, 3, cli.members("total"))
	require.NoError(t, c.Members[2].Restart(t))
	require.Eventuallyf(t, func() bool { return cli.members("ready") == 3 }, 15*time.Second, 20*time.Millisecond, "the restarted member never became ready")
	assert.EqualValues(t, 3, cli.members("total"))
}

func TestTheOnlyMemberIsStillUsedWhenRefused(t *testing.T) {
	c := start(t, 1, false)
	cli := c.client(t, tuning())
	gets(t, cli, 12) // past warm-up, so slow samples count

	// Nothing else can take the call, so the policy falls back to the refused
	// member and counts it. One refused of one is within the cap.
	c.Slow(0, slow, 0)
	require.Truef(t, rounds(t, cli, 10, 10, func() bool { return cli.picks("fallback", "") > 0 }), "the fallback never fired")
	assert.Zero(t, cli.quorum())
	assert.Zerof(t, cli.rpcs("etcd.result", "failure"), "every call went through")
}

func TestAStreamIsCountedButNeverTimed(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)
	success, ignored := cli.rpcs("etcd.result", "success"), cli.rpcs("etcd.result", "ignored")

	// One Put, and one watch that replays it and closes with its context.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	put, err := cli.Put(ctx, "k", "v")
	require.NoError(t, err)
	wctx, stop := context.WithCancel(ctx)
	events := cli.Watch(wctx, "k", clientv3.WithRev(put.Header.Revision))
	require.NotEmptyf(t, (<-events).Events, "the stream is live")
	stop()
	for resp := range events {
		require.NoError(t, resp.Err())
	}

	// The Put is a timed success; the stream ends Canceled, counted and ignored.
	require.Eventuallyf(t, func() bool { return cli.outcomes() == success+ignored+2 }, 5*time.Second, 20*time.Millisecond, "the Put and the closed stream are both counted")
	assert.Equal(t, success+1, cli.rpcs("etcd.result", "success"))
	assert.Equal(t, ignored+1, cli.rpcs("etcd.result", "ignored", "grpc.status", "Canceled"))
}

func TestTheSurvivorsSplitARefusedMembersShareEvenly(t *testing.T) {
	c := start(t, 5, false)
	cli := c.client(t, with(steady(), func(cfg *clientv3.LatencyAwareConfig) {
		cfg.RetryPeriod = time.Minute // no probe while counting
	}))
	settle(t, cli, 5)
	c.Slow(0, 2*slow, 0)
	refuse(t, cli, c.addr(0))

	// The picker takes the (cursor mod 4)-th admitted member, a strict rotation
	// over the four. Moving on to the next member instead would give the one
	// after the refused member a double share.
	before := c.Ranges()
	gets(t, cli, 400)
	assert.Equal(t, []int{0, 100, 100, 100, 100}, delta(before, c.Ranges()))
}
