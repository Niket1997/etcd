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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Contention, throughput and lifecycle: what a policy on the hot path must not cost.

// deadlines counts the results that ended at their deadline.
func deadlines(rs results) int {
	n := 0
	for _, r := range rs {
		if errors.Is(r.err, context.DeadlineExceeded) || status.Code(r.err) == codes.DeadlineExceeded {
			n++
		}
	}
	return n
}

func TestPicksSurviveModeChangesAndEndpointChurnUnderContention(t *testing.T) {
	c := start(t, 3, false)
	eps := c.Endpoints()
	cli := enabled(t, c)
	allowed, outcomes := cli.picks("allowed", ""), cli.outcomes()

	// Two hundred goroutines call while the mode cycles every 10 ms and the
	// endpoint set shrinks and grows every 50 ms. Nothing fails or deadlocks,
	// and the policy keeps admitting and completing picks throughout.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var calls, failed atomic.Int64
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				gctx, gcancel := context.WithTimeout(ctx, time.Second)
				_, err := get(cli)(gctx)
				gcancel()
				calls.Add(1)
				if err != nil && ctx.Err() == nil {
					failed.Add(1)
				}
			}
		}()
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		modes := []string{clientv3.LatencyAwareOff, clientv3.LatencyAwareShadow, clientv3.LatencyAwareEnabled}
		for i := 0; ctx.Err() == nil; i++ {
			assert.NoError(t, cli.SetLatencyAwareConfig(*with(tuning(), func(cfg *clientv3.LatencyAwareConfig) { cfg.Mode = modes[i%3] })))
			time.Sleep(10 * time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			if i%2 == 0 {
				cli.SetEndpoints(eps[1], eps[2])
			} else {
				cli.SetEndpoints(eps...)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	wg.Wait()

	assert.Zero(t, failed.Load())
	assert.Greater(t, calls.Load(), int64(1000))
	assert.Greaterf(t, cli.picks("allowed", ""), allowed, "the policy admitted picks")
	assert.Greaterf(t, cli.outcomes(), outcomes, "picks completed")
}

func TestAnEnabledPolicyCostsNothingAtTwoThousandRPS(t *testing.T) {
	c := start(t, 3, false)
	off := c.client(t, nil)
	// Steady's 100 ms line clears the scheduling jitter 2000 rps adds.
	on := c.client(t, steady())
	warm(t, off, 3)
	warm(t, on, 3)
	load := func(cli *client) results {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return run(ctx, loadConfig{rate: 2000, maxInFlight: 64, timeout: time.Second, req: get(cli)})
	}

	// Two seconds at 2000 rps without the policy and with it, twice and
	// interleaved so a noisy neighbour hits both alike: as many calls, none
	// refused, and the median within a fifth plus 100 µs. The p99 follows the
	// box's scheduling more than the policy, so it is logged, not asserted.
	var offRS, onRS results
	for range 2 {
		offRS = append(offRS, load(off)...)
		onRS = append(onRS, load(on)...)
	}
	assert.Zero(t, offRS.failed())
	assert.Zero(t, onRS.failed())
	assert.InDeltaf(t, len(offRS), len(onRS), float64(len(offRS))/10, "throughput off %d vs on %d", len(offRS), len(onRS))
	p50Off, p50On := offRS.percentile(50), onRS.percentile(50)
	t.Logf("p50 off %v on %v; p99 off %v on %v", p50Off, p50On, offRS.percentile(99), onRS.percentile(99))
	assert.Lessf(t, p50On, p50Off*6/5+100*time.Microsecond, "median on %v vs off %v", p50On, p50Off)
	assert.Zero(t, on.refused(""))
}

func TestAHundredClientsComeAndGoAndLeaveNothingBehind(t *testing.T) {
	// Each client's state is built with its connection and dropped with it. A
	// hundred come and go, each counting the three members on its own, and the
	// leak check every test here ends with finds nothing they left.
	c := start(t, 3, false)
	for range 100 {
		cli := c.client(t, tuning())
		gets(t, cli, 1)
		assert.EqualValues(t, 3, cli.members("total"))
		require.NoError(t, cli.Close())
	}
}

func TestWithoutThePolicyAHungMemberCostsAThirdOfTheCalls(t *testing.T) {
	c := start(t, 3, false)
	off := c.client(t, with(tuning(), func(cfg *clientv3.LatencyAwareConfig) { cfg.Mode = clientv3.LatencyAwareOff }))
	settle(t, off, 3)
	on := enabled(t, c)
	c.Hang(0)
	load := func(cli *client) results {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return run(ctx, loadConfig{rate: 100, maxInFlight: 64, timeout: 200 * time.Millisecond, req: get(cli)})
	}

	// Off is plain round robin: a third of the calls go to the hung member and
	// every one of them dies at its deadline.
	offRS := load(off)
	assert.InDeltaf(t, len(offRS)/3, offRS.failed(), float64(len(offRS))/10, "off: %d of %d failed", offRS.failed(), len(offRS))
	assert.Equalf(t, offRS.failed(), deadlines(offRS), "every failure is a deadline")

	// Enabled loses the calls in flight when the first deadlines land, refuses
	// the member on them, and then only a probe per retry period.
	onRS := load(on)
	t.Logf("failed off %d of %d, on %d of %d", offRS.failed(), len(offRS), onRS.failed(), len(onRS))
	assert.Lessf(t, onRS.failed(), len(onRS)/10, "enabled: %d of %d failed", onRS.failed(), len(onRS))
	assert.Equalf(t, onRS.failed(), deadlines(onRS), "every failure is a deadline")
	assert.Positive(t, on.refused(c.addr(0)))
}
