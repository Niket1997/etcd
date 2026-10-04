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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// The policy's core: what refusing a slow member costs and how fast it comes,
// what re-testing it costs, how a healed member comes back, and the edges of
// the signal: warm-up, floor, the EMA's memory and the errors that count.

func TestOneSlowMemberIsRefusedWithinThreeSecondsAndThenSeesOnlyProbes(t *testing.T) {
	for _, members := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d members", members), func(t *testing.T) {
			c := start(t, members, false)
			cli := enabled(t, c)
			c.Slow(0, slow, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			done := make(chan results, 1)
			go func() { done <- run(ctx, loadConfig{rate: 200, maxInFlight: 8, timeout: time.Second, req: get(cli)}) }()

			// Past warm-up, refusing needs a few slow samples and a rescan: well
			// inside 3 s at 200 rps.
			require.Eventuallyf(t, func() bool { return cli.refused(c.addr(0)) > 0 }, 3*time.Second, 10*time.Millisecond, "member 0 was not refused within 3 s")
			before := c.Ranges()
			rs := <-done
			after := delta(before, c.Ranges())
			total := sum(after)

			assert.Zero(t, rs.failed())
			// Only probes reach a refused member: the first timed pick due each retry period.
			assert.LessOrEqualf(t, after[0], total/20, "the slow member served %d of %d after refusal", after[0], total)
			for m := 1; m < members; m++ {
				assert.GreaterOrEqualf(t, after[m], 9*total/(10*(members-1)), "survivor %d served %d of %d", m, after[m], total)
			}
		})
	}
}

func TestSubThresholdSlownessIsNeverRefused(t *testing.T) {
	c := start(t, 3, false)
	cli := c.client(t, steady())
	settle(t, cli, 3)

	// 40 ms is under the 100 ms line: clearly slow, clearly fine, so the member
	// keeps its round-robin third.
	c.Slow(0, 40*time.Millisecond, 0)
	served := during(t, cli, c, 3*steady().RetryPeriod)
	assert.Zero(t, cli.refused(""))
	assert.GreaterOrEqualf(t, served[0], sum(served)*3/10, "the 40 ms member keeps its third: %v", served)
}

func TestTheThresholdDecidesWhetherTheSameFaultRefuses(t *testing.T) {
	// Over a 20 ms floor the lines are 40 ms at threshold 2 and 60 ms at 3; a
	// 50 ms fault sits between them with 10 ms to spare each way.
	for _, tc := range []struct {
		threshold float64
		refuses   bool
	}{{2, true}, {3, false}} {
		t.Run(fmt.Sprintf("threshold %v", tc.threshold), func(t *testing.T) {
			c := start(t, 3, false)
			cli := c.client(t, with(tuning(), func(cfg *clientv3.LatencyAwareConfig) {
				cfg.MinLatencyFloor, cfg.ExclusionThreshold = 20*time.Millisecond, tc.threshold
			}))
			settle(t, cli, 3)

			c.Slow(0, 50*time.Millisecond, 0)
			assert.Equal(t, tc.refuses, rounds(t, cli, 30, 6, func() bool { return cli.refused(c.addr(0)) > 0 }))
		})
	}
}

func TestAHealedMemberIsReadmittedWithinTwoRetryPeriods(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)
	c.Slow(0, slow, 0)
	refuse(t, cli, c.addr(0))

	c.Heal(0)
	healed := time.Now()
	// One probe gets through per retry period. The first fast one leaves the
	// average near 15 ms (its weight is 0.85 after a 2 s gap); the second
	// brings it under the line.
	for round(t, cli, c)[0] < 8 {
		require.Lessf(t, time.Since(healed), 2*tuning().RetryPeriod+500*time.Millisecond, "member 0 did not get its share back within two retry periods")
	}
	assert.Zero(t, cli.rpcs("etcd.result", "failure"))
}

func TestAStillSlowMemberCostsOneProbePerRetryPeriodEvenUnderLoad(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)
	c.Slow(0, slow, 0)
	refuse(t, cli, c.addr(0))
	period := tuning().RetryPeriod

	// Sequentially, one probe per retry period reaches the member and its own
	// sample refuses it again: at most one call per period, three allow for a boundary.
	served := during(t, cli, c, 2*period+200*time.Millisecond)
	assert.GreaterOrEqualf(t, served[0], 1, "the member is still being re-tested")
	assert.LessOrEqualf(t, served[0], 3, "probes in two retry periods: %d", served[0])

	// Under load the same: the pick that claims the probe goes through, and
	// every other pick for the member while it is in flight does not.
	before := c.Ranges()
	ctx, cancel := context.WithTimeout(context.Background(), period+300*time.Millisecond)
	defer cancel()
	rs := run(ctx, loadConfig{rate: 200, maxInFlight: 8, timeout: time.Second, req: get(cli)})
	probes := delta(before, c.Ranges())[0]
	assert.Zero(t, rs.failed())
	assert.GreaterOrEqualf(t, probes, 1, "the member is still being re-tested under load")
	assert.LessOrEqualf(t, probes, 2, "one probe per retry period in %d calls, two allow for a boundary", len(rs))
}

func TestALateSlowMemberGetsItsWarmupSamplesAndIsThenJudged(t *testing.T) {
	c := start(t, 3, false)
	eps := c.Endpoints()
	cli := enabled(t, c, eps[:2]...)

	// A member is judged only after its warm-up samples, so exactly ten calls
	// reach the late member, whatever they cost, and none after.
	c.Slow(2, slow, 0)
	cli.SetEndpoints(eps...)
	require.Truef(t, rounds(t, cli, 30, 20, func() bool { return cli.refused(c.addr(2)) > 0 }), "the late member was never judged")
	assert.Equalf(t, 10, c.Ranges()[2], "calls that reached the late member before its first refusal")
}

func TestAClusterThatStartsSlowEverywhereRefusesNobody(t *testing.T) {
	c := start(t, 3, false)
	for m := range 3 {
		c.Slow(m, 30*time.Millisecond, 0)
	}
	cli := enabled(t, c)

	// The policy is relative: the minimum is 30 ms, the line 60 ms, everyone is fine.
	served := during(t, cli, c, 500*time.Millisecond)
	assert.Zero(t, cli.refused(""))
	assert.Zero(t, cli.quorum())
	minimum, _ := cli.gauge("cluster_min")
	assert.Greaterf(t, minimum, int64(25000), "the minimum follows the members, not the floor")
	even(t, served)
}

func TestAFastClusterSitsOnTheFloorAndNeverRefuses(t *testing.T) {
	c := start(t, 3, false)
	cli := c.client(t, steady())
	settle(t, cli, 3)

	// Members answer in well under the 50 ms floor, so the minimum is the floor
	// and the line 100 ms: no jitter between near-equal members crosses it.
	served := during(t, cli, c, 3*steady().RetryPeriod)
	assert.Zero(t, cli.refused(""))
	minimum, _ := cli.gauge("cluster_min")
	assert.EqualValues(t, 50000, minimum)
	even(t, served)
}

func TestRemovingTheFastestMemberRefreshesTheClusterMinimum(t *testing.T) {
	c := start(t, 3, false)
	c.Slow(1, 12*time.Millisecond, 0)
	c.Slow(2, 12*time.Millisecond, 0)
	eps := c.Endpoints()
	cli := enabled(t, c)

	// Next to the fast member the floor is the minimum, 12 ms is over the 10 ms
	// line for both others, and two refused are more than the cap of one.
	require.Truef(t, rounds(t, cli, 30, 20, func() bool { return cli.quorum() > 0 }), "the quorum fallback never started")

	// Without the fast member the pair's own 12 ms is the minimum, the line is
	// 24 ms, and both are fine again after the rescan.
	cli.SetEndpoints(eps[1], eps[2])
	require.Truef(t, rounds(t, cli, 30, 20, func() bool {
		minimum, _ := cli.gauge("cluster_min")
		return cli.members("refused") == 0 && minimum >= 10000
	}), "the minimum was not refreshed")
	quorum, refused := cli.quorum(), cli.refused("")
	gets(t, cli, 60)
	assert.Equalf(t, quorum, cli.quorum(), "no new fallback picks")
	assert.Equalf(t, refused, cli.refused(""), "no new refusals")
}

func TestEMAScaleSetsHowManySlowSamplesRefusingTakes(t *testing.T) {
	// A sample's weight grows with the gap since the member's previous one:
	// 100 ms gaps weigh 0.3 at a 100 ms scale, so the first slow sample
	// refuses, and 0.005 at a 10 s scale, where it takes about twenty.
	for _, tc := range []struct {
		scale            time.Duration
		withinTenSamples bool
	}{{100 * time.Millisecond, true}, {10 * time.Second, false}} {
		t.Run(fmt.Sprintf("scale %v", tc.scale), func(t *testing.T) {
			c := start(t, 3, false)
			cli := c.client(t, with(tuning(), func(cfg *clientv3.LatencyAwareConfig) { cfg.EMAScale = tc.scale }))
			settle(t, cli, 3)

			c.Slow(0, slow, 0)
			gets(t, cli, 30) // ten slow samples
			assert.Equal(t, tc.withinTenSamples, cli.refused(c.addr(0)) > 0)
			require.Truef(t, rounds(t, cli, 30, 5, func() bool { return cli.refused(c.addr(0)) > 0 }),
				"sustained slowness refuses within sixty slow samples at any scale")
		})
	}
}

func TestATrippableErrorRefusesOnTheThirdFailureInOneRetryPeriod(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault func(*cluster)
		code  string
	}{
		// etcd sends its apply backpressure as a plain error, so it arrives as Unknown.
		{"too many requests", func(c *cluster) { c.Fail(0, rpctypes.ErrTooManyRequests) }, "Unknown"},
		{"hang", func(c *cluster) { c.Hang(0) }, "DeadlineExceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := start(t, 3, false)
			cli := c.client(t, with(steady(), func(cfg *clientv3.LatencyAwareConfig) {
				cfg.RetryPeriod = 10 * time.Second // a stall cannot split the three strikes
			}))
			settle(t, cli, 3)

			// Paced calls, every third on the faulty member, until it is refused:
			// two failures are strikes, the third records the synthetic 1 s sample,
			// which crosses the line at once.
			tc.fault(c)
			for deadline := time.Now().Add(10 * time.Second); cli.refused(c.addr(0)) == 0 && time.Now().Before(deadline); {
				sequential(1, 200*time.Millisecond, get(cli))
				time.Sleep(50 * time.Millisecond)
			}
			assert.EqualValuesf(t, 3, cli.rpcs("etcd.endpoint", c.addr(0), "etcd.result", "failure", "grpc.status", tc.code), "three failures, then refusal")
			assert.Positive(t, cli.refused(c.addr(0)))
		})
	}
}

func TestTwoTrippableFailuresInOneRetryPeriodAreForgiven(t *testing.T) {
	c := start(t, 3, false)
	// Steady: under the 10 ms line, scheduling jitter could judge member 0 slow
	// before it fails once, and a refused member's failure counts at once.
	cli := c.client(t, steady())
	settle(t, cli, 3)

	c.Fail(0, rpctypes.ErrTooManyRequests)
	failures := func() int64 { return cli.rpcs("etcd.endpoint", c.addr(0), "etcd.result", "failure") }
	for i := 0; i < 12 && failures() < 2; i++ {
		sequential(1, time.Second, get(cli))
	}
	require.EqualValues(t, 2, failures())
	c.Heal(0)

	served := during(t, cli, c, 500*time.Millisecond)
	assert.Zerof(t, cli.refused(c.addr(0)), "two failures do not refuse")
	assert.GreaterOrEqualf(t, served[0], sum(served)/4, "member 0 kept its share: %v", served)
}

func TestNonTrippableErrorsAreCountedButNeverRefuse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		code    string
		retried bool // clientv3 retries these on another member
	}{
		{"unavailable", status.Error(codes.Unavailable, "down"), "Unavailable", true},
		{"canceled by the server", status.Error(codes.Canceled, "no"), "Canceled", true}, // the caller's context is alive
		{"internal", status.Error(codes.Internal, "boom"), "Internal", false},
		{"compacted", rpctypes.ErrGRPCCompacted, "OutOfRange", false},
		{"no space", rpctypes.ErrGRPCNoSpace, "ResourceExhausted", false}, // cluster-wide, not this member
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := start(t, 3, false)
			cli := enabled(t, c)

			c.Fail(0, tc.err)
			rs := sequential(30, 2*time.Second, get(cli))
			assert.Positive(t, cli.rpcs("etcd.endpoint", c.addr(0), "etcd.result", "ignored", "grpc.status", tc.code))
			assert.Zero(t, cli.rpcs("etcd.result", "failure"))
			assert.Zero(t, cli.refused(c.addr(0)))
			if tc.retried {
				assert.Zero(t, rs.failed())
			} else {
				assert.Equalf(t, 10, rs.failed(), "a third of the calls hit the failing member")
			}
		})
	}
}

func TestALongCompactionIsWorkNotSlowness(t *testing.T) {
	// A legitimately long unary call, a compaction held 3 s on every member,
	// is counted but not timed: whoever served it keeps its share.
	c := start(t, 3, false)
	cli := c.client(t, steady())
	settle(t, cli, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	put, err := cli.Put(ctx, "k", "v")
	require.NoError(t, err)

	for m := range 3 {
		c.Slow(m, 3*time.Second, 0)
	}
	_, err = cli.Compact(ctx, put.Header.Revision)
	require.NoError(t, err)
	for m := range 3 {
		c.Heal(m)
	}

	served := during(t, cli, c, 500*time.Millisecond)
	assert.Zerof(t, cli.refused(""), "nobody is refused")
	for m := range 3 {
		avg, _ := cli.gauge("endpoint_latency", "etcd.endpoint", c.addr(m))
		assert.Lessf(t, avg, int64(time.Second/time.Microsecond), "member %d averages %d us: the compaction was timed", m, avg)
	}
	even(t, served)
}
