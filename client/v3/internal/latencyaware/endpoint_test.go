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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock starts an hour in, so "never" (zero) is always in the past.
type fakeClock struct{ t atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.t.Store(int64(time.Hour))
	return c
}

func (c *fakeClock) now() int64              { return c.t.Load() }
func (c *fakeClock) advance(d time.Duration) { c.t.Add(int64(d)) }

// testConfig is the default config with a 100 ms EMA
// scale, a small warm-up and an hour-long retry period, so nothing expires
// unless a test moves the clock that far.
func testConfig(t *testing.T) *Config {
	t.Helper()
	c, err := Config{EMAScale: 100 * time.Millisecond, WarmupSamples: 3, RetryPeriod: time.Hour, MinLatencyFloor: time.Millisecond}.withDefaults()
	require.NoError(t, err)
	return &c
}

// pin sets a member's state directly: n samples averaging avg, the last at last.
func pin(e *endpoint, n int, avg time.Duration, last int64) {
	e.n.Store(int64(n))
	e.avg.Store(int64(avg))
	e.last.Store(last)
}

// sample records one success of latency d that completes now.
func sample(e *endpoint, clk *fakeClock, c *Config, d time.Duration) {
	e.success(clk.now, clk.now()-int64(d), c)
}

func TestWarmupIsTheRunningMean(t *testing.T) {
	clk, c, e := newFakeClock(), testConfig(t), &endpoint{}
	for _, d := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond} {
		sample(e, clk, c, d)
	}
	assert.EqualValues(t, 3, e.n.Load())
	assert.Equalf(t, int64(20*time.Millisecond), e.avg.Load(), "the mean of every warm-up sample")

	e = &endpoint{}
	pin(e, 2, 20, 0)
	sample(e, clk, c, 5)
	assert.EqualValuesf(t, 15, e.avg.Load(), "a sample below the mean lowers it")
}

func TestTheEMAKeepsLog1pWeight(t *testing.T) {
	c := testConfig(t)
	c.WarmupSamples = 1
	for name, tc := range map[string]struct {
		gap  time.Duration
		want float64
	}{
		// d = 1: the old average keeps ln 2 of the weight.
		"one EMA scale": {100 * time.Millisecond, 2e7 - math.Ln2*1e7},
		// d = 100: the new sample replaces most of the average.
		"a long gap": {10 * time.Second, 2e7 - math.Log(101)/100*1e7},
	} {
		t.Run(name, func(t *testing.T) {
			clk, e := newFakeClock(), &endpoint{}
			sample(e, clk, c, 10*time.Millisecond)
			clk.advance(tc.gap)
			sample(e, clk, c, 20*time.Millisecond)
			assert.InDelta(t, tc.want, float64(e.avg.Load()), 1)
		})
	}
}

func TestTheEMAConvergesOnAStableLatency(t *testing.T) {
	clk, c, e := newFakeClock(), testConfig(t), &endpoint{}
	sample(e, clk, c, 50*time.Millisecond)
	for range 30 {
		clk.advance(c.EMAScale)
		sample(e, clk, c, 5*time.Millisecond)
	}
	assert.InEpsilon(t, float64(5*time.Millisecond), float64(e.avg.Load()), 0.01)
}

func TestASameInstantSampleCountsWithAlmostNoWeight(t *testing.T) {
	clk, c, e := newFakeClock(), testConfig(t), &endpoint{}
	pin(e, 10, 10*time.Millisecond, clk.now())
	sample(e, clk, c, time.Second)
	assert.EqualValuesf(t, 11, e.n.Load(), "counted, not dropped")
	assert.InDeltaf(t, float64(10*time.Millisecond), float64(e.avg.Load()), 10, "a 1s sample moves it by nanoseconds")
}

func TestTheVerdict(t *testing.T) {
	c := testConfig(t)
	ref := int64(10 * time.Millisecond)
	e := &endpoint{}

	pin(e, 2, time.Hour, 0)
	assert.Falsef(t, e.refusedBy(c, ref), "a member warming up is never refused")

	pin(e, 3, time.Hour, 0)
	assert.True(t, e.refusedBy(c, ref))
	assert.Falsef(t, e.refusedBy(c, 0), "no reference yet means no verdict")

	pin(e, 3, 20*time.Millisecond, 0)
	assert.Falsef(t, e.refusedBy(c, ref), "exactly at the line is not above it")
	pin(e, 3, 20*time.Millisecond+1, 0)
	assert.True(t, e.refusedBy(c, ref))

	c.ExclusionThreshold = 1.5
	pin(e, 3, 15*time.Millisecond, 0)
	assert.False(t, e.refusedBy(c, ref))
	pin(e, 3, 15*time.Millisecond+1, 0)
	assert.Truef(t, e.refusedBy(c, ref), "a fractional threshold works")
}

func TestStrikesFireOnTheThirdInsideOneWindow(t *testing.T) {
	window := int64(30 * time.Second)
	s := int64(time.Second)
	e := &endpoint{}

	assert.False(t, e.strike(1000*s, window))
	assert.False(t, e.strike(1001*s, window))
	assert.Truef(t, e.strike(1002*s, window), "the third inside the window fires")
	assert.Falsef(t, e.strike(1003*s, window), "and clears: the next opens a new window")

	e = &endpoint{}
	e.strike(1000*s, window)
	e.strike(1001*s, window)
	assert.Falsef(t, e.strike(1031*s, window), "a strike after the window starts over")
	assert.Equal(t, 1, e.strikeN)
}

func TestAnAdmittedMemberCountsOnlyTheThirdFailure(t *testing.T) {
	clk, c, e := newFakeClock(), testConfig(t), &endpoint{}
	ref := int64(time.Millisecond)
	pin(e, 3, time.Millisecond, clk.now())

	e.failure(clk.now, c, ref, false)
	e.failure(clk.now, c, ref, false)
	assert.EqualValuesf(t, 3, e.n.Load(), "two failures in one period record nothing")
	assert.False(t, e.refusedBy(c, ref))

	clk.advance(time.Second)
	e.failure(clk.now, c, ref, false)
	assert.EqualValuesf(t, 4, e.n.Load(), "the third records the synthetic sample")
	assert.Truef(t, e.refusedBy(c, ref), "and the member is refused")
}

func TestStrikesOlderThanARetryPeriodAreForgotten(t *testing.T) {
	clk, c, e := newFakeClock(), testConfig(t), &endpoint{}
	ref := int64(time.Millisecond)
	pin(e, 3, time.Millisecond, clk.now())
	e.failure(clk.now, c, ref, false)
	e.failure(clk.now, c, ref, false)

	clk.advance(2 * c.RetryPeriod)
	e.failure(clk.now, c, ref, false)
	assert.EqualValues(t, 3, e.n.Load())
	assert.Equalf(t, 1, e.strikeN, "the first of a new window")
}

func TestARefusedMemberOrAProbeCountsAtOnce(t *testing.T) {
	c := testConfig(t)
	ref := int64(time.Millisecond)

	clk, e := newFakeClock(), &endpoint{}
	pin(e, 3, time.Second, clk.now())
	e.failure(clk.now, c, ref, false)
	assert.EqualValuesf(t, 4, e.n.Load(), "a refused member is not given strikes")

	clk, e = newFakeClock(), &endpoint{}
	pin(e, 3, time.Millisecond, clk.now())
	e.failure(clk.now, c, ref, true)
	assert.EqualValuesf(t, 4, e.n.Load(), "a failed probe counts even if the member looks fine by now")
}

func TestATransportFailureCountsAtOnce(t *testing.T) {
	clk, c, e := newFakeClock(), testConfig(t), &endpoint{}
	pin(e, 3, time.Millisecond, clk.now())
	clk.advance(time.Second)
	e.transportFailure(clk.now, c, int64(time.Millisecond))
	assert.EqualValues(t, 4, e.n.Load())
	assert.True(t, e.refusedBy(c, int64(time.Millisecond)))
}

func TestTheSyntheticSample(t *testing.T) {
	c := testConfig(t)
	assert.Equalf(t, int64(5*time.Second), synthetic(c, 0), "no reference: the floor")
	assert.Equalf(t, int64(5*time.Second), synthetic(c, int64(5*time.Millisecond)), "20 × 5ms is under the 5s floor")

	c.MinSyntheticLatency = time.Millisecond
	assert.Equalf(t, int64(time.Second), synthetic(c, int64(50*time.Millisecond)), "20 × 50ms")
	assert.Equalf(t, int64(1e18), synthetic(c, math.MaxInt64/2), "capped, never wrapped")
	c.MinSyntheticLatency = math.MaxInt64
	assert.Equalf(t, int64(1e18), synthetic(c, 0), "capped whatever the config")
}

func TestAnAbsurdSyntheticFloorNeverOverflowsTheAverage(t *testing.T) {
	clk, c, e := newFakeClock(), testConfig(t), &endpoint{}
	c.WarmupSamples, c.MinSyntheticLatency = 1, math.MaxInt64
	e.transportFailure(clk.now, c, 0)
	clk.advance(time.Second)
	e.transportFailure(clk.now, c, 0)
	assert.InDelta(t, 1e18, float64(e.avg.Load()), 1e3)
}

func TestOneProbePerRetryPeriod(t *testing.T) {
	clk, e := newFakeClock(), &endpoint{}
	retry := int64(time.Minute)
	pin(e, 3, time.Second, clk.now())

	clk.advance(time.Minute / 2)
	assert.Falsef(t, e.claimProbe(clk.now(), retry), "the last sample is too recent")

	clk.advance(time.Minute)
	assert.True(t, e.claimProbe(clk.now(), retry))
	assert.Falsef(t, e.claimProbe(clk.now(), retry), "held until a retry period passes")

	clk.advance(time.Minute + 1)
	assert.Truef(t, e.claimProbe(clk.now(), retry), "the probe sent nothing back: try again")
}

func TestConcurrentPicksAgreeOnOneProbe(t *testing.T) {
	clk, e := newFakeClock(), &endpoint{}
	pin(e, 3, time.Second, 0)

	var wg sync.WaitGroup
	var won atomic.Int32
	for range 100 {
		wg.Go(func() {
			if e.claimProbe(clk.now(), int64(time.Minute)) {
				won.Add(1)
			}
		})
	}
	wg.Wait()
	assert.EqualValues(t, 1, won.Load())
}

func TestEverySampleLandsUnderContention(t *testing.T) {
	c, e, now := testConfig(t), &endpoint{}, monotonic()
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for range 1000 {
				e.success(now, now(), c)
			}
		})
	}
	wg.Wait()
	assert.EqualValues(t, 64000, e.n.Load())
}

func TestANewRetryPeriodAppliesToAClaimedProbe(t *testing.T) {
	clk, e := newFakeClock(), &endpoint{}
	pin(e, 3, time.Second, 0)
	require.True(t, e.claimProbe(clk.now(), int64(30*time.Minute)))
	clk.advance(2 * time.Second)
	assert.Truef(t, e.claimProbe(clk.now(), int64(time.Second)), "the claim is 2s old, past the new 1s period")
}

func TestAProbeHandedBackIsFreeAgain(t *testing.T) {
	clk, e := newFakeClock(), &endpoint{}
	retry := int64(time.Minute)
	pin(e, 3, time.Second, 0)

	t1 := clk.now()
	require.True(t, e.claimProbe(t1, retry))
	e.releaseProbe(t1)
	require.Truef(t, e.claimProbe(t1, retry), "handed back")

	clk.advance(time.Minute + 1)
	t2 := clk.now()
	require.True(t, e.claimProbe(t2, retry))
	e.releaseProbe(t1)
	assert.Falsef(t, e.claimProbe(t2, retry), "a stale hand-back leaves the newer claim")
	e.releaseProbe(0)
	assert.Falsef(t, e.claimProbe(t2, retry), "no claim, nothing handed back")
}
