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
	"time"
)

// clock reads monotonic nanoseconds; tests drive their own.
type clock func() int64

func monotonic() clock {
	start := time.Now()
	return func() int64 { return int64(time.Since(start)) }
}

// strikesToCount is how many trippable failures within one RetryPeriod an
// admitted member takes before one counts.
const strikesToCount = 3

// endpoint is one member's latency state. It lives on the balancer, so it
// survives picker rebuilds, and goes when the member leaves the resolver state.
// Readers load the atomics without locking, so they may mix fields of adjacent
// samples; that leaves a verdict or the cluster minimum one sample behind.
type endpoint struct {
	addr string
	lbl  labels

	n    atomic.Int64 // samples so far
	avg  atomic.Int64 // ns: running mean while warming up, then the EMA
	last atomic.Int64 // clock time of the last sample

	probeAt atomic.Int64 // clock time of the last probe claimed, 0 for none
	refused atomic.Bool  // the last verdict seen, so only changes are logged

	mu         sync.Mutex // serialises samples and strikes
	strikeFrom int64
	strikeN    int

	down bool // in a TRANSIENT_FAILURE episode; guarded by the balancer's mu
}

// success records a call's latency, from its pick at start until now.
func (e *endpoint) success(now clock, start int64, c *Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := now()
	e.add(t, t-start, c)
}

// failure handles a trippable failure: it counts at once for a probe or a
// member refused now, otherwise only as the strikesToCount-th within one
// RetryPeriod, so one caller's tight deadline cannot eject a healthy member.
func (e *endpoint) failure(now clock, c *Config, ref int64, probe bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := now()
	if probe || e.refusedBy(c, ref) || e.strike(t, int64(c.RetryPeriod)) {
		e.add(t, synthetic(c, ref), c)
	}
}

// transportFailure counts a lost connection at once.
func (e *endpoint) transportFailure(now clock, c *Config, ref int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.add(now(), synthetic(c, ref), c)
}

// add folds in one sample: a running mean for the first WarmupSamples, then a
// time-decayed EMA in which the previous average keeps weight ln(1+d)/d, d
// being the gap since the last sample over EMAScale. A long gap lets a new
// sample replace most of the average, which is how a probe re-admits a member.
// Callers hold mu, and read t under it, so gaps never run backwards.
func (e *endpoint) add(t, sample int64, c *Config) {
	n, avg := e.n.Load()+1, e.avg.Load()
	if n <= int64(c.WarmupSamples) {
		avg += (sample - avg) / n
	} else {
		d := float64(max(t-e.last.Load(), 1)) / float64(c.EMAScale)
		w := math.Log1p(d) / d
		avg = int64((1-w)*float64(sample) + w*float64(avg))
	}
	e.avg.Store(avg)
	e.last.Store(t)
	e.n.Store(n)
}

// strike reports whether this failure is the strikesToCount-th inside one
// window; that one clears the count. Callers hold mu.
func (e *endpoint) strike(t, window int64) bool {
	if e.strikeN == 0 || t-e.strikeFrom > window {
		e.strikeFrom, e.strikeN = t, 0
	}
	e.strikeN++
	if e.strikeN < strikesToCount {
		return false
	}
	e.strikeN = 0
	return true
}

// refusedBy is the verdict: warmed up and averaging above ExclusionThreshold ×
// ref, the cluster minimum. No reference yet means no verdict.
func (e *endpoint) refusedBy(c *Config, ref int64) bool {
	return ref > 0 && e.n.Load() >= int64(c.WarmupSamples) && float64(e.avg.Load()) > c.ExclusionThreshold*float64(ref)
}

// claimProbe lets one pick per RetryPeriod through to a refused member whose
// last sample is older than that. Concurrent callers agree on one winner. It
// keeps the claim's time, not a deadline, so a new RetryPeriod applies at once.
func (e *endpoint) claimProbe(t, retry int64) bool {
	at := e.probeAt.Load()
	return t-e.last.Load() > retry && (at == 0 || t-at > retry) && e.probeAt.CompareAndSwap(at, t)
}

// releaseProbe hands back the claim made at t (0: none, a no-op) when its pick
// sent nothing, unless a newer claim replaced it. The claim before t was over
// a retry period old, so clearing it to none changes nothing else.
func (e *endpoint) releaseProbe(t int64) { e.probeAt.CompareAndSwap(t, 0) }

// maxSample caps a synthetic sample, whatever the config, so the EMA's float
// conversions stay defined; real latencies are nowhere near it.
const maxSample = 1e18

// synthetic is the sample a counted failure records.
func synthetic(c *Config, ref int64) int64 {
	return min(max(int64(c.MinSyntheticLatency), int64(math.Min(c.SyntheticMultiplier*float64(ref), maxSample))), maxSample)
}
