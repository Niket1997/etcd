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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// A reference production run, reproduced: five members,
// 8k reads a second with 1 s timeouts, and a follower fault that delays 60% of
// its calls by 1000-1100 ms, so each delayed call times out. The policy runs
// at the reference settings next to a round-robin baseline, on the same cluster
// and fault. The reference clocks run ten times faster here; ETCD_LA_FULL_SCALE=1
// runs them as they were, about thirteen minutes.

const (
	refusedMsg    = "latency-aware: member refused"
	readmittedMsg = "latency-aware: member readmitted"
)

// reference is one reference timeline: a cluster, the policy and baseline
// clients, and their load.
type reference struct {
	*cluster
	t         *testing.T
	scale     int // reference time over test time
	on, off   *client
	followers []int
	ids       map[uint64]int // member ID to member
	stop      func() (on, off results)
}

// mark names the phase that starts at.
type mark struct {
	name string
	at   time.Time
}

func newReference(t *testing.T) *reference {
	s := &reference{cluster: start(t, 5, false), t: t, scale: 10, ids: map[uint64]int{}}
	if os.Getenv("ETCD_LA_FULL_SCALE") == "1" {
		s.scale = 1
	}
	s.on = s.client(t, &clientv3.LatencyAwareConfig{
		ExclusionThreshold: 3, MinLatencyFloor: s.at(50 * time.Millisecond), EMAScale: s.at(10 * time.Second),
		RetryPeriod: s.at(30 * time.Second), MaxUnhealthyEndpoints: 1,
		MinSyntheticLatency: s.at(5 * time.Second), RescanInterval: s.at(2 * time.Second),
	})
	s.off = s.client(t, nil)
	leader := s.WaitLeader(t)
	for m, mem := range s.Members {
		s.ids[uint64(mem.ID())] = m
		if m != leader {
			s.followers = append(s.followers, m)
		}
	}
	warm(t, s.on, 5)
	warm(t, s.off, 5)

	// Each client sends half of the 8k reads a second.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var on, off results
	load := func(cli *client, out *results) {
		defer wg.Done()
		rate := 4000 / s.scale
		*out = run(ctx, loadConfig{rate: rate, maxInFlight: rate, timeout: s.at(time.Second), req: get(cli)})
	}
	wg.Add(2)
	go load(s.on, &on)
	go load(s.off, &off)
	s.stop = func() (results, results) {
		cancel()
		wg.Wait()
		return on, off
	}
	t.Cleanup(func() { s.stop() })
	return s
}

// at is a reference duration on this run's clock.
func (s *reference) at(d time.Duration) time.Duration { return d / time.Duration(s.scale) }

// pass lets d of reference time go by.
func (s *reference) pass(d time.Duration) { time.Sleep(s.at(d)) }

// delay starts the reference fault on member m in 10 ms, so a call that set off
// before the caller's mark reaches its member first.
func (s *reference) delay(m int) {
	time.Sleep(10 * time.Millisecond)
	s.SlowSome(m, s.at(1050*time.Millisecond), s.at(50*time.Millisecond), 0.6)
}

// when is when cli first logged msg at or after since, about endpoint ep
// unless ep is empty. It is zero if it never did.
func when(cli *client, msg, ep string, since time.Time) time.Time {
	for _, e := range cli.logs.FilterMessage(msg).All() {
		if !e.Time.Before(since) && (ep == "" || e.ContextMap()["endpoint"] == ep) {
			return e.Time
		}
	}
	return time.Time{}
}

// event requires that the policy logged msg about endpoint ep (any when
// empty) after since, and returns when, logging the delay on both clocks next
// to what the reference run saw.
func (s *reference) event(what, msg, ep string, since time.Time, want string) time.Time {
	s.t.Helper()
	at := when(s.on, msg, ep, since)
	require.Falsef(s.t, at.IsZero(), "never %s", what)
	d := at.Sub(since)
	s.t.Logf("%s after %v, %v at the reference clock (reference: %s)", what, d.Round(time.Millisecond), (d * time.Duration(s.scale)).Round(100*time.Millisecond), want)
	return at
}

// quiet asserts that no call failed for either client in [from, to) and that
// the policy refused nobody.
func (s *reference) quiet(on, off results, from, to time.Time) {
	s.t.Helper()
	assert.Zero(s.t, on.window(from, to).failed())
	assert.Zero(s.t, off.window(from, to).failed())
	refused := when(s.on, refusedMsg, "", from)
	assert.Truef(s.t, refused.IsZero() || !refused.Before(to), "a member was refused %v in", refused.Sub(from))
}

// served is how many of rs each member answered.
func (s *reference) served(rs results) []int {
	out := make([]int, len(s.Members))
	for id, n := range rs.byMember() {
		out[s.ids[id]] += n
	}
	return out
}

// report logs each phase's failures for the policy and the baseline, by the
// phase each call set off in.
func (s *reference) report(on, off results, marks ...mark) {
	for i := 0; i+1 < len(marks); i++ {
		a, b := on.window(marks[i].at, marks[i+1].at), off.window(marks[i].at, marks[i+1].at)
		s.t.Logf("%-22s policy %5d of %7d failed (%4.1f%%) %v, round robin %5d of %7d (%4.1f%%) %v",
			marks[i].name, a.failed(), len(a), 100*a.rate(), a.codes(), b.failed(), len(b), 100*b.rate(), b.codes())
	}
}

// codes counts the failures in rs by gRPC code.
func (rs results) codes() map[codes.Code]int {
	out := map[codes.Code]int{}
	for _, r := range rs {
		if r.err != nil {
			out[code(r.err)]++
		}
	}
	return out
}

// window is the results that started in [from, to).
func (rs results) window(from, to time.Time) results {
	var out results
	for _, r := range rs {
		if !r.start.Before(from) && r.start.Before(to) {
			out = append(out, r)
		}
	}
	return out
}

// rate is the share of rs that failed.
func (rs results) rate() float64 { return float64(rs.failed()) / float64(max(len(rs), 1)) }

func TestReferenceOneSlowFollower(t *testing.T) {
	s := newReference(t)
	f, retry, timeout := s.followers[0], s.at(30*time.Second), s.at(time.Second)

	// S1, control: no fault, so the policy does nothing.
	control := time.Now()
	s.pass(30 * time.Second)

	// S2: refused within ~10 s and then sent only probes; the client loses only
	// the calls already on the member when the delay began.
	fault := time.Now()
	s.delay(f)
	s.pass(90 * time.Second)

	// S4: let back in by one to five fast probes after the heal, with its full share.
	heal := time.Now()
	s.Heal(f)
	s.pass(240 * time.Second)
	on, off := s.stop()
	end := time.Now()
	s.report(on, off, mark{"S1 control", control}, mark{"S2 one slow follower", fault}, mark{"S4 heal", heal}, mark{"", end})

	// Quiet checks keep a timeout clear of each change, for a call held up on
	// its way to its member.
	s.quiet(on, off, control, fault.Add(-timeout))
	assert.Zerof(t, s.on.logged(fallbackMsg), "one slow member is within the cap")

	// A fast probe soon after the refusal can let the faulty member back in,
	// at most once a retry period, until strikes refuse it again a few calls
	// later; the reference run's failures in the first minutes include these.
	refused := s.event("refused", refusedMsg, s.addr(f), fault, "within ~10 s")
	assert.LessOrEqual(t, refused.Sub(fault), s.at(10*time.Second))
	assert.Less(t, on.window(fault, heal).rate(), 0.02)
	assert.InDeltaf(t, 0.12, off.window(fault, heal).rate(), 0.03, "round robin loses 60%% of a fifth")
	out := on.window(refused.Add(timeout), heal)
	assert.Lessf(t, s.served(out)[f], len(out)/50, "the refused member is out of rotation")

	back := s.event("readmitted", readmittedMsg, s.addr(f), heal, "~75 s")
	assert.LessOrEqual(t, back.Sub(heal), 6*retry)
	s.quiet(on, off, heal.Add(timeout), end)
	even(t, s.served(on.window(back, end)))
}

func TestReferenceTwoSlowFollowers(t *testing.T) {
	s := newReference(t)
	f1, f2, timeout := s.followers[0], s.followers[1], s.at(time.Second)
	control := time.Now()
	s.pass(30 * time.Second)

	// S2: the first slow follower is refused, as above.
	first := time.Now()
	s.delay(f1)
	s.pass(60 * time.Second)

	// S3: a second one is more than MaxUnhealthyEndpoints allows, so within
	// ~13 s the fallback routes plain round robin: about 24% of calls time
	// out, as with no policy, by design.
	second := time.Now()
	s.delay(f2)
	s.pass(60 * time.Second)
	ready := s.on.members("ready")

	// S5: both heal, 20 s apart. Errors stop with the fault, the fallback ends
	// on its own about a minute later, and probes let the second member back
	// in about two minutes after the first heal.
	heal1 := time.Now()
	s.Heal(f1)
	s.pass(20 * time.Second)
	heal2 := time.Now()
	s.Heal(f2)
	s.pass(220 * time.Second)
	on, off := s.stop()
	end := time.Now()
	s.report(on, off, mark{"S1 control", control}, mark{"S2 one slow follower", first}, mark{"S3 two slow followers", second},
		mark{"S5 one healed", heal1}, mark{"S5 both healed", heal2}, mark{"", end})

	s.quiet(on, off, control, first.Add(-timeout))
	refused := s.event("first refused", refusedMsg, s.addr(f1), first, "within ~10 s")
	assert.LessOrEqual(t, refused.Sub(first), s.at(10*time.Second))

	fallback := s.event("fallback started", fallbackMsg, "", second, "within ~13 s")
	assert.LessOrEqual(t, fallback.Sub(second), s.at(13*time.Second))
	assert.Positive(t, s.on.fallbacks("unhealthy", zapcore.WarnLevel))
	assert.EqualValuesf(t, 5, ready, "every member stays READY")
	assert.InDelta(t, 0.24, on.window(fallback.Add(timeout), heal1).rate(), 0.05)
	assert.InDelta(t, 0.24, off.window(fallback.Add(timeout), heal1).rate(), 0.05)
	var quorum []int
	for m := range 5 {
		quorum = append(quorum, int(s.on.picks("quorum", s.addr(m))))
	}
	even(t, quorum)

	assert.InDeltaf(t, 0.12, on.window(heal1.Add(timeout), heal2).rate(), 0.05, "the fallback still sends the slow one its fifth")
	assert.InDelta(t, 0.12, off.window(heal1.Add(timeout), heal2).rate(), 0.05)
	s.quiet(on, off, heal2.Add(timeout), end)
	ended := s.event("fallback ended", fallbackEndMsg, "", heal1, "~70 s after the first heal")
	assert.LessOrEqual(t, ended.Sub(heal1), s.at(120*time.Second))
	back := s.event("second readmitted", readmittedMsg, s.addr(f2), ended, "~60 s after the fallback ended")
	assert.LessOrEqual(t, back.Sub(heal1), s.at(210*time.Second))
	even(t, s.served(on.window(back, end)))
}
