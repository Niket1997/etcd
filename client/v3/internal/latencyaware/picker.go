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
	"errors"
	"math/rand/v2"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// member is a READY child and its member's state.
type member struct {
	picker balancer.Picker
	ep     *endpoint
}

// picker routes around refused members. It takes no locks: everything it
// reads is an atomic on the balancer or on a member.
type picker struct {
	b     *lb
	ready []member
	total int // every resolved member, READY or not
	next  atomic.Uint32
}

func newPicker(b *lb, ready []member, total int) *picker {
	p := &picker{b: b, ready: ready, total: total}
	p.next.Store(rand.Uint32()) //nolint:gosec // spreads load; not security sensitive
	return p
}

// Pick is plain round robin in off mode. Otherwise it judges every READY
// member and, unless too many are unhealthy, picks as choose says. Shadow
// counts that decision but routes plain round robin.
func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	c := p.b.shared.Load().config()
	cur := p.next.Add(1)
	rr := int(cur % uint32(len(p.ready)))
	if c.Mode == ModeOff {
		return p.ready[rr].picker.Pick(info)
	}

	ref := p.b.min.Load()
	var buf [8]bool
	refused, k := buf[:0], 0
	for _, m := range p.ready {
		r := m.ep.refusedBy(c, ref)
		if r {
			k++
		}
		refused = append(refused, r)
		p.b.noteVerdict(m.ep, r, c, ref)
	}
	missing := max(p.total-len(p.ready), 0)
	guard := k > 0 && k+missing > c.MaxUnhealthyEndpoints
	p.b.noteFallback(guard, c, k, missing, len(p.ready), p.total)

	tm := timed(info.FullMethodName)
	chosen, out := rr, outcomeQuorum
	if !guard {
		chosen, out = p.choose(cur, rr, refused, k, c, c.Mode == ModeEnabled && tm)
	}
	if refused[rr] && chosen != rr {
		p.b.countPick(p.ready[rr].ep, outcomeRefused)
	}
	p.b.countPick(p.ready[chosen].ep, out)
	if c.Mode == ModeShadow {
		chosen = rr
	}
	return p.pick(info, p.ready[chosen], tm, out == outcomeProbe)
}

// choose applies the verdicts to one pick: a due probe first, then an even
// split over the admitted members, then the member at the cursor if every
// member is refused.
func (p *picker) choose(cur uint32, rr int, refused []bool, k int, c *Config, probes bool) (int, outcome) {
	if k == 0 {
		return rr, outcomeAllowed
	}
	if probes {
		t := p.b.now()
		for i := range refused {
			if j := (rr + i) % len(refused); refused[j] && p.ready[j].ep.claimProbe(t, int64(c.RetryPeriod)) {
				return j, outcomeProbe
			}
		}
	}
	if k == len(refused) {
		return rr, outcomeFallback
	}
	// The (cur mod admitted)-th admitted member. Moving on to the next member
	// instead would hand a refused member's share to its neighbour alone.
	want, j := int(cur%uint32(len(refused)-k)), 0
	for ; refused[j] || want > 0; j++ {
		if !refused[j] {
			want--
		}
	}
	return j, outcomeAllowed
}

// pick asks the member's child for a SubConn and learns from how the call ends.
func (p *picker) pick(info balancer.PickInfo, m member, tm, probe bool) (balancer.PickResult, error) {
	res, err := m.picker.Pick(info)
	if err != nil {
		return res, err
	}
	b, start, done := p.b, p.b.now(), res.Done
	res.Done = func(di balancer.DoneInfo) {
		if done != nil {
			done(di)
		}
		// A zero DoneInfo is a pick gRPC abandoned before the call went out.
		if di.Err != nil || di.BytesSent || di.BytesReceived {
			b.finish(m.ep, di.Err, start, tm, probe)
		}
	}
	return res, nil
}

// timed reports the calls whose duration is the member's latency: the KV and
// Lease data path. Streams, work-sized calls (Compact, Maintenance, Auth),
// admin calls and other services on the connection, such as a v3lock Lock
// waiting for its lock, are counted but never teach.
func timed(method string) bool {
	switch method {
	case pb.KV_Range_FullMethodName, pb.KV_Put_FullMethodName, pb.KV_DeleteRange_FullMethodName, pb.KV_Txn_FullMethodName,
		pb.Lease_LeaseGrant_FullMethodName, pb.Lease_LeaseRevoke_FullMethodName, pb.Lease_LeaseTimeToLive_FullMethodName, pb.Lease_LeaseLeases_FullMethodName:
		return true
	}
	return false
}

// trippable reports the per-member slowness signals: a deadline, or etcd's
// apply backpressure. The latter reaches the client as codes.Unknown with
// etcd's message, so rpctypes matches it; NOSPACE is cluster-wide and is not one.
func trippable(err error) bool {
	return status.Code(err) == codes.DeadlineExceeded || errors.Is(rpctypes.Error(err), rpctypes.ErrTooManyRequests)
}
