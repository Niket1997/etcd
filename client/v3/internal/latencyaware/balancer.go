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

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/endpointsharding"
	"google.golang.org/grpc/balancer/pickfirst"
	"google.golang.org/grpc/connectivity"
	estats "google.golang.org/grpc/experimental/stats"
	"google.golang.org/grpc/resolver"
)

func init() {
	balancer.Register(builder{})
}

type builder struct{}

func (builder) Name() string { return Name }

func (builder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	return newBalancer(cc, opts, balancer.Get(pickfirst.Name).Build, monotonic())
}

// lb wraps endpointsharding, which runs a pick_first child per member as
// round_robin does, and replaces its picker with one that routes around slow
// members. Member state lives here, so it survives picker rebuilds.
type lb struct {
	balancer.ClientConn // UpdateState from the child comes through lb
	child               balancer.Balancer
	shared              atomic.Pointer[Shared]
	now                 clock
	target              string
	rec                 estats.MetricsRecorder

	mu  sync.Mutex // guards eps
	eps *resolver.EndpointMap[*endpoint]

	view     atomic.Pointer[[]*endpoint] // every member, for the cluster-minimum scan
	min      atomic.Int64                // cluster minimum, floor-clamped; 0 until a member qualifies
	nextScan atomic.Int64
	scanMu   sync.Mutex
	fallback atomic.Bool // quorum fallback active, so only changes are logged
}

// newBalancer is Build with the child policy and clock injectable for tests.
func newBalancer(cc balancer.ClientConn, opts balancer.BuildOptions, child endpointsharding.ChildBuilderFunc, now clock) *lb {
	b := &lb{
		ClientConn: cc,
		now:        now,
		target:     opts.Target.String(),
		rec:        cc.MetricsRecorder(),
		eps:        resolver.NewEndpointMap[*endpoint](),
	}
	s, _ := NewShared(Config{}, nil) // defaults until the resolver hands over the client's
	b.shared.Store(s)
	b.view.Store(&[]*endpoint{})
	b.child = endpointsharding.NewBalancer(b, opts, child, endpointsharding.Options{})
	return b
}

func (b *lb) UpdateClientConnState(s balancer.ClientConnState) error {
	if sh := sharedFrom(s.ResolverState.Attributes); sh != nil {
		b.shared.Store(sh)
	}
	return b.child.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: pickfirst.EnableHealthListener(s.ResolverState),
	})
}

func (b *lb) ResolverError(err error) { b.child.ResolverError(err) }

// UpdateSubConnState is never called: the pick_first children own the SubConns.
func (b *lb) UpdateSubConnState(balancer.SubConn, balancer.SubConnState) {}

func (b *lb) ExitIdle() { b.child.ExitIdle() }

func (b *lb) Close() { b.child.Close() }

// UpdateState receives every child's state from endpointsharding. It keeps
// the member set in step, counts one failure per TRANSIENT_FAILURE episode and
// publishes a picker over the READY children. With none READY it passes
// endpointsharding's picker through, which queues or fails picks exactly as
// round_robin does. endpointsharding holds its lock while calling here, so
// this never calls back into the child.
func (b *lb) UpdateState(state balancer.State) {
	children := endpointsharding.ChildStatesFromPicker(state.Picker)
	c := b.shared.Load().config()
	ready := make([]member, 0, len(children))

	b.mu.Lock()
	changed := b.syncLocked(children)
	for _, ch := range children {
		e, _ := b.eps.Get(ch.Endpoint)
		switch ch.State.ConnectivityState {
		case connectivity.Ready:
			e.down = false
			ready = append(ready, member{picker: ch.State.Picker, ep: e})
		case connectivity.TransientFailure:
			if !e.down {
				e.down = true // off tracks the episode too, so a flip mid-outage can't count it
				if c.Mode != ModeOff {
					e.transportFailure(b.now, c, b.min.Load())
				}
			}
		}
	}
	b.mu.Unlock()

	if changed {
		b.rescan(true)
	}
	b.recordMembers(len(children), len(ready))
	if len(ready) == 0 {
		b.ClientConn.UpdateState(state)
		return
	}
	b.ClientConn.UpdateState(balancer.State{ConnectivityState: connectivity.Ready, Picker: newPicker(b, ready, len(children))})
}

// syncLocked makes eps match the children: members the resolver dropped are
// forgotten, new ones start empty. It reports whether the set changed.
func (b *lb) syncLocked(children []endpointsharding.ChildState) bool {
	changed := len(children) != b.eps.Len()
	next := resolver.NewEndpointMap[*endpoint]()
	view := make([]*endpoint, 0, len(children))
	for _, ch := range children {
		e, ok := b.eps.Get(ch.Endpoint)
		if !ok {
			e, changed = b.newEndpoint(ch.Endpoint), true
		}
		next.Set(ch.Endpoint, e)
		view = append(view, e)
	}
	if changed {
		b.eps = next
		b.view.Store(&view)
	}
	return changed
}

// finish learns from a completed call: a timed success is a sample, a timed
// trippable failure a strike or a synthetic sample.
func (b *lb) finish(e *endpoint, err error, start int64, timed, probe bool) {
	b.recordRPC(e, err)
	if !timed {
		return
	}
	c := b.shared.Load().config()
	switch {
	case err == nil:
		e.success(b.now, start, c)
	case trippable(err):
		e.failure(b.now, c, b.min.Load(), probe)
	default:
		return
	}
	b.rescan(false)
}

// rescan recomputes the cluster minimum: the fastest warmed-up member with a
// sample in the last RetryPeriod, floor-clamped. Unless forced it runs at most
// every RescanInterval; nothing qualifying keeps the last value and lets the
// next sample try again.
func (b *lb) rescan(force bool) {
	t := b.now()
	if !force && t < b.nextScan.Load() {
		return
	}
	if force {
		b.scanMu.Lock()
	} else if !b.scanMu.TryLock() {
		return
	}
	defer b.scanMu.Unlock()
	c := b.shared.Load().config()
	view := *b.view.Load()
	best := int64(math.MaxInt64)
	for _, e := range view {
		if a := e.avg.Load(); a > 0 && a < best && e.n.Load() >= int64(c.WarmupSamples) && t-e.last.Load() <= int64(c.RetryPeriod) {
			best = a
		}
	}
	if best == math.MaxInt64 {
		return
	}
	ref := max(best, int64(c.MinLatencyFloor))
	b.min.Store(ref)
	b.nextScan.Store(t + int64(c.RescanInterval))
	b.recordScan(view, c, ref)
}
