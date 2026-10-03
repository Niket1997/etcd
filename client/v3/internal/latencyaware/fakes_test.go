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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	estats "google.golang.org/grpc/experimental/stats"
	"google.golang.org/grpc/resolver"
)

// harness is a balancer on a fake clock, recording into a fake recorder and
// a log observer, with fake pick_first children under the real endpointsharding.
type harness struct {
	t    *testing.T
	clk  *fakeClock
	rec  *fakeRecorder
	cc   *fakeCC
	kids *fakeChildren
	b    *lb
	sh   *Shared
	logs *observer.ObservedLogs
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{t: t, clk: newFakeClock(), rec: newFakeRecorder(t), kids: &fakeChildren{byAddr: map[string]*fakeChild{}}}
	h.cc = &fakeCC{rec: h.rec}
	h.b = newBalancer(h.cc, balancer.BuildOptions{}, h.kids.build, h.clk.now)
	core, logs := observer.New(zap.DebugLevel)
	sh, err := NewShared(cfg, zap.New(core))
	require.NoError(t, err)
	h.sh, h.logs = sh, logs
	h.b.shared.Store(sh)
	return h
}

// picker builds a picker over READY members with these addresses, out of
// total resolved members, its cursor set so the first pick lands on the first.
func (h *harness) picker(total int, addrs ...string) *picker {
	ready := make([]member, len(addrs))
	for i, a := range addrs {
		ready[i] = member{picker: newFakeChildPicker(a), ep: h.b.newEndpoint(endpointOf(a))}
	}
	p := newPicker(h.b, ready, total)
	p.next.Store(^uint32(0))
	return p
}

// resolve sends the resolver state the client would, carrying h.sh.
func (h *harness) resolve(addrs ...string) {
	eps := make([]resolver.Endpoint, len(addrs))
	for i, a := range addrs {
		eps[i] = endpointOf(a)
	}
	require.NoError(h.t, h.b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: resolver.State{Endpoints: eps, Attributes: h.sh.Attributes()},
	}))
}

// set moves a member's pick_first child to state s.
func (h *harness) set(addr string, s connectivity.State) { h.kids.get(h.t, addr).set(s) }

// member returns the balancer's state for addr.
func (h *harness) member(addr string) *endpoint {
	h.b.mu.Lock()
	defer h.b.mu.Unlock()
	e, ok := h.b.eps.Get(endpointOf(addr))
	require.Truef(h.t, ok, "no member %s", addr)
	return e
}

// logged waits until the queued transition lines are written and returns them.
func (h *harness) logged() *observer.ObservedLogs {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		h.b.logMu.Lock()
		defer h.b.logMu.Unlock()
		return !h.b.logging
	}, 10*time.Second, time.Millisecond)
	return h.logs
}

// stuckSink is a log sink whose writes wait until release is closed.
type stuckSink struct {
	release chan struct{}
	entered atomic.Bool
	lines   atomic.Int32
}

func (s *stuckSink) Write(p []byte) (int, error) {
	s.entered.Store(true)
	<-s.release
	s.lines.Add(1)
	return len(p), nil
}

func endpointOf(addr string) resolver.Endpoint {
	return resolver.Endpoint{Addresses: []resolver.Address{{Addr: addr}}}
}

// warm pins a member as warmed up, averaging avg, last sampled age ago.
func (h *harness) warm(e *endpoint, avg, age time.Duration) {
	pin(e, h.sh.config().WarmupSamples, avg, h.clk.now()-int64(age))
}

// pickAll picks n times and counts the picks per member address.
func pickAll(t *testing.T, p *picker, n int, method string) map[string]int {
	t.Helper()
	got := map[string]int{}
	for range n {
		res, err := p.Pick(balancer.PickInfo{FullMethodName: method})
		require.NoError(t, err)
		got[res.SubConn.(*fakeSubConn).addr]++
	}
	return got
}

// fakeCC is the ClientConn gRPC gives the balancer.
type fakeCC struct {
	balancer.ClientConn
	rec    estats.MetricsRecorder
	mu     sync.Mutex
	states []balancer.State
}

func (cc *fakeCC) MetricsRecorder() estats.MetricsRecorder { return cc.rec }

func (cc *fakeCC) UpdateState(s balancer.State) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.states = append(cc.states, s)
}

func (cc *fakeCC) last(t *testing.T) balancer.State {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	require.NotEmpty(t, cc.states)
	return cc.states[len(cc.states)-1]
}

// fakeChildren stands in for pick_first: one child per member, whose state a
// test sets by address.
type fakeChildren struct {
	mu           sync.Mutex
	byAddr       map[string]*fakeChild
	resolverErrs int
	exitIdles    int
	closed       int
}

func (f *fakeChildren) build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	return &fakeChild{cc: cc, f: f}
}

func (f *fakeChildren) get(t *testing.T, addr string) *fakeChild {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.byAddr[addr]
	require.Truef(t, ok, "no child for %s", addr)
	return c
}

type fakeChild struct {
	cc     balancer.ClientConn
	f      *fakeChildren
	picker *fakeChildPicker
}

func (c *fakeChild) UpdateClientConnState(s balancer.ClientConnState) error {
	addr := s.ResolverState.Endpoints[0].Addresses[0].Addr
	c.picker = newFakeChildPicker(addr)
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.byAddr[addr] = c
	return nil
}

func (c *fakeChild) ResolverError(error) { c.f.mu.Lock(); c.f.resolverErrs++; c.f.mu.Unlock() }
func (c *fakeChild) ExitIdle()           { c.f.mu.Lock(); c.f.exitIdles++; c.f.mu.Unlock() }
func (c *fakeChild) Close()              { c.f.mu.Lock(); c.f.closed++; c.f.mu.Unlock() }

func (c *fakeChild) UpdateSubConnState(balancer.SubConn, balancer.SubConnState) {}

func (c *fakeChild) set(s connectivity.State) {
	c.cc.UpdateState(balancer.State{ConnectivityState: s, Picker: c.picker})
}

// fakeChildPicker is a READY pick_first child's picker: it hands out its
// member's SubConn and calls done, if set, when the call ends.
type fakeChildPicker struct {
	sc   *fakeSubConn
	err  error
	done func(balancer.DoneInfo)
}

func newFakeChildPicker(addr string) *fakeChildPicker {
	return &fakeChildPicker{sc: &fakeSubConn{addr: addr}}
}

func (p *fakeChildPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	if p.err != nil {
		return balancer.PickResult{}, p.err
	}
	return balancer.PickResult{SubConn: p.sc, Done: p.done}, nil
}

type fakeSubConn struct {
	balancer.SubConn
	addr string
}

// fakeRecorder keeps every count and gauge by name and label values, and
// fails the test when a call's labels don't match its descriptor: the
// OpenTelemetry recorder would panic.
type fakeRecorder struct {
	estats.MetricsRecorder
	t      *testing.T
	mu     sync.Mutex
	counts map[string]int64
	gauges map[string]int64
}

func newFakeRecorder(t *testing.T) *fakeRecorder {
	return &fakeRecorder{t: t, counts: map[string]int64{}, gauges: map[string]int64{}}
}

func (r *fakeRecorder) key(d *estats.MetricDescriptor, labels []string) string {
	if len(labels) != len(d.Labels) {
		r.t.Errorf("%s recorded with %d labels, declared %d", d.Name, len(labels), len(d.Labels))
	}
	return d.Name + "{" + strings.Join(labels, ",") + "}"
}

func (r *fakeRecorder) RecordInt64Count(h *estats.Int64CountHandle, v int64, labels ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[r.key(h.Descriptor(), labels)] += v
}

func (r *fakeRecorder) RecordInt64Gauge(h *estats.Int64GaugeHandle, v int64, labels ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[r.key(h.Descriptor(), labels)] = v
}

func (r *fakeRecorder) count(name string, labels ...string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[name+"{"+strings.Join(labels, ",")+"}"]
}

func (r *fakeRecorder) gauge(name string, labels ...string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gauges[name+"{"+strings.Join(labels, ",")+"}"]
}

// nopRecorder records nothing, for measuring the pick path alone.
type nopRecorder struct{ estats.MetricsRecorder }

func (nopRecorder) RecordInt64Count(*estats.Int64CountHandle, int64, ...string) {}
func (nopRecorder) RecordInt64Gauge(*estats.Int64GaugeHandle, int64, ...string) {}
