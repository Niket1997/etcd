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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/endpointsharding"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

const membersName = "etcd.client.latency_aware.endpoints"

func TestThePolicyIsRegistered(t *testing.T) {
	bb := balancer.Get(Name)
	require.NotNil(t, bb)
	assert.Equal(t, Name, bb.Name())
	b := bb.Build(&fakeCC{rec: newFakeRecorder(t)}, balancer.BuildOptions{})
	defer b.Close()
	assert.IsType(t, &lb{}, b)
}

func TestNothingReadyPassesEndpointShardingThrough(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a", "b")
	h.set("a", connectivity.Connecting)
	h.set("b", connectivity.TransientFailure)

	s := h.cc.last(t)
	assert.Equal(t, connectivity.Connecting, s.ConnectivityState)
	assert.Lenf(t, endpointsharding.ChildStatesFromPicker(s.Picker), 2, "endpointsharding's own picker, which queues picks")
}

func TestReadyMembersGetTheLatencyAwarePicker(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a", "b", "c")
	h.set("a", connectivity.Ready)

	s := h.cc.last(t)
	assert.Equal(t, connectivity.Ready, s.ConnectivityState)
	p, ok := s.Picker.(*picker)
	require.True(t, ok)
	assert.Len(t, p.ready, 1)
	assert.Equalf(t, 3, p.total, "every resolved member counts, READY or not")
	assert.EqualValues(t, 3, h.rec.gauge(membersName, h.b.target, "total"))
	assert.EqualValues(t, 1, h.rec.gauge(membersName, h.b.target, "ready"))
}

func TestATransientFailureEpisodeCountsOnce(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a")
	e := h.member("a")

	h.set("a", connectivity.TransientFailure)
	assert.EqualValues(t, 1, e.n.Load())
	h.set("a", connectivity.Connecting)
	h.set("a", connectivity.TransientFailure)
	assert.EqualValuesf(t, 1, e.n.Load(), "gRPC's reconnect loop is one episode")
	h.set("a", connectivity.Ready)
	assert.EqualValuesf(t, 1, e.n.Load(), "READY is not a success")
	h.set("a", connectivity.TransientFailure)
	assert.EqualValuesf(t, 2, e.n.Load(), "a new episode counts again")
}

func TestOffTracksAnEpisodeButRecordsNothing(t *testing.T) {
	cfg := pickerConfig()
	cfg.Mode = ModeOff
	h := newHarness(t, cfg)
	h.resolve("a")
	e := h.member("a")

	h.set("a", connectivity.TransientFailure)
	assert.Zero(t, e.n.Load())
	require.NoError(t, h.sh.Set(pickerConfig()))
	h.set("a", connectivity.Connecting)
	h.set("a", connectivity.TransientFailure)
	assert.Zerof(t, e.n.Load(), "an episode that began while off stays unrecorded")
	h.set("a", connectivity.Ready)
	h.set("a", connectivity.TransientFailure)
	assert.EqualValues(t, 1, e.n.Load())
}

func TestARemovedMemberIsForgottenAndTheMinimumRecomputed(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a", "b", "c")
	h.warm(h.member("a"), 2*ms, 0)
	h.warm(h.member("b"), 5*ms, 0)
	h.warm(h.member("c"), 9*ms, 0)
	h.b.rescan(true)
	require.Equal(t, int64(2*ms), h.b.min.Load())

	h.resolve("b", "c")
	assert.Equalf(t, int64(5*ms), h.b.min.Load(), "survivors are not judged against a member that is gone")

	h.resolve("a", "b", "c")
	assert.Zerof(t, h.member("a").n.Load(), "a returning member starts afresh")
}

func TestMemberStateSurvivesPickerRebuilds(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a", "b")
	h.set("a", connectivity.Ready)
	p1 := h.cc.last(t).Picker.(*picker)
	res, _ := pick(t, p1, kvRange)

	h.set("b", connectivity.Ready)
	res.Done(balancer.DoneInfo{BytesReceived: true})
	assert.NotSame(t, p1, h.cc.last(t).Picker)
	assert.EqualValuesf(t, 1, h.member("a").n.Load(), "a call on the old picker lands on the same member state")
}

func TestTheBalancerUsesTheClientsConfig(t *testing.T) {
	h := newHarness(t, pickerConfig())
	b := newBalancer(h.cc, balancer.BuildOptions{}, h.kids.build, h.clk.now)
	assert.Equalf(t, ModeEnabled, b.shared.Load().config().Mode, "defaults until the resolver hands one over")

	state := resolver.State{Endpoints: []resolver.Endpoint{endpointOf("a")}, Attributes: h.sh.Attributes()}
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{ResolverState: state}))
	assert.Same(t, h.sh, b.shared.Load())

	state.Attributes = nil
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{ResolverState: state}))
	assert.Samef(t, h.sh, b.shared.Load(), "a state without one keeps it")
}

func TestNoEndpointsAsksForReResolution(t *testing.T) {
	h := newHarness(t, pickerConfig())
	err := h.b.UpdateClientConnState(balancer.ClientConnState{})
	require.ErrorIs(t, err, balancer.ErrBadResolverState)
}

func TestCallsReachTheChildren(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a", "b")

	h.b.ResolverError(errors.New("no such host"))
	h.b.ExitIdle()
	h.b.UpdateSubConnState(nil, balancer.SubConnState{})
	h.b.Close()
	assert.Equal(t, 2, h.kids.resolverErrs)
	assert.Equal(t, 2, h.kids.exitIdles)
	assert.Equal(t, 2, h.kids.closed)
}

func TestTheClusterMinimum(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a", "b", "c")
	a, b, c := h.member("a"), h.member("b"), h.member("c")
	pin(a, 2, ms/10, h.clk.now()) // warming up: never the reference
	h.warm(b, 4*ms, 2*time.Hour)  // no sample for a retry period
	h.warm(c, 6*ms, 0)

	h.b.rescan(false)
	assert.Equal(t, int64(6*ms), h.b.min.Load())

	h.warm(c, ms/2, 0)
	h.b.rescan(false)
	assert.Equalf(t, int64(6*ms), h.b.min.Load(), "at most once per rescan interval")
	h.clk.advance(2 * time.Second)
	h.b.rescan(false)
	assert.Equal(t, int64(ms/2), h.b.min.Load())
	assert.Equalf(t, int64(ms), h.b.ref(h.sh.config()), "clamped up to the floor when read")

	h.warm(c, 3*ms, 2*time.Hour)
	h.clk.advance(2 * time.Second)
	h.b.rescan(false)
	assert.Equalf(t, int64(ms/2), h.b.min.Load(), "nothing qualifying keeps the last value")
}

func TestANewRescanIntervalAppliesAtOnce(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a")
	a := h.member("a")
	h.warm(a, 6*ms, 0)
	h.b.rescan(false)
	require.Equal(t, int64(6*ms), h.b.min.Load())

	cfg := pickerConfig()
	cfg.RescanInterval = 100 * ms
	require.NoError(t, h.sh.Set(cfg))
	h.clk.advance(200 * ms)
	h.warm(a, 3*ms, 0)
	h.b.rescan(false)
	assert.Equalf(t, int64(3*ms), h.b.min.Load(), "200ms is past the new 100ms interval, though not the old 2s")
}

func TestAScanInFlightIsNotDuplicated(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a")
	h.warm(h.member("a"), 3*ms, 0)

	h.b.scanMu.Lock()
	h.b.rescan(false)
	h.b.scanMu.Unlock()
	assert.Zerof(t, h.b.min.Load(), "a sample finding a scan under way leaves it to that scan")
}

func TestASampleTriggersARescan(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a")
	h.set("a", connectivity.Ready)
	p := h.cc.last(t).Picker.(*picker)
	for range 3 {
		res, _ := pick(t, p, kvRange)
		h.clk.advance(4 * ms)
		res.Done(balancer.DoneInfo{BytesReceived: true})
	}
	assert.Equalf(t, int64(4*ms), h.b.min.Load(), "the warm-up's last sample set the minimum")
}
