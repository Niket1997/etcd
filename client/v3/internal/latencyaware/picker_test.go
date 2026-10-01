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
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

const (
	ms        = time.Millisecond
	kvRange   = "/etcdserverpb.KV/Range"
	picksName = "etcd.client.latency_aware.picks"
	rpcsName  = "etcd.client.latency_aware.rpcs"

	fallbackStarted = "latency-aware: too many members unhealthy, routing plain round robin"
	fallbackEnded   = "latency-aware: quorum fallback ended"
)

var deadline = status.Error(codes.DeadlineExceeded, "context deadline exceeded")

// pickerConfig judges after 3 samples, against a 1ms floor, and never lets a
// retry period pass unless a test moves the clock an hour.
func pickerConfig() Config {
	return Config{WarmupSamples: 3, RetryPeriod: time.Hour, MinLatencyFloor: ms}
}

// judge sets the cluster minimum to 1ms and warms member i at avgs[i], last
// sampled just now so no probe is due.
func (h *harness) judge(p *picker, avgs ...time.Duration) {
	h.b.min.Store(int64(ms))
	for i, a := range avgs {
		h.warm(p.ready[i].ep, a, 0)
	}
}

func pick(t *testing.T, p *picker, method string) (balancer.PickResult, string) {
	t.Helper()
	res, err := p.Pick(balancer.PickInfo{FullMethodName: method})
	require.NoError(t, err)
	return res, res.SubConn.(*fakeSubConn).addr
}

func TestOffIsPlainRoundRobin(t *testing.T) {
	cfg := pickerConfig()
	cfg.Mode = ModeOff
	h := newHarness(t, cfg)
	p := h.picker(3, "a", "b", "c")
	h.judge(p, ms, ms, 50*ms)

	assert.Equal(t, map[string]int{"a": 100, "b": 100, "c": 100}, pickAll(t, p, 300, kvRange))
	res, _ := pick(t, p, kvRange)
	assert.Nilf(t, res.Done, "off attaches no Done")
	assert.Zero(t, h.rec.count(picksName, h.b.target, "c", "refused"))
}

func TestEnabledRoutesAroundARefusedMember(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(3, "a", "b", "c")
	h.judge(p, ms, ms, 50*ms)

	assert.Equal(t, map[string]int{"a": 150, "b": 150}, pickAll(t, p, 300, kvRange))
	assert.EqualValuesf(t, 100, h.rec.count(picksName, h.b.target, "c", "refused"), "the picks round robin would have sent it")
	assert.EqualValues(t, 150, h.rec.count(picksName, h.b.target, "a", "allowed"))
}

func TestSurvivorsSplitARefusedMembersShareEvenly(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(5, "a", "b", "c", "d", "e")
	h.judge(p, ms, ms, 50*ms, ms, ms)

	assert.Equal(t, map[string]int{"a": 100, "b": 100, "d": 100, "e": 100}, pickAll(t, p, 400, kvRange))
}

func TestEveryMemberRefusedFallsBackToTheCursor(t *testing.T) {
	cfg := pickerConfig()
	cfg.MaxUnhealthyEndpoints = 2
	h := newHarness(t, cfg)
	p := h.picker(2, "a", "b")
	h.judge(p, 50*ms, 50*ms)

	assert.Equalf(t, map[string]int{"a": 5, "b": 5}, pickAll(t, p, 10, kvRange), "a pick never fails while anything is READY")
	assert.EqualValues(t, 5, h.rec.count(picksName, h.b.target, "a", "fallback"))
}

func TestADueProbeGoesOutOncePerRetryPeriod(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(2, "a", "c")
	h.judge(p, ms, 50*ms)
	h.warm(p.ready[1].ep, 50*ms, 2*time.Hour) // c's last sample is old: its probe is due

	assert.Equalf(t, map[string]int{"a": 10}, pickAll(t, p, 10, pb.Watch_Watch_FullMethodName), "untimed picks never take it")
	_, got := pick(t, p, kvRange)
	assert.Equalf(t, "c", got, "the first timed pick is the probe")
	assert.Equalf(t, map[string]int{"a": 10}, pickAll(t, p, 10, kvRange), "one per retry period")
	assert.EqualValues(t, 1, h.rec.count(picksName, h.b.target, "c", "probe"))
}

func TestShadowCountsTheDecisionButRoutesRoundRobin(t *testing.T) {
	cfg := pickerConfig()
	cfg.Mode = ModeShadow
	h := newHarness(t, cfg)
	p := h.picker(2, "a", "c")
	h.judge(p, ms, 50*ms)
	h.warm(p.ready[1].ep, 50*ms, 2*time.Hour)

	assert.Equal(t, map[string]int{"a": 50, "c": 50}, pickAll(t, p, 100, kvRange))
	assert.EqualValues(t, 50, h.rec.count(picksName, h.b.target, "c", "refused"))
	assert.EqualValues(t, 100, h.rec.count(picksName, h.b.target, "a", "allowed"))
	assert.Truef(t, p.ready[1].ep.claimProbe(h.clk.now(), int64(time.Hour)), "shadow never spends the probe")
}

func TestTheQuorumGuard(t *testing.T) {
	t.Run("a missing member tips it", func(t *testing.T) {
		h := newHarness(t, pickerConfig())
		p := h.picker(3, "a", "c") // the third member is not READY
		h.judge(p, ms, 50*ms)

		assert.Equalf(t, map[string]int{"a": 50, "c": 50}, pickAll(t, p, 100, kvRange), "plain round robin")
		assert.EqualValues(t, 50, h.rec.count(picksName, h.b.target, "c", "quorum"))
		started := h.logs.FilterMessage(fallbackStarted).All()
		require.Lenf(t, started, 1, "logged when it starts, not per pick")
		assert.Equal(t, zapcore.WarnLevel, started[0].Level)
		assert.Equal(t, "missing", started[0].ContextMap()["cause"])

		h.warm(p.ready[1].ep, ms, 0)
		assert.Equal(t, map[string]int{"a": 5, "c": 5}, pickAll(t, p, 10, kvRange))
		assert.Equal(t, 1, h.logs.FilterMessage(fallbackEnded).Len())
	})
	t.Run("refused members alone tip it", func(t *testing.T) {
		h := newHarness(t, pickerConfig())
		p := h.picker(3, "a", "b", "c")
		h.judge(p, ms, 50*ms, 50*ms)

		assert.Equal(t, map[string]int{"a": 10, "b": 10, "c": 10}, pickAll(t, p, 30, kvRange))
		assert.Equal(t, "unhealthy", h.logs.FilterMessage(fallbackStarted).All()[0].ContextMap()["cause"])
	})
	t.Run("shadow logs it at info", func(t *testing.T) {
		cfg := pickerConfig()
		cfg.Mode = ModeShadow
		h := newHarness(t, cfg)
		p := h.picker(3, "a", "c")
		h.judge(p, ms, 50*ms)

		pickAll(t, p, 10, kvRange)
		assert.Equal(t, zapcore.InfoLevel, h.logs.FilterMessage(fallbackStarted).All()[0].Level)
	})
	t.Run("nothing refused means nothing to guard", func(t *testing.T) {
		h := newHarness(t, pickerConfig())
		p := h.picker(3, "a")
		h.judge(p, ms)

		assert.Equal(t, map[string]int{"a": 10}, pickAll(t, p, 10, kvRange))
		assert.Zero(t, h.logs.FilterMessage(fallbackStarted).Len())
	})
}

func TestVerdictChangesAreLoggedOnce(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(2, "a", "c")
	h.judge(p, ms, 50*ms)

	pickAll(t, p, 20, kvRange)
	refused := h.logs.FilterMessage("latency-aware: member refused").All()
	require.Len(t, refused, 1)
	assert.Equal(t, "c", refused[0].ContextMap()["endpoint"])

	h.warm(p.ready[1].ep, ms, 0)
	pickAll(t, p, 20, kvRange)
	assert.Equal(t, 1, h.logs.FilterMessage("latency-aware: member readmitted").Len())
}

func TestAModeChangeAppliesOnTheNextPick(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(2, "a", "c")
	h.judge(p, ms, 50*ms)
	assert.Equal(t, map[string]int{"a": 10}, pickAll(t, p, 10, kvRange))

	off := pickerConfig()
	off.Mode = ModeOff
	require.NoError(t, h.sh.Set(off))
	assert.Equal(t, map[string]int{"a": 5, "c": 5}, pickAll(t, p, 10, kvRange))

	require.NoError(t, h.sh.Set(pickerConfig()))
	assert.Equal(t, map[string]int{"a": 10}, pickAll(t, p, 10, kvRange))
}

func TestDoneTeachesOnlyTimedCalls(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(1, "a")
	e := p.ready[0].ep
	call := func(method string, di balancer.DoneInfo) {
		res, _ := pick(t, p, method)
		h.clk.advance(5 * ms)
		res.Done(di)
	}
	ok := balancer.DoneInfo{BytesSent: true, BytesReceived: true}

	call(kvRange, ok)
	assert.EqualValues(t, 1, e.n.Load())
	assert.Equalf(t, int64(5*ms), e.avg.Load(), "the sample runs from the pick to Done")

	call(pb.Watch_Watch_FullMethodName, ok)
	call(pb.Maintenance_Defragment_FullMethodName, balancer.DoneInfo{Err: deadline})
	assert.EqualValuesf(t, 1, e.n.Load(), "untimed calls never teach")
	assert.Zero(t, e.strikeN)

	call(kvRange, balancer.DoneInfo{Err: rpctypes.ErrGRPCNoSpace})
	assert.EqualValuesf(t, 1, e.n.Load(), "a non-trippable error teaches nothing")

	call(kvRange, balancer.DoneInfo{Err: deadline})
	assert.Equalf(t, 1, e.strikeN, "an admitted member's deadline is a strike")

	call(kvRange, balancer.DoneInfo{})
	tg := h.b.target
	assert.EqualValues(t, 2, h.rec.count(rpcsName, tg, "a", "success", "OK"))
	assert.EqualValues(t, 2, h.rec.count(rpcsName, tg, "a", "failure", "DeadlineExceeded"))
	assert.EqualValuesf(t, 1, h.rec.count(rpcsName, tg, "a", "ignored", "ResourceExhausted"), "the abandoned pick is not a call")
}

func TestAFailedProbeCountsAtOnce(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(2, "a", "c")
	h.judge(p, ms, 50*ms)
	c := p.ready[1].ep
	h.warm(c, 50*ms, 2*time.Hour)

	res, got := pick(t, p, kvRange)
	require.Equal(t, "c", got)
	h.warm(c, ms, 2*time.Hour) // a sample landed meanwhile: c no longer looks slow
	res.Done(balancer.DoneInfo{Err: deadline})
	assert.EqualValuesf(t, h.sh.config().WarmupSamples+1, c.n.Load(), "the failed probe is no forgiven strike")
}

func TestTheChildsDoneStillRuns(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(1, "a")
	calls := 0
	p.ready[0].picker.(*fakeChildPicker).done = func(balancer.DoneInfo) { calls++ }

	res, _ := pick(t, p, kvRange)
	res.Done(balancer.DoneInfo{})
	assert.Equalf(t, 1, calls, "even for an abandoned pick")
}

func TestAChildPickErrorIsReturned(t *testing.T) {
	h := newHarness(t, pickerConfig())
	p := h.picker(1, "a")
	p.ready[0].picker.(*fakeChildPicker).err = balancer.ErrNoSubConnAvailable

	_, err := p.Pick(balancer.PickInfo{FullMethodName: kvRange})
	require.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)
}

func TestAPickAllocatesOnlyItsDoneCallback(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.b.rec = nopRecorder{}
	p := h.picker(3, "a", "b", "c")
	h.judge(p, ms, ms, 50*ms)
	info, ok := balancer.PickInfo{FullMethodName: kvRange}, balancer.DoneInfo{BytesReceived: true}

	allocs := testing.AllocsPerRun(1000, func() {
		res, _ := p.Pick(info)
		res.Done(ok)
	})
	assert.LessOrEqual(t, allocs, 1.0)
}

func TestTrippable(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"deadline":                   {deadline, true},
		"too many requests":          {rpctypes.ErrGRPCRequestTooManyRequests, true},
		"too many requests, as sent": {status.Error(codes.Unknown, "etcdserver: too many requests"), true},
		"no space":                   {rpctypes.ErrGRPCNoSpace, false},
		"unavailable":                {status.Error(codes.Unavailable, "connection refused"), false},
		"canceled":                   {status.Error(codes.Canceled, "context canceled"), false},
		"not a status":               {errors.New("boom"), false},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, trippable(tc.err))
		})
	}
}

// TestEveryEtcdRPCIsClassified walks the generated services, so a new etcd
// RPC fails here until someone decides whether its duration is latency.
func TestEveryEtcdRPCIsClassified(t *testing.T) {
	timedRPCs := []string{
		"/etcdserverpb.KV/Range", "/etcdserverpb.KV/Put", "/etcdserverpb.KV/DeleteRange", "/etcdserverpb.KV/Txn",
		"/etcdserverpb.Lease/LeaseGrant", "/etcdserverpb.Lease/LeaseRevoke",
		"/etcdserverpb.Lease/LeaseTimeToLive", "/etcdserverpb.Lease/LeaseLeases",
	}
	untimedRPCs := []string{
		"/etcdserverpb.KV/Compact",
		"/etcdserverpb.Cluster/MemberAdd", "/etcdserverpb.Cluster/MemberRemove", "/etcdserverpb.Cluster/MemberUpdate",
		"/etcdserverpb.Cluster/MemberList", "/etcdserverpb.Cluster/MemberPromote",
		"/etcdserverpb.Maintenance/Alarm", "/etcdserverpb.Maintenance/Status", "/etcdserverpb.Maintenance/Defragment",
		"/etcdserverpb.Maintenance/Hash", "/etcdserverpb.Maintenance/HashKV", "/etcdserverpb.Maintenance/MoveLeader",
		"/etcdserverpb.Maintenance/Downgrade",
		"/etcdserverpb.Auth/AuthEnable", "/etcdserverpb.Auth/AuthDisable", "/etcdserverpb.Auth/AuthStatus",
		"/etcdserverpb.Auth/Authenticate", "/etcdserverpb.Auth/UserAdd", "/etcdserverpb.Auth/UserGet",
		"/etcdserverpb.Auth/UserList", "/etcdserverpb.Auth/UserDelete", "/etcdserverpb.Auth/UserChangePassword",
		"/etcdserverpb.Auth/UserGrantRole", "/etcdserverpb.Auth/UserRevokeRole", "/etcdserverpb.Auth/RoleAdd",
		"/etcdserverpb.Auth/RoleGet", "/etcdserverpb.Auth/RoleList", "/etcdserverpb.Auth/RoleDelete",
		"/etcdserverpb.Auth/RoleGrantPermission", "/etcdserverpb.Auth/RoleRevokePermission",
	}
	want := map[string]bool{}
	for _, m := range timedRPCs {
		want[m] = true
	}
	for _, m := range untimedRPCs {
		want[m] = false
	}
	for _, sd := range []grpc.ServiceDesc{pb.KV_ServiceDesc, pb.Watch_ServiceDesc, pb.Lease_ServiceDesc, pb.Cluster_ServiceDesc, pb.Maintenance_ServiceDesc, pb.Auth_ServiceDesc} {
		for _, m := range sd.Methods {
			name := "/" + sd.ServiceName + "/" + m.MethodName
			w, known := want[name]
			if assert.Truef(t, known, "new RPC %s: decide whether its duration is the member's latency", name) {
				assert.Equalf(t, w, timed(name), name)
			}
		}
		for _, s := range sd.Streams {
			assert.Falsef(t, timed("/"+sd.ServiceName+"/"+s.StreamName), "a stream's duration is its lifetime")
		}
	}
	assert.Falsef(t, timed("/v3lockpb.Lock/Lock"), "other services on the connection are never timed")
}
