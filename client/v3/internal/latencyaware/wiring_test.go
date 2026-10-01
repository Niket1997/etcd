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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// These tests run the policy inside a real gRPC client against small KV
// servers, for what fakes cannot show: the method names and DoneInfo gRPC
// really delivers, connection handling, and rebuilds.

const testPolicy = Name + "_test"

// built maps a client's target to the balancer gRPC last built for it.
var built sync.Map

type testBuilder struct{}

func (testBuilder) Name() string { return testPolicy }

func (testBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := builder{}.Build(cc, opts)
	built.Store(opts.Target.String(), b)
	return b
}

func init() { balancer.Register(testBuilder{}) }

// kvServer is a member: Range answers after its delay, or with its error.
type kvServer struct {
	pb.UnimplementedKVServer
	addr  string
	srv   *grpc.Server
	delay atomic.Int64
	err   atomic.Pointer[error]
	calls atomic.Int32
	conns atomic.Int32
}

func (s *kvServer) Range(ctx context.Context, _ *pb.RangeRequest) (*pb.RangeResponse, error) {
	s.calls.Add(1)
	select {
	case <-time.After(time.Duration(s.delay.Load())):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := s.err.Load(); err != nil {
		return nil, *err
	}
	return &pb.RangeResponse{Header: &pb.ResponseHeader{}}, nil
}

// watchServer holds every watch open until the client goes away.
type watchServer struct{ pb.UnimplementedWatchServer }

func (watchServer) Watch(s pb.Watch_WatchServer) error {
	<-s.Context().Done()
	return nil
}

type countingListener struct {
	net.Listener
	n *atomic.Int32
}

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.n.Add(1)
	}
	return c, err
}

func startServers(t *testing.T, n int) []*kvServer {
	t.Helper()
	srvs := make([]*kvServer, n)
	for i := range srvs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		s := &kvServer{addr: ln.Addr().String(), srv: grpc.NewServer()}
		pb.RegisterKVServer(s.srv, s)
		pb.RegisterWatchServer(s.srv, watchServer{})
		go s.srv.Serve(countingListener{ln, &s.conns})
		t.Cleanup(s.srv.Stop)
		srvs[i] = s
	}
	return srvs
}

// client is a gRPC client of the servers, balanced by the policy.
type client struct {
	t      *testing.T
	cc     *grpc.ClientConn
	kv     pb.KVClient
	sh     *Shared
	target string
}

func dial(t *testing.T, cfg Config, srvs []*kvServer, opts ...grpc.DialOption) *client {
	t.Helper()
	sh, err := NewShared(cfg, zap.NewNop())
	require.NoError(t, err)
	eps := make([]resolver.Endpoint, len(srvs))
	for i, s := range srvs {
		eps[i] = endpointOf(s.addr)
	}
	r := manual.NewBuilderWithScheme("latencyaware-test")
	r.InitialState(resolver.State{Endpoints: eps, Attributes: sh.Attributes()})
	opts = append(opts,
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"`+testPolicy+`":{}}]}`),
	)
	cc, err := grpc.NewClient(r.Scheme()+":///"+t.Name(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { cc.Close() })
	return &client{t: t, cc: cc, kv: pb.NewKVClient(cc), sh: sh, target: r.Scheme() + ":///" + t.Name()}
}

// lb is the balancer gRPC built for the client most recently.
func (c *client) lb() *lb {
	b, ok := built.Load(c.target)
	require.True(c.t, ok)
	return b.(*lb)
}

// member reads the client's state for a server, under the balancer's lock.
func (c *client) member(s *kvServer) (n int64, down bool) {
	b := c.lb()
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.eps.Get(endpointOf(s.addr))
	require.True(c.t, ok)
	return e.n.Load(), e.down
}

// ranges makes n Range calls, each with timeout.
func (c *client) ranges(n int, timeout time.Duration) {
	for range n {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		c.kv.Range(ctx, &pb.RangeRequest{})
		cancel()
	}
}

// calls snapshots each server's call count.
func calls(srvs []*kvServer) []int32 {
	out := make([]int32, len(srvs))
	for i, s := range srvs {
		out[i] = s.calls.Load()
	}
	return out
}

// untilRefused calls until a batch of 30 sends s nothing.
func (c *client) untilRefused(s *kvServer, timeout time.Duration) {
	c.t.Helper()
	require.Eventuallyf(c.t, func() bool {
		before := s.calls.Load()
		c.ranges(30, timeout)
		return s.calls.Load() == before
	}, 20*time.Second, time.Millisecond, "%s never refused", s.addr)
}

// wireConfig judges after 5 samples against a 10ms floor, generous enough
// for a loaded race-enabled run, and reacts within a few calls.
func wireConfig() Config {
	return Config{WarmupSamples: 5, EMAScale: 10 * ms, RescanInterval: 10 * ms, MinLatencyFloor: 10 * ms, RetryPeriod: time.Hour}
}

func TestOverGRPCASlowMemberIsRefusedAndTheRestSplitEvenly(t *testing.T) {
	srvs := startServers(t, 3)
	srvs[2].delay.Store(int64(60 * ms))
	c := dial(t, wireConfig(), srvs)

	c.untilRefused(srvs[2], time.Second)
	before := calls(srvs)
	c.ranges(300, time.Second)
	after := calls(srvs)
	assert.Equal(t, []int32{150, 150, 0}, []int32{after[0] - before[0], after[1] - before[1], after[2] - before[2]})
}

func TestOverGRPCTrippableErrorsRefuseAMember(t *testing.T) {
	for name, tc := range map[string]struct {
		err   error
		delay time.Duration
	}{
		// etcd sends this as a plain error, so it arrives as codes.Unknown.
		"apply backpressure": {err: rpctypes.ErrTooManyRequests},
		"client deadline":    {delay: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			srvs := startServers(t, 3)
			c := dial(t, wireConfig(), srvs)
			c.ranges(60, time.Second)
			if tc.err != nil {
				srvs[2].err.Store(&tc.err)
			}
			srvs[2].delay.Store(int64(tc.delay))

			c.untilRefused(srvs[2], 50*ms)
		})
	}
}

func TestOverGRPCNoSpaceNeverRefusesAMember(t *testing.T) {
	srvs := startServers(t, 3)
	c := dial(t, wireConfig(), srvs)
	c.ranges(60, time.Second)
	noSpace := rpctypes.ErrGRPCNoSpace
	srvs[2].err.Store(&noSpace)

	before := srvs[2].calls.Load()
	c.ranges(300, time.Second)
	assert.EqualValuesf(t, 100, srvs[2].calls.Load()-before, "a cluster-wide error is no reason to route around a member")
}

func TestOverGRPCAModeChangeNeverReconnects(t *testing.T) {
	srvs := startServers(t, 3)
	c := dial(t, wireConfig(), srvs)
	c.ranges(30, time.Second)
	conns := func() (n int32) {
		for _, s := range srvs {
			n += s.conns.Load()
		}
		return n
	}
	require.Eventually(t, func() bool { return conns() == 3 }, 10*time.Second, 10*ms)

	for _, mode := range []string{ModeOff, ModeShadow, ModeEnabled, ModeOff, ModeEnabled} {
		cfg := wireConfig()
		cfg.Mode = mode
		require.NoError(t, c.sh.Set(cfg))
		c.ranges(30, time.Second)
	}
	assert.EqualValues(t, 3, conns())
}

func TestOverGRPCAStoppedMemberCountsOneFailure(t *testing.T) {
	srvs := startServers(t, 3)
	c := dial(t, wireConfig(), srvs)
	c.ranges(30, time.Second)
	before, _ := c.member(srvs[2])

	srvs[2].srv.Stop()
	require.Eventually(t, func() bool {
		c.ranges(3, time.Second)
		_, down := c.member(srvs[2])
		return down
	}, 10*time.Second, 10*ms)
	for range 20 { // keep calling through gRPC's reconnect attempts
		c.ranges(5, time.Second)
		time.Sleep(50 * ms)
	}
	n, _ := c.member(srvs[2])
	assert.Equalf(t, before+1, n, "one outage, one failure")
}

func TestOverGRPCAStreamsDeadlineNeverCounts(t *testing.T) {
	srvs := startServers(t, 1)
	c := dial(t, wireConfig(), srvs)
	w := pb.NewWatchClient(c.cc)
	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*ms)
		s, err := w.Watch(ctx)
		require.NoError(t, err)
		_, err = s.Recv()
		require.Error(t, err)
		cancel()
	}
	n, _ := c.member(srvs[0])
	assert.Zerof(t, n, "five expired watches: no strike, no sample")

	c.ranges(1, time.Second)
	n, _ = c.member(srvs[0])
	assert.EqualValues(t, 1, n)
}

func TestOverGRPCAnIdleRebuildKeepsTheClientsConfig(t *testing.T) {
	srvs := startServers(t, 2)
	c := dial(t, wireConfig(), srvs, grpc.WithIdleTimeout(50*ms))
	c.ranges(10, time.Second)
	first := c.lb()

	require.Eventually(t, func() bool { return c.cc.GetState() == connectivity.Idle }, 10*time.Second, 10*ms)
	c.ranges(1, time.Second)
	assert.NotSame(t, first, c.lb())
	assert.Samef(t, c.sh, c.lb().shared.Load(), "the resolver hands the rebuilt balancer the same config")
}

func TestOverGRPCAFailFastCallFailsWhenEveryMemberIsDown(t *testing.T) {
	srvs := startServers(t, 2)
	c := dial(t, wireConfig(), srvs)
	c.ranges(10, time.Second)
	for _, s := range srvs {
		s.srv.Stop()
	}
	require.Eventually(t, func() bool { return c.cc.GetState() == connectivity.TransientFailure }, 10*time.Second, 10*ms)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.kv.Range(ctx, &pb.RangeRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NoErrorf(t, ctx.Err(), "a fail-fast call fails at once instead of waiting out its deadline")
}
