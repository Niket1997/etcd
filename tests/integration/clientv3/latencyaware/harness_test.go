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
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	estats "google.golang.org/grpc/experimental/stats"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

// cluster is real members, each behind a fault dial, with per-member call
// counts. A server interceptor finds the member by the listener the call came in on.
type cluster struct {
	*integration.Cluster
	mu    sync.Mutex
	index map[string]int // listener address to member
	calls []map[string]int
	dials []atomic.Pointer[dial]
}

// dial is one member's fault; nil is healthy.
type dial struct {
	delay, jitter time.Duration // each slowed call, and each stream message, waits delay ± jitter
	fraction      float64       // of calls slowed, when set
	hang          bool          // calls and stream opens wait for the caller
	holdStreams   bool          // stream opens wait for the caller
	err           error         // calls fail with it
}

// start runs size members; bridge puts each behind a proxy that can drop or
// blackhole its connections. Bridged members listen on unix sockets, the only
// kind the bridge serves.
func start(t *testing.T, size int, bridge bool) *cluster {
	t.Helper()
	integration.BeforeTest(t)
	c := &cluster{index: map[string]int{}, calls: make([]map[string]int, size), dials: make([]atomic.Pointer[dial], size)}
	for m := range c.calls {
		c.calls[m] = map[string]int{}
	}
	c.Cluster = integration.NewCluster(t, &integration.ClusterConfig{
		Size: size, UseTCP: !bridge, UseBridge: bridge,
		GRPCAdditionalServerOptions: []grpc.ServerOption{grpc.ChainUnaryInterceptor(c.unary), grpc.ChainStreamInterceptor(c.stream)},
	})
	t.Cleanup(func() { c.Terminate(t) })
	c.mu.Lock()
	defer c.mu.Unlock()
	for m, mem := range c.Members {
		c.index[mem.GRPCListener.Addr().String()] = m
	}
	return c
}

// addr is member m's endpoint as the policy labels it.
func (c *cluster) addr(m int) string {
	if path, ok := strings.CutPrefix(c.Members[m].GRPCURL, "unix://"); ok {
		return "unix:" + path
	}
	return strings.TrimPrefix(c.Members[m].GRPCURL, "http://")
}

func (c *cluster) set(m int, d *dial) { c.dials[m].Store(d) }

func (c *cluster) Slow(m int, delay, jitter time.Duration) {
	c.set(m, &dial{delay: delay, jitter: jitter})
}

// SlowSome slows a fraction of member m's calls.
func (c *cluster) SlowSome(m int, delay, jitter time.Duration, fraction float64) {
	c.set(m, &dial{delay: delay, jitter: jitter, fraction: fraction})
}
func (c *cluster) Hang(m int)            { c.set(m, &dial{hang: true}) }
func (c *cluster) HoldStreams(m int)     { c.set(m, &dial{holdStreams: true}) }
func (c *cluster) Fail(m int, err error) { c.set(m, &dial{err: err}) }
func (c *cluster) Heal(m int)            { c.set(m, nil) }

// Disconnect drops a bridged member's client connections and holds new ones;
// the member itself keeps running.
func (c *cluster) Disconnect(m int) {
	c.Members[m].Bridge().PauseConnections()
	c.Members[m].Bridge().DropConnections()
}

func (c *cluster) Reconnect(m int) { c.Members[m].Bridge().UnpauseConnections() }

// Calls is how many calls of method each member has seen.
func (c *cluster) Calls(method string) []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, len(c.calls))
	for m := range out {
		out[m] = c.calls[m][method]
	}
	return out
}

// Ranges is how many Range calls each member has seen.
func (c *cluster) Ranges() []int { return c.Calls(pb.KV_Range_FullMethodName) }

// enter counts a call against its member and returns the member's dial,
// nil for a healthy member or a call to no member of the cluster.
func (c *cluster) enter(ctx context.Context, method string) (*atomic.Pointer[dial], *dial) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.index[p.LocalAddr.String()]
	if !ok {
		return nil, nil
	}
	c.calls[m][method]++
	return &c.dials[m], c.dials[m].Load()
}

func (c *cluster) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, d := c.enter(ctx, info.FullMethod); d != nil {
		if err := d.wait(ctx, d.hang); err != nil {
			return nil, err
		}
		if d.err != nil {
			return nil, d.err
		}
	}
	return handler(ctx, req)
}

func (c *cluster) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	cur, d := c.enter(ss.Context(), info.FullMethod)
	if d != nil && (d.hang || d.holdStreams) {
		return d.wait(ss.Context(), true)
	}
	if cur != nil {
		ss = &pacedStream{ServerStream: ss, dial: cur}
	}
	return handler(srv, ss)
}

// wait holds a call for the dial's delay, or until the caller gives up when hang.
func (d *dial) wait(ctx context.Context, hang bool) error {
	var timeout <-chan time.Time
	if !hang {
		if d.fraction > 0 && rand.Float64() >= d.fraction {
			return nil
		}
		wait := d.delay
		if d.jitter > 0 {
			wait += time.Duration(rand.Int64N(int64(2*d.jitter)+1)) - d.jitter
		}
		if wait <= 0 {
			return nil
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-timeout:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

// pacedStream follows its member's dial for every message it sends.
type pacedStream struct {
	grpc.ServerStream
	dial *atomic.Pointer[dial]
}

func (s *pacedStream) SendMsg(m any) error {
	if d := s.dial.Load(); d != nil {
		if err := d.wait(s.Context(), false); err != nil {
			return err
		}
	}
	return s.ServerStream.SendMsg(m)
}

// client is a clientv3 client with the policy's metrics and logs.
type client struct {
	*clientv3.Client
	metrics *metrics
	logs    *observer.ObservedLogs
}

// client connects to eps, all members when empty, with cfg; nil is no policy.
func (c *cluster) client(t *testing.T, cfg *clientv3.LatencyAwareConfig, eps ...string) *client {
	t.Helper()
	if len(eps) == 0 {
		eps = c.Endpoints()
	}
	core, logs := observer.New(zap.InfoLevel)
	m := &metrics{series: map[string]*series{}}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:    eps,
		LatencyAware: cfg,
		Logger:       zap.New(zapcore.NewTee(core, zaptest.NewLogger(t).Core())),
		DialOptions:  []grpc.DialOption{grpc.WithStatsHandler(m)},
	})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })
	return &client{Client: cli, metrics: m, logs: logs}
}

const metricPrefix = "etcd.client.latency_aware."

// metrics keeps the last value of every series the policy reports. It is a
// stats.Handler so gRPC hands it the channel's metrics.
type metrics struct {
	// Never called: every method is below.
	estats.MetricsRecorder
	mu     sync.Mutex
	series map[string]*series
}

type series struct {
	labels map[string]string
	v      int64
}

func (m *metrics) record(d *estats.MetricDescriptor, v int64, add bool, values []string) {
	if !strings.HasPrefix(d.Name, metricPrefix) {
		return
	}
	key := d.Name + " " + strings.Join(values, " ")
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.series[key]
	if s == nil {
		s = &series{labels: map[string]string{"name": strings.TrimPrefix(d.Name, metricPrefix)}}
		for i, l := range d.Labels {
			s.labels[l] = values[i]
		}
		m.series[key] = s
	}
	if !add {
		s.v = 0
	}
	s.v += v
}

// sum adds up the series whose labels include every name, value pair given.
func (m *metrics) sum(pairs ...string) (total int64, found bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
next:
	for _, s := range m.series {
		for i := 0; i < len(pairs); i += 2 {
			if s.labels[pairs[i]] != pairs[i+1] {
				continue next
			}
		}
		total, found = total+s.v, true
	}
	return total, found
}

// contract is every metric reported so far, with its label names.
func (m *metrics) contract() map[string][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string][]string{}
	for _, s := range m.series {
		out[s.labels["name"]] = slices.DeleteFunc(slices.Sorted(maps.Keys(s.labels)), func(l string) bool { return l == "name" })
	}
	return out
}

func (m *metrics) RecordInt64Count(h *estats.Int64CountHandle, incr int64, labels ...string) {
	m.record(h.Descriptor(), incr, true, labels)
}

func (m *metrics) RecordInt64Gauge(h *estats.Int64GaugeHandle, v int64, labels ...string) {
	m.record(h.Descriptor(), v, false, labels)
}
func (*metrics) RecordFloat64Count(*estats.Float64CountHandle, float64, ...string)       {}
func (*metrics) RecordInt64Histo(*estats.Int64HistoHandle, int64, ...string)             {}
func (*metrics) RecordFloat64Histo(*estats.Float64HistoHandle, float64, ...string)       {}
func (*metrics) RecordInt64UpDownCount(*estats.Int64UpDownCountHandle, int64, ...string) {}
func (*metrics) RegisterAsyncReporter(estats.AsyncMetricReporter, ...estats.AsyncMetric) func() {
	return func() {}
}
func (*metrics) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (*metrics) HandleRPC(context.Context, stats.RPCStats)                         {}
func (*metrics) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (*metrics) HandleConn(context.Context, stats.ConnStats)                       {}

// picks counts the picks with outcome, for ep or every member when empty.
func (cli *client) picks(outcome, ep string) int64 {
	pairs := []string{"name", "picks", "etcd.outcome", outcome}
	if ep != "" {
		pairs = append(pairs, "etcd.endpoint", ep)
	}
	n, _ := cli.metrics.sum(pairs...)
	return n
}

// refused is how many picks round robin would have sent to ep went elsewhere.
func (cli *client) refused(ep string) int64 { return cli.picks("refused", ep) }

// quorum is how many picks the quorum fallback routed.
func (cli *client) quorum() int64 { return cli.picks("quorum", "") }

// rpcs counts the completed calls matching the label pairs.
func (cli *client) rpcs(pairs ...string) int64 {
	n, _ := cli.metrics.sum(append([]string{"name", "rpcs"}, pairs...)...)
	return n
}

// outcomes is every call the policy saw complete.
func (cli *client) outcomes() int64 { return cli.rpcs() }

// gauge is the last value of a gauge series.
func (cli *client) gauge(name string, pairs ...string) (int64, bool) {
	return cli.metrics.sum(append([]string{"name", name}, pairs...)...)
}

// members is the endpoints gauge for state: total, ready or refused.
func (cli *client) members(state string) int64 {
	n, _ := cli.gauge("endpoints", "etcd.state", state)
	return n
}

// logged counts the log entries with msg.
func (cli *client) logged(msg string) int { return cli.logs.FilterMessage(msg).Len() }

const slow = 100 * time.Millisecond // ten times the 10 ms line

// tuning is the policy with shorter clocks: a 10 ms line (threshold 2 over a
// 5 ms floor), a 100 ms EMA scale and a 2 s retry period, so 10 ms of added
// latency refuses a member and a probe comes every two seconds. Timings in
// assertions derive from these numbers.
func tuning() *clientv3.LatencyAwareConfig {
	return &clientv3.LatencyAwareConfig{
		ExclusionThreshold: 2, MinLatencyFloor: 5 * time.Millisecond, EMAScale: 100 * time.Millisecond,
		RetryPeriod: 2 * time.Second, WarmupSamples: 10, SyntheticMultiplier: 20,
		MinSyntheticLatency: time.Second, RescanInterval: 200 * time.Millisecond,
	}
}

// steady is tuning with a 50 ms floor: a 100 ms line no scheduling jitter crosses.
func steady() *clientv3.LatencyAwareConfig {
	cfg := tuning()
	cfg.MinLatencyFloor = 50 * time.Millisecond
	return cfg
}

// with is cfg after edit.
func with(cfg *clientv3.LatencyAwareConfig, edit func(*clientv3.LatencyAwareConfig)) *clientv3.LatencyAwareConfig {
	edit(cfg)
	return cfg
}

// enabled connects an enforcing client to eps, every member when empty, and settles it.
func enabled(t *testing.T, c *cluster, eps ...string) *client {
	t.Helper()
	cli := c.client(t, tuning(), eps...)
	settle(t, cli, len(cli.Endpoints()))
	return cli
}

// settle gets every member past warm-up, so the cluster minimum is known.
func settle(t *testing.T, cli *client, members int) {
	t.Helper()
	warm(t, cli, members)
	gets(t, cli, 10*members)
}

// rounds sends rounds of per Gets until done, giving up after limit rounds.
func rounds(t *testing.T, cli *client, per, limit int, done func() bool) bool {
	t.Helper()
	for range limit {
		gets(t, cli, per)
		if done() {
			return true
		}
	}
	return false
}

// refuse drives rounds until the policy refuses ep.
func refuse(t *testing.T, cli *client, ep string) {
	t.Helper()
	require.Truef(t, rounds(t, cli, 30, 20, func() bool { return cli.refused(ep) > 0 }), "%s was never refused", ep)
}

// round is how many of 30 Gets each member served.
func round(t *testing.T, cli *client, c *cluster) []int {
	t.Helper()
	before := c.Ranges()
	gets(t, cli, 30)
	return delta(before, c.Ranges())
}

// during drives rounds for d and returns how many calls each member served.
func during(t *testing.T, cli *client, c *cluster, d time.Duration) []int {
	t.Helper()
	before := c.Ranges()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); {
		gets(t, cli, 30)
	}
	return delta(before, c.Ranges())
}

func delta(before, after []int) []int {
	out := make([]int, len(after))
	for i := range after {
		out[i] = after[i] - before[i]
	}
	return out
}

func sum(xs []int) (total int) {
	for _, x := range xs {
		total += x
	}
	return total
}

// even asserts every member carried at least three quarters of its share.
func even(t *testing.T, served []int) {
	t.Helper()
	for i, n := range served {
		assert.GreaterOrEqualf(t, n, sum(served)*3/(4*len(served)), "member %d served %d of %v", i, n, served)
	}
}
