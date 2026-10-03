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
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	estats "google.golang.org/grpc/experimental/stats"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

// Instruments are off by default; a user enables them in gRPC's OpenTelemetry plugin.
var (
	picksMetric = estats.RegisterInt64Count(estats.MetricDescriptor{
		Name:        "etcd.client.latency_aware.picks",
		Description: "EXPERIMENTAL. Picks by outcome: allowed; refused, a pick round robin would have sent to this refused member; probe; fallback, every READY member refused; quorum, too many members unhealthy so plain round robin.",
		Unit:        "{pick}",
		Labels:      []string{"grpc.target", "etcd.endpoint", "etcd.outcome"},
	})
	rpcsMetric = estats.RegisterInt64Count(estats.MetricDescriptor{
		Name:        "etcd.client.latency_aware.rpcs",
		Description: "EXPERIMENTAL. Completed calls by result: success; failure, a deadline or apply backpressure; ignored, any other error.",
		Unit:        "{call}",
		Labels:      []string{"grpc.target", "etcd.endpoint", "etcd.result", "grpc.status"},
	})
	membersMetric = estats.RegisterInt64Gauge(estats.MetricDescriptor{
		Name:        "etcd.client.latency_aware.endpoints",
		Description: "EXPERIMENTAL. Members by state: total resolved, ready, refused.",
		Unit:        "{endpoint}",
		Labels:      []string{"grpc.target", "etcd.state"},
	})
	clusterMinMetric = estats.RegisterInt64Gauge(estats.MetricDescriptor{
		Name:        "etcd.client.latency_aware.cluster_min",
		Description: "EXPERIMENTAL. The reference latency members are judged against.",
		Unit:        "us",
		Labels:      []string{"grpc.target"},
	})
	latencyMetric = estats.RegisterInt64Gauge(estats.MetricDescriptor{
		Name:        "etcd.client.latency_aware.endpoint_latency",
		Description: "EXPERIMENTAL. A member's latency average.",
		Unit:        "us",
		Labels:      []string{"grpc.target", "etcd.endpoint"},
	})
)

type outcome int

const (
	outcomeAllowed outcome = iota
	outcomeRefused
	outcomeProbe
	outcomeFallback
	outcomeQuorum
	numOutcomes
)

var outcomeNames = [numOutcomes]string{"allowed", "refused", "probe", "fallback", "quorum"}

// labels are a member's label values for the per-call instruments, built once
// so recording a pick or a success allocates nothing.
type labels struct {
	picks   [numOutcomes][]string
	success []string
	latency []string
}

func (b *lb) newEndpoint(ep resolver.Endpoint) *endpoint {
	e := &endpoint{}
	if len(ep.Addresses) > 0 {
		e.addr = ep.Addresses[0].Addr
	}
	for o := range e.lbl.picks {
		e.lbl.picks[o] = []string{b.target, e.addr, outcomeNames[o]}
	}
	e.lbl.success = []string{b.target, e.addr, "success", codes.OK.String()}
	e.lbl.latency = []string{b.target, e.addr}
	return e
}

func (b *lb) countPick(e *endpoint, o outcome) { picksMetric.Record(b.rec, 1, e.lbl.picks[o]...) }

func (b *lb) recordRPC(e *endpoint, err error) {
	if err == nil {
		rpcsMetric.Record(b.rec, 1, e.lbl.success...)
		return
	}
	result := "ignored"
	if trippable(err) {
		result = "failure"
	}
	rpcsMetric.Record(b.rec, 1, b.target, e.addr, result, status.Code(err).String())
}

func (b *lb) recordMembers(total, ready int) {
	membersMetric.Record(b.rec, int64(total), b.target, "total")
	membersMetric.Record(b.rec, int64(ready), b.target, "ready")
}

// recordScan publishes the gauges a rescan refreshes.
func (b *lb) recordScan(view []*endpoint, c *Config, ref int64) {
	refused := 0
	for _, e := range view {
		if e.refusedBy(c, ref) {
			refused++
		}
		latencyMetric.Record(b.rec, e.avg.Load()/1e3, e.lbl.latency...)
	}
	membersMetric.Record(b.rec, int64(refused), b.target, "refused")
	clusterMinMetric.Record(b.rec, ref/1e3, b.target)
}

func (b *lb) lg() *zap.Logger { return b.shared.Load().lg }

// maxQueuedLogs bounds the transition lines waiting on a stuck log sink.
const maxQueuedLogs = 100

// emit queues a transition log line. Pick must not block, and a log sink can,
// so a goroutine writes the queue in order and exits once it's empty; a line
// past maxQueuedLogs is dropped.
func (b *lb) emit(line func()) {
	b.logMu.Lock()
	defer b.logMu.Unlock()
	if len(b.logQ) >= maxQueuedLogs {
		return
	}
	b.logQ = append(b.logQ, line)
	if !b.logging {
		b.logging = true
		go b.writeLogs()
	}
}

func (b *lb) writeLogs() {
	for {
		b.logMu.Lock()
		lines := b.logQ
		b.logQ, b.logging = nil, len(lines) > 0
		b.logMu.Unlock()
		if len(lines) == 0 {
			return
		}
		for _, line := range lines {
			line()
		}
	}
}

// noteVerdict logs a member's verdict when it changes.
func (b *lb) noteVerdict(e *endpoint, refused bool, c *Config, ref int64) {
	if e.refused.Load() == refused || !e.refused.CompareAndSwap(!refused, refused) {
		return
	}
	msg := "latency-aware: member readmitted"
	if refused {
		msg = "latency-aware: member refused"
	}
	lg, fields := b.lg(), []zap.Field{
		zap.String("endpoint", e.addr),
		zap.String("mode", c.Mode),
		zap.Duration("avg", time.Duration(e.avg.Load())),
		zap.Duration("cluster-min", time.Duration(ref)),
		zap.Duration("line", time.Duration(c.ExclusionThreshold*float64(ref))),
	}
	b.emit(func() { lg.Info(msg, fields...) })
}

// noteFallback logs the quorum fallback starting and ending. Its cause is
// "unhealthy" when refused members alone exceed the cap, else "missing".
func (b *lb) noteFallback(on bool, c *Config, refused, missing, ready, total int) {
	if b.fallback.Load() == on || !b.fallback.CompareAndSwap(!on, on) {
		return
	}
	lg, mode := b.lg(), zap.String("mode", c.Mode)
	if !on {
		b.emit(func() { lg.Info("latency-aware: quorum fallback ended", mode) })
		return
	}
	cause, level := "missing", zapcore.WarnLevel
	if refused > c.MaxUnhealthyEndpoints {
		cause = "unhealthy"
	}
	if c.Mode == ModeShadow {
		level = zapcore.InfoLevel // nothing was being enforced, so routing is unchanged
	}
	fields := []zap.Field{
		mode,
		zap.String("cause", cause),
		zap.Int("refused", refused),
		zap.Int("missing", missing),
		zap.Int("ready", ready),
		zap.Int("total", total),
		zap.Int("max-unhealthy", c.MaxUnhealthyEndpoints),
	}
	b.emit(func() {
		lg.Log(level, "latency-aware: too many members unhealthy, routing plain round robin", fields...)
	})
}
