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

// Package latencyaware is a gRPC load-balancing policy for etcd clients. It
// routes around members much slower than the fastest one, as this client sees
// them, and never takes more than a configured number out of rotation.
package latencyaware

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/attributes"
)

// Name is the policy's registered name.
const Name = "etcd_latency_aware"

// ServiceConfig selects the policy. Its settings travel in the resolver
// attributes (see Shared), so the JSON never changes.
const ServiceConfig = `{"loadBalancingConfig":[{"` + Name + `":{}}]}`

// Modes.
const (
	ModeEnabled = "enabled" // route around refused members
	ModeShadow  = "shadow"  // decide, learn and emit metrics, but route plain round robin
	ModeOff     = "off"     // plain round robin, no breaker work
)

// Config tunes the policy. Zero fields take the defaults.
type Config struct {
	// Mode is ModeEnabled (default), ModeShadow or ModeOff.
	Mode string `json:"mode,omitempty"`
	// ExclusionThreshold refuses a member averaging above this many times the
	// reference latency. Must be above 1; default 2.
	ExclusionThreshold float64 `json:"exclusion-threshold,omitempty"`
	// MinLatencyFloor is the least reference latency, so sub-millisecond noise
	// between healthy members is not a gap. Default 10ms.
	MinLatencyFloor time.Duration `json:"min-latency-floor,omitempty"`
	// EMAScale is the time constant of a member's latency average: larger is
	// steadier but slower to recover. Default 1s.
	EMAScale time.Duration `json:"ema-scale,omitempty"`
	// RetryPeriod spaces a refused member's probes and is the failure-strike
	// window. Default 30s.
	RetryPeriod time.Duration `json:"retry-period,omitempty"`
	// WarmupSamples is how many samples a member needs before it is judged.
	// Default 50.
	WarmupSamples int `json:"warmup-samples,omitempty"`
	// MaxUnhealthyEndpoints caps refused plus not-ready members; past it the
	// policy routes plain round robin. 1 (default) or 2.
	MaxUnhealthyEndpoints int `json:"max-unhealthy-endpoints,omitempty"`
	// MinSyntheticLatency and SyntheticMultiplier size the sample a counted
	// failure records: max(MinSyntheticLatency, SyntheticMultiplier × cluster
	// minimum). Defaults 5s and 20.
	MinSyntheticLatency time.Duration `json:"min-synthetic-latency,omitempty"`
	SyntheticMultiplier float64       `json:"synthetic-multiplier,omitempty"`
	// RescanInterval spaces recomputations of the cluster minimum. Default 2s.
	RescanInterval time.Duration `json:"rescan-interval,omitempty"`
}

// withDefaults fills zero fields and rejects bad ones.
func (c Config) withDefaults() (Config, error) {
	orDefault(&c.Mode, ModeEnabled)
	orDefault(&c.ExclusionThreshold, 2)
	orDefault(&c.MinLatencyFloor, 10*time.Millisecond)
	orDefault(&c.EMAScale, time.Second)
	orDefault(&c.RetryPeriod, 30*time.Second)
	orDefault(&c.WarmupSamples, 50)
	orDefault(&c.MaxUnhealthyEndpoints, 1)
	orDefault(&c.MinSyntheticLatency, 5*time.Second)
	orDefault(&c.SyntheticMultiplier, 20)
	orDefault(&c.RescanInterval, 2*time.Second)
	switch {
	case c.Mode != ModeEnabled && c.Mode != ModeShadow && c.Mode != ModeOff:
		return c, fmt.Errorf("latency-aware: unknown mode %q", c.Mode)
	case !(c.ExclusionThreshold > 1) || math.IsInf(c.ExclusionThreshold, 1):
		return c, fmt.Errorf("latency-aware: exclusion threshold must be above 1, got %v", c.ExclusionThreshold)
	case !(c.SyntheticMultiplier >= 1) || math.IsInf(c.SyntheticMultiplier, 1):
		return c, fmt.Errorf("latency-aware: synthetic multiplier must be at least 1, got %v", c.SyntheticMultiplier)
	case c.MaxUnhealthyEndpoints < 1 || c.MaxUnhealthyEndpoints > 2:
		return c, fmt.Errorf("latency-aware: max unhealthy endpoints must be 1 or 2, got %d", c.MaxUnhealthyEndpoints)
	case c.MinLatencyFloor < 0 || c.EMAScale < 0 || c.RetryPeriod < 0 || c.MinSyntheticLatency < 0 || c.RescanInterval < 0 || c.WarmupSamples < 0:
		return c, errors.New("latency-aware: durations and sample counts must not be negative")
	}
	return c, nil
}

func orDefault[T comparable](v *T, def T) {
	var zero T
	if *v == zero {
		*v = def
	}
}

// Shared is one client's live config and logger. The resolver attributes carry
// a fixed pointer to it; Set swaps the config behind it, and decisions use the
// new one from the next pick while learned state carries over.
type Shared struct {
	cfg atomic.Pointer[Config]
	lg  *zap.Logger
}

// NewShared validates cfg. A nil logger logs nothing.
func NewShared(cfg Config, lg *zap.Logger) (*Shared, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	s := &Shared{lg: lg}
	if err := s.Set(cfg); err != nil {
		return nil, err
	}
	return s, nil
}

// Set validates cfg and makes it live. A bad config changes nothing.
func (s *Shared) Set(cfg Config) error {
	c, err := cfg.withDefaults()
	if err != nil {
		return err
	}
	if old := s.cfg.Swap(&c); old != nil && *old != c {
		s.lg.Info("latency-aware balancing config changed", zap.Any("old", *old), zap.Any("new", c))
	}
	return nil
}

func (s *Shared) config() *Config { return s.cfg.Load() }

type attrKey struct{}

// Attributes carries s to the balancer in the resolver state.
func (s *Shared) Attributes() *attributes.Attributes { return attributes.New(attrKey{}, s) }

func sharedFrom(a *attributes.Attributes) *Shared {
	s, _ := a.Value(attrKey{}).(*Shared)
	return s
}
