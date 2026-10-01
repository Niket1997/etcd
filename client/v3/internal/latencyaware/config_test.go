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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestZeroConfigTakesTheDefaults(t *testing.T) {
	c, err := Config{}.withDefaults()
	require.NoError(t, err)
	assert.Equal(t, Config{
		Mode:                  ModeEnabled,
		ExclusionThreshold:    2,
		MinLatencyFloor:       10 * time.Millisecond,
		EMAScale:              100 * time.Millisecond,
		RetryPeriod:           30 * time.Second,
		WarmupSamples:         50,
		MaxUnhealthyEndpoints: 1,
		MinSyntheticLatency:   5 * time.Second,
		SyntheticMultiplier:   20,
		RescanInterval:        2 * time.Second,
	}, c)
}

func TestExplicitConfigIsKept(t *testing.T) {
	in := Config{
		Mode:                  ModeShadow,
		ExclusionThreshold:    1.5,
		MinLatencyFloor:       time.Millisecond,
		EMAScale:              10 * time.Second,
		RetryPeriod:           time.Minute,
		WarmupSamples:         7,
		MaxUnhealthyEndpoints: 2,
		MinSyntheticLatency:   time.Second,
		SyntheticMultiplier:   3,
		RescanInterval:        time.Second,
	}
	c, err := in.withDefaults()
	require.NoError(t, err)
	assert.Equal(t, in, c)
}

func TestBadConfigIsAnError(t *testing.T) {
	for name, c := range map[string]Config{
		"unknown mode":           {Mode: "enforce"},
		"threshold of 1":         {ExclusionThreshold: 1},
		"threshold below 1":      {ExclusionThreshold: 0.5},
		"NaN threshold":          {ExclusionThreshold: math.NaN()},
		"infinite threshold":     {ExclusionThreshold: math.Inf(1)},
		"multiplier below 1":     {SyntheticMultiplier: 0.5},
		"NaN multiplier":         {SyntheticMultiplier: math.NaN()},
		"infinite multiplier":    {SyntheticMultiplier: math.Inf(1)},
		"three unhealthy":        {MaxUnhealthyEndpoints: 3},
		"negative unhealthy":     {MaxUnhealthyEndpoints: -1},
		"negative floor":         {MinLatencyFloor: -1},
		"negative EMA scale":     {EMAScale: -1},
		"negative retry":         {RetryPeriod: -1},
		"negative synthetic":     {MinSyntheticLatency: -1},
		"negative rescan":        {RescanInterval: -1},
		"negative warmup counts": {WarmupSamples: -1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := c.withDefaults()
			require.Error(t, err)
		})
	}
}

func TestSetSwapsTheLiveConfigAndLogsOnlyChanges(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	s, err := NewShared(Config{}, zap.New(core))
	require.NoError(t, err)
	assert.Equal(t, ModeEnabled, s.config().Mode)
	assert.Zerof(t, logs.Len(), "the first config is not a change")

	require.NoError(t, s.Set(Config{Mode: ModeShadow}))
	assert.Equal(t, ModeShadow, s.config().Mode)
	assert.Equal(t, 1, logs.FilterMessage("latency-aware balancing config changed").Len())

	require.NoError(t, s.Set(Config{Mode: ModeShadow}))
	assert.Equalf(t, 1, logs.Len(), "the same config again is not a change")

	require.Error(t, s.Set(Config{Mode: "bogus"}))
	assert.Equalf(t, ModeShadow, s.config().Mode, "a bad config changes nothing")
}

func TestNewSharedRejectsABadConfig(t *testing.T) {
	_, err := NewShared(Config{ExclusionThreshold: 1}, nil)
	require.Error(t, err)
}

func TestSharedTravelsInTheResolverAttributes(t *testing.T) {
	s, err := NewShared(Config{}, nil)
	require.NoError(t, err)
	assert.Same(t, s, sharedFrom(s.Attributes()))
	assert.Nil(t, sharedFrom(nil))
}
