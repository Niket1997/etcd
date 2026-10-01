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

package clientv3

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestNewRejectsABadLatencyAwareConfig(t *testing.T) {
	_, err := NewClient(t, Config{Endpoints: []string{"127.0.0.1:0"}, LatencyAware: &LatencyAwareConfig{Mode: "fast"}})
	require.ErrorContains(t, err, `unknown mode "fast"`)
}

func TestSetLatencyAwareConfig(t *testing.T) {
	plain, err := NewClient(t, Config{Endpoints: []string{"127.0.0.1:0"}})
	require.NoError(t, err)
	defer plain.Close()
	require.ErrorIs(t, plain.SetLatencyAwareConfig(LatencyAwareConfig{}), ErrNoLatencyAware)

	c, err := NewClient(t, Config{Endpoints: []string{"127.0.0.1:0"}, LatencyAware: &LatencyAwareConfig{}})
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetLatencyAwareConfig(LatencyAwareConfig{Mode: LatencyAwareShadow}))
	require.ErrorContains(t, c.SetLatencyAwareConfig(LatencyAwareConfig{ExclusionThreshold: 1}), "exclusion threshold")
}

// slowKV answers Range after its delay.
type slowKV struct {
	etcdserverpb.UnimplementedKVServer
	delay time.Duration
	calls atomic.Int32
}

func (s *slowKV) Range(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.calls.Add(1)
	time.Sleep(s.delay)
	return &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{}}, nil
}

func TestLatencyAwareClientRoutesAroundASlowMember(t *testing.T) {
	kvs, eps := make([]*slowKV, 3), make([]string, 3)
	for i := range kvs {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		srv := grpc.NewServer()
		kvs[i], eps[i] = &slowKV{}, lis.Addr().String()
		etcdserverpb.RegisterKVServer(srv, kvs[i])
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
	}
	kvs[2].delay = 50 * time.Millisecond
	c, err := NewClient(t, Config{Endpoints: eps, LatencyAware: &LatencyAwareConfig{WarmupSamples: 5}})
	require.NoError(t, err)
	defer c.Close()
	// slowGets is how many of 30 Gets reach the slow member.
	slowGets := func() int32 {
		before := kvs[2].calls.Load()
		for range 30 {
			_, err := c.Get(context.Background(), "k")
			require.NoError(t, err)
		}
		return kvs[2].calls.Load() - before
	}

	for deadline := time.Now().Add(20 * time.Second); slowGets() != 0; {
		require.Truef(t, time.Now().Before(deadline), "the slow member is never refused")
	}
	require.NoError(t, c.SetLatencyAwareConfig(LatencyAwareConfig{Mode: LatencyAwareOff}))
	assert.EqualValuesf(t, 10, slowGets(), "off is plain round robin")
}
