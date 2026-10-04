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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

func TestALatencyAwareClientServesARealCluster(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)
	cli, err := integration.NewClient(t, clientv3.Config{Endpoints: clus.Endpoints(), LatencyAware: &clientv3.LatencyAwareConfig{}})
	require.NoError(t, err)
	defer cli.Close()
	ctx := t.Context()

	w := cli.Watch(ctx, "k")
	for _, mode := range []string{clientv3.LatencyAwareEnabled, clientv3.LatencyAwareShadow, clientv3.LatencyAwareOff, clientv3.LatencyAwareEnabled} {
		require.NoError(t, cli.SetLatencyAwareConfig(clientv3.LatencyAwareConfig{Mode: mode}))
		lease, err := cli.Grant(ctx, 60)
		require.NoError(t, err)
		_, err = cli.Put(ctx, "k", mode, clientv3.WithLease(lease.ID))
		require.NoError(t, err)
		resp, err := cli.Get(ctx, "k")
		require.NoError(t, err)
		require.Equal(t, mode, string(resp.Kvs[0].Value))
		select {
		case ev := <-w:
			require.Equal(t, mode, string(ev.Events[0].Kv.Value))
		case <-time.After(10 * time.Second):
			t.Fatalf("no watch event in mode %s", mode)
		}
	}

	clus.Members[2].Stop(t)
	for range 30 {
		_, err := cli.Get(ctx, "k")
		require.NoError(t, err)
	}
}
