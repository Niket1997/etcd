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

//go:build !cluster_proxy

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
)

// TestLatencyAwareFailoverOnDefrag is TestFailoverOnDefrag's case where plain
// round robin loses a third of the calls to the member blocked by defrag. The
// latency-aware policy keeps the failures within the 1% bound that test sets
// for health checking with StopGRPCServiceOnDefrag, and needs neither.
func TestLatencyAwareFailoverOnDefrag(t *testing.T) {
	e2e.BeforeTest(t)
	clus, err := e2e.NewEtcdProcessCluster(t.Context(), t,
		e2e.WithClusterSize(3),
		e2e.WithServerFeatureGate("StopGRPCServiceOnDefrag", false),
		e2e.WithServerFeatureGate("NonBlockingDefrag", false),
		e2e.WithGoFailEnabled(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { clus.Stop() })
	cli, _ := latencyAwareClient(t, clus.EndpointsGRPC())

	done := make(chan []timedGet)
	go func() { done <- getFor(t.Context(), cli, clientRuntime, requestTimeout) }()
	triggerDefrag(t, clus.Procs[0])
	gets := <-done

	failed := 0
	for _, g := range gets {
		if g.err != nil {
			failed++
		}
	}
	qps := float64(len(gets)) / clientRuntime.Seconds()
	t.Logf("%d of %d calls failed, %.0f calls a second", failed, len(gets), qps)
	require.GreaterOrEqual(t, qps, 20.0)
	require.LessOrEqual(t, float64(failed)/float64(len(gets)), 0.01)
}

// TestLatencyAwareFailoverOnSlowDisk stalls one follower's raft writes, as a
// failing disk does, so each linearizable read it serves waits 300-600 ms.
// The policy refuses the follower after its first slow read, and lets it
// back in once the disk recovers.
func TestLatencyAwareFailoverOnSlowDisk(t *testing.T) {
	e2e.BeforeTest(t)
	clus, err := e2e.NewEtcdProcessCluster(t.Context(), t, e2e.WithClusterSize(3), e2e.WithGoFailEnabled(true))
	require.NoError(t, err)
	t.Cleanup(func() { clus.Stop() })
	cli, logs := latencyAwareClient(t, clus.EndpointsGRPC())
	follower := clus.Procs[(clus.WaitLeader(t)+1)%3]

	require.NoError(t, follower.Failpoints().SetupHTTP(t.Context(), "raftBeforeSave", `sleep("300ms")`))
	// Time enough for a slow read twice over, so none fails.
	gets := getFor(t.Context(), cli, clientRuntime, 2*time.Second)
	slow := 0
	for _, g := range gets {
		require.NoError(t, g.err)
		if g.took > 100*time.Millisecond {
			slow++
		}
	}
	t.Logf("%d of %d calls took over 100 ms", slow, len(gets))
	// Round robin would send the follower a third of them.
	require.LessOrEqual(t, float64(slow)/float64(len(gets)), 0.01)
	require.Positive(t, logs.FilterMessage("latency-aware: member refused").Len())

	require.NoError(t, follower.Failpoints().DeactivateHTTP(t.Context(), "raftBeforeSave"))
	for deadline := time.Now().Add(30 * time.Second); logs.FilterMessage("latency-aware: member readmitted").Len() == 0; {
		require.Truef(t, time.Now().Before(deadline), "the follower was never let back in")
		getFor(t.Context(), cli, 100*time.Millisecond, time.Second)
	}
}

// latencyAwareClient connects to endpoints with the policy at its defaults,
// but for a 2 s retry period so probes come within a test, and takes every
// member past warm-up.
func latencyAwareClient(t *testing.T, endpoints []string) (*clientv3.Client, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zap.InfoLevel)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:    endpoints,
		DialTimeout:  dialTimeout,
		LatencyAware: &clientv3.LatencyAwareConfig{RetryPeriod: 2 * time.Second},
		Logger:       zap.New(core),
	})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })
	for range 300 {
		_, err := cli.Get(t.Context(), "k")
		require.NoError(t, err)
	}
	return cli, logs
}

type timedGet struct {
	took time.Duration
	err  error
}

// getFor sends Gets one after another, each with timeout, for d.
func getFor(ctx context.Context, cli *clientv3.Client, d, timeout time.Duration) []timedGet {
	var out []timedGet
	for end := time.Now().Add(d); time.Now().Before(end); {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		start := time.Now()
		_, err := cli.Get(cctx, "k")
		cancel()
		out = append(out, timedGet{time.Since(start), err})
	}
	return out
}
