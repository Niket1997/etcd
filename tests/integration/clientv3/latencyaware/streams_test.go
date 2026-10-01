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
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	lockpb "go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
)

// Streams and other untimed calls: the picker counts them but they never
// teach the policy, while the pick that opens one still honours it.

// openWatch opens a watch on a gRPC stream of its own: clientv3 shares one
// stream per distinct outgoing metadata.
func openWatch(cli *client, id int) (clientv3.WatchChan, context.CancelFunc) {
	ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(context.Background(), "stream", strconv.Itoa(id)))
	return cli.Watch(ctx, "k"), cancel
}

// opened waits until n streams of method have opened since before and
// returns how many each member got.
func opened(t *testing.T, c *cluster, method string, before []int, n int) []int {
	t.Helper()
	var got []int
	require.Eventuallyf(t, func() bool {
		got = delta(before, c.Calls(method))
		return sum(got) >= n
	}, 10*time.Second, 5*time.Millisecond, "%d of %d streams opened", sum(got), n)
	return got
}

// onlyOne is the member with the single stream.
func onlyOne(t *testing.T, counts []int) int {
	t.Helper()
	for m, n := range counts {
		if n > 0 {
			require.Equalf(t, []int{n}, []int{sum(counts)}, "more than one member has the stream: %v", counts)
			return m
		}
	}
	require.FailNowf(t, "no member has the stream", "%v", counts)
	return -1
}

func TestWatchChurnIsCountedAsIgnoredAndTeachesNothing(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)
	quiet := sequential(100, time.Second, get(cli)).percentile(99)

	// Streams open and close under the unary traffic. Each close ends its
	// stream Canceled, which is counted and nothing more.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan int, 1)
	go func() {
		n := 0
		for ; ctx.Err() == nil; n++ {
			_, stop := openWatch(cli, n)
			time.Sleep(10 * time.Millisecond)
			stop()
			time.Sleep(10 * time.Millisecond)
		}
		done <- n
	}()
	rs := run(ctx, loadConfig{rate: 200, maxInFlight: 8, timeout: time.Second, req: get(cli)})
	n := <-done

	assert.Greater(t, n, 20)
	assert.Zero(t, rs.failed())
	assert.Eventuallyf(t, func() bool {
		return cli.rpcs("etcd.result", "ignored", "grpc.status", "Canceled") >= int64(n)
	}, 5*time.Second, 10*time.Millisecond, "every closed stream is counted")
	assert.Zero(t, cli.refused(""))
	assert.Lessf(t, rs.percentile(99), quiet+50*time.Millisecond, "unary p99 is untouched by the churn")
}

func TestAStreamThatEndsAtItsDeadlineIsAFailureThatNeverRefuses(t *testing.T) {
	c := start(t, 3, false)
	cli := c.client(t, steady())
	settle(t, cli, 3)
	for m := range 3 {
		c.HoldStreams(m)
	}

	// Fifteen snapshot streams, round robin over the members, each ending
	// DeadlineExceeded after 50 ms: a trippable code, but on a stream, so it is
	// counted and never fed back. The unary calls between them stay even.
	for range 15 {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		rc, err := cli.Snapshot(ctx)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, rc)
		require.Error(t, err)
		require.NoError(t, rc.Close())
		cancel()
		gets(t, cli, 3)
	}
	assert.Eventuallyf(t, func() bool {
		return cli.rpcs("etcd.result", "failure", "grpc.status", "DeadlineExceeded") >= 15
	}, 2*time.Second, 10*time.Millisecond, "every timed-out stream is counted as a failure")
	for m := range 3 {
		assert.Positivef(t, cli.rpcs("etcd.endpoint", c.addr(m), "etcd.result", "failure"), "member %d served a timed-out stream", m)
	}
	assert.Zero(t, cli.refused(""))
}

func TestANewStreamHonoursThePolicyOnlyWhenEnforcing(t *testing.T) {
	for _, tc := range []struct {
		mode        string
		reachesSlow bool
	}{{clientv3.LatencyAwareEnabled, false}, {clientv3.LatencyAwareShadow, true}} {
		t.Run(tc.mode, func(t *testing.T) {
			c := start(t, 3, false)
			cli := c.client(t, with(tuning(), func(cfg *clientv3.LatencyAwareConfig) { cfg.Mode = tc.mode }))
			settle(t, cli, 3)
			c.Slow(0, slow, 0)
			refuse(t, cli, c.addr(0)) // shadow judges too, it just does not act

			// Thirty watches, a gRPC stream each. Enabled skips the refused member
			// for streams as for unary calls; shadow round-robins.
			before := c.Calls(pb.Watch_Watch_FullMethodName)
			for i := range 30 {
				_, cancel := openWatch(cli, i)
				defer cancel()
			}
			got := opened(t, c, pb.Watch_Watch_FullMethodName, before, 30)
			if tc.reachesSlow {
				assert.GreaterOrEqualf(t, got[0], 8, "shadow opened %v", got)
			} else {
				assert.Equalf(t, []int{0, 30}, []int{got[0], got[1] + got[2]}, "enabled opened %v", got)
			}
		})
	}
}

func TestAUnaryFaultStillRefusesAMemberWithLiveStreams(t *testing.T) {
	c := start(t, 3, false)
	cli := c.client(t, steady())
	settle(t, cli, 3)

	// One watch per member, then member 0 turns slow for unary calls: its open
	// stream does not shield it, and keeps delivering while it is refused.
	before := c.Calls(pb.Watch_Watch_FullMethodName)
	var chans []clientv3.WatchChan
	for i := range 3 {
		ch, cancel := openWatch(cli, i)
		defer cancel()
		chans = append(chans, ch)
	}
	require.Equal(t, []int{1, 1, 1}, opened(t, c, pb.Watch_Watch_FullMethodName, before, 3))
	c.Slow(0, 5*slow, 0)
	refuse(t, cli, c.addr(0))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.Put(ctx, "k", "v")
	require.NoError(t, err)
	for i, ch := range chans {
		select {
		case resp := <-ch:
			require.NoErrorf(t, resp.Err(), "watch %d", i)
			assert.Lenf(t, resp.Events, 1, "watch %d", i)
		case <-ctx.Done():
			t.Fatalf("watch %d saw no event", i)
		}
	}
}

func TestASlowSnapshotTransferLeavesNoTraceOnThePolicy(t *testing.T) {
	c := start(t, 3, false)
	// Steady: the bulk Puts forwarded by followers cross a 10 ms line on a busy
	// box; a second-long stream sample would still cross 100 ms.
	cli := c.client(t, steady())
	settle(t, cli, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range 60 { // twenty 32 KB snapshot chunks
		_, err := cli.Put(ctx, fmt.Sprintf("bulk/%d", i), strings.Repeat("x", 10<<10))
		require.NoError(t, err)
	}

	// The snapshot that lands on member 0 is paced to 100 ms a chunk and runs
	// for over a second. Had the picker timed it, member 0 would be refused.
	c.Slow(0, slow, 0)
	var rc io.ReadCloser
	for i := 0; i < 3 && rc == nil; i++ {
		before := c.Calls(pb.Maintenance_Snapshot_FullMethodName)
		r, err := cli.Snapshot(ctx)
		require.NoError(t, err)
		if opened(t, c, pb.Maintenance_Snapshot_FullMethodName, before, 1)[0] == 1 {
			rc = r
			continue
		}
		_, err = io.Copy(io.Discard, r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
	}
	require.NotNilf(t, rc, "no snapshot landed on member 0 in three picks")
	began := time.Now()
	n, err := io.Copy(io.Discard, rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	took := time.Since(began)
	c.Heal(0)
	assert.Greaterf(t, took, time.Second, "%d bytes paced at 100 ms a chunk", n)

	served := during(t, cli, c, 300*time.Millisecond)
	assert.Zero(t, cli.refused(""))
	assert.GreaterOrEqualf(t, served[0], sum(served)/4, "member 0 keeps its share: %v", served)
}

func TestALongLivedWatchResumesElsewhereWhenItsMemberDrops(t *testing.T) {
	c := start(t, 3, true)
	cli := enabled(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ch, stop := openWatch(cli, 0)
	defer stop()
	served := onlyOne(t, opened(t, c, pb.Watch_Watch_FullMethodName, make([]int, 3), 1))
	put := func(v string) {
		_, err := cli.Put(ctx, "k", v)
		require.NoError(t, err)
	}
	expect := func(v string) {
		select {
		case resp := <-ch:
			require.NoError(t, resp.Err())
			require.Len(t, resp.Events, 1)
			assert.Equal(t, v, string(resp.Events[0].Kv.Value))
		case <-time.After(10 * time.Second):
			t.Fatalf("no event for %q", v)
		}
	}
	put("1")
	expect("1")

	// The client loses the member behind the watch. clientv3 reopens the
	// watch through the policy on a live member, from the revision it had,
	// and the same channel keeps delivering. Nothing was slow, so nobody is refused.
	c.Disconnect(served)
	put("2")
	expect("2")
	after := c.Calls(pb.Watch_Watch_FullMethodName)
	assert.Equalf(t, 1, after[served], "the dropped member saw no reopen")
	assert.Equalf(t, 2, sum(after), "the watch was reopened once, elsewhere")
	assert.Zero(t, cli.refused(""))
	assert.Positivef(t, cli.rpcs("etcd.result", "ignored"), "the broken stream was counted and nothing more")
}

func TestALeaseKeepAliveSurvivesItsMemberDropping(t *testing.T) {
	c := start(t, 3, true)
	cli := enabled(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease, err := cli.Grant(ctx, 3)
	require.NoError(t, err)
	keepalives, err := cli.KeepAlive(metadata.AppendToOutgoingContext(ctx, "stream", "lease"), lease.ID)
	require.NoError(t, err)
	select {
	case ka := <-keepalives:
		require.NotNil(t, ka)
	case <-time.After(10 * time.Second):
		t.Fatal("no keepalive arrived")
	}
	served := onlyOne(t, c.Calls(pb.Lease_LeaseKeepAlive_FullMethodName))

	// The client loses the member behind the keepalive stream. clientv3
	// reopens the stream on a live member well inside the 3 s TTL, so the
	// lease is still alive after it would have expired.
	c.Disconnect(served)
	time.Sleep(4 * time.Second)
	ttl, err := cli.TimeToLive(ctx, lease.ID)
	require.NoError(t, err)
	assert.Positivef(t, ttl.TTL, "the lease expired")
	after := c.Calls(pb.Lease_LeaseKeepAlive_FullMethodName)
	assert.Equalf(t, 1, after[served], "the dropped member saw no reopen")
	assert.GreaterOrEqualf(t, sum(after), 2, "the stream was reopened elsewhere")
	assert.Zero(t, cli.refused(""))
	assert.Positivef(t, cli.rpcs("etcd.result", "ignored"), "the broken stream was counted and nothing more")
}

func TestALockHeldPastTheRetryPeriodIsWaitingNotSlowness(t *testing.T) {
	// A Lock call blocks on its member until the lock is free: here three
	// seconds, past the 2 s retry period. It is counted but never timed, so the
	// member that held the waiter keeps its share. Timing every call, as a
	// stream interceptor does, would refuse it.
	c := start(t, 3, false)
	holder := c.client(t, nil)
	cli := c.client(t, steady())
	settle(t, cli, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	locks := lockpb.NewLockClient(holder.ActiveConnection())
	held, err := locks.Lock(ctx, &lockpb.LockRequest{Name: []byte("l")})
	require.NoError(t, err)

	waited := make(chan error, 1)
	go func() {
		_, lockErr := lockpb.NewLockClient(cli.ActiveConnection()).Lock(ctx, &lockpb.LockRequest{Name: []byte("l")})
		waited <- lockErr
	}()
	served := during(t, cli, c, 3*time.Second)
	_, err = locks.Unlock(ctx, &lockpb.UnlockRequest{Key: held.Key})
	require.NoError(t, err)
	require.NoError(t, <-waited)

	gets(t, cli, 30)
	assert.Zero(t, cli.refused(""))
	even(t, served)
}
