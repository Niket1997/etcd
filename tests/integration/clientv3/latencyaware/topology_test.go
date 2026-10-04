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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Members that stop, connections that break or go silent, endpoint sets that
// change, a leader that stops: the connection handling around the policy.

// share drives rounds until member m carries a quarter of the calls, giving up after d.
func share(t *testing.T, cli *client, c *cluster, m int, d time.Duration) bool {
	t.Helper()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); {
		if served := during(t, cli, c, 200*time.Millisecond); served[m] >= sum(served)/4 {
			return true
		}
	}
	return false
}

func TestAStoppedMemberIsReadmittedWhenBack(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)

	// Member 0 is down for a second. gRPC cycles its connection through
	// CONNECTING and TRANSIENT_FAILURE, and the others carry every call.
	c.Members[0].Stop(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rs := run(ctx, loadConfig{rate: 200, maxInFlight: 8, timeout: time.Second, req: get(cli)})
	require.NoError(t, c.Members[0].Restart(t))
	assert.Zero(t, rs.failed())
	assert.Len(t, rs.byMember(), 2)
	assert.Zerof(t, cli.quorum(), "a missing member alone never starts the fallback")

	// Back, it reconnects after gRPC's backoff. Its outage entered its average
	// as one synthetic sample, which a few probes clear.
	require.Truef(t, share(t, cli, c, 0, 15*time.Second), "member 0 was never readmitted")
}

func TestAConnectionDropMidCallIsRetriedAndIgnored(t *testing.T) {
	c := start(t, 3, true)
	cli := enabled(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A call is held on member 0 when its connections drop; it accepts new
	// ones at once. The held call ends Unavailable, which is counted, ignored
	// and retried by the client; nobody is refused.
	c.Hang(0)
	before := c.Ranges()[0]
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := get(cli)(ctx)
			assert.NoError(t, err)
		}()
	}
	require.Eventuallyf(t, func() bool { return c.Ranges()[0] > before }, 5*time.Second, 5*time.Millisecond, "no call reached member 0")
	c.Members[0].Bridge().DropConnections()
	c.Heal(0)
	wg.Wait()

	assert.Positive(t, cli.rpcs("etcd.endpoint", c.addr(0), "etcd.result", "ignored", "grpc.status", "Unavailable"))
	assert.Zero(t, cli.refused(""))
	served := during(t, cli, c, 300*time.Millisecond)
	assert.GreaterOrEqualf(t, served[0], sum(served)/4, "member 0 is back in rotation: %v", served)
}

func TestASilentPartitionIsRefusedOnItsDeadlines(t *testing.T) {
	c := start(t, 3, true)
	cli := enabled(t, c)

	// Member 0's packets vanish and TCP stays up: no transport failure, only
	// calls that never return. Their deadlines are strikes, the third refuses
	// the member, and after that only a probe per retry period is lost. Round
	// robin would lose a third of the calls.
	c.Members[0].Bridge().Blackhole()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rs := run(ctx, loadConfig{rate: 200, maxInFlight: 16, timeout: 500 * time.Millisecond, req: get(cli)})
	c.Members[0].Bridge().Unblackhole()

	t.Logf("failed %d of %d", rs.failed(), len(rs))
	assert.Positive(t, cli.refused(c.addr(0)))
	assert.Positivef(t, rs.failed(), "the calls in flight when it went silent")
	assert.Lessf(t, rs.failed(), len(rs)/10, "%d of %d failed", rs.failed(), len(rs))
}

func TestARemovedMemberIsForgottenAndAnAddedOneWarmsUp(t *testing.T) {
	c := start(t, 4, false)
	eps := c.Endpoints()
	cli := enabled(t, c, eps[:3]...)
	c.Slow(0, slow, 0)
	refuse(t, cli, c.addr(0))

	// Member 0 leaves the endpoint set, and its history with it. Put back, it
	// is new: its warm-up calls reach it, slow or not, and then it is refused again.
	cli.SetEndpoints(eps[1], eps[2])
	require.Eventually(t, func() bool { return cli.members("total") == 2 }, 5*time.Second, 10*time.Millisecond)
	refused, before := cli.refused(c.addr(0)), c.Ranges()[0]
	cli.SetEndpoints(eps[:3]...)
	require.Truef(t, rounds(t, cli, 30, 20, func() bool { return cli.refused(c.addr(0)) > refused }), "member 0 was never judged again")
	assert.Equalf(t, 10, c.Ranges()[0]-before, "warm-up calls that reached the re-added member")

	// A brand-new member goes through the same warm-up.
	c.Slow(3, slow, 0)
	cli.SetEndpoints(eps[1], eps[2], eps[3])
	require.Truef(t, rounds(t, cli, 30, 20, func() bool { return cli.refused(c.addr(3)) > 0 }), "member 3 was never judged")
	assert.Equalf(t, 10, c.Ranges()[3], "warm-up calls that reached the new member")
}

func TestALeaderStopIsAbsorbedByTheNewLeader(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)
	leader := c.WaitLeader(t)
	leaderID := uint64(c.Members[leader].ID())

	// The leader stops. Writes stall only for the election; the client retries
	// across the gap, so most land, and a surviving member takes over.
	c.Members[leader].Stop(t)
	put := func(ctx context.Context) (uint64, error) {
		resp, err := cli.Put(ctx, "k", "v")
		if err != nil {
			return 0, err
		}
		return resp.Header.MemberId, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rs := run(ctx, loadConfig{rate: 100, maxInFlight: 4, timeout: 2 * time.Second, req: put})
	assert.Greaterf(t, len(rs), 100, "traffic flowed")
	assert.Greaterf(t, len(rs)-rs.failed(), len(rs)/2, "most writes landed across the election")
	survivors := rs.byMember()
	delete(survivors, leaderID)
	assert.NotEmptyf(t, survivors, "a surviving member took the writes")
	require.Eventuallyf(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := cli.Put(ctx, "k", "v")
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "no new leader accepted writes")
}

func TestNoReadyMemberFailsTheCallAndTheFirstBackCarriesIt(t *testing.T) {
	c := start(t, 3, false)
	cli := enabled(t, c)
	for m := range 3 {
		c.Members[m].Stop(t)
	}
	require.Eventuallyf(t, func() bool { return cli.members("ready") == 0 }, 5*time.Second, 10*time.Millisecond, "some member still reads READY")

	// Nothing is READY, so a call waits for a connection until its deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err := get(cli)(ctx)
	cancel()
	require.Error(t, err)

	// The first member back takes every call; the others follow when their
	// backoff lets them reconnect.
	require.NoError(t, c.Members[1].Restart(t))
	require.Eventuallyf(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := get(cli)(ctx)
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "member 1 never came back")
	served := during(t, cli, c, 200*time.Millisecond)
	assert.Equalf(t, sum(served), served[1], "only member 1 is READY: %v", served)
	require.NoError(t, c.Members[0].Restart(t))
	require.NoError(t, c.Members[2].Restart(t))
	require.Truef(t, share(t, cli, c, 0, 15*time.Second), "member 0 never came back")
	require.Truef(t, share(t, cli, c, 2, 15*time.Second), "member 2 never came back")
}
