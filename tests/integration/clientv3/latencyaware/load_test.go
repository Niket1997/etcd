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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// request is one call; it returns the ID of the member that answered.
type request func(ctx context.Context) (member uint64, err error)

// result is what one request saw.
type result struct {
	start   time.Time
	latency time.Duration
	member  uint64 // 0 when the request failed
	err     error
}

// results holds one entry per request, in start order.
type results []result

// loadConfig is an open-loop run: a request every 1/rate seconds whether or
// not the earlier ones came back, up to maxInFlight outstanding.
type loadConfig struct {
	rate        int
	maxInFlight int
	timeout     time.Duration // per request
	req         request
}

// get reads one key. Every etcd reply names the member that served it. The read
// is serializable, so it times the member alone: a linearizable read waits on
// the leader, and an election in the loaded test process would read as a slow member.
func get(cli *client) request {
	return func(ctx context.Context) (uint64, error) {
		resp, err := cli.Get(ctx, "k", clientv3.WithSerializable())
		if err != nil {
			return 0, err
		}
		return resp.Header.MemberId, nil
	}
}

// gets issues n Gets one after another and fails the test on the first error.
func gets(t *testing.T, cli *client, n int) {
	t.Helper()
	for _, r := range sequential(n, 2*time.Second, get(cli)) {
		require.NoError(t, r.err)
	}
}

// warm sends Gets one at a time until every member has answered once: the
// first picks happen while some connections are still being made.
func warm(t *testing.T, cli *client, members int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	seen := map[uint64]bool{}
	for len(seen) < members {
		left := time.Until(deadline)
		require.Positivef(t, left, "not every member came up")
		ctx, cancel := context.WithTimeout(context.Background(), min(2*time.Second, left))
		m, err := get(cli)(ctx)
		cancel()
		require.NoErrorf(t, err, "not every member came up")
		seen[m] = true
	}
}

// sequential sends n requests one after another, each with its own timeout.
func sequential(n int, timeout time.Duration, req request) results {
	out := make(results, 0, n)
	for range n {
		r := result{start: time.Now()}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		r.member, r.err = req(ctx)
		cancel()
		r.latency = time.Since(r.start)
		out = append(out, r)
	}
	return out
}

// run sends per cfg until ctx ends, then waits for the requests in flight. A
// beat the loop missed is not sent late: that would turn the box's
// scheduling into bursts at the policy under test.
func run(ctx context.Context, cfg loadConfig) results {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		inFlight int
		out      results
	)
	tick := time.NewTicker(time.Second / time.Duration(cfg.rate))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			slices.SortFunc(out, func(a, b result) int { return a.start.Compare(b.start) })
			return out
		case <-tick.C:
		}
		mu.Lock()
		if ctx.Err() != nil || inFlight >= cfg.maxInFlight {
			mu.Unlock()
			continue
		}
		inFlight++
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := result{start: time.Now()}
			defer func() {
				if p := recover(); p != nil {
					r.err = fmt.Errorf("request panicked: %v", p)
				}
				r.latency = time.Since(r.start)
				mu.Lock()
				out = append(out, r)
				inFlight--
				mu.Unlock()
			}()
			rctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
			defer cancel()
			r.member, r.err = cfg.req(rctx)
		}()
	}
}

// failed counts the requests that came back with an error.
func (rs results) failed() int {
	n := 0
	for _, r := range rs {
		if r.err != nil {
			n++
		}
	}
	return n
}

// byMember counts the successful requests each member answered.
func (rs results) byMember() map[uint64]int {
	out := map[uint64]int{}
	for _, r := range rs {
		if r.err == nil {
			out[r.member]++
		}
	}
	return out
}

// percentile is the p-th (0-100) latency of the successful requests, 0 when
// there were none.
func (rs results) percentile(p float64) time.Duration {
	var ok []time.Duration
	for _, r := range rs {
		if r.err == nil {
			ok = append(ok, r.latency)
		}
	}
	if len(ok) == 0 {
		return 0
	}
	slices.Sort(ok)
	return ok[max(int(float64(len(ok))*p/100+0.999999)-1, 0)]
}
