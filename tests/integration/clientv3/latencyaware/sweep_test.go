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
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestSweepDefaults measures candidate defaults on the faults that pull them
// apart, in real time: 200 ms and 1 s stalls on a healthy member (a jumpy
// average refuses it for a retry period each time), the reference fault and its
// heal (detection, loss, recovery), and a uniformly slower member
// (sensitivity). It reports and asserts nothing; ETCD_LA_SWEEP=1 runs it, in
// about forty minutes.
func TestSweepDefaults(t *testing.T) {
	if os.Getenv("ETCD_LA_SWEEP") == "" {
		t.Skip("ETCD_LA_SWEEP=1 runs the defaults sweep")
	}
	c := start(t, 5, false)
	leader := c.WaitLeader(t)
	a, b := (leader+1)%5, (leader+2)%5
	for _, tc := range []struct {
		name string
		cfg  clientv3.LatencyAwareConfig
	}{
		{"defaults (EMA 1s)", clientv3.LatencyAwareConfig{}},
		{"EMA 100ms", clientv3.LatencyAwareConfig{EMAScale: 100 * time.Millisecond}},
		{"EMA 10s", clientv3.LatencyAwareConfig{EMAScale: 10 * time.Second}},
		{"EMA 10s, retry 10s", clientv3.LatencyAwareConfig{EMAScale: 10 * time.Second, RetryPeriod: 10 * time.Second}},
		{"reference (3 x 50ms, EMA 10s)", clientv3.LatencyAwareConfig{ExclusionThreshold: 3, MinLatencyFloor: 50 * time.Millisecond, EMAScale: 10 * time.Second}},
	} {
		cli := c.client(t, &tc.cfg)
		warm(t, cli, 5)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan results, 1)
		go func() {
			done <- run(ctx, loadConfig{rate: 1000, maxInFlight: 1000, timeout: time.Second, req: get(cli)})
		}()
		time.Sleep(10 * time.Second)
		// admitted waits, up to six retry periods, until member m is let back in.
		admitted := func(m int) {
			for from := time.Now(); time.Since(from) < 6*30*time.Second && !refusedAt(cli, c, m, time.Now()).IsZero(); {
				time.Sleep(time.Second)
			}
		}

		// Six stalls of 300 ms on member a, ten seconds apart: 200 ms ones, then
		// 1 s ones. Each phase ends once a is back.
		stalls := func(d time.Duration) {
			for range 6 {
				c.Slow(a, 300*time.Millisecond, 0)
				time.Sleep(d)
				c.Heal(a)
				time.Sleep(10*time.Second - d)
			}
			admitted(a)
		}
		short := time.Now()
		stalls(200 * time.Millisecond)
		long := time.Now()
		stalls(time.Second)

		// The reference fault on member b for a minute, then its heal.
		fault := time.Now()
		c.SlowSome(b, 1050*time.Millisecond, 50*time.Millisecond, 0.6)
		time.Sleep(time.Minute)
		heal := time.Now()
		c.Heal(b)
		admitted(b)

		// Every call on member b 50 ms slower, for a minute.
		uniform := time.Now()
		c.Slow(b, 50*time.Millisecond, 0)
		time.Sleep(time.Minute)
		c.Heal(b)
		end := time.Now()
		cancel()
		rs := <-done
		cli.Close()

		shortOut, shortEpisodes := outOfRotation(cli, c, a, short, long)
		longOut, longEpisodes := outOfRotation(cli, c, a, long, fault)
		faultOut, faultEpisodes := outOfRotation(cli, c, b, fault, heal)
		_, uniformEpisodes := outOfRotation(cli, c, b, uniform, end)
		t.Logf("%-28s 200ms stalls: refused %d times, out %v | 1s stalls: refused %d times, out %v | fault: refused after %v, %d of %d failed, refused %d times, out %v of 1m | heal: back after %v | +50ms: refused %d times after %v",
			tc.name, shortEpisodes, shortOut.Round(100*time.Millisecond), longEpisodes, longOut.Round(100*time.Millisecond),
			since(when(cli, refusedMsg, c.addr(b), fault), fault), rs.window(fault, heal).failed(), len(rs.window(fault, heal)), faultEpisodes, faultOut.Round(100*time.Millisecond),
			since(when(cli, readmittedMsg, c.addr(b), heal), heal),
			uniformEpisodes, since(when(cli, refusedMsg, c.addr(b), uniform), uniform))
	}
}

// outOfRotation is how long, and in how many spells, the policy kept member m
// refused within [from, to).
func outOfRotation(cli *client, c *cluster, m int, from, to time.Time) (total time.Duration, episodes int) {
	var refused time.Time
	for _, e := range cli.logs.All() {
		if e.ContextMap()["endpoint"] != c.addr(m) || e.Time.Before(from) || !e.Time.Before(to) {
			continue
		}
		switch {
		case e.Message == refusedMsg && refused.IsZero():
			refused, episodes = e.Time, episodes+1
		case e.Message == readmittedMsg && !refused.IsZero():
			total, refused = total+e.Time.Sub(refused), time.Time{}
		}
	}
	if !refused.IsZero() {
		total += to.Sub(refused)
	}
	return total, episodes
}

// refusedAt is when member m's current refusal began, zero if it is admitted at t.
func refusedAt(cli *client, c *cluster, m int, t time.Time) time.Time {
	var refused time.Time
	for _, e := range cli.logs.All() {
		if e.ContextMap()["endpoint"] != c.addr(m) || e.Time.After(t) {
			continue
		}
		switch e.Message {
		case refusedMsg:
			refused = e.Time
		case readmittedMsg:
			refused = time.Time{}
		}
	}
	return refused
}

// since is at's delay after t, or -1 if at never came.
func since(at, t time.Time) time.Duration {
	if at.IsZero() {
		return -1
	}
	return at.Sub(t).Round(10 * time.Millisecond)
}
