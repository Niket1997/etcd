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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
)

// BenchmarkPick is a pick and its completion on five members from many
// goroutines at once, on the real clock. Off is plain round robin over the
// same children: the baseline.
func BenchmarkPick(b *testing.B) {
	for _, tc := range []struct {
		name    string
		mode    string
		refused int
	}{
		{"off", ModeOff, 0},
		{"shadow", ModeShadow, 0},
		{"enabled", ModeEnabled, 0},
		{"enabled, one refused", ModeEnabled, 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			sh, err := NewShared(Config{Mode: tc.mode}, zap.NewNop())
			require.NoError(b, err)
			now := monotonic()
			lb := newBalancer(&fakeCC{rec: nopRecorder{}}, balancer.BuildOptions{}, (&fakeChildren{byAddr: map[string]*fakeChild{}}).build, now)
			lb.shared.Store(sh)
			c := sh.config()
			lb.min.Store(int64(c.MinLatencyFloor))
			ready := make([]member, 5)
			for i := range ready {
				addr := fmt.Sprint("m", i)
				ready[i] = member{picker: newFakeChildPicker(addr), ep: lb.newEndpoint(endpointOf(addr))}
				avg := ms
				if i < tc.refused {
					avg = 10 * c.MinLatencyFloor
				}
				pin(ready[i].ep, c.WarmupSamples, avg, now())
			}
			p := newPicker(lb, ready, len(ready))
			info, ok := balancer.PickInfo{FullMethodName: kvRange}, balancer.DoneInfo{BytesReceived: true}

			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					res, _ := p.Pick(info)
					if res.Done != nil {
						res.Done(ok)
					}
				}
			})
		})
	}
}
