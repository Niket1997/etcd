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

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/resolver"
)

func TestARescanPublishesTheGauges(t *testing.T) {
	h := newHarness(t, pickerConfig())
	h.resolve("a", "b")
	h.warm(h.member("a"), 2*ms, 0)
	h.warm(h.member("b"), 50*ms, 0)

	h.b.rescan(true)
	tg := h.b.target
	assert.EqualValues(t, 2000, h.rec.gauge("etcd.client.latency_aware.cluster_min", tg))
	assert.EqualValues(t, 2000, h.rec.gauge("etcd.client.latency_aware.endpoint_latency", tg, "a"))
	assert.EqualValues(t, 50000, h.rec.gauge("etcd.client.latency_aware.endpoint_latency", tg, "b"))
	assert.EqualValues(t, 1, h.rec.gauge(membersName, tg, "refused"))
}

func TestAMemberWithoutAnAddressStillGetsLabels(t *testing.T) {
	h := newHarness(t, pickerConfig())
	e := h.b.newEndpoint(resolver.Endpoint{})
	assert.Empty(t, e.addr)
	assert.Equal(t, []string{h.b.target, "", "allowed"}, e.lbl.picks[outcomeAllowed])
}
