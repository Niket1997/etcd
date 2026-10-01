// Copyright 2021 The etcd Authors
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

package resolver

import (
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/serviceconfig"

	"go.etcd.io/etcd/client/v3/internal/endpoint"
)

const (
	Schema = "etcd-endpoints"
)

// EtcdManualResolver is a Resolver (and resolver.Builder) that can be updated
// using SetEndpoints.
type EtcdManualResolver struct {
	*manual.Resolver
	endpoints     []string
	policy        string                 // service config selecting the balancing policy
	attrs         *attributes.Attributes // for the policy, sent in every update
	serviceConfig *serviceconfig.ParseResult
}

func New(endpoints ...string) *EtcdManualResolver {
	return NewWithPolicy(`{"loadBalancingPolicy": "round_robin"}`, nil, endpoints...)
}

// NewWithPolicy is New with the service config that selects the balancing
// policy, and attributes the policy reads from every update.
func NewWithPolicy(serviceConfig string, attrs *attributes.Attributes, endpoints ...string) *EtcdManualResolver {
	r := manual.NewBuilderWithScheme(Schema)
	return &EtcdManualResolver{Resolver: r, endpoints: endpoints, policy: serviceConfig, attrs: attrs}
}

// Build returns itself for Resolver, because it's both a builder and a resolver.
func (r *EtcdManualResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.serviceConfig = cc.ParseServiceConfig(r.policy)
	if r.serviceConfig.Err != nil {
		return nil, r.serviceConfig.Err
	}
	r.Resolver.InitialState(r.state())
	return r.Resolver.Build(target, cc, opts)
}

func (r *EtcdManualResolver) SetEndpoints(endpoints []string) {
	r.endpoints = endpoints
	r.updateState()
}

func (r EtcdManualResolver) updateState() {
	if getCC(r) != nil {
		r.UpdateState(r.state())
	}
}

// state builds the resolver state (endpoints + ServiceConfig + attributes)
// from the endpoints currently stored in r.
func (r EtcdManualResolver) state() resolver.State {
	eps := make([]resolver.Endpoint, len(r.endpoints))
	for i, ep := range r.endpoints {
		addr, serverName := endpoint.Interpret(ep)
		eps[i] = resolver.Endpoint{Addresses: []resolver.Address{
			{Addr: addr, ServerName: serverName},
		}}
	}
	return resolver.State{
		Endpoints:     eps,
		ServiceConfig: r.serviceConfig,
		Attributes:    r.attrs,
	}
}

func getCC(r EtcdManualResolver) (cc resolver.ClientConn) {
	defer func() {
		if rec := recover(); rec != nil {
			cc = nil
		}
	}()

	return r.CC()
}
