/*
Copyright (c) 2026 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package controllers

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

type fakeNetworkClassesClient struct {
	objects      []*privatev1.NetworkClass
	updates      []*privatev1.NetworkClassesUpdateRequest
	listCalls    int
	updateErr    error
	updateObject *privatev1.NetworkClass
}

func (f *fakeNetworkClassesClient) List(
	_ context.Context,
	_ *privatev1.NetworkClassesListRequest,
	_ ...grpc.CallOption,
) (*privatev1.NetworkClassesListResponse, error) {
	f.listCalls++
	return privatev1.NetworkClassesListResponse_builder{
		Items: f.objects,
		Size:  int32(len(f.objects)),
		Total: int32(len(f.objects)),
	}.Build(), nil
}

func (f *fakeNetworkClassesClient) Update(
	_ context.Context,
	request *privatev1.NetworkClassesUpdateRequest,
	_ ...grpc.CallOption,
) (*privatev1.NetworkClassesUpdateResponse, error) {
	f.updates = append(f.updates, proto.Clone(request).(*privatev1.NetworkClassesUpdateRequest))
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	object := request.GetObject()
	if f.updateObject != nil {
		object = f.updateObject
	}
	return privatev1.NetworkClassesUpdateResponse_builder{Object: object}.Build(), nil
}

type fakeHubsListClient struct {
	items    []*privatev1.Hub
	listErr  error
	listCall int
}

func (f *fakeHubsListClient) List(
	_ context.Context,
	_ *privatev1.HubsListRequest,
	_ ...grpc.CallOption,
) (*privatev1.HubsListResponse, error) {
	f.listCall++
	if f.listErr != nil {
		return nil, f.listErr
	}
	return privatev1.HubsListResponse_builder{
		Items: f.items,
		Size:  int32(len(f.items)),
		Total: int32(len(f.items)),
	}.Build(), nil
}

type fakeNetworkingHubCache struct {
	entries map[string]*HubEntry
	errors  map[string]error
	calls   []string
}

func (f *fakeNetworkingHubCache) Get(_ context.Context, id string) (*HubEntry, error) {
	f.calls = append(f.calls, id)
	if err := f.errors[id]; err != nil {
		return nil, err
	}
	return f.entries[id], nil
}

var _ = Describe("NetworkingHubResolver", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("persists and returns the only Hub when the NetworkClass has no canonical reference", func() {
		networkClass := testNetworkClass("nc-a", "", privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING, "")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-a")}}
		cache := &fakeNetworkingHubCache{entries: map[string]*HubEntry{
			"hub-a": {Namespace: "networking", Client: nil},
		}}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		result, err := resolver.Resolve(ctx)

		Expect(err).ToNot(HaveOccurred())
		Expect(result.ID).To(Equal("hub-a"))
		Expect(result.Namespace).To(Equal("networking"))
		Expect(hubs.listCall).To(Equal(1))
		Expect(cache.calls).To(Equal([]string{"hub-a"}))
		Expect(networkClasses.updates).To(HaveLen(2))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetHub()).To(Equal("hub-a"))
		Expect(networkClasses.updates[0].GetUpdateMask()).To(Equal(&fieldmaskpb.FieldMask{
			Paths: []string{"status.hub", "status.state", "status.message"},
		}))
		Expect(networkClasses.updates[0].GetLock()).To(BeTrue())
		Expect(networkClasses.updates[1].GetObject().GetStatus().GetState()).To(
			Equal(privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY))
		Expect(networkClasses.updates[1].GetObject().GetStatus().HasMessage()).To(BeFalse())
	})

	It("reports pending when there are no Hubs and does not call the cache", func() {
		networkClass := testNetworkClass("nc-a", "", privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, "old message")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{}
		cache := &fakeNetworkingHubCache{}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		_, err := resolver.Resolve(ctx)

		Expect(errors.Is(err, ErrNoNetworkingHubs)).To(BeTrue())
		Expect(networkClasses.updates).To(HaveLen(1))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetState()).To(
			Equal(privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetMessage()).To(
			Equal("expected exactly one active networking hub, found none"))
		Expect(cache.calls).To(BeEmpty())
	})

	It("reports pending when multiple Hubs exist and does not choose one", func() {
		networkClass := testNetworkClass("nc-a", "", privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, "")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-a"), testHub("hub-b")}}
		cache := &fakeNetworkingHubCache{}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		_, err := resolver.Resolve(ctx)

		Expect(errors.Is(err, ErrMultipleNetworkingHubs)).To(BeTrue())
		Expect(networkClasses.updates).To(HaveLen(1))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetState()).To(
			Equal(privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetMessage()).To(
			Equal("expected exactly one active networking hub, found multiple"))
		Expect(cache.calls).To(BeEmpty())
	})

	It("uses the persisted canonical reference without listing Hubs", func() {
		networkClass := testNetworkClass("nc-a", "hub-a", privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING, "old message")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-b")}}
		cache := &fakeNetworkingHubCache{entries: map[string]*HubEntry{
			"hub-a": {Namespace: "canonical", Client: nil},
		}}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		result, err := resolver.Resolve(ctx)

		Expect(err).ToNot(HaveOccurred())
		Expect(result.ID).To(Equal("hub-a"))
		Expect(result.Namespace).To(Equal("canonical"))
		Expect(hubs.listCall).To(Equal(0))
		Expect(networkClasses.updates).To(HaveLen(1))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetState()).To(
			Equal(privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY))
		Expect(networkClasses.updates[0].GetObject().GetStatus().HasMessage()).To(BeFalse())
	})

	It("does not rewrite an already healthy canonical status", func() {
		networkClass := testNetworkClass("nc-a", "hub-a", privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, "")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-b")}}
		cache := &fakeNetworkingHubCache{entries: map[string]*HubEntry{
			"hub-a": {Namespace: "canonical", Client: nil},
		}}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		result, err := resolver.Resolve(ctx)

		Expect(err).ToNot(HaveOccurred())
		Expect(result.ID).To(Equal("hub-a"))
		Expect(networkClasses.updates).To(BeEmpty())
	})

	It("reuses a resolved canonical Hub without relisting NetworkClasses", func() {
		networkClass := testNetworkClass("nc-a", "hub-a", privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, "")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{}
		cache := &fakeNetworkingHubCache{entries: map[string]*HubEntry{
			"hub-a": {Namespace: "canonical", Client: nil},
		}}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		first, err := resolver.Resolve(ctx)
		Expect(err).ToNot(HaveOccurred())
		second, err := resolver.Resolve(ctx)
		Expect(err).ToNot(HaveOccurred())

		Expect(second.ID).To(Equal(first.ID))
		Expect(second.Namespace).To(Equal(first.Namespace))
		Expect(networkClasses.listCalls).To(Equal(1))
		Expect(cache.calls).To(Equal([]string{"hub-a", "hub-a"}))
	})

	It("reuses a recent resolution failure without relisting NetworkClasses", func() {
		networkClass := testNetworkClass("nc-a", "", privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, "")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{}
		cache := &fakeNetworkingHubCache{}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		_, firstErr := resolver.Resolve(ctx)
		_, secondErr := resolver.Resolve(ctx)

		Expect(errors.Is(firstErr, ErrNoNetworkingHubs)).To(BeTrue())
		Expect(errors.Is(secondErr, ErrNoNetworkingHubs)).To(BeTrue())
		Expect(networkClasses.listCalls).To(Equal(1))
		Expect(hubs.listCall).To(Equal(1))
		Expect(networkClasses.updates).To(HaveLen(1))
	})

	It("reports an invalid canonical reference without falling back", func() {
		networkClass := testNetworkClass("nc-a", "missing", privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING, "")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-a")}}
		cache := &fakeNetworkingHubCache{errors: map[string]error{
			"missing": ErrHubNotFound,
		}}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		_, err := resolver.Resolve(ctx)

		Expect(errors.Is(err, ErrCanonicalHubNotFound)).To(BeTrue())
		Expect(hubs.listCall).To(Equal(0))
		Expect(cache.calls).To(Equal([]string{"missing"}))
		Expect(networkClasses.updates).To(HaveLen(1))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetHub()).To(Equal("missing"))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetState()).To(
			Equal(privatev1.NetworkClassState_NETWORK_CLASS_STATE_FAILED))
	})

	It("reports an unavailable canonical reference without falling back", func() {
		networkClass := testNetworkClass("nc-a", "hub-a", privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, "")
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{networkClass}}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-b")}}
		cache := &fakeNetworkingHubCache{errors: map[string]error{
			"hub-a": errors.New("kubeconfig endpoint unavailable"),
		}}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		_, err := resolver.Resolve(ctx)

		Expect(errors.Is(err, ErrCanonicalHubUnavailable)).To(BeTrue())
		Expect(hubs.listCall).To(Equal(0))
		Expect(cache.calls).To(Equal([]string{"hub-a"}))
		Expect(networkClasses.updates).To(HaveLen(1))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetHub()).To(Equal("hub-a"))
		Expect(networkClasses.updates[0].GetObject().GetStatus().GetState()).To(
			Equal(privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING))
	})

	It("returns an update error without selecting another Hub", func() {
		networkClass := testNetworkClass("nc-a", "", privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING, "")
		networkClasses := &fakeNetworkClassesClient{
			objects:   []*privatev1.NetworkClass{networkClass},
			updateErr: errors.New("optimistic lock failed"),
		}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-a")}}
		cache := &fakeNetworkingHubCache{entries: map[string]*HubEntry{
			"hub-a": {Namespace: "networking", Client: nil},
		}}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		_, err := resolver.Resolve(ctx)

		Expect(err).To(MatchError("failed to persist canonical networking hub: optimistic lock failed"))
		Expect(cache.calls).To(BeEmpty())
	})

	It("reports no NetworkClass when the provider singleton is absent", func() {
		networkClasses := &fakeNetworkClassesClient{}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-a")}}
		cache := &fakeNetworkingHubCache{}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		_, err := resolver.Resolve(ctx)

		Expect(errors.Is(err, ErrNoNetworkClass)).To(BeTrue())
		Expect(hubs.listCall).To(Equal(0))
		Expect(cache.calls).To(BeEmpty())
	})

	It("reports multiple NetworkClasses without selecting one", func() {
		networkClasses := &fakeNetworkClassesClient{objects: []*privatev1.NetworkClass{
			testNetworkClass("nc-a", "", privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, ""),
			testNetworkClass("nc-b", "", privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, ""),
		}}
		hubs := &fakeHubsListClient{items: []*privatev1.Hub{testHub("hub-a")}}
		cache := &fakeNetworkingHubCache{}

		resolver := mustBuildNetworkingHubResolver(networkClasses, hubs, cache)

		_, err := resolver.Resolve(ctx)

		Expect(errors.Is(err, ErrMultipleNetworkClasses)).To(BeTrue())
		Expect(hubs.listCall).To(Equal(0))
		Expect(cache.calls).To(BeEmpty())
	})
})

func mustBuildNetworkingHubResolver(
	networkClasses *fakeNetworkClassesClient,
	hubs *fakeHubsListClient,
	cache *fakeNetworkingHubCache,
) NetworkingHubResolver {
	resolver, err := NewNetworkingHubResolver().
		SetNetworkClassesClient(networkClasses).
		SetHubsClient(hubs).
		SetHubCache(cache).
		Build()
	if err != nil {
		panic(err)
	}
	return resolver
}

func testNetworkClass(id, hub string, state privatev1.NetworkClassState, message string) *privatev1.NetworkClass {
	status := privatev1.NetworkClassStatus_builder{State: state, Hub: hub}
	if message != "" {
		status.Message = &message
	}
	return privatev1.NetworkClass_builder{
		Id:     id,
		Status: status.Build(),
	}.Build()
}

func testHub(id string) *privatev1.Hub {
	return privatev1.Hub_builder{Id: id}.Build()
}
