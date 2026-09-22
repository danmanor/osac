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
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	clnt "sigs.k8s.io/controller-runtime/pkg/client"

	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

var (
	ErrNoNetworkClass          = errors.New("no active network class")
	ErrMultipleNetworkClasses  = errors.New("multiple active network classes")
	ErrNoNetworkingHubs        = errors.New("no active networking hubs")
	ErrMultipleNetworkingHubs  = errors.New("multiple active networking hubs")
	ErrCanonicalHubNotFound    = errors.New("canonical networking hub not found")
	ErrCanonicalHubUnavailable = errors.New("canonical networking hub unavailable")
)

const (
	canonicalHubNegativeCacheTTL    = time.Second
	canonicalHubNoCandidatesMessage = "expected exactly one active networking hub, found none"
	canonicalHubMultipleMessage     = "expected exactly one active networking hub, found multiple"
	canonicalHubStatusMessage       = "status.hub"
	canonicalHubStatusStateMessage  = "status.state"
	canonicalHubStatusMessageField  = "status.message"
)

type networkClassesClient interface {
	List(ctx context.Context, in *privatev1.NetworkClassesListRequest, opts ...grpc.CallOption) (*privatev1.NetworkClassesListResponse, error)
	Update(ctx context.Context, in *privatev1.NetworkClassesUpdateRequest, opts ...grpc.CallOption) (*privatev1.NetworkClassesUpdateResponse, error)
}

type hubsListClient interface {
	List(ctx context.Context, in *privatev1.HubsListRequest, opts ...grpc.CallOption) (*privatev1.HubsListResponse, error)
}

// NetworkingHubResolver resolves the provider-owned canonical networking Hub.
type NetworkingHubResolver interface {
	Resolve(ctx context.Context) (NetworkingHub, error)
}

// NetworkingHub is the stable Hub selection and the already-resolved client.
type NetworkingHub struct {
	ID        string
	Namespace string
	Client    clnt.Client
}

// NetworkingHubResolverBuilder contains the dependencies needed to construct a canonical Hub resolver.
type NetworkingHubResolverBuilder struct {
	networkClassesClient networkClassesClient
	hubsClient           hubsListClient
	hubCache             HubCache
}

type networkingHubResolver struct {
	networkClassesClient networkClassesClient
	hubsClient           hubsListClient
	hubCache             HubCache
	mu                   sync.Mutex
	cachedHub            *NetworkingHub
	cachedError          error
	errorExpiresAt       time.Time
}

// NewNetworkingHubResolver creates a builder for a canonical networking Hub resolver.
func NewNetworkingHubResolver() *NetworkingHubResolverBuilder {
	return &NetworkingHubResolverBuilder{}
}

// SetNetworkClassesClient sets the private NetworkClass client.
func (b *NetworkingHubResolverBuilder) SetNetworkClassesClient(value networkClassesClient) *NetworkingHubResolverBuilder {
	b.networkClassesClient = value
	return b
}

// SetHubsClient sets the private Hub list client.
func (b *NetworkingHubResolverBuilder) SetHubsClient(value hubsListClient) *NetworkingHubResolverBuilder {
	b.hubsClient = value
	return b
}

// SetHubCache sets the cache used to resolve a Hub's Kubernetes client.
func (b *NetworkingHubResolverBuilder) SetHubCache(value HubCache) *NetworkingHubResolverBuilder {
	b.hubCache = value
	return b
}

// Build validates the dependencies and creates a canonical networking Hub resolver.
func (b *NetworkingHubResolverBuilder) Build() (NetworkingHubResolver, error) {
	if b.networkClassesClient == nil {
		return nil, errors.New("network classes client is mandatory")
	}
	if b.hubsClient == nil {
		return nil, errors.New("hubs client is mandatory")
	}
	if b.hubCache == nil {
		return nil, errors.New("hub cache is mandatory")
	}
	return &networkingHubResolver{
		networkClassesClient: b.networkClassesClient,
		hubsClient:           b.hubsClient,
		hubCache:             b.hubCache,
	}, nil
}

func (r *networkingHubResolver) Resolve(ctx context.Context) (NetworkingHub, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cachedHub != nil {
		entry, err := r.hubCache.Get(ctx, r.cachedHub.ID)
		if err == nil && entry != nil {
			return NetworkingHub{ID: r.cachedHub.ID, Namespace: entry.Namespace, Client: entry.Client}, nil
		}
		r.cachedHub = nil
	}
	if r.cachedError != nil && time.Now().Before(r.errorExpiresAt) {
		return NetworkingHub{}, r.cachedError
	}
	r.cachedError = nil
	r.errorExpiresAt = time.Time{}

	result, err := r.resolve(ctx)
	if err == nil {
		r.cachedHub = &result
	} else {
		r.cachedError = err
		r.errorExpiresAt = time.Now().Add(canonicalHubNegativeCacheTTL)
	}
	return result, err
}

func (r *networkingHubResolver) resolve(ctx context.Context) (NetworkingHub, error) {
	networkClass, err := r.findNetworkClass(ctx)
	if err != nil {
		return NetworkingHub{}, err
	}

	canonicalHubID := networkClass.GetStatus().GetHub()
	if canonicalHubID != "" {
		return r.resolveCanonicalHub(ctx, networkClass, canonicalHubID)
	}

	hub, err := r.findOnlyHub(ctx)
	if err != nil {
		_, statusErr := r.updateStatus(ctx, networkClass, "", privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING, canonicalHubErrorMessage(err))
		if statusErr != nil {
			return NetworkingHub{}, fmt.Errorf("%w; failed to update network class status", errors.Join(err, statusErr))
		}
		return NetworkingHub{}, err
	}

	updatedNetworkClass, err := r.updateStatus(
		ctx,
		networkClass,
		hub.GetId(),
		privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING,
		"",
	)
	if err != nil {
		return NetworkingHub{}, fmt.Errorf("failed to persist canonical networking hub: %w", err)
	}
	return r.resolveCanonicalHub(ctx, updatedNetworkClass, hub.GetId())
}

func (r *networkingHubResolver) findNetworkClass(ctx context.Context) (*privatev1.NetworkClass, error) {
	response, err := r.networkClassesClient.List(ctx, privatev1.NetworkClassesListRequest_builder{}.Build())
	if err != nil {
		return nil, fmt.Errorf("failed to list network classes: %w", err)
	}
	if response == nil {
		return nil, errors.New("network classes list returned an empty response")
	}

	return findOnlyActive(
		response.GetItems(),
		func(networkClass *privatev1.NetworkClass) bool {
			return networkClass.GetMetadata().HasDeletionTimestamp()
		},
		ErrNoNetworkClass,
		func(count int) error { return fmt.Errorf("%w: found %d", ErrMultipleNetworkClasses, count) },
	)
}

func (r *networkingHubResolver) findOnlyHub(ctx context.Context) (*privatev1.Hub, error) {
	response, err := r.hubsClient.List(ctx, privatev1.HubsListRequest_builder{}.Build())
	if err != nil {
		return nil, fmt.Errorf("failed to list networking hubs: %w", err)
	}
	if response == nil {
		return nil, errors.New("networking hubs list returned an empty response")
	}

	hub, err := findOnlyActive(
		response.GetItems(),
		func(hub *privatev1.Hub) bool { return hub.GetMetadata().HasDeletionTimestamp() },
		ErrNoNetworkingHubs,
		func(int) error { return ErrMultipleNetworkingHubs },
	)
	if err != nil {
		return nil, err
	}
	if hub.GetId() == "" {
		return nil, fmt.Errorf("%w: candidate Hub has no identifier", ErrCanonicalHubNotFound)
	}
	return hub, nil
}

func findOnlyActive[T any](
	items []*T,
	isDeleting func(*T) bool,
	noItemsErr error,
	multipleItemsErr func(int) error,
) (*T, error) {
	active := make([]*T, 0, len(items))
	for _, item := range items {
		if item != nil && !isDeleting(item) {
			active = append(active, item)
		}
	}

	switch len(active) {
	case 0:
		return nil, noItemsErr
	case 1:
		return active[0], nil
	default:
		return nil, multipleItemsErr(len(active))
	}
}

func (r *networkingHubResolver) resolveCanonicalHub(
	ctx context.Context,
	networkClass *privatev1.NetworkClass,
	hubID string,
) (NetworkingHub, error) {
	entry, err := r.hubCache.Get(ctx, hubID)
	if err != nil {
		state := privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING
		kind := ErrCanonicalHubUnavailable
		if errors.Is(err, ErrHubNotFound) {
			state = privatev1.NetworkClassState_NETWORK_CLASS_STATE_FAILED
			kind = ErrCanonicalHubNotFound
		}
		return r.canonicalHubFailure(ctx, networkClass, hubID, state, kind, err)
	}
	if entry == nil {
		return r.canonicalHubFailure(
			ctx,
			networkClass,
			hubID,
			privatev1.NetworkClassState_NETWORK_CLASS_STATE_PENDING,
			ErrCanonicalHubUnavailable,
			errors.New("hub cache returned an empty entry"),
		)
	}

	if _, err = r.updateStatus(ctx, networkClass, hubID, privatev1.NetworkClassState_NETWORK_CLASS_STATE_READY, ""); err != nil {
		return NetworkingHub{}, fmt.Errorf("failed to update network class status: %w", err)
	}
	return NetworkingHub{ID: hubID, Namespace: entry.Namespace, Client: entry.Client}, nil
}

func (r *networkingHubResolver) canonicalHubFailure(
	ctx context.Context,
	networkClass *privatev1.NetworkClass,
	hubID string,
	state privatev1.NetworkClassState,
	kind error,
	err error,
) (NetworkingHub, error) {
	message := fmt.Sprintf("canonical networking hub %q is unavailable", hubID)
	if errors.Is(kind, ErrCanonicalHubNotFound) {
		message = fmt.Sprintf("canonical networking hub %q is not registered", hubID)
	}
	if _, statusErr := r.updateStatus(ctx, networkClass, hubID, state, message); statusErr != nil {
		return NetworkingHub{}, fmt.Errorf("%w; failed to update network class status", errors.Join(kind, err, statusErr))
	}
	return NetworkingHub{}, fmt.Errorf("%w: %q: %w", kind, hubID, err)
}

func (r *networkingHubResolver) updateStatus(
	ctx context.Context,
	networkClass *privatev1.NetworkClass,
	hubID string,
	state privatev1.NetworkClassState,
	message string,
) (*privatev1.NetworkClass, error) {
	object := proto.Clone(networkClass).(*privatev1.NetworkClass)
	if !object.HasStatus() {
		object.SetStatus(&privatev1.NetworkClassStatus{})
	}
	status := object.GetStatus()
	if hubID != "" {
		status.SetHub(hubID)
	}
	status.SetState(state)
	if message == "" {
		status.ClearMessage()
	} else {
		status.SetMessage(message)
	}
	if networkClass.HasStatus() && proto.Equal(networkClass.GetStatus(), status) {
		return networkClass, nil
	}

	response, err := r.networkClassesClient.Update(ctx, privatev1.NetworkClassesUpdateRequest_builder{
		Object: object,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
			canonicalHubStatusMessage,
			canonicalHubStatusStateMessage,
			canonicalHubStatusMessageField,
		}},
		Lock: true,
	}.Build())
	if err != nil {
		return nil, err
	}
	if response == nil || response.GetObject() == nil {
		return object, nil
	}
	return response.GetObject(), nil
}

func canonicalHubErrorMessage(err error) string {
	switch {
	case errors.Is(err, ErrNoNetworkingHubs):
		return canonicalHubNoCandidatesMessage
	case errors.Is(err, ErrMultipleNetworkingHubs):
		return canonicalHubMultipleMessage
	default:
		return err.Error()
	}
}
