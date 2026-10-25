/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package networktask

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

// VirtualNetworkIDLabel is derived from immutable Subnet.spec.virtualNetwork.
const VirtualNetworkIDLabel = "osac.openshift.io/virtual-network-id"

// SegmentClaimIndex coordinates VNI ownership through Kubernetes CAS writes.
// Fresh API reads validate owners before stale claims are removed.
type SegmentClaimIndex struct {
	client      client.Client
	reader      client.Reader
	namespace   string
	ready       atomic.Bool
	bootstrapMu sync.Mutex
	// Recorder receives upgrade conflict warnings on both affected Subnets.
	Recorder events.EventRecorder
}

// NewSegmentClaimIndex creates an unready index in the networking namespace.
func NewSegmentClaimIndex(c client.Client, reader client.Reader, namespace string) *SegmentClaimIndex {
	return &SegmentClaimIndex{client: c, reader: reader, namespace: namespace}
}

// Ready reports whether all legacy labels and claims have been persisted.
func (index *SegmentClaimIndex) Ready() bool { return index != nil && index.ready.Load() }

type claimRoles struct {
	vni    int32
	l2, l3 bool
}

type segmentClaimRequest struct {
	subnet  *v1alpha1.Subnet
	segment *v1alpha1.NetworkSegment
	roles   claimRoles
}

type claimRequestFilter struct {
	requesterUIDs map[types.UID]struct{}
	l3Networks    map[string]struct{}
	hasL2Request  bool
}

func newClaimRequestFilter(requests []segmentClaimRequest) claimRequestFilter {
	filter := claimRequestFilter{
		requesterUIDs: make(map[types.UID]struct{}, len(requests)),
		l3Networks:    make(map[string]struct{}),
	}
	for _, request := range requests {
		filter.requesterUIDs[request.subnet.UID] = struct{}{}
		filter.hasL2Request = filter.hasL2Request || request.roles.l2
		if request.roles.l3 {
			filter.l3Networks[request.subnet.Spec.VirtualNetwork] = struct{}{}
		}
	}
	return filter
}

func (filter claimRequestFilter) needsOwnerValidation(owner v1alpha1.SegmentClaimOwner,
	claim *v1alpha1.NetworkSegmentClaim) bool {
	if _, requested := filter.requesterUIDs[owner.UID]; requested {
		return false
	}
	if filter.hasL2Request {
		return true
	}
	if len(filter.l3Networks) == 0 {
		return false
	}
	if len(filter.l3Networks) != 1 {
		return true
	}
	if _, sameNetwork := filter.l3Networks[claim.Spec.L3VirtualNetwork]; !sameNetwork {
		return true
	}
	return claim.Spec.L2Owner != nil && claim.Spec.L2Owner.UID == owner.UID
}

type subnetOwnerSnapshot struct {
	byVirtualNetwork map[string]map[client.ObjectKey]*v1alpha1.Subnet
	complete         bool
}

func segmentRoles(segment *v1alpha1.NetworkSegment) []claimRoles {
	if segment.L2VNI == segment.L3VNI {
		return []claimRoles{{segment.L2VNI, true, true}}
	}
	roles := []claimRoles{{segment.L2VNI, true, false}, {segment.L3VNI, false, true}}
	slices.SortFunc(roles, func(a, b claimRoles) int { return int(a.vni - b.vni) })
	return roles
}

func claimOwner(subnet *v1alpha1.Subnet) v1alpha1.SegmentClaimOwner {
	return v1alpha1.SegmentClaimOwner{Namespace: subnet.Namespace, Name: subnet.Name, UID: subnet.UID}
}

func (index *SegmentClaimIndex) key(vni int32) client.ObjectKey {
	return client.ObjectKey{Namespace: index.namespace, Name: fmt.Sprintf("vni-%d", vni)}
}

// Acquire reserves distinct VNIs in numeric order before the Subnet status write.
// A failed status write leaves ownership for the same UID's retry.
func (index *SegmentClaimIndex) Acquire(ctx context.Context, subnet *v1alpha1.Subnet, segment *v1alpha1.NetworkSegment) error {
	if subnet.UID == "" {
		return errors.New("cannot claim a segment for a Subnet without a UID")
	}
	if subnet.Status.NetworkSegment != nil && *subnet.Status.NetworkSegment != *segment {
		return Reject("network segment differs from the segment already accepted")
	}
	siblings := &v1alpha1.SubnetList{}
	if err := index.reader.List(ctx, siblings, client.MatchingLabels{VirtualNetworkIDLabel: subnet.Spec.VirtualNetwork}); err != nil {
		return err
	}
	snapshot := &subnetOwnerSnapshot{byVirtualNetwork: map[string]map[client.ObjectKey]*v1alpha1.Subnet{
		subnet.Spec.VirtualNetwork: make(map[client.ObjectKey]*v1alpha1.Subnet, len(siblings.Items)),
	}}
	for i := range siblings.Items {
		other := &siblings.Items[i]
		snapshot.byVirtualNetwork[subnet.Spec.VirtualNetwork][client.ObjectKeyFromObject(other)] = other
	}
	provisional, err := index.siblingClaimOwners(ctx, subnet, segment.L3VNI, snapshot)
	if err != nil {
		return err
	}
	self := subnet.DeepCopy()
	self.Status.NetworkSegment = nil
	waiting := CheckSegment(ctx, self, segment, append(siblings.Items, provisional...))
	if waiting != nil && !errors.Is(waiting, ErrWaitForDelete) {
		return waiting
	}
	var acquired []claimRoles
	for _, roles := range segmentRoles(segment) {
		added, err := index.acquireOne(ctx, subnet, segment, roles, snapshot)
		if added.l2 || added.l3 {
			acquired = append(acquired, added)
		}
		if errors.Is(err, ErrWaitForDelete) {
			waiting = err
			continue
		}
		if err != nil {
			return index.rollback(ctx, subnet.UID, acquired, err)
		}
	}
	if waiting != nil {
		return index.rollback(ctx, subnet.UID, acquired, waiting)
	}
	return nil
}

func claimRetry(err error) bool {
	return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) || apierrors.IsNotFound(err)
}

// siblingClaimOwners makes a provisional L3 choice visible even when the
// accepting Subnet's status write failed. Only conflicting L3 claims of this
// VirtualNetwork require owner GETs; matching L3 values remain uncontended.
func (index *SegmentClaimIndex) siblingClaimOwners(ctx context.Context, subnet *v1alpha1.Subnet, l3VNI int32,
	snapshot *subnetOwnerSnapshot) ([]v1alpha1.Subnet, error) {
	claims := &v1alpha1.NetworkSegmentClaimList{}
	if err := index.reader.List(ctx, claims, client.InNamespace(index.namespace), client.MatchingLabels{VirtualNetworkIDLabel: subnet.Spec.VirtualNetwork}); err != nil {
		return nil, err
	}
	var siblings []v1alpha1.Subnet
	for _, listed := range claims.Items {
		if listed.Spec.VNI == l3VNI {
			continue
		}
		var observed []v1alpha1.Subnet
		err := retry.OnError(retry.DefaultRetry, claimRetry, func() error {
			observed = nil
			claim := &v1alpha1.NetworkSegmentClaim{}
			if err := index.reader.Get(ctx, client.ObjectKeyFromObject(&listed), claim); err != nil {
				return client.IgnoreNotFound(err)
			}
			if claim.Spec.L3VirtualNetwork != subnet.Spec.VirtualNetwork {
				return nil
			}
			l3Owners := make(map[types.UID]bool, len(claim.Spec.L3Owners))
			for _, owner := range claim.Spec.L3Owners {
				l3Owners[owner.UID] = true
			}
			owners, cleaned, err := index.liveOwners(ctx, claim, func(owner v1alpha1.SegmentClaimOwner) bool {
				return owner.UID != subnet.UID && l3Owners[owner.UID]
			}, snapshot)
			if err != nil {
				return err
			}
			if cleaned {
				if claim.Spec.L2Owner == nil && len(claim.Spec.L3Owners) == 0 {
					if err := index.client.Delete(ctx, claim, client.Preconditions{UID: &claim.UID, ResourceVersion: &claim.ResourceVersion}); client.IgnoreNotFound(err) != nil {
						return err
					}
				} else if err := index.client.Update(ctx, claim); err != nil {
					return err
				}
			}
			for _, owner := range owners {
				segmentFromClaim(owner, claim)
				observed = append(observed, *owner)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		siblings = append(siblings, observed...)
	}
	return siblings, nil
}

// syncL3ClaimLabel keeps only L3 claims in the VirtualNetwork lookup index.
func syncL3ClaimLabel(claim *v1alpha1.NetworkSegmentClaim) bool {
	before, exists := claim.Labels[VirtualNetworkIDLabel]
	if len(claim.Spec.L3Owners) == 0 {
		delete(claim.Labels, VirtualNetworkIDLabel)
		return exists
	}
	if exists && before == claim.Spec.L3VirtualNetwork {
		return false
	}
	if claim.Labels == nil {
		claim.Labels = map[string]string{}
	}
	claim.Labels[VirtualNetworkIDLabel] = claim.Spec.L3VirtualNetwork
	return true
}

func (index *SegmentClaimIndex) acquireOne(ctx context.Context, subnet *v1alpha1.Subnet, segment *v1alpha1.NetworkSegment,
	roles claimRoles, snapshot *subnetOwnerSnapshot) (claimRoles, error) {
	return index.acquireClaim(ctx, []segmentClaimRequest{{subnet: subnet, segment: segment, roles: roles}}, snapshot)
}

func validateClaimRequests(ctx context.Context, requests []segmentClaimRequest, owners map[types.UID]*v1alpha1.Subnet,
	claim *v1alpha1.NetworkSegmentClaim) (bool, error) {
	var waiting bool
	for _, request := range requests {
		for _, other := range owners {
			if other.UID == request.subnet.UID {
				continue
			}
			segmentFromClaim(other, claim)
			self := request.subnet.DeepCopy()
			self.Status.NetworkSegment = nil
			err := CheckSegment(ctx, self, request.segment, []v1alpha1.Subnet{*other})
			if errors.Is(err, ErrWaitForDelete) {
				waiting = true
				continue
			}
			if err != nil {
				return false, err
			}
		}
	}
	return waiting, nil
}

func addClaimRequests(claim *v1alpha1.NetworkSegmentClaim, requests []segmentClaimRequest) claimRoles {
	added := claimRoles{vni: requests[0].roles.vni}
	l3OwnerUIDs := make(map[types.UID]struct{}, len(claim.Spec.L3Owners)+len(requests))
	for _, owner := range claim.Spec.L3Owners {
		l3OwnerUIDs[owner.UID] = struct{}{}
	}
	claim.Spec.L3Owners = slices.Grow(claim.Spec.L3Owners, len(requests))
	for i, request := range requests {
		owner := claimOwner(request.subnet)
		if request.roles.l2 && claim.Spec.L2Owner == nil {
			claim.Spec.L2Owner = &owner
			if i == 0 {
				added.l2 = true
			}
		}
		if request.roles.l3 {
			if _, exists := l3OwnerUIDs[request.subnet.UID]; !exists {
				claim.Spec.L3VirtualNetwork = request.subnet.Spec.VirtualNetwork
				claim.Spec.L3Owners = append(claim.Spec.L3Owners, owner)
				l3OwnerUIDs[request.subnet.UID] = struct{}{}
				if i == 0 {
					added.l3 = true
				}
			}
		}
	}
	return added
}

// acquireClaim validates and persists all owners for one VNI in one write.
// Bootstrap uses it to avoid rewriting a shared L3 owner list per Subnet.
func (index *SegmentClaimIndex) acquireClaim(ctx context.Context, requests []segmentClaimRequest,
	snapshot *subnetOwnerSnapshot) (claimRoles, error) {
	if len(requests) == 0 {
		return claimRoles{}, nil
	}
	added := claimRoles{vni: requests[0].roles.vni}
	err := retry.OnError(retry.DefaultRetry, claimRetry, func() error {
		added = claimRoles{vni: requests[0].roles.vni}
		claim := &v1alpha1.NetworkSegmentClaim{}
		err := index.reader.Get(ctx, index.key(requests[0].roles.vni), claim)
		missing := apierrors.IsNotFound(err)
		if err != nil && !missing {
			return err
		}
		if missing {
			claim = &v1alpha1.NetworkSegmentClaim{ObjectMeta: metav1.ObjectMeta{Name: index.key(requests[0].roles.vni).Name, Namespace: index.namespace}, Spec: v1alpha1.NetworkSegmentClaimSpec{VNI: requests[0].roles.vni}}
		}
		filter := newClaimRequestFilter(requests)
		owners, cleaned, err := index.liveOwners(ctx, claim, func(owner v1alpha1.SegmentClaimOwner) bool {
			return filter.needsOwnerValidation(owner, claim)
		}, snapshot)
		if err != nil {
			return err
		}
		if cleaned {
			if err := index.client.Update(ctx, claim); err != nil {
				return err
			}
		}
		waiting, err := validateClaimRequests(ctx, requests, owners, claim)
		if err != nil {
			return err
		}
		if waiting {
			return ErrWaitForDelete
		}
		added = addClaimRequests(claim, requests)
		labelChanged := syncL3ClaimLabel(claim)
		if missing {
			return index.client.Create(ctx, claim)
		}
		if !added.l2 && !added.l3 && !labelChanged {
			return nil
		}
		return index.client.Update(ctx, claim)
	})
	if err != nil {
		return claimRoles{vni: requests[0].roles.vni}, err
	}
	return added, nil
}

func bootstrapClaimRequests(subnets []v1alpha1.Subnet) ([]int32, map[int32][]segmentClaimRequest) {
	requestsByVNI := make(map[int32][]segmentClaimRequest)
	claimOrder := make([]int32, 0)
	for i := range subnets {
		subnet := &subnets[i]
		if subnet.Status.NetworkSegment == nil || segmentFreed(subnet) {
			continue
		}
		for _, roles := range segmentRoles(subnet.Status.NetworkSegment) {
			if _, exists := requestsByVNI[roles.vni]; !exists {
				claimOrder = append(claimOrder, roles.vni)
			}
			requestsByVNI[roles.vni] = append(requestsByVNI[roles.vni], segmentClaimRequest{
				subnet: subnet, segment: subnet.Status.NetworkSegment, roles: roles,
			})
		}
	}
	return claimOrder, requestsByVNI
}

type bootstrapCandidate struct {
	position int
	subnet   *v1alpha1.Subnet
}

func (index *SegmentClaimIndex) validateBootstrapSegments(ctx context.Context, subnets *v1alpha1.SubnetList) error {
	byVNI := map[int32]bootstrapCandidate{}
	byL2VNI := map[int32]bootstrapCandidate{}
	byL3VNI := map[int32]bootstrapCandidate{}
	byVirtualNetwork := map[string]bootstrapCandidate{}
	for i := range subnets.Items {
		subnet := &subnets.Items[i]
		if label, exists := subnet.Labels[VirtualNetworkIDLabel]; exists && label != subnet.Spec.VirtualNetwork {
			return fmt.Errorf("subnet %s virtual-network label %q disagrees with spec %q", client.ObjectKeyFromObject(subnet), label, subnet.Spec.VirtualNetwork)
		}
		if subnet.Labels[VirtualNetworkIDLabel] == "" {
			if subnet.Labels == nil {
				subnet.Labels = map[string]string{}
			}
			subnet.Labels[VirtualNetworkIDLabel] = subnet.Spec.VirtualNetwork
			if err := index.client.Update(ctx, subnet); err != nil {
				return err
			}
		}
		if subnet.Status.NetworkSegment == nil || segmentFreed(subnet) {
			continue
		}
		segment := subnet.Status.NetworkSegment
		var conflict *bootstrapCandidate
		consider := func(candidate bootstrapCandidate, ok bool) {
			if ok && segmentConflictRule(subnet, segment, candidate.subnet) != "" &&
				(conflict == nil || candidate.position < conflict.position) {
				selected := candidate
				conflict = &selected
			}
		}
		if candidate, ok := byVNI[segment.L2VNI]; ok {
			consider(candidate, true)
		}
		if candidate, ok := byL2VNI[segment.L3VNI]; ok {
			consider(candidate, true)
		}
		if candidate, ok := byL3VNI[segment.L3VNI]; ok {
			consider(candidate, true)
		}
		if candidate, ok := byVirtualNetwork[subnet.Spec.VirtualNetwork]; ok {
			consider(candidate, true)
		}
		if conflict != nil {
			self := subnet.DeepCopy()
			self.Status.NetworkSegment = nil
			other := conflict.subnet
			if err := CheckSegment(ctx, self, segment, []v1alpha1.Subnet{*other}); err != nil {
				message := fmt.Sprintf("Conflicting network segments on Subnets %s (%s) and %s (%s): %v", client.ObjectKeyFromObject(subnet), subnet.UID, client.ObjectKeyFromObject(other), other.UID, err)
				ctrllog.FromContext(ctx).Error(err, message)
				if index.Recorder != nil {
					index.Recorder.Eventf(subnet, other, corev1.EventTypeWarning, "SegmentClaimConflict", "Bootstrap", "%s", message)
					index.Recorder.Eventf(other, subnet, corev1.EventTypeWarning, "SegmentClaimConflict", "Bootstrap", "%s", message)
				}
				return errors.New(message)
			}
		}
		accepted := bootstrapCandidate{position: i, subnet: subnet}
		if _, ok := byVNI[segment.L2VNI]; !ok {
			byVNI[segment.L2VNI] = accepted
		}
		if _, ok := byVNI[segment.L3VNI]; !ok {
			byVNI[segment.L3VNI] = accepted
		}
		if _, ok := byL2VNI[segment.L2VNI]; !ok {
			byL2VNI[segment.L2VNI] = accepted
		}
		if _, ok := byL3VNI[segment.L3VNI]; !ok {
			byL3VNI[segment.L3VNI] = accepted
		}
		if _, ok := byVirtualNetwork[subnet.Spec.VirtualNetwork]; !ok {
			byVirtualNetwork[subnet.Spec.VirtualNetwork] = accepted
		}
	}
	return nil
}

// liveOwners removes only identities proven absent, replaced, or Fabric-freed.
func (index *SegmentClaimIndex) liveOwners(ctx context.Context, claim *v1alpha1.NetworkSegmentClaim,
	check func(v1alpha1.SegmentClaimOwner) bool, snapshot *subnetOwnerSnapshot) (map[types.UID]*v1alpha1.Subnet, bool, error) {
	owners := append([]v1alpha1.SegmentClaimOwner(nil), claim.Spec.L3Owners...)
	if claim.Spec.L2Owner != nil {
		owners = append(owners, *claim.Spec.L2Owner)
	}
	live := map[types.UID]*v1alpha1.Subnet{}
	stale := map[types.UID]bool{}
	seen := map[types.UID]bool{}
	var candidates []v1alpha1.SegmentClaimOwner
	for _, owner := range owners {
		if (check == nil || check(owner)) && !seen[owner.UID] {
			seen[owner.UID] = true
			candidates = append(candidates, owner)
		}
	}
	if len(candidates) == 0 {
		return live, false, nil
	}
	l3OwnerUIDs := make(map[types.UID]bool, len(claim.Spec.L3Owners))
	for _, owner := range claim.Spec.L3Owners {
		l3OwnerUIDs[owner.UID] = true
	}
	needsL3Snapshot := false
	for _, owner := range candidates {
		if l3OwnerUIDs[owner.UID] {
			needsL3Snapshot = true
			break
		}
	}
	var l3Owners map[client.ObjectKey]*v1alpha1.Subnet
	hasSnapshot := false
	if snapshot != nil {
		if owners, ok := snapshot.byVirtualNetwork[claim.Spec.L3VirtualNetwork]; ok {
			l3Owners, hasSnapshot = owners, true
		} else if snapshot.complete {
			l3Owners, hasSnapshot = map[client.ObjectKey]*v1alpha1.Subnet{}, true
		}
	}
	if needsL3Snapshot && claim.Spec.L3VirtualNetwork != "" && !hasSnapshot {
		ownerSnapshot := &v1alpha1.SubnetList{}
		if err := index.reader.List(ctx, ownerSnapshot, client.MatchingLabels{VirtualNetworkIDLabel: claim.Spec.L3VirtualNetwork}); err != nil {
			return nil, false, err
		}
		l3Owners = make(map[client.ObjectKey]*v1alpha1.Subnet, len(ownerSnapshot.Items))
		for i := range ownerSnapshot.Items {
			subnet := &ownerSnapshot.Items[i]
			l3Owners[client.ObjectKeyFromObject(subnet)] = subnet
		}
	}
	for _, owner := range candidates {
		key := client.ObjectKey{Namespace: owner.Namespace, Name: owner.Name}
		if l3OwnerUIDs[owner.UID] {
			if subnet := l3Owners[key]; subnet != nil {
				if subnet.UID != owner.UID || segmentFreed(subnet) {
					stale[owner.UID] = true
					continue
				}
				live[owner.UID] = subnet
				continue
			}
		}
		other := &v1alpha1.Subnet{}
		err := index.reader.Get(ctx, key, other)
		if apierrors.IsNotFound(err) || (err == nil && (other.UID != owner.UID || segmentFreed(other))) {
			stale[owner.UID] = true
			continue
		}
		if err != nil {
			return nil, false, err
		}
		live[owner.UID] = other
	}
	for uid := range stale {
		removeClaimOwner(claim, uid, true, true)
	}
	return live, len(stale) > 0, nil
}

func removeClaimOwner(claim *v1alpha1.NetworkSegmentClaim, uid types.UID, l2, l3 bool) {
	if l2 && claim.Spec.L2Owner != nil && claim.Spec.L2Owner.UID == uid {
		claim.Spec.L2Owner = nil
	}
	if l3 {
		claim.Spec.L3Owners = slices.DeleteFunc(claim.Spec.L3Owners, func(o v1alpha1.SegmentClaimOwner) bool { return o.UID == uid })
	}
	if len(claim.Spec.L3Owners) == 0 {
		claim.Spec.L3VirtualNetwork = ""
	}
	syncL3ClaimLabel(claim)
}

func (index *SegmentClaimIndex) releaseOne(ctx context.Context, uid types.UID, roles claimRoles) error {
	return retry.OnError(retry.DefaultRetry, claimRetry, func() error {
		claim := &v1alpha1.NetworkSegmentClaim{}
		if err := index.reader.Get(ctx, index.key(roles.vni), claim); err != nil {
			return client.IgnoreNotFound(err)
		}
		removeClaimOwner(claim, uid, roles.l2, roles.l3)
		if claim.Spec.L2Owner == nil && len(claim.Spec.L3Owners) == 0 {
			return client.IgnoreNotFound(index.client.Delete(ctx, claim, client.Preconditions{UID: &claim.UID, ResourceVersion: &claim.ResourceVersion}))
		}
		return index.client.Update(ctx, claim)
	})
}

func (index *SegmentClaimIndex) rollback(ctx context.Context, uid types.UID, acquired []claimRoles, cause error) error {
	var rollbackErr error
	for _, roles := range acquired {
		rollbackErr = errors.Join(rollbackErr, index.releaseOne(ctx, uid, roles))
	}
	if rollbackErr != nil {
		// Do not wrap a RejectError: the engine must retry a failed rollback.
		return fmt.Errorf("segment acquisition failed (%v); rolling back new claims: %w", cause, rollbackErr)
	}
	return cause
}

// Release removes the UID from all claims, including interrupted partial claims.
// The caller first persists Fabric deletion success, or proves no create ran.
func (index *SegmentClaimIndex) Release(ctx context.Context, subnet *v1alpha1.Subnet) error {
	claims := &v1alpha1.NetworkSegmentClaimList{}
	if err := index.reader.List(ctx, claims, client.InNamespace(index.namespace)); err != nil {
		return err
	}
	for _, claim := range claims.Items {
		if (claim.Spec.L2Owner != nil && claim.Spec.L2Owner.UID == subnet.UID) || slices.ContainsFunc(claim.Spec.L3Owners, func(o v1alpha1.SegmentClaimOwner) bool { return o.UID == subnet.UID }) {
			if err := index.releaseOne(ctx, subnet.UID, claimRoles{claim.Spec.VNI, true, true}); err != nil {
				return err
			}
		}
	}
	return nil
}

// Bootstrap repairs upgrade labels and seeds accepted, non-freed segments.
// No creation is allowed until every required write succeeds.
func (index *SegmentClaimIndex) Bootstrap(ctx context.Context) error {
	index.bootstrapMu.Lock()
	defer index.bootstrapMu.Unlock()
	if index.Ready() {
		return nil
	}
	subnets := &v1alpha1.SubnetList{}
	if err := index.reader.List(ctx, subnets); err != nil {
		return err
	}
	if err := index.validateBootstrapSegments(ctx, subnets); err != nil {
		return err
	}
	ownerSnapshot := &subnetOwnerSnapshot{
		byVirtualNetwork: make(map[string]map[client.ObjectKey]*v1alpha1.Subnet),
		complete:         true,
	}
	for i := range subnets.Items {
		subnet := &subnets.Items[i]
		owners := ownerSnapshot.byVirtualNetwork[subnet.Spec.VirtualNetwork]
		if owners == nil {
			owners = make(map[client.ObjectKey]*v1alpha1.Subnet)
			ownerSnapshot.byVirtualNetwork[subnet.Spec.VirtualNetwork] = owners
		}
		owners[client.ObjectKeyFromObject(subnet)] = subnet
	}
	if err := index.cleanClaims(ctx); err != nil {
		return err
	}
	claimOrder, requestsByVNI := bootstrapClaimRequests(subnets.Items)
	for _, vni := range claimOrder {
		// The complete legacy set was checked and snapshotted; avoid further lists.
		if _, err := index.acquireClaim(ctx, requestsByVNI[vni], ownerSnapshot); err != nil {
			return err
		}
	}
	index.ready.Store(true)
	return nil
}

func (index *SegmentClaimIndex) cleanClaims(ctx context.Context) error {
	claims := &v1alpha1.NetworkSegmentClaimList{}
	if err := index.reader.List(ctx, claims, client.InNamespace(index.namespace)); err != nil {
		return err
	}
	for _, claim := range claims.Items {
		// Running create tasks keep partial claims after a failed status write.
		if err := retry.OnError(retry.DefaultRetry, claimRetry, func() error {
			current := &v1alpha1.NetworkSegmentClaim{}
			if err := index.reader.Get(ctx, index.key(claim.Spec.VNI), current); err != nil {
				return client.IgnoreNotFound(err)
			}
			owners, _, err := index.liveOwners(ctx, current, nil, nil)
			if err != nil {
				return err
			}
			for uid, owner := range owners {
				job := FindJob(owner.Status.Jobs, CreateSubnet, v1alpha1.NetworkJobManagerFabric)
				inFlight := job != nil && (job.State == v1alpha1.NetworkJobStateRunning || job.State == v1alpha1.NetworkJobStateRetrying)
				if owner.Status.NetworkSegment == nil && !inFlight {
					removeClaimOwner(current, uid, true, true)
				}
			}
			if current.Spec.L2Owner == nil && len(current.Spec.L3Owners) == 0 {
				return client.IgnoreNotFound(index.client.Delete(ctx, current, client.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion}))
			}
			syncL3ClaimLabel(current)
			return index.client.Update(ctx, current)
		}); err != nil {
			return err
		}
	}
	return nil
}

// segmentFromClaim reconstructs a provisional owner's roles without inventing
// its as-yet-unrecorded VNI for the other claim.
func segmentFromClaim(owner *v1alpha1.Subnet, claim *v1alpha1.NetworkSegmentClaim) {
	if owner.Status.NetworkSegment != nil {
		return
	}
	owner.Status.NetworkSegment = &v1alpha1.NetworkSegment{L2VNI: -1, L3VNI: -1}
	l2Owner := claim.Spec.L2Owner != nil && claim.Spec.L2Owner.UID == owner.UID
	if l2Owner {
		owner.Status.NetworkSegment.L2VNI = claim.Spec.VNI
	}
	// Owners come from the union of claim roles. Every owner except the one
	// L2 owner is therefore an L3 owner; avoid rescanning a shared L3 list.
	if !l2Owner || slices.ContainsFunc(claim.Spec.L3Owners, func(l3 v1alpha1.SegmentClaimOwner) bool { return l3.UID == owner.UID }) {
		owner.Status.NetworkSegment.L3VNI = claim.Spec.VNI
	}
}
