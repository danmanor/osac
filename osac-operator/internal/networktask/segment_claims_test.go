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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

func claimSubnet(name, vn string) *v1alpha1.Subnet {
	return &v1alpha1.Subnet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-" + name,
		UID: types.UID(name), Labels: map[string]string{VirtualNetworkIDLabel: vn}},
		Spec: v1alpha1.SubnetSpec{VirtualNetwork: vn}}
}

func TestSegmentClaimsSurviveFailedStatusWrite(t *testing.T) {
	h := newHarness(t, subnetObj("s1", "s1", "vn1"))
	index := NewSegmentClaimIndex(h.client, h.client, "networking")
	if err := index.Bootstrap(h.ctx); err != nil {
		t.Fatal(err)
	}
	req := fabricReq(CreateSubnet)
	req.Slot = &SubnetSlot{VirtualNetworkID: "vn1"}
	req.Accept = func(ctx context.Context, result Result) error {
		return AcceptSegment(ctx, index, h.lastGet, "evpn", result)
	}
	out := h.runKeep("s1", req)
	h.aap.finish(out.Entry.JobID, "successful", `{"osac_result":{"network_segment":{"type":"evpn","l2_vni":100,"l3_vni":900}}}`, "")
	failure := errors.New("status temporarily unavailable")
	h.engine.client = interceptor.NewClient(h.client.(client.WithWatch), interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		if obj.(*v1alpha1.Subnet).Status.NetworkSegment != nil {
			return failure
		}
		return c.SubResource(subresource).Update(ctx, obj, opts...)
	}})
	h.lastGet = h.get("s1")
	if _, err := h.engine.Run(h.ctx, h.lastGet, req); !errors.Is(err, failure) {
		t.Fatalf("status failure = %v", err)
	}
	if len(h.engine.vnLocks.locks) != 0 {
		t.Fatal("failed acceptance status write retained its VirtualNetwork lock")
	}
	if h.get("s1").Status.NetworkSegment != nil {
		t.Fatal("failed status persisted")
	}
	claim := &v1alpha1.NetworkSegmentClaim{}
	if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: "networking", Name: "vni-100"}, claim); err != nil {
		t.Fatal(err)
	}
	if claim.Spec.L2Owner == nil || claim.Spec.L2Owner.UID != h.lastGet.UID {
		t.Fatal("claim released on status failure")
	}
	h.engine.client = h.client
	if out := h.runKeep("s1", req); !out.Succeeded() {
		t.Fatalf("same UID did not recover: %+v", out)
	}
	if h.get("s1").Status.NetworkSegment == nil {
		t.Fatal("retry did not persist segment")
	}
}

func TestSegmentClaimRollbackFailureRetainsRetryOwnership(t *testing.T) {
	ctx := context.Background()
	first, second := claimSubnet("first", "vn1"), claimSubnet("second", "vn2")
	first.Status.NetworkSegment = &v1alpha1.NetworkSegment{L2VNI: 900, L3VNI: 901}
	c := claimClient(t, first, second)
	failure := errors.New("claim delete temporarily unavailable")
	failing := interceptor.NewClient(c, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		return failure
	}})
	index := NewSegmentClaimIndex(failing, c, "networking")
	if err := index.Acquire(ctx, first, first.Status.NetworkSegment); err != nil {
		t.Fatal(err)
	}
	err := index.Acquire(ctx, second, &v1alpha1.NetworkSegment{L2VNI: 100, L3VNI: 900})
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), SegmentL3InUse) {
		t.Fatalf("lost acquisition/rollback cause: %v", err)
	}
	var rejected *RejectError
	if errors.As(err, &rejected) {
		t.Fatal("failed rollback classified as terminal rejection")
	}
	claim := &v1alpha1.NetworkSegmentClaim{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "networking", Name: "vni-100"}, claim); err != nil {
		t.Fatal(err)
	}
	if claim.Spec.L2Owner == nil || claim.Spec.L2Owner.UID != second.UID {
		t.Fatal("retry ownership lost")
	}
	index = NewSegmentClaimIndex(c, c, "networking")
	expectReject(t, index.Acquire(ctx, second, &v1alpha1.NetworkSegment{L2VNI: 100, L3VNI: 900}), SegmentL3InUse)
	if err := c.Get(ctx, client.ObjectKey{Namespace: "networking", Name: "vni-100"}, claim); err != nil {
		t.Fatal("retry rolled back pre-existing ownership", err)
	}
	if err := index.Release(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := index.Acquire(ctx, second, &v1alpha1.NetworkSegment{L2VNI: 100, L3VNI: 900}); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentClaimActiveConflictWinsOverDeletingOwner(t *testing.T) {
	ctx := context.Background()
	a, b, self := claimSubnet("a", "vn-a"), claimSubnet("b", "vn-b"), claimSubnet("self", "vn-self")
	a.Status.NetworkSegment = &v1alpha1.NetworkSegment{L2VNI: 100, L3VNI: 905}
	a.Status.Jobs = []v1alpha1.NetworkJobStatus{{Task: DeleteSubnet, Manager: v1alpha1.NetworkJobManagerFabric, State: v1alpha1.NetworkJobStateRunning}}
	b.Status.NetworkSegment = &v1alpha1.NetworkSegment{L2VNI: 101, L3VNI: 900}
	c := claimClient(t, a, b, self)
	index := NewSegmentClaimIndex(c, c, "networking")
	for _, owner := range []*v1alpha1.Subnet{a, b} {
		if err := index.Acquire(ctx, owner, owner.Status.NetworkSegment); err != nil {
			t.Fatal(err)
		}
	}
	expectReject(t, index.Acquire(ctx, self, &v1alpha1.NetworkSegment{L2VNI: 100, L3VNI: 900}), SegmentL3InUse)
}

func TestSegmentClaimBootstrapWriteFailureStaysUnready(t *testing.T) {
	ctx := context.Background()
	a := claimSubnet("a", "vn-a")
	a.Status.NetworkSegment = &v1alpha1.NetworkSegment{L2VNI: 100, L3VNI: 900}
	c := claimClient(t, a)
	failure := errors.New("claim API unavailable")
	failing := interceptor.NewClient(c, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		return failure
	}})
	index := NewSegmentClaimIndex(failing, c, "networking")
	if err := index.Bootstrap(ctx); !errors.Is(err, failure) || index.Ready() {
		t.Fatalf("failed write opened readiness: %v", err)
	}
	index.client = c
	if err := index.Bootstrap(ctx); err != nil || !index.Ready() {
		t.Fatalf("retry did not recover: %v", err)
	}
}

func TestBootstrapBatchesSharedL3Owners(t *testing.T) {
	ctx := context.Background()
	const ownerCount = 8
	objects := make([]client.Object, 0, ownerCount)
	for i := range ownerCount {
		subnet := claimSubnet(fmt.Sprintf("owner-%d", i), "vn1")
		subnet.Status.NetworkSegment = &v1alpha1.NetworkSegment{
			Type: "evpn", L2VNI: int32(100 + i), L3VNI: 900,
		}
		objects = append(objects, subnet)
	}
	c := claimClient(t, objects...)
	l3ClaimWrites := 0
	batched := interceptor.NewClient(c, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if claim, ok := obj.(*v1alpha1.NetworkSegmentClaim); ok && claim.Spec.VNI == 900 {
				l3ClaimWrites++
			}
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if claim, ok := obj.(*v1alpha1.NetworkSegmentClaim); ok && claim.Spec.VNI == 900 {
				l3ClaimWrites++
			}
			return c.Update(ctx, obj, opts...)
		},
	})
	index := NewSegmentClaimIndex(batched, c, "networking")
	if err := index.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if l3ClaimWrites != 1 {
		t.Fatalf("persisted shared L3 claim %d times, want once", l3ClaimWrites)
	}
	claim := &v1alpha1.NetworkSegmentClaim{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "networking", Name: "vni-900"}, claim); err != nil {
		t.Fatal(err)
	}
	if len(claim.Spec.L3Owners) != ownerCount {
		t.Fatalf("shared L3 owners = %d, want %d", len(claim.Spec.L3Owners), ownerCount)
	}
}

func bootstrapClaimFixture(ownerCount int) []client.Object {
	objects := make([]client.Object, 0, ownerCount*3)
	for i := range ownerCount {
		virtualNetwork := fmt.Sprintf("vn-%d", i)
		l2VNI, l3VNI := int32(100+i), int32(900+i)
		subnet := claimSubnet(fmt.Sprintf("owner-%d", i), virtualNetwork)
		subnet.Status.NetworkSegment = &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: l2VNI, L3VNI: l3VNI}
		owner := claimOwner(subnet)
		objects = append(objects, subnet,
			&v1alpha1.NetworkSegmentClaim{
				ObjectMeta: metav1.ObjectMeta{Namespace: "networking", Name: fmt.Sprintf("vni-%d", l2VNI)},
				Spec:       v1alpha1.NetworkSegmentClaimSpec{VNI: l2VNI, L2Owner: &owner},
			},
			&v1alpha1.NetworkSegmentClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "networking", Name: fmt.Sprintf("vni-%d", l3VNI),
					Labels: map[string]string{VirtualNetworkIDLabel: virtualNetwork},
				},
				Spec: v1alpha1.NetworkSegmentClaimSpec{
					VNI: l3VNI, L3VirtualNetwork: virtualNetwork,
					L3Owners: []v1alpha1.SegmentClaimOwner{owner},
				},
			},
		)
	}
	return objects
}

func TestBootstrapUsesOneSubnetSnapshotForClaimCleanup(t *testing.T) {
	ctx := context.Background()
	c := claimClient(t, bootstrapClaimFixture(3)...)
	var subnetGets, subnetLists int
	observed := interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1alpha1.Subnet); ok {
				subnetGets++
			}
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*v1alpha1.SubnetList); ok {
				subnetLists++
			}
			return c.List(ctx, list, opts...)
		},
	})
	index := NewSegmentClaimIndex(observed, observed, "networking")
	if err := index.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if subnetGets != 0 {
		t.Fatalf("read %d Subnets individually during bootstrap cleanup, want none", subnetGets)
	}
	if subnetLists != 1 {
		t.Fatalf("listed Subnets %d times during bootstrap, want one complete snapshot", subnetLists)
	}
}

func TestBootstrapSkipsUnchangedClaimUpdates(t *testing.T) {
	ctx := context.Background()
	c := claimClient(t, bootstrapClaimFixture(3)...)
	var claimUpdates int
	observed := interceptor.NewClient(c, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*v1alpha1.NetworkSegmentClaim); ok {
				claimUpdates++
			}
			return c.Update(ctx, obj, opts...)
		},
	})
	index := NewSegmentClaimIndex(observed, observed, "networking")
	if err := index.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if claimUpdates != 0 {
		t.Fatalf("updated %d unchanged claims during bootstrap", claimUpdates)
	}
}

func TestSegmentClaimsSharingDoesNotReadUncontendedOwners(t *testing.T) {
	ctx := context.Background()
	first, second := claimSubnet("first", "vn1"), claimSubnet("second", "vn1")
	first.Status.NetworkSegment = &v1alpha1.NetworkSegment{L2VNI: 100, L3VNI: 900}
	c := claimClient(t, first, second)
	index := NewSegmentClaimIndex(c, c, "networking")
	if err := index.Acquire(ctx, first, first.Status.NetworkSegment); err != nil {
		t.Fatal(err)
	}
	reader := interceptor.NewClient(c, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*v1alpha1.Subnet); ok {
			return errors.New("uncontended owner read")
		}
		return c.Get(ctx, key, obj, opts...)
	}})
	index = NewSegmentClaimIndex(c, reader, "networking")
	if err := index.Acquire(ctx, second, &v1alpha1.NetworkSegment{L2VNI: 101, L3VNI: 900}); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentClaimAcquisitionListsL3OwnersOnce(t *testing.T) {
	ctx := context.Background()
	const ownerCount = 8
	objects := make([]client.Object, 0, ownerCount+1)
	owners := make([]*v1alpha1.Subnet, 0, ownerCount)
	for i := range ownerCount {
		owner := claimSubnet(fmt.Sprintf("owner-%d", i), "vn1")
		owner.Status.NetworkSegment = &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: int32(100 + i), L3VNI: 900}
		owners = append(owners, owner)
		objects = append(objects, owner)
	}
	candidate := claimSubnet("candidate", "vn1")
	objects = append(objects, candidate)
	c := claimClient(t, objects...)
	index := NewSegmentClaimIndex(c, c, "networking")
	for _, owner := range owners {
		if err := index.Acquire(ctx, owner, owner.Status.NetworkSegment); err != nil {
			t.Fatal(err)
		}
	}

	subnetGets := 0
	subnetLists := 0
	reader := interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1alpha1.Subnet); ok {
				subnetGets++
			}
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*v1alpha1.SubnetList); ok {
				subnetLists++
			}
			return c.List(ctx, list, opts...)
		},
	})
	index = NewSegmentClaimIndex(c, reader, "networking")
	err := index.Acquire(ctx, candidate, &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: 200, L3VNI: 901})
	expectReject(t, err, SegmentL3DiffersSibling)
	if subnetGets != 0 {
		t.Fatalf("read %d live owners individually instead of using the snapshot", subnetGets)
	}
	if subnetLists != 1 {
		t.Fatalf("listed Subnets %d times, want the sibling list reused for owner validation", subnetLists)
	}
}

func TestProvisionalSiblingL3Claim(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleting=%t", deleting), func(t *testing.T) {
			ctx := context.Background()
			first, second := claimSubnet("first", "vn1"), claimSubnet("second", "vn1")
			if deleting {
				first.Status.Jobs = []v1alpha1.NetworkJobStatus{{Task: DeleteSubnet, Manager: v1alpha1.NetworkJobManagerFabric, State: v1alpha1.NetworkJobStateRunning}}
			}
			c := claimClient(t, first, second)
			index := NewSegmentClaimIndex(c, c, "networking")
			if err := index.Acquire(ctx, first, &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: 100, L3VNI: 900}); err != nil {
				t.Fatal(err)
			}
			// The first owner has claims but no status segment, as after a failed status write.
			err := index.Acquire(ctx, second, &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: 101, L3VNI: 901})
			if deleting {
				if !errors.Is(err, ErrWaitForDelete) {
					t.Fatalf("got %v", err)
				}
			} else {
				expectReject(t, err, SegmentL3DiffersSibling)
			}
			claim := &v1alpha1.NetworkSegmentClaim{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: "networking", Name: "vni-900"}, claim); err != nil {
				t.Fatal(err)
			}
			if claim.Labels[VirtualNetworkIDLabel] != "vn1" {
				t.Fatal("L3 claim missing its VirtualNetwork lookup label")
			}
			if err := index.Release(ctx, first); err != nil {
				t.Fatal(err)
			}
			if err := index.Acquire(ctx, second, &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: 101, L3VNI: 901}); err != nil {
				t.Fatalf("released provisional owner still blocks sibling: %v", err)
			}
		})
	}
}

func TestL3ClaimLabelLifecycle(t *testing.T) {
	ctx := context.Background()
	first := claimSubnet("first", "vn1")
	c := claimClient(t, first)
	index := NewSegmentClaimIndex(c, c, "networking")
	if err := index.Acquire(ctx, first, &v1alpha1.NetworkSegment{L2VNI: 100, L3VNI: 100}); err != nil {
		t.Fatal(err)
	}
	if err := index.releaseOne(ctx, first.UID, claimRoles{vni: 100, l3: true}); err != nil {
		t.Fatal(err)
	}
	claim := &v1alpha1.NetworkSegmentClaim{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "networking", Name: "vni-100"}, claim); err != nil {
		t.Fatal(err)
	}
	if _, exists := claim.Labels[VirtualNetworkIDLabel]; exists {
		t.Fatal("L2-only claim remains in L3 lookup index")
	}
	if claim.Spec.L2Owner == nil || claim.Spec.L2Owner.UID != first.UID {
		t.Fatal("removing L3 ownership removed L2 owner")
	}
}

func TestBootstrapLabelsLegacyProvisionalL3Claims(t *testing.T) {
	ctx := context.Background()
	first, second := claimSubnet("first", "vn1"), claimSubnet("second", "vn1")
	first.Status.Jobs = []v1alpha1.NetworkJobStatus{{Task: CreateSubnet, Manager: v1alpha1.NetworkJobManagerFabric, State: v1alpha1.NetworkJobStateRunning}}
	claim := &v1alpha1.NetworkSegmentClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "networking", Name: "vni-900"},
		Spec: v1alpha1.NetworkSegmentClaimSpec{VNI: 900, L3VirtualNetwork: "vn1", L3Owners: []v1alpha1.SegmentClaimOwner{{Namespace: first.Namespace, Name: first.Name, UID: first.UID}}}}
	c := claimClient(t, first, second, claim)
	index := NewSegmentClaimIndex(c, c, "networking")
	if err := index.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(claim), claim); err != nil {
		t.Fatal(err)
	}
	if claim.Labels[VirtualNetworkIDLabel] != "vn1" {
		t.Fatal("legacy provisional claim was not indexed")
	}
	expectReject(t, index.Acquire(ctx, second, &v1alpha1.NetworkSegment{L2VNI: 101, L3VNI: 901}), SegmentL3DiffersSibling)
}

func claimClient(t *testing.T, objects ...client.Object) client.WithWatch {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Subnet{}).WithObjects(objects...).Build()
}

func TestSegmentClaimIndexAcquire(t *testing.T) {
	cases := []struct {
		name                                 string
		firstL2, firstL3, secondL2, secondL3 int32
		sibling, retry, deleting             bool
		want                                 string
	}{
		{name: "L2 collision", firstL2: 100, firstL3: 900, secondL2: 100, secondL3: 901, want: SegmentVNIInUse},
		{name: "L2 conflicts with another L3 owner", firstL2: 100, firstL3: 900, secondL2: 900, secondL3: 901, want: SegmentVNIInUse},
		{name: "L3 conflicts with another L2 owner", firstL2: 900, firstL3: 901, secondL2: 100, secondL3: 900, want: SegmentL3InUse},
		{name: "sibling L3 may be shared", firstL2: 100, firstL3: 900, secondL2: 101, secondL3: 900, sibling: true},
		{name: "L3 across VirtualNetworks", firstL2: 100, firstL3: 900, secondL2: 101, secondL3: 900, want: SegmentL3InUse},
		{name: "sibling L3 must agree", firstL2: 100, firstL3: 900, secondL2: 101, secondL3: 901, sibling: true, want: SegmentL3DiffersSibling},
		{name: "same VNI for both roles", firstL2: 100, firstL3: 100, secondL2: 101, secondL3: 901},
		{name: "same owner retry after failed status write", firstL2: 100, firstL3: 900, secondL2: 100, secondL3: 900, retry: true},
		{name: "deleting owner waits", firstL2: 100, firstL3: 900, secondL2: 100, secondL3: 901, deleting: true, want: "wait"},
		{name: "rolls back lower VNI on later conflict", firstL2: 900, firstL3: 901, secondL2: 100, secondL3: 900, want: SegmentL3InUse},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			first, second := claimSubnet("first", "vn1"), claimSubnet("second", "vn2")
			if tc.sibling {
				second.Spec.VirtualNetwork = "vn1"
				second.Labels[VirtualNetworkIDLabel] = "vn1"
			}
			if tc.retry {
				second = first.DeepCopy()
			}
			firstSegment := &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: tc.firstL2, L3VNI: tc.firstL3}
			if !tc.retry {
				first.Status.NetworkSegment = firstSegment
			}
			if tc.deleting {
				first.Status.Jobs = []v1alpha1.NetworkJobStatus{{Task: DeleteSubnet, Manager: v1alpha1.NetworkJobManagerFabric, State: v1alpha1.NetworkJobStateRunning}}
			}
			objects := []client.Object{first}
			if !tc.retry {
				objects = append(objects, second)
			}
			c := claimClient(t, objects...)
			index := NewSegmentClaimIndex(c, c, "networking")
			if err := index.Acquire(ctx, first, firstSegment); err != nil {
				t.Fatal(err)
			}
			err := index.Acquire(ctx, second, &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: tc.secondL2, L3VNI: tc.secondL3})
			switch tc.want {
			case "":
				if err != nil {
					t.Fatal(err)
				}
			case "wait":
				if !errors.Is(err, ErrWaitForDelete) {
					t.Fatalf("got %v", err)
				}
			default:
				expectReject(t, err, tc.want)
			}
			claims := &v1alpha1.NetworkSegmentClaimList{}
			if err := c.List(ctx, claims); err != nil {
				t.Fatal(err)
			}
			for _, claim := range claims.Items {
				if tc.want != "" && claim.Spec.L2Owner != nil && claim.Spec.L2Owner.UID == second.UID {
					t.Fatalf("rejected owner retained: %+v", claim.Spec)
				}
			}
			if tc.sibling && tc.want == "" {
				claim := &v1alpha1.NetworkSegmentClaim{}
				if err := c.Get(ctx, client.ObjectKey{Namespace: "networking", Name: "vni-900"}, claim); err != nil {
					t.Fatal(err)
				}
				if len(claim.Spec.L3Owners) != 2 {
					t.Fatalf("sharing owners = %+v", claim.Spec.L3Owners)
				}
			}
			if tc.retry && len(claims.Items) != 2 {
				t.Fatalf("retry claims = %d", len(claims.Items))
			}
		})
	}
}
