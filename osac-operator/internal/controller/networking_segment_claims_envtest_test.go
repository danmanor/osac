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

package controller

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/internal/networktask"
)

// This isolated real API server keeps upgrade inventory independent of the
// shared controller suite's intentionally retained lifecycle fixtures.
func TestSegmentClaimsEnvtest(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true, BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s", "1.31.0-"+runtime.GOOS+"-"+runtime.GOARCH)}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := k8sruntime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, namespace := range []string{"claims", "tenant-a", "tenant-b"} {
		if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("new Subnet persists lookup label before creation and waits for bootstrap", func(t *testing.T) { claimCreationGate(t, c) })
	t.Run("concurrent cross namespace exclusivity", func(t *testing.T) { claimConcurrentExclusivity(t, c) })
	t.Run("sibling L3 sharing and delete wait", func(t *testing.T) { claimSiblingSharing(t, c) })
	t.Run("idempotent bootstrap backfills labels and ignores freed segments", func(t *testing.T) { claimBootstrapIdempotent(t, c) })
	t.Run("legacy conflicts fail closed with both identities in Warning events", func(t *testing.T) { claimBootstrapConflict(t, c) })
	t.Run("label mismatch fails closed", func(t *testing.T) { claimLabelMismatch(t, c) })
	t.Run("restart preserves partial claims of a live create retry and cleans replaced UID", func(t *testing.T) { claimPartialRecovery(t, c) })
	t.Run("delete keeps claims until success is persisted", func(t *testing.T) { claimDeleteStatusFailure(t, c) })
}

func claimDeleteStatusFailure(t *testing.T, c client.Client) {
	ctx := context.Background()
	cleanupSegmentClaims(t, c)
	a := newClaimSubnet(t, c, "a", "tenant-a", "vn-a", claimSegment(100, 900))
	a.Labels = map[string]string{networktask.IDLabel("Subnet"): "a"}
	a.Annotations = map[string]string{networktask.TenantAnnotation: "tenant-a"}
	a.Finalizers = []string{osacSubnetFinalizer}
	if err := c.Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	a.Status.NetworkManagers = testManagers(false)
	a.Status.Jobs = []v1alpha1.NetworkJobStatus{{Task: networktask.CreateSubnet, Manager: v1alpha1.NetworkJobManagerFabric, Attempt: 1, State: v1alpha1.NetworkJobStateSucceeded}}
	if err := c.Status().Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	index := networktask.NewSegmentClaimIndex(c, c, "claims")
	if err := index.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(a), a); err != nil {
		t.Fatal(err)
	}
	aap := newNetFakeAAP()
	engine := networktask.New(c, c, aap, networktask.Config{ProvisioningEnabled: true, TemplatePrefix: "osac", Organization: "osac"})
	r := &SubnetReconciler{Client: c, Engine: engine, Claims: index}
	if err := c.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(a)}
	if _, err := r.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if loadSegmentClaim(t, c, "vni-100").Spec.L2Owner.UID != a.UID {
		t.Fatal("running delete lost claim")
	}
	aap.succeed(networktask.DeleteSubnet, netFabric)
	failure := errors.New("delete status unavailable")
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		if networktask.Succeeded(obj.(*v1alpha1.Subnet), networktask.DeleteSubnet, v1alpha1.NetworkJobManagerFabric) {
			return failure
		}
		return c.SubResource(subresource).Update(ctx, obj, opts...)
	}})
	r.Engine = networktask.New(failing, c, aap, networktask.Config{ProvisioningEnabled: true, TemplatePrefix: "osac", Organization: "osac"})
	if _, err := r.Reconcile(ctx, request); !errors.Is(err, failure) {
		t.Fatalf("status failure = %v", err)
	}
	if loadSegmentClaim(t, c, "vni-100").Spec.L2Owner.UID != a.UID {
		t.Fatal("failed status write released claim")
	}
	r.Engine = engine
	if _, err := r.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	claims := &v1alpha1.NetworkSegmentClaimList{}
	if err := c.List(ctx, claims, client.InNamespace("claims")); err != nil {
		t.Fatal(err)
	}
	if len(claims.Items) != 0 {
		t.Fatal("successful delete retained claims")
	}
}

func newClaimSubnet(t *testing.T, c client.Client, name, namespace, vn string, segment *v1alpha1.NetworkSegment) *v1alpha1.Subnet {
	ctx := context.Background()
	t.Helper()
	s := &v1alpha1.Subnet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: v1alpha1.SubnetSpec{VirtualNetwork: vn, IPv4CIDR: "10.0.1.0/24", AddressPlan: v1alpha1.SubnetAddressPlan{Fabric: 16}}}
	if err := c.Create(ctx, s); err != nil {
		t.Fatal(err)
	}
	if segment != nil {
		s.Status.NetworkSegment = segment
		if err := c.Status().Update(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if err := client.IgnoreNotFound(c.Delete(ctx, s)); err != nil {
			t.Error(err)
		}
	})
	return s
}
func claimSegment(l2, l3 int32) *v1alpha1.NetworkSegment {
	return &v1alpha1.NetworkSegment{Type: "evpn", L2VNI: l2, L3VNI: l3}
}
func cleanupSegmentClaims(t *testing.T, c client.Client) {
	ctx := context.Background()
	t.Cleanup(func() {
		if err := c.DeleteAllOf(ctx, &v1alpha1.NetworkSegmentClaim{}, client.InNamespace("claims")); err != nil {
			t.Error(err)
		}
	})
}
func loadSegmentClaim(t *testing.T, c client.Client, name string) *v1alpha1.NetworkSegmentClaim {
	ctx := context.Background()
	t.Helper()
	claim := &v1alpha1.NetworkSegmentClaim{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "claims", Name: name}, claim); err != nil {
		t.Fatal(err)
	}
	return claim
}

func claimCreationGate(t *testing.T, c client.Client) {
	ctx := context.Background()
	cleanupSegmentClaims(t, c)
	a := newClaimSubnet(t, c, "a", "tenant-a", "vn-a", nil)
	index := networktask.NewSegmentClaimIndex(c, c, "claims")
	r := &SubnetReconciler{Client: c, Engine: networktask.New(c, c, nil, networktask.Config{ProvisioningEnabled: true}), Claims: index}
	result, err := r.create(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("label write did not request another reconcile")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(a), a); err != nil {
		t.Fatal(err)
	}
	if a.Labels[networktask.VirtualNetworkIDLabel] != "vn-a" {
		t.Fatal("label not persisted")
	}
	result, err = r.create(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 || a.Status.TaskInput != nil || len(a.Status.Jobs) != 0 {
		t.Fatal("unready bootstrap allowed creation")
	}
}

func claimConcurrentExclusivity(t *testing.T, c client.Client) {
	ctx := context.Background()
	cleanupSegmentClaims(t, c)
	a := newClaimSubnet(t, c, "a", "tenant-a", "vn-a", nil)
	b := newClaimSubnet(t, c, "b", "tenant-b", "vn-b", nil)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, s := range []*v1alpha1.Subnet{a, b} {
		go func() {
			<-start
			results <- networktask.NewSegmentClaimIndex(c, c, "claims").Acquire(ctx, s, claimSegment(100, 900))
		}()
	}
	close(start)
	x, y := <-results, <-results
	if (x == nil) == (y == nil) {
		t.Fatalf("expected one winner: %v, %v", x, y)
	}
	var rejection *networktask.RejectError
	if x == nil {
		x = y
	}
	if !errors.As(x, &rejection) {
		t.Fatalf("loser error: %v", x)
	}
	claim := loadSegmentClaim(t, c, "vni-100")
	if claim.Spec.L2Owner == nil {
		t.Fatal("no exclusive owner")
	}
}

func claimSiblingSharing(t *testing.T, c client.Client) {
	ctx := context.Background()
	cleanupSegmentClaims(t, c)
	a := newClaimSubnet(t, c, "a", "tenant-a", "vn-a", claimSegment(100, 900))
	b := newClaimSubnet(t, c, "b", "tenant-b", "vn-a", nil)
	index := networktask.NewSegmentClaimIndex(c, c, "claims")
	if err := index.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if err := index.Acquire(ctx, b, claimSegment(101, 900)); err != nil {
		t.Fatal(err)
	}
	if len(loadSegmentClaim(t, c, "vni-900").Spec.L3Owners) != 2 {
		t.Fatal("sibling ownership lost")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(a), a); err != nil {
		t.Fatal(err)
	}
	a.Status.Jobs = []v1alpha1.NetworkJobStatus{{Task: networktask.DeleteSubnet, Manager: v1alpha1.NetworkJobManagerFabric, Attempt: 1, State: v1alpha1.NetworkJobStateRunning}}
	if err := c.Status().Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	other := newClaimSubnet(t, c, "c", "tenant-b", "vn-c", nil)
	if err := index.Acquire(ctx, other, claimSegment(100, 902)); !errors.Is(err, networktask.ErrWaitForDelete) {
		t.Fatalf("got %v", err)
	}
	if err := index.Release(ctx, b); err != nil {
		t.Fatal(err)
	}
	if len(loadSegmentClaim(t, c, "vni-900").Spec.L3Owners) != 1 {
		t.Fatal("release removed sibling")
	}
}

func claimBootstrapIdempotent(t *testing.T, c client.Client) {
	ctx := context.Background()
	cleanupSegmentClaims(t, c)
	a := newClaimSubnet(t, c, "a", "tenant-a", "vn-a", claimSegment(100, 900))
	b := newClaimSubnet(t, c, "b", "tenant-b", "vn-b", claimSegment(100, 900))
	b.Status.Jobs = []v1alpha1.NetworkJobStatus{{Task: networktask.DeleteSubnet, Manager: v1alpha1.NetworkJobManagerFabric, Attempt: 1, State: v1alpha1.NetworkJobStateSucceeded}}
	if err := c.Status().Update(ctx, b); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		index := networktask.NewSegmentClaimIndex(c, c, "claims")
		if err := index.Bootstrap(ctx); err != nil {
			t.Fatal(err)
		}
		if !index.Ready() {
			t.Fatal("not ready")
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(a), a); err != nil {
		t.Fatal(err)
	}
	if a.Labels[networktask.VirtualNetworkIDLabel] != "vn-a" {
		t.Fatal("legacy label not backfilled")
	}
	if loadSegmentClaim(t, c, "vni-100").Spec.L2Owner.UID != a.UID {
		t.Fatal("freed segment seeded")
	}
}

func claimBootstrapConflict(t *testing.T, c client.Client) {
	ctx := context.Background()
	cleanupSegmentClaims(t, c)
	a := newClaimSubnet(t, c, "a", "tenant-a", "vn-a", claimSegment(100, 900))
	b := newClaimSubnet(t, c, "b", "tenant-b", "vn-b", claimSegment(100, 901))
	index := networktask.NewSegmentClaimIndex(c, c, "claims")
	recorder := events.NewFakeRecorder(4)
	index.Recorder = recorder
	if err := index.Bootstrap(ctx); err == nil {
		t.Fatal("conflict accepted")
	}
	if index.Ready() {
		t.Fatal("conflicting index ready")
	}
	for range 2 {
		select {
		case event := <-recorder.Events:
			if !strings.Contains(event, "Warning") || !strings.Contains(event, string(a.UID)) || !strings.Contains(event, string(b.UID)) {
				t.Fatalf("event = %s", event)
			}
		default:
			t.Fatal("missing conflict Warning")
		}
	}
}

func claimLabelMismatch(t *testing.T, c client.Client) {
	ctx := context.Background()
	cleanupSegmentClaims(t, c)
	a := newClaimSubnet(t, c, "a", "tenant-a", "vn-a", nil)
	a.Labels = map[string]string{networktask.VirtualNetworkIDLabel: "wrong"}
	if err := c.Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	index := networktask.NewSegmentClaimIndex(c, c, "claims")
	if err := index.Bootstrap(ctx); err == nil || index.Ready() {
		t.Fatalf("mismatch accepted: %v", err)
	}
}

func claimPartialRecovery(t *testing.T, c client.Client) {
	ctx := context.Background()
	cleanupSegmentClaims(t, c)
	a := newClaimSubnet(t, c, "a", "tenant-a", "vn-a", nil)
	a.Status.Jobs = []v1alpha1.NetworkJobStatus{{Task: networktask.CreateSubnet, Manager: v1alpha1.NetworkJobManagerFabric, Attempt: 1, State: v1alpha1.NetworkJobStateRunning}}
	if err := c.Status().Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	index := networktask.NewSegmentClaimIndex(c, c, "claims")
	if err := index.Acquire(ctx, a, claimSegment(100, 900)); err != nil {
		t.Fatal(err)
	}
	claim := loadSegmentClaim(t, c, "vni-900")
	claim.Spec.L3Owners[0].UID = "replaced-uid"
	if err := c.Update(ctx, claim); err != nil {
		t.Fatal(err)
	}
	index = networktask.NewSegmentClaimIndex(c, c, "claims")
	if err := index.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if loadSegmentClaim(t, c, "vni-100").Spec.L2Owner.UID != a.UID {
		t.Fatal("live partial claim discarded")
	}
	if err := index.Acquire(ctx, a, claimSegment(100, 900)); err != nil {
		t.Fatal(err)
	}
	if len(loadSegmentClaim(t, c, "vni-900").Spec.L3Owners) != 1 || loadSegmentClaim(t, c, "vni-900").Spec.L3Owners[0].UID != a.UID {
		t.Fatal("stale UID retained")
	}
}
