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
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

var _ = Describe("ClusterOrder automatic external access hold", func() {
	const (
		networkingNS = "default"
		clusterUUID  = "test-cluster-uuid"
		pollInterval = 50 * time.Millisecond
	)

	ctx := context.Background()

	newReconciler := func(networkingNamespace string, indexedObjects ...client.Object) *ClusterOrderReconciler {
		for _, obj := range indexedObjects {
			waitForIndexedClientObject(obj)
		}
		engine, _ := newTestEngine(true)
		return &ClusterOrderReconciler{
			Client:              autoExternalIPCleanupTestClient(),
			apiReader:           k8sClient,
			NetworkingNamespace: networkingNamespace,
			StatusPollInterval:  pollInterval,
			Engine:              engine,
		}
	}

	newClusterOrder := func(uuid string) *v1alpha1.ClusterOrder {
		labels := map[string]string{}
		if uuid != "" {
			labels[osacClusterOrderIDLabel] = uuid
		}
		return &v1alpha1.ClusterOrder{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-co-",
				Namespace:    networkingNS,
				Labels:       labels,
			},
			Spec: v1alpha1.ClusterOrderSpec{TemplateID: "t"},
		}
	}

	newAutoEIA := func(clusterID string) *v1alpha1.ExternalIPAttachment {
		clusterIDCopy := clusterID
		ep := v1alpha1.ExternalIPAttachmentTargetEndpointAPI
		return &v1alpha1.ExternalIPAttachment{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-eia-",
				Namespace:    networkingNS,
				Labels:       map[string]string{autoCreatedLabel: labelValueTrue},
			},
			Spec: v1alpha1.ExternalIPAttachmentSpec{
				ExternalIP:     "test-eip",
				Cluster:        &clusterIDCopy,
				TargetEndpoint: &ep,
			},
		}
	}

	newAutoEIP := func(clusterID string) *v1alpha1.ExternalIP {
		return &v1alpha1.ExternalIP{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-eip-",
				Namespace:    networkingNS,
				Labels: map[string]string{
					autoCreatedLabel:    labelValueTrue,
					autoCreatedForLabel: clusterID,
				},
			},
			Spec: v1alpha1.ExternalIPSpec{Pool: "test-pool"},
		}
	}

	owner := autoExternalIPOwner{kind: clusterOrderOwner, id: clusterUUID}

	AfterEach(func() {
		for _, list := range []client.ObjectList{&v1alpha1.ExternalIPAttachmentList{}, &v1alpha1.ExternalIPList{}} {
			Expect(k8sClient.List(ctx, list, client.InNamespace(networkingNS))).To(Succeed())
			Expect(apimeta.EachListItem(list, func(obj runtime.Object) error {
				object := obj.(client.Object)
				object.SetFinalizers(nil)
				_ = k8sClient.Update(ctx, object)
				return client.IgnoreNotFound(k8sClient.Delete(ctx, object))
			})).To(Succeed())
		}
		Eventually(func() bool {
			attachments := &v1alpha1.ExternalIPAttachmentList{}
			addresses := &v1alpha1.ExternalIPList{}
			return indexedClient.List(ctx, attachments, client.InNamespace(networkingNS)) == nil &&
				indexedClient.List(ctx, addresses, client.InNamespace(networkingNS)) == nil &&
				len(attachments.Items) == 0 && len(addresses.Items) == 0
		}).WithTimeout(5 * time.Second).Should(BeTrue())
	})

	It("holds ClusterOrder teardown until the automatic attachment and address are gone, deleting neither", func() {
		co := newClusterOrder(clusterUUID)
		co.Finalizers = []string{osacFinalizer}
		Expect(k8sClient.Create(ctx, co)).To(Succeed())
		DeferCleanup(func() {
			stored := &v1alpha1.ClusterOrder{}
			if k8sClient.Get(ctx, client.ObjectKeyFromObject(co), stored) == nil {
				stored.Finalizers = nil
				Expect(k8sClient.Update(ctx, stored)).To(Succeed())
			}
		})

		eia := newAutoEIA(clusterUUID)
		Expect(k8sClient.Create(ctx, eia)).To(Succeed())
		eip := newAutoEIP(clusterUUID)
		Expect(k8sClient.Create(ctx, eip)).To(Succeed())

		Expect(k8sClient.Delete(ctx, co)).To(Succeed())
		r := newReconciler(networkingNS, eia, eip)
		key := types.NamespacedName{Name: co.Name, Namespace: co.Namespace}
		reconcile := func() ctrl.Result {
			result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			return result
		}
		assertParentHeld := func() {
			stored := &v1alpha1.ClusterOrder{}
			Expect(k8sClient.Get(ctx, key, stored)).To(Succeed())
			Expect(stored.Finalizers).To(ContainElement(osacFinalizer))
			deleting := apimeta.FindStatusCondition(stored.Status.Conditions, v1alpha1.ConditionDeleting)
			Expect(deleting).NotTo(BeNil())
			Expect(deleting.Message).To(Equal(clusterOrderDeletingWaitExternal))
		}

		Expect(reconcile().RequeueAfter).To(Equal(pollInterval))
		assertParentHeld()
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(eia), eia)).To(Succeed())
		Expect(eia.DeletionTimestamp).To(BeNil(), "the ClusterOrder controller deletes no automatic resource")
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(eip), eip)).To(Succeed())
		Expect(eip.DeletionTimestamp).To(BeNil())

		// The fulfillment service deletes the attachment, then the address.
		Expect(k8sClient.Delete(ctx, eia)).To(Succeed())
		waitForIndexedClientObjectGone(eia)
		Expect(reconcile().RequeueAfter).To(Equal(pollInterval))
		assertParentHeld()

		Expect(k8sClient.Delete(ctx, eip)).To(Succeed())
		waitForIndexedClientObjectGone(eip)
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, key, &v1alpha1.ClusterOrder{}))
		}).WithTimeout(5 * time.Second).Should(BeTrue())
	})

	It("preserves deletion when a stale provisioning reconcile writes Ready", func() {
		co := newClusterOrder(clusterUUID)
		co.Finalizers = []string{osacFinalizer}
		co.Status.Phase = v1alpha1.ClusterOrderPhaseProgressing
		Expect(k8sClient.Create(ctx, co)).To(Succeed())
		DeferCleanup(func() {
			stored := &v1alpha1.ClusterOrder{}
			if k8sClient.Get(ctx, client.ObjectKeyFromObject(co), stored) == nil {
				stored.Finalizers = nil
				Expect(k8sClient.Update(ctx, stored)).To(Succeed())
			}
		})
		Expect(k8sClient.Delete(ctx, co)).To(Succeed())

		r := &ClusterOrderReconciler{Client: k8sClient, apiReader: k8sClient}
		_, err := r.patchStatusWithRetry(ctx, client.ObjectKeyFromObject(co), v1alpha1.ClusterOrderStatus{
			Phase: v1alpha1.ClusterOrderPhaseReady,
			ProvisioningJobs: []v1alpha1.JobStatus{{
				JobID:     "provider-create",
				Type:      v1alpha1.JobTypeProvision,
				Timestamp: metav1.Now(),
				State:     v1alpha1.JobStateRunning,
			}},
			Conditions: []metav1.Condition{
				{Type: v1alpha1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonReady},
				{Type: v1alpha1.ConditionDeleting, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonAsExpected},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		latest := &v1alpha1.ClusterOrder{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(co), latest)).To(Succeed())
		Expect(latest.Status.Phase).To(Equal(v1alpha1.ClusterOrderPhaseDeleting))
		Expect(latest.Status.ProvisioningJobs).To(HaveLen(1))
		var deleting *metav1.Condition
		for i := range latest.Status.Conditions {
			if latest.Status.Conditions[i].Type == v1alpha1.ConditionDeleting {
				deleting = &latest.Status.Conditions[i]
				break
			}
		}
		Expect(deleting).NotTo(BeNil())
		Expect(deleting.Status).To(Equal(metav1.ConditionTrue))
		Expect(latest.GetFinalizers()).To(ContainElement(osacFinalizer))
	})

	Context("automaticExternalAccessRemains", func() {
		It("is false without a networking namespace or an owner ID", func() {
			Expect(automaticExternalAccessRemains(ctx, autoExternalIPCleanupTestClient(), "", owner)).To(BeFalse())
			Expect(automaticExternalAccessRemains(ctx, autoExternalIPCleanupTestClient(), networkingNS,
				autoExternalIPOwner{kind: clusterOrderOwner})).To(BeFalse())
		})

		It("counts only automatic resources of the owner", func() {
			foreignEIA := newAutoEIA("other-cluster")
			Expect(k8sClient.Create(ctx, foreignEIA)).To(Succeed())
			manualEIA := newAutoEIA(clusterUUID)
			manualEIA.Labels = nil
			Expect(k8sClient.Create(ctx, manualEIA)).To(Succeed())
			foreignEIP := newAutoEIP("other-cluster")
			Expect(k8sClient.Create(ctx, foreignEIP)).To(Succeed())
			for _, obj := range []client.Object{foreignEIA, manualEIA, foreignEIP} {
				waitForIndexedClientObject(obj)
			}
			Expect(automaticExternalAccessRemains(ctx, autoExternalIPCleanupTestClient(), networkingNS, owner)).To(BeFalse())

			eip := newAutoEIP(clusterUUID)
			Expect(k8sClient.Create(ctx, eip)).To(Succeed())
			waitForIndexedClientObject(eip)
			Expect(automaticExternalAccessRemains(ctx, autoExternalIPCleanupTestClient(), networkingNS, owner)).To(BeTrue())
		})

		It("counts an automatic attachment of the owner", func() {
			eia := newAutoEIA(clusterUUID)
			Expect(k8sClient.Create(ctx, eia)).To(Succeed())
			waitForIndexedClientObject(eia)
			Expect(automaticExternalAccessRemains(ctx, autoExternalIPCleanupTestClient(), networkingNS, owner)).To(BeTrue())
		})
	})
})
