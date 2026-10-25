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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ckv1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

func TestComputeInstanceFeedbackMapsNetworkReady(t *testing.T) {
	ci := &ckv1alpha1.ComputeInstance{}
	ci.SetStatusCondition(ckv1alpha1.ComputeInstanceConditionNetworkReady, metav1.ConditionFalse,
		"the VM is not running", ckv1alpha1.ComputeInstanceReasonVMNotRunning)
	remote := privatev1.ComputeInstance_builder{Status: &privatev1.ComputeInstanceStatus{}}.Build()
	syncCIConditions(ci, remote)
	var found *privatev1.ComputeInstanceCondition
	for _, c := range remote.GetStatus().GetConditions() {
		if c.GetType() == privatev1.ComputeInstanceConditionType_COMPUTE_INSTANCE_CONDITION_TYPE_NETWORK_READY {
			found = c
		}
	}
	if found == nil {
		t.Fatal("NetworkReady not mapped to COMPUTE_INSTANCE_CONDITION_TYPE_NETWORK_READY")
	}
	if found.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_FALSE ||
		found.GetReason() != ckv1alpha1.ComputeInstanceReasonVMNotRunning || found.GetMessage() != "the VM is not running" {
		t.Fatalf("NETWORK_READY = %v/%s/%q", found.GetStatus(), found.GetReason(), found.GetMessage())
	}
}

func newFeedbackCluster() *privatev1.Cluster {
	return privatev1.Cluster_builder{
		Spec:   &privatev1.ClusterSpec{},
		Status: &privatev1.ClusterStatus{},
	}.Build()
}

func clusterCondition(remote *privatev1.Cluster, kind privatev1.ClusterConditionType) *privatev1.ClusterCondition {
	return findExistingClusterCondition(remote, kind)
}

func TestClusterOrderFeedbackCopiesNetworking(t *testing.T) {
	co := &ckv1alpha1.ClusterOrder{}
	co.Status.Phase = ckv1alpha1.ClusterOrderPhaseProgressing
	co.Status.ApiEndpoint = "198.51.100.7"
	co.Status.IngressEndpoint = "10.0.1.200"
	co.Status.APIURL = "https://api.c1.example.com:6443"
	co.Status.ConsoleURL = "https://console-openshift-console.apps.c1.example.com"
	co.SetStatusCondition(ckv1alpha1.ConditionProgressing, metav1.ConditionTrue, "", ckv1alpha1.ReasonWorkersJoining)
	co.SetStatusCondition(string(ckv1alpha1.ClusterOrderConditionManagementPublicationReady), metav1.ConditionTrue, "", ckv1alpha1.ReasonAsExpected)
	co.SetStatusCondition(string(ckv1alpha1.ClusterOrderConditionEndpointsReady), metav1.ConditionFalse,
		"publish_public_dns_api (AAP job 12): DNS zone not found", ckv1alpha1.ClusterOrderReasonDNSPublishFailed)
	co.SetStatusCondition(string(ckv1alpha1.ClusterOrderConditionWorkerNetworkingReady), metav1.ConditionFalse,
		"worker BareMetalInstance w1 failed: NetworkHandoffFailed: no reboot", ckv1alpha1.ClusterOrderReasonWorkerNetworkingFailed)
	remote := newFeedbackCluster()

	if err := syncClusterOrderUpdate(context.Background(), co, remote); err != nil {
		t.Fatal(err)
	}
	status := remote.GetStatus()
	if !status.GetManagementPublicationReady() {
		t.Error("management_publication_ready not copied")
	}
	if status.GetApiEndpoint() != "198.51.100.7" || status.GetIngressEndpoint() != "10.0.1.200" {
		t.Errorf("endpoints = %q/%q", status.GetApiEndpoint(), status.GetIngressEndpoint())
	}
	if status.GetApiUrl() != co.Status.APIURL || status.GetConsoleUrl() != co.Status.ConsoleURL {
		t.Errorf("URLs = %q/%q", status.GetApiUrl(), status.GetConsoleUrl())
	}
	progressing := clusterCondition(remote, privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_PROGRESSING)
	if progressing == nil || progressing.GetMessage() != "DNSPublishFailed: publish_public_dns_api (AAP job 12): DNS zone not found" {
		t.Errorf("PROGRESSING message = %q", progressing.GetMessage())
	}
	failed := clusterCondition(remote, privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_WORKER_PROVISIONING_FAILED)
	if failed == nil || failed.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_TRUE ||
		failed.GetMessage() != "worker network attachment failed" {
		t.Errorf("WORKER_PROVISIONING_FAILED = %v", failed)
	}
}

func TestClusterOrderFeedbackPublicationFalseBlocksWorkers(t *testing.T) {
	co := &ckv1alpha1.ClusterOrder{}
	co.Status.Phase = ckv1alpha1.ClusterOrderPhaseProgressing
	co.SetStatusCondition(string(ckv1alpha1.ClusterOrderConditionManagementPublicationReady), metav1.ConditionFalse,
		"waiting for the management API VIP", ckv1alpha1.ClusterOrderReasonManagementPublicationPending)
	remote := newFeedbackCluster()
	if err := syncClusterOrderUpdate(context.Background(), co, remote); err != nil {
		t.Fatal(err)
	}
	if remote.GetStatus().GetManagementPublicationReady() {
		t.Error("management_publication_ready must be false")
	}
	blocked := clusterCondition(remote, privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_WORKER_PROVISIONING_BLOCKED)
	if blocked == nil || blocked.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_TRUE {
		t.Fatalf("WORKER_PROVISIONING_BLOCKED = %v", blocked)
	}
}

func TestClusterOrderFeedbackClusterHealthyDrivesDegraded(t *testing.T) {
	co := &ckv1alpha1.ClusterOrder{}
	co.Status.Phase = ckv1alpha1.ClusterOrderPhaseReady
	co.SetStatusCondition(string(ckv1alpha1.ClusterOrderConditionClusterHealthy), metav1.ConditionFalse,
		"ClusterOperator ingress is not Available", clusterHealthyReasonUnhealthy)
	remote := newFeedbackCluster()
	if err := syncClusterOrderUpdate(context.Background(), co, remote); err != nil {
		t.Fatal(err)
	}
	degraded := clusterCondition(remote, privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_DEGRADED)
	if degraded == nil || degraded.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_TRUE ||
		degraded.GetReason() != clusterUnhealthyReason || degraded.GetMessage() != "ClusterOperator ingress is not Available" {
		t.Fatalf("DEGRADED = %v", degraded)
	}

	co.SetStatusCondition(string(ckv1alpha1.ClusterOrderConditionClusterHealthy), metav1.ConditionTrue, "", clusterHealthyReasonHealthy)
	if err := syncClusterOrderUpdate(context.Background(), co, remote); err != nil {
		t.Fatal(err)
	}
	if degraded.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_FALSE {
		t.Fatalf("DEGRADED not cleared: %v", degraded)
	}
}

func TestClusterOrderFeedbackDeletingShowsBlockingStep(t *testing.T) {
	co := &ckv1alpha1.ClusterOrder{}
	co.Status.Phase = ckv1alpha1.ClusterOrderPhaseDeleting
	co.SetStatusCondition(ckv1alpha1.ConditionDeleting, metav1.ConditionTrue,
		clusterOrderDeletingReleaseIngress+": release_cluster_ingress_ip (fabric) attempt 2 failed: busy", ckv1alpha1.ReasonDeleting)
	remote := newFeedbackCluster()
	if err := syncClusterOrderDelete(context.Background(), co, remote); err != nil {
		t.Fatal(err)
	}
	progressing := clusterCondition(remote, privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_PROGRESSING)
	if progressing == nil || progressing.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_TRUE ||
		progressing.GetMessage() != "releasing the guest ingress address: release_cluster_ingress_ip (fabric) attempt 2 failed: busy" {
		t.Fatalf("PROGRESSING while deleting = %v", progressing)
	}
	if remote.GetStatus().GetState() != privatev1.ClusterState_CLUSTER_STATE_DELETING {
		t.Fatalf("state = %v", remote.GetStatus().GetState())
	}
}
