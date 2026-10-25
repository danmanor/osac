// Copyright (c) 2026 Red Hat Inc.
// SPDX-License-Identifier: Apache-2.0

package baremetalworker

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

func networkReadyCondition(state privatev1.ConditionStatus, reason string, at time.Time) *privatev1.BareMetalInstanceCondition {
	return privatev1.BareMetalInstanceCondition_builder{
		Type:               privatev1.BareMetalInstanceConditionType_BARE_METAL_INSTANCE_CONDITION_TYPE_NETWORK_READY,
		Status:             state,
		Reason:             &reason,
		Message:            ptrTo(reason + " message"),
		LastTransitionTime: timestamppb.New(at),
	}.Build()
}

func ptrTo[T any](v T) *T { return &v }

func networkedBMI(id, ip string, conditions ...*privatev1.BareMetalInstanceCondition) *privatev1.BareMetalInstance {
	var statuses []*privatev1.BareMetalNetworkAttachmentStatus
	if ip != "" {
		statuses = []*privatev1.BareMetalNetworkAttachmentStatus{
			privatev1.BareMetalNetworkAttachmentStatus_builder{IpAddress: ip}.Build(),
		}
	}
	return privatev1.BareMetalInstance_builder{
		Id: id,
		Status: privatev1.BareMetalInstanceStatus_builder{
			Conditions:                conditions,
			NetworkAttachmentStatuses: statuses,
		}.Build(),
	}.Build()
}

func agentWithInterface(name, mac string, addresses ...any) unstructured.Unstructured {
	agent := unstructured.Unstructured{Object: map[string]any{}}
	agent.SetName(name)
	_ = unstructured.SetNestedSlice(agent.Object, []any{
		map[string]any{"macAddress": mac, "ipV4Addresses": addresses},
	}, "status", "inventory", "interfaces")
	return agent
}

type networkAttachmentCountingReader struct {
	client.Reader
	listCalls int
	listErr   error
}

func (r *networkAttachmentCountingReader) List(
	ctx context.Context, list client.ObjectList, opts ...client.ListOption,
) error {
	if _, ok := list.(*v1alpha1.NetworkAttachmentList); ok {
		r.listCalls++
		if r.listErr != nil {
			return r.listErr
		}
	}
	return r.Reader.List(ctx, list, opts...)
}

var _ = Describe("CaaS worker BMI create request", func() {
	It("creates the worker in tenant system with API ID references and the fabric interface", func() {
		r, _, co := nodeSetHarness("bmi-request", nodeRequest("standard", 1))
		co.Spec.NetworkAttachment = &v1alpha1.ClusterNetworkAttachment{
			SubnetRef: "subnet-id", SecurityGroupRefs: []string{"sg-user", "sg-auto"},
		}
		nr := co.Spec.NodeRequests[0]
		nr.FabricInterface = "ens2f0"
		cluster := privatev1.Cluster_builder{
			Id:       "cluster-api-id",
			Metadata: privatev1.Metadata_builder{Tenant: "customer", Project: "customer-project"}.Build(),
		}.Build()

		req := r.buildBMICreateRequest(co, cluster, nr, "worker-1", nil, "{}")

		Expect(req.GetMetadata().GetTenant()).To(Equal("system"))
		Expect(req.GetMetadata().GetProject()).To(BeEmpty(), "a system worker has no project")
		Expect(req.GetMetadata().GetAnnotations()).To(HaveKeyWithValue("osac.openshift.io/cluster-api-id", "cluster-api-id"))
		Expect(req.GetMetadata().GetAnnotations()).To(HaveKeyWithValue(ownerReferenceAnnotation, "ClusterOrder/bmi-request"))
		Expect(req.GetStatus().GetCluster().GetId()).To(Equal("cluster-api-id"))
		attachments := req.GetSpec().GetNetworkAttachments()
		Expect(attachments).To(HaveLen(1))
		Expect(attachments[0].GetSubnet().GetId()).To(Equal("subnet-id"))
		Expect(attachments[0].GetSubnet().GetName()).To(BeEmpty())
		Expect(attachments[0].GetSecurityGroups()).To(HaveLen(2))
		Expect(attachments[0].GetSecurityGroups()[0].GetId()).To(Equal("sg-user"))
		Expect(attachments[0].GetSecurityGroups()[1].GetId()).To(Equal("sg-auto"), "the automatic group stays last")
		Expect(attachments[0].GetInterface()).To(Equal("ens2f0"))
		Expect(attachments[0].GetPrimary()).To(BeTrue())
	})

	It("refuses to create an attached worker without a fabric interface", func() {
		r, fc, co := nodeSetHarness("bmi-no-fabric", nodeRequest("standard", 1))
		co.Spec.NetworkAttachment = &v1alpha1.ClusterNetworkAttachment{SubnetRef: "subnet-id"}
		Expect(r.Update(context.Background(), co)).To(Succeed())
		_, err := runWorkerCapacityStage(r, co) // Reservation.
		Expect(err).NotTo(HaveOccurred())
		Expect(r.apiReader.Get(context.Background(), client.ObjectKeyFromObject(co), co)).To(Succeed())
		_, err = runWorkerCapacityStage(r, co)
		Expect(err).To(MatchError(ContainSubstring("has no fabricInterface")))
		Expect(fc.names).To(BeEmpty())
	})

	It("checks the typed Cluster reference and annotation for ownership", func() {
		_, _, co := nodeSetHarness("bmi-owner", nodeRequest("standard", 1))
		bmi := privatev1.BareMetalInstance_builder{Id: "id", Metadata: privatev1.Metadata_builder{
			Name: "w", Tenant: workerBMITenant, Labels: map[string]string{clusterOrderLabel: co.Name},
			Annotations: map[string]string{ownerReferenceAnnotation: "ClusterOrder/" + co.Name, clusterAPIIDAnnotation: "other-cluster"},
		}.Build(), Status: privatev1.BareMetalInstanceStatus_builder{
			Cluster: privatev1.ClusterLocalReference_builder{Id: co.Labels[clusterOrderIDLabel]}.Build(),
		}.Build()}.Build()
		Expect(checkWorkerBMI(co, workerBMITenant, "w", bmi)).To(HaveOccurred())
		bmi.GetMetadata().GetAnnotations()[clusterAPIIDAnnotation] = co.Labels[clusterOrderIDLabel]
		Expect(checkWorkerBMI(co, workerBMITenant, "w", bmi)).To(Succeed())
		bmi.SetStatus(privatev1.BareMetalInstanceStatus_builder{
			Cluster: privatev1.ClusterLocalReference_builder{Id: "other-cluster"}.Build(),
		}.Build())
		Expect(checkWorkerBMI(co, workerBMITenant, "w", bmi)).To(HaveOccurred())
		bmi.SetStatus(privatev1.BareMetalInstanceStatus_builder{
			Cluster: privatev1.ClusterLocalReference_builder{Id: co.Labels[clusterOrderIDLabel]}.Build(),
		}.Build())
		Expect(checkWorkerBMI(co, "customer", "w", bmi)).To(HaveOccurred(), "a worker outside tenant system is foreign")
	})
})

var _ = Describe("Worker creation gate", func() {
	DescribeTable("blocks creation", func(mutate func(*v1alpha1.ClusterOrder), want string) {
		co := &v1alpha1.ClusterOrder{}
		allowWorkerCreation(co)
		Expect(workerCreationBlocker(co)).To(BeEmpty())
		mutate(co)
		Expect(workerCreationBlocker(co)).To(ContainSubstring(want))
	},
		Entry("HostedCluster missing", func(co *v1alpha1.ClusterOrder) {
			co.SetStatusCondition(v1alpha1.ConditionControlPlaneCreated, metav1.ConditionFalse, "", "Pending")
		}, "HostedCluster"),
		Entry("publication not ready", func(co *v1alpha1.ClusterOrder) {
			co.SetStatusCondition(string(v1alpha1.ClusterOrderConditionManagementPublicationReady), metav1.ConditionFalse, "", "Pending")
		}, "ManagementPublicationReady"),
		Entry("order failed", func(co *v1alpha1.ClusterOrder) {
			co.Status.Phase = v1alpha1.ClusterOrderPhaseFailed
		}, "failed"),
	)

	It("neither reserves nor creates while the gate is closed", func() {
		r, fc, co := nodeSetHarness("gate-closed", nodeRequest("standard", 1))
		co.SetStatusCondition(string(v1alpha1.ClusterOrderConditionManagementPublicationReady), metav1.ConditionFalse, "", "Pending")
		Expect(r.Status().Update(context.Background(), co)).To(Succeed())
		ctx := context.Background()
		created, res, err := r.reconcileWorkerCreation(ctx, co, authoritativeTestCluster(ctx, r, co),
			capacityObservation(fc), infraEnvEvidence{})
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeFalse())
		Expect(res.RequeueAfter).To(Equal(workerCreationGateRecheck))
		Expect(r.apiReader.Get(ctx, client.ObjectKeyFromObject(co), co)).To(Succeed())
		Expect(co.Status.Workers).To(BeEmpty())
		Expect(fc.names).To(BeEmpty())
	})
})

var _ = Describe("BMI Create refused with FAILED_PRECONDITION", func() {
	It("keeps the slot, counts no failure and retries after a backoff", func() {
		r, fc, co := nodeSetHarness("precondition", nodeRequest("standard", 1))
		co.Annotations = map[string]string{tenantAnnotationKey: "tenant"}
		Expect(r.Update(context.Background(), co)).To(Succeed())
		fc.createErr = status.Error(codes.FailedPrecondition, "the cluster is not authorized for workers yet")
		step := func() (time.Duration, error) {
			res, err := runWorkerCapacityStage(r, co)
			Expect(r.apiReader.Get(context.Background(), client.ObjectKeyFromObject(co), co)).To(Succeed())
			return res.RequeueAfter, err
		}
		_, err := step() // Reservation.
		Expect(err).NotTo(HaveOccurred())
		after, err := step() // Attempt journal, refused Create, re-arm.
		Expect(err).NotTo(HaveOccurred())
		Expect(after).To(Equal(bmiCreatePreconditionBackoff))
		Expect(fc.names).To(HaveLen(1))
		w := co.Status.Workers[0]
		Expect(w.Phase).To(Equal(workerPhaseProvisioning))
		Expect(w.BMICreateState).To(Equal(v1alpha1.WorkerBMICreateStateReserved))
		Expect(w.AttemptCount).To(BeZero())
		Expect(w.LastFailureReason).To(BeEmpty())

		fc.createErr = nil
		_, err = step()
		Expect(err).NotTo(HaveOccurred())
		Expect(fc.names).To(HaveLen(2), "the same slot is created on the next attempt")
		Expect(fc.names[1]).To(Equal(fc.names[0]))
		Expect(co.Status.Workers[0].BareMetalInstance.ID).NotTo(BeEmpty())
	})
})

var _ = Describe("Worker networking", func() {
	waitingWorker := func(id string, started time.Time) v1alpha1.WorkerStatus {
		w := newWorkerStatus("standard", "standard", "slot", id, workerPhaseWaitingForAgent)
		origin := metav1.NewTime(started)
		w.AttemptStartedAt = &origin
		return w
	}

	It("fails a worker whose BMI network failed, with the long backoff", func() {
		r, _, co := nodeSetHarness("network-failed", nodeRequest("standard", 1))
		o := indexWorkerBMIs([]*privatev1.BareMetalInstance{networkedBMI("bmi-1", "",
			networkReadyCondition(privatev1.ConditionStatus_CONDITION_STATUS_FALSE, "NetworkHandoffFailed", time.Now()))})
		workers := r.checkBMINetworkFailures(context.Background(), co,
			[]v1alpha1.WorkerStatus{waitingWorker("bmi-1", time.Now())}, o, time.Now())
		Expect(workers[0].Phase).To(Equal(workerPhaseFailed))
		Expect(workers[0].LastFailureReason).To(Equal("NetworkHandoffFailed"))
		Expect(ClassifyFailure(workers[0].LastFailureReason)).To(Equal(FailureCategoryResource))
	})

	It("ignores a BMI whose network is still progressing", func() {
		r, _, co := nodeSetHarness("network-pending", nodeRequest("standard", 1))
		o := indexWorkerBMIs([]*privatev1.BareMetalInstance{networkedBMI("bmi-1", "",
			networkReadyCondition(privatev1.ConditionStatus_CONDITION_STATUS_FALSE, "LeasePending", time.Now()))})
		workers := r.checkBMINetworkFailures(context.Background(), co,
			[]v1alpha1.WorkerStatus{waitingWorker("bmi-1", time.Now())}, o, time.Now())
		Expect(workers[0].Phase).To(Equal(workerPhaseWaitingForAgent))
	})

	It("starts the registration clock at the latest network, publication and DNS time", func() {
		r, _, co := nodeSetHarness("deadline", nodeRequest("standard", 1))
		r.SetNetworking("osac-networking", true)
		co.Spec.NetworkAttachment = &v1alpha1.ClusterNetworkAttachment{SubnetRef: "subnet-id"}
		base := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
		w := waitingWorker("bmi-1", base)
		for i := range co.Status.Conditions {
			if co.Status.Conditions[i].Type == string(v1alpha1.ClusterOrderConditionManagementPublicationReady) {
				co.Status.Conditions[i].LastTransitionTime = metav1.NewTime(base.Add(20 * time.Minute))
			}
		}

		notReady := indexWorkerBMIs([]*privatev1.BareMetalInstance{networkedBMI("bmi-1", "10.0.0.5",
			networkReadyCondition(privatev1.ConditionStatus_CONDITION_STATUS_FALSE, "LeasePending", base))})
		_, started := r.registrationStart(context.Background(), co, w, notReady)
		Expect(started).To(BeFalse(), "no clock before the BMI network is ready")

		networkAt := base.Add(10 * time.Minute)
		ready := indexWorkerBMIs([]*privatev1.BareMetalInstance{networkedBMI("bmi-1", "10.0.0.5",
			networkReadyCondition(privatev1.ConditionStatus_CONDITION_STATUS_TRUE, "IPDiscovered", networkAt))})
		_, started = r.registrationStart(context.Background(), co, w, ready)
		Expect(started).To(BeFalse(), "no clock before the API records are published")

		published := metav1.NewTime(base.Add(50 * time.Minute))
		co.Status.APIRecordsPublishedTime = &published
		start, started := r.registrationStart(context.Background(), co, w, ready)
		Expect(started).To(BeTrue())
		Expect(start).To(BeTemporally("==", published.Time))

		workers := r.checkAgentRegistrationTimeout(context.Background(), co, []v1alpha1.WorkerStatus{w}, ready,
			published.Add(agentRegistrationTimeout-time.Minute))
		Expect(workers[0].Phase).To(Equal(workerPhaseWaitingForAgent))
		workers = r.checkAgentRegistrationTimeout(context.Background(), co, []v1alpha1.WorkerStatus{w}, ready,
			published.Add(agentRegistrationTimeout+time.Minute))
		Expect(workers[0].Phase).To(Equal(workerPhaseFailed))
	})

	It("lists attachments once and binds only Agents matching each worker IP and MAC", func() {
		r, _, co := nodeSetHarness("binding-gate", nodeRequest("standard", 3))
		r.SetNetworking("test", true)
		co.Spec.NetworkAttachment = &v1alpha1.ClusterNetworkAttachment{SubnetRef: "subnet-id"}
		ctx := context.Background()
		createAttachment := func(namespace, id, mac string) {
			Expect(r.Create(ctx, &v1alpha1.NetworkAttachment{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace,
					Name: v1alpha1.NetworkAttachmentName(v1alpha1.NetworkAttachmentTargetBareMetalInstance, id)},
				Spec: v1alpha1.NetworkAttachmentSpec{Interface: v1alpha1.NetworkAttachmentInterface{
					Type: v1alpha1.NetworkInterfacePhysical, Name: "ens2f0", MACAddress: mac}},
			})).To(Succeed())
		}
		createAttachment("test", "bmi-1", "AA:BB:CC:DD:EE:01")
		createAttachment("test", "bmi-2", "aa:bb:cc:dd:ee:02")
		// A matching name outside the configured namespace must not satisfy the gate.
		createAttachment("other", "bmi-3", "aa:bb:cc:dd:ee:03")
		reader := &networkAttachmentCountingReader{Reader: r.apiReader}
		r.apiReader = reader
		o := indexWorkerBMIs([]*privatev1.BareMetalInstance{
			networkedBMI("bmi-1", "10.0.0.5"),
			networkedBMI("bmi-2", "10.0.0.6"),
			networkedBMI("bmi-3", "10.0.0.7"),
		})
		workers := []v1alpha1.WorkerStatus{
			waitingWorker("bmi-1", time.Now()),
			waitingWorker("bmi-2", time.Now()),
			waitingWorker("bmi-3", time.Now()),
		}
		agents := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{
			agentWithInterface("bound-1", "aa:bb:cc:dd:ee:01", "10.0.0.5/24"),
			agentWithInterface("bound-2", "aa:bb:cc:dd:ee:02", "10.0.0.6/24"),
			agentWithInterface("wrong-address", "aa:bb:cc:dd:ee:01", "192.168.1.9/24"),
			agentWithInterface("wrong-mac", "aa:bb:cc:dd:ee:99", "10.0.0.5/24"),
			agentWithInterface("outside-namespace", "aa:bb:cc:dd:ee:03", "10.0.0.7/24"),
		}}
		original := func(context.Context, string) []string { return []string{"aa:bb:cc:dd:ee:99"} }

		_, filtered, err := r.gatedAgentMatch(ctx, co, workers, agents, o, original)
		Expect(err).NotTo(HaveOccurred())
		Expect(reader.listCalls).To(Equal(0), "do not list before API records are published")
		Expect(filtered).To(BeIdenticalTo(agents))

		published := metav1.Now()
		co.Status.APIRecordsPublishedTime = &published
		macs, filtered, err := r.gatedAgentMatch(ctx, co, workers, agents, o, original)
		Expect(err).NotTo(HaveOccurred())
		Expect(reader.listCalls).To(Equal(1), "list the configured networking namespace once")
		Expect(macs(ctx, "bmi-1")).To(Equal([]string{"aa:bb:cc:dd:ee:01"}))
		Expect(macs(ctx, "bmi-2")).To(Equal([]string{"aa:bb:cc:dd:ee:02"}))
		Expect(macs(ctx, "bmi-3")).To(BeEmpty(), "missing in-namespace attachment is skipped")
		names := make([]string, 0, len(filtered.Items))
		for _, agent := range filtered.Items {
			names = append(names, agent.GetName())
		}
		Expect(names).To(Equal([]string{"bound-1", "bound-2"}))
	})

	It("propagates NetworkAttachment list errors", func() {
		r, _, co := nodeSetHarness("binding-gate-list-error", nodeRequest("standard", 1))
		r.SetNetworking("test", true)
		co.Spec.NetworkAttachment = &v1alpha1.ClusterNetworkAttachment{SubnetRef: "subnet-id"}
		published := metav1.Now()
		co.Status.APIRecordsPublishedTime = &published
		reader := &networkAttachmentCountingReader{Reader: r.apiReader, listErr: errors.New("list unavailable")}
		r.apiReader = reader
		o := indexWorkerBMIs([]*privatev1.BareMetalInstance{networkedBMI("bmi-1", "10.0.0.5")})
		_, _, err := r.gatedAgentMatch(context.Background(), co,
			[]v1alpha1.WorkerStatus{waitingWorker("bmi-1", time.Now())},
			&unstructured.UnstructuredList{}, o, func(context.Context, string) []string { return nil })
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("list unavailable"))
		Expect(reader.listCalls).To(Equal(1))
	})

	It("leaves correlation unchanged with provisioning disabled", func() {
		r, _, co := nodeSetHarness("binding-disabled", nodeRequest("standard", 1))
		co.Spec.NetworkAttachment = &v1alpha1.ClusterNetworkAttachment{SubnetRef: "subnet-id"}
		agents := &unstructured.UnstructuredList{}
		original := func(context.Context, string) []string { return []string{"aa"} }
		macs, filtered, err := r.gatedAgentMatch(context.Background(), co, nil, agents, nil, original)
		Expect(err).NotTo(HaveOccurred())
		Expect(macs(context.Background(), "x")).To(Equal([]string{"aa"}))
		Expect(filtered).To(BeIdenticalTo(agents))
	})
})
