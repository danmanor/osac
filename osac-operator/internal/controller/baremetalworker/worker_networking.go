// Copyright (c) 2026 Red Hat Inc.
// SPDX-License-Identifier: Apache-2.0

package baremetalworker

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

// Worker networking (CaaS design, Worker BMIs): the binding gate, the
// registration deadline and the BMI network failures.

// bmiNetworkFailureReasons are the BMI NetworkReady reasons that fail a worker.
// They are recorded as the worker's lastFailureReason and retried with the long
// (resource) backoff.
var bmiNetworkFailureReasons = map[string]bool{
	"NetworkAttachmentFailed": true,
	"NetworkHandoffFailed":    true,
	"IPDiscoveryFailed":       true,
}

// SetNetworking configures provider networking: the namespace of the
// NetworkAttachment CRs and whether provisioning is enabled
// (global.networking.provisioningEnabled). With provisioning disabled no
// binding gate applies.
func (r *Reconciler) SetNetworking(networkingNamespace string, provisioningEnabled bool) {
	r.networkingNamespace = networkingNamespace
	r.provisioningEnabled = provisioningEnabled
}

// networkGateApplies reports whether the order's workers are bound only after
// their network is ready: provisioning is enabled and the order attaches to a
// Subnet.
func (r *Reconciler) networkGateApplies(co *v1alpha1.ClusterOrder) bool {
	return r.provisioningEnabled && co.Spec.NetworkAttachment != nil && co.Spec.NetworkAttachment.SubnetRef != ""
}

// bmiNetworkReady returns the BMI's NetworkReady condition, or nil.
func bmiNetworkReady(bmi *privatev1.BareMetalInstance) *privatev1.BareMetalInstanceCondition {
	for _, c := range bmi.GetStatus().GetConditions() {
		if c.GetType() == privatev1.BareMetalInstanceConditionType_BARE_METAL_INSTANCE_CONDITION_TYPE_NETWORK_READY {
			return c
		}
	}
	return nil
}

// observedBMI returns the worker's BMI from the invocation observation, or nil.
func (r *Reconciler) observedBMI(ctx context.Context, o *workerObservation, w v1alpha1.WorkerStatus) *privatev1.BareMetalInstance {
	if o == nil || w.BareMetalInstance.ID == "" {
		return nil
	}
	if r.fulfillment == nil {
		return o.byID[w.BareMetalInstance.ID]
	}
	bmi, err := o.getBMI(ctx, r.fulfillment, w.BareMetalInstance.ID)
	if err != nil {
		return nil
	}
	return bmi
}

// checkBMINetworkFailures fails WaitingForAgent workers whose BMI reports a
// network failure, recording the BMI reason as lastFailureReason.
func (r *Reconciler) checkBMINetworkFailures(
	ctx context.Context, co *v1alpha1.ClusterOrder, workers []v1alpha1.WorkerStatus, o *workerObservation, now time.Time,
) []v1alpha1.WorkerStatus {
	for i := range workers {
		w := &workers[i]
		if !eligibleForAgentObservation(*w) || w.Phase != workerPhaseWaitingForAgent {
			continue
		}
		cond := bmiNetworkReady(r.observedBMI(ctx, o, *w))
		if cond == nil || cond.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_FALSE ||
			!bmiNetworkFailureReasons[cond.GetReason()] {
			continue
		}
		failTime := metav1.NewTime(now)
		w.Phase = workerPhaseFailed
		w.LastFailureReason = cond.GetReason()
		w.LastFailureMessage = cond.GetMessage()
		w.LastFailureTime = &failTime
		observeProvisioningFailure(tenantOf(co), *w)
		ctrllog.FromContext(ctx).Info("worker network failed", "worker", w.Name, "reason", cond.GetReason())
		r.recorder.Eventf(co, nil, corev1.EventTypeWarning, cond.GetReason(), "CheckNetwork",
			"worker %s: BMI network failed: %s", w.Name, cond.GetMessage())
	}
	return workers
}

// registrationStart returns when the worker's Agent registration clock starts,
// or false when it has not started. The deadline is agentRegistrationTimeout
// after the latest of the attempt origin, the BMI NetworkReady=True transition,
// the last ManagementPublicationReady=True transition and the first API record
// publication. With the network gate the clock starts only once the BMI network
// is ready and the API records are published.
func (r *Reconciler) registrationStart(
	ctx context.Context, co *v1alpha1.ClusterOrder, w v1alpha1.WorkerStatus, o *workerObservation,
) (time.Time, bool) {
	if w.AttemptStartedAt == nil {
		return time.Time{}, false
	}
	start := w.AttemptStartedAt.Time
	later := func(t time.Time) {
		if t.After(start) {
			start = t
		}
	}
	if c := apimeta.FindStatusCondition(co.Status.Conditions, string(v1alpha1.ClusterOrderConditionManagementPublicationReady)); c != nil &&
		c.Status == metav1.ConditionTrue {
		later(c.LastTransitionTime.Time)
	}
	if co.Status.APIRecordsPublishedTime != nil {
		later(co.Status.APIRecordsPublishedTime.Time)
	}
	cond := bmiNetworkReady(r.observedBMI(ctx, o, w))
	networkReady := cond != nil && cond.GetStatus() == privatev1.ConditionStatus_CONDITION_STATUS_TRUE
	if networkReady && cond.GetLastTransitionTime() != nil {
		later(cond.GetLastTransitionTime().AsTime())
	}
	if r.networkGateApplies(co) && (!networkReady || co.Status.APIRecordsPublishedTime == nil) {
		return time.Time{}, false
	}
	return start, true
}

// gatedAgentMatch narrows Agent correlation to the binding gate: the API
// records are published and the Agent inventory interface whose MAC equals the
// worker NetworkAttachment's spec.interface.macAddress has an ipV4Addresses
// entry whose IP equals the BMI's networkAttachmentStatuses[0].ipAddress. It
// returns the resolver that yields only that MAC and the Agents that pass the
// IP check. Without the network gate both inputs are returned unchanged.
func (r *Reconciler) gatedAgentMatch(
	ctx context.Context, co *v1alpha1.ClusterOrder, workers []v1alpha1.WorkerStatus,
	agents *unstructured.UnstructuredList, o *workerObservation, macs MACResolver,
) (MACResolver, *unstructured.UnstructuredList, error) {
	if !r.networkGateApplies(co) {
		return macs, agents, nil
	}
	none := func(context.Context, string) []string { return nil }
	if co.Status.APIRecordsPublishedTime == nil {
		return none, agents, nil
	}
	ipByBMI := map[string]string{}
	for _, w := range workers {
		if !eligibleForAgentObservation(w) || w.Phase != workerPhaseWaitingForAgent {
			continue
		}
		bmi := r.observedBMI(ctx, o, w)
		statuses := bmi.GetStatus().GetNetworkAttachmentStatuses()
		if len(statuses) == 0 || statuses[0].GetIpAddress() == "" {
			continue
		}
		ipByBMI[w.BareMetalInstance.ID] = statuses[0].GetIpAddress()
	}
	if len(ipByBMI) == 0 {
		return none, &unstructured.UnstructuredList{}, nil
	}
	attachments := &v1alpha1.NetworkAttachmentList{}
	if err := r.apiReader.List(ctx, attachments, client.InNamespace(r.networkingNamespace)); err != nil {
		return nil, nil, fmt.Errorf("listing NetworkAttachments in namespace %q: %w", r.networkingNamespace, err)
	}
	attachmentsByName := make(map[string]*v1alpha1.NetworkAttachment, len(attachments.Items))
	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		attachmentsByName[attachment.Name] = attachment
	}

	ipByMAC := make(map[string]string, len(ipByBMI))
	macByBMI := make(map[string]string, len(ipByBMI))
	for bmiID, ip := range ipByBMI {
		name := v1alpha1.NetworkAttachmentName(v1alpha1.NetworkAttachmentTargetBareMetalInstance, bmiID)
		na := attachmentsByName[name]
		if na == nil {
			continue
		}
		mac := strings.ToLower(na.Spec.Interface.MACAddress)
		if mac == "" {
			continue
		}
		ipByMAC[mac] = ip
		macByBMI[bmiID] = mac
	}
	filtered := &unstructured.UnstructuredList{}
	for _, agent := range agents.Items {
		if agentHasAddress(&agent, ipByMAC) {
			filtered.Items = append(filtered.Items, agent)
		}
	}
	resolver := func(_ context.Context, id string) []string {
		if mac, ok := macByBMI[id]; ok {
			return []string{mac}
		}
		return nil
	}
	return resolver, filtered, nil
}

// agentHasAddress reports whether one of the Agent's inventory interfaces has a
// MAC in ipByMAC and an ipV4Addresses entry with that MAC's IP.
func agentHasAddress(agent *unstructured.Unstructured, ipByMAC map[string]string) bool {
	interfaces, _, _ := unstructured.NestedSlice(agent.Object, "status", "inventory", "interfaces")
	for _, item := range interfaces {
		iface, ok := item.(map[string]any)
		if !ok {
			continue
		}
		mac, _ := iface["macAddress"].(string)
		want, ok := ipByMAC[strings.ToLower(mac)]
		if !ok {
			continue
		}
		addresses, _ := iface["ipV4Addresses"].([]any)
		for _, address := range addresses {
			cidr, _ := address.(string)
			ip, _, err := net.ParseCIDR(cidr)
			if err != nil {
				ip = net.ParseIP(cidr)
			}
			if ip != nil && ip.Equal(net.ParseIP(want)) {
				return true
			}
		}
	}
	return false
}
