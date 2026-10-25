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
	"fmt"
	"time"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	bmfov1alpha1 "github.com/osac-project/osac/bare-metal-fulfillment-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/internal/networktask"
	"github.com/osac-project/osac/osac-operator/pkg/provisioning"
)

// Tasks of the ClusterOrder in status.jobs (CaaS design, API Extensions).
const (
	// Guest endpoint job tasks (manager none, template <prefix>-cluster-endpoints).
	configureGuestIngressTask   = "configure_guest_ingress"
	publishPublicDNSAPITask     = "publish_public_dns_api"
	removePublicDNSAPITask      = "remove_public_dns_api"
	publishPublicDNSIngressTask = "publish_public_dns_ingress"
	removePublicDNSIngressTask  = "remove_public_dns_ingress"
	// postInstallTask is the hosted cluster post-install job (manager none),
	// launched once the ClusterOrder is Ready.
	postInstallTask = "post_install"

	// publicDNSFinalizer keeps an automatic ExternalIPAttachment until its
	// public DNS records are removed.
	publicDNSFinalizer = osacPrefix + "/public-dns"
	// bareMetalWorkerFinalizer is held by the BareMetalWorkerReconciler until
	// every worker BMI record is gone.
	bareMetalWorkerFinalizer = osacPrefix + "/baremetalworker-finalizer"

	// clusterNetworkingRecheck is the recheck interval of the publication gate,
	// the guest API test and the automatic external access requests.
	clusterNetworkingRecheck = 30 * time.Second

	eventReasonEndpointsDiscovered     = "EndpointsDiscovered"
	eventReasonEndpointDNSFailed       = "EndpointDNSFailed"
	eventReasonWorkerNetworkingFailed  = "WorkerNetworkingFailed"
	reasonClusterOperatorsProgressing  = "ClusterOperatorsProgressing"
	clusterHealthyReasonHealthy        = "Healthy"
	clusterHealthyReasonUnhealthy      = "Unhealthy"
	clusterOrderDeletingWaitExternal   = "waiting for the automatic external access of the cluster to be removed"
	clusterOrderDeletingWaitJobs       = "waiting for the cancelled cluster jobs to end"
	clusterOrderDeletingReleaseIngress = "releasing the guest ingress address"
)

// dnsTasks maps an automatic ExternalIPAttachment endpoint to its publish and remove tasks.
var dnsTasks = map[string][2]string{
	autoCreatedEndpointAPI:     {publishPublicDNSAPITask, removePublicDNSAPITask},
	autoCreatedEndpointIngress: {publishPublicDNSIngressTask, removePublicDNSIngressTask},
}

// clusterNetworkingConditions are owned by the networking pass of the
// ClusterOrder controller; the main status patch never writes them.
var clusterNetworkingConditions = map[string]bool{
	string(v1alpha1.ClusterOrderConditionManagementPublicationReady): true,
	string(v1alpha1.ClusterOrderConditionEndpointsReady):             true,
	string(v1alpha1.ClusterOrderConditionWorkerNetworkingReady):      true,
	string(v1alpha1.ClusterOrderConditionClusterHealthy):             true,
}

// clusterNetworking is the state one networking pass works on.
type clusterNetworking struct {
	co        *v1alpha1.ClusterOrder
	hc        *hypershiftv1beta1.HostedCluster
	clusterID string
	enabled   bool
	subnet    *v1alpha1.Subnet
	vn        *v1alpha1.VirtualNetwork
	result    ctrl.Result
}

func (n *clusterNetworking) requeue(after time.Duration) {
	n.result = mergeReconcileResult(n.result, ctrl.Result{RequeueAfter: after})
}

func (n *clusterNetworking) setCondition(conditionType v1alpha1.ClusterOrderConditionType, status metav1.ConditionStatus, reason, message string) {
	n.co.SetStatusCondition(string(conditionType), status, message, reason)
}

// reconcileNetworking is the networking pass of the ClusterOrder controller
// (CaaS networking design). It runs after the main pass on a fresh read of the
// ClusterOrder, launches and tracks the ClusterOrder's jobs through the
// network task engine (status.jobs, status.taskInput) and owns the conditions
// ManagementPublicationReady, EndpointsReady, WorkerNetworkingReady and
// ClusterHealthy. Every status write is conditional on the resourceVersion read.
func (r *ClusterOrderReconciler) reconcileNetworking(ctx context.Context, key client.ObjectKey) (ctrl.Result, error) {
	co := &v1alpha1.ClusterOrder{}
	if err := r.apiReader.Get(ctx, key, co); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if co.DeletionTimestamp.IsZero() && co.Annotations[osacManagementStateAnnotation] == ManagementStateUnmanaged {
		return ctrl.Result{}, nil
	}
	n := &clusterNetworking{
		co:        co,
		clusterID: co.Labels[osacClusterOrderIDLabel],
		enabled:   r.Engine.ProvisioningEnabled(),
	}
	if n.clusterID == "" {
		return ctrl.Result{}, nil
	}
	hc, err := r.hostedClusterOf(ctx, co)
	if err != nil {
		return ctrl.Result{}, err
	}
	n.hc = hc
	if n.enabled {
		if err := r.resolveClusterSubnet(ctx, n); err != nil {
			return ctrl.Result{}, err
		}
	}
	persisted := co.Status.DeepCopy()

	if n.enabled {
		if err := r.reconcilePublicDNS(ctx, n); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !co.DeletionTimestamp.IsZero() {
		if err := r.reconcileNetworkingDelete(ctx, n); err != nil {
			return ctrl.Result{}, err
		}
	} else if co.Status.Phase != v1alpha1.ClusterOrderPhaseFailed {
		if err := r.reconcileNetworkingUpdate(ctx, n); err != nil {
			return ctrl.Result{}, err
		}
	}

	// The engine writes status as it records jobs; persisted tracks the last write.
	if !equality.Semantic.DeepEqual(co.Status, *persisted) {
		if err := r.Status().Update(ctx, co); err != nil {
			return ctrl.Result{}, err
		}
	}
	return n.result, nil
}

// hostedClusterOf returns the ClusterOrder's HostedCluster, or nil.
func (r *ClusterOrderReconciler) hostedClusterOf(ctx context.Context, co *v1alpha1.ClusterOrder) (*hypershiftv1beta1.HostedCluster, error) {
	ref := co.Status.ClusterReference
	if ref == nil || ref.Namespace == "" || ref.HostedClusterName == "" {
		return nil, nil
	}
	hc := &hypershiftv1beta1.HostedCluster{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.HostedClusterName}, hc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return hc, nil
}

// resolveClusterSubnet finds the Subnet of the cluster's network attachment
// and its VirtualNetwork by API ID.
func (r *ClusterOrderReconciler) resolveClusterSubnet(ctx context.Context, n *clusterNetworking) error {
	attachment := n.co.Spec.NetworkAttachment
	if attachment == nil || attachment.SubnetRef == "" || r.NetworkingNamespace == "" {
		return nil
	}
	subnets := &v1alpha1.SubnetList{}
	if err := r.List(ctx, subnets, client.InNamespace(r.NetworkingNamespace),
		client.MatchingLabels{osacSubnetIDLabel: attachment.SubnetRef}); err != nil {
		return fmt.Errorf("list Subnet %s: %w", attachment.SubnetRef, err)
	}
	if len(subnets.Items) != 1 {
		return nil
	}
	n.subnet = &subnets.Items[0]
	vns := &v1alpha1.VirtualNetworkList{}
	if err := r.List(ctx, vns, client.InNamespace(r.NetworkingNamespace),
		client.MatchingLabels{networktask.IDLabel("VirtualNetwork"): n.subnet.Spec.VirtualNetwork}); err != nil {
		return fmt.Errorf("list VirtualNetwork %s: %w", n.subnet.Spec.VirtualNetwork, err)
	}
	if len(vns.Items) == 1 {
		n.vn = &vns.Items[0]
	}
	return nil
}

func (r *ClusterOrderReconciler) event(obj *v1alpha1.ClusterOrder, eventType, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, nil, eventType, reason, reason, format, args...)
	}
}

func (r *ClusterOrderReconciler) reconcileNetworkingUpdate(ctx context.Context, n *clusterNetworking) error {
	co := n.co
	r.reconcilePublication(ctx, n)
	if n.enabled {
		if err := r.reconcileGuestIngress(ctx, n); err != nil {
			return err
		}
		if err := r.requestAutomaticExternalIPAttachments(ctx, n); err != nil {
			return err
		}
		if err := r.reconcileEndpointsReady(ctx, n); err != nil {
			return err
		}
		if err := r.reconcileWorkerNetworking(ctx, n); err != nil {
			return err
		}
	} else {
		n.setCondition(v1alpha1.ClusterOrderConditionEndpointsReady, metav1.ConditionTrue, v1alpha1.ReasonProvisioningDisabled,
			networktask.MessageNetworkingDisabled)
		n.setCondition(v1alpha1.ClusterOrderConditionWorkerNetworkingReady, metav1.ConditionTrue, v1alpha1.ReasonProvisioningDisabled,
			networktask.MessageNetworkingDisabled)
	}
	if co.Status.Phase == v1alpha1.ClusterOrderPhaseReady {
		r.reconcileClusterHealthy(ctx, n)
		if err := r.runPostInstall(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

// reconcilePublication recomputes ManagementPublicationReady (CaaS design,
// Publication gate) and records status.apiEndpoint with its first True.
func (r *ClusterOrderReconciler) reconcilePublication(ctx context.Context, n *clusterNetworking) {
	co := n.co
	excluded := []string{defaultClusterPodCIDR, defaultClusterServiceCIDR}
	if network := co.Spec.Network; network != nil {
		if network.PodCIDR != "" {
			excluded[0] = network.PodCIDR
		}
		if network.ServiceCIDR != "" {
			excluded[1] = network.ServiceCIDR
		}
	}
	if n.vn != nil && n.vn.Spec.IPv4CIDR != "" {
		excluded = append(excluded, n.vn.Spec.IPv4CIDR)
	}
	result := checkManagementPublication(ctx, r.apiReader, r.Prober, co, n.hc, excluded)
	n.requeue(clusterNetworkingRecheck)
	if !result.ready {
		n.setCondition(v1alpha1.ClusterOrderConditionManagementPublicationReady, metav1.ConditionFalse, result.reason, result.message)
		return
	}
	if co.Status.ApiEndpoint == "" {
		co.Status.ApiEndpoint = result.vip
		r.event(co, corev1.EventTypeNormal, eventReasonEndpointsDiscovered, "API endpoint %s recorded", result.vip)
	}
	n.setCondition(v1alpha1.ClusterOrderConditionManagementPublicationReady, metav1.ConditionTrue, v1alpha1.ReasonAsExpected, "")
}

// clusterReserveInput is the stored input of reserve_cluster_ingress_ip and
// release_cluster_ingress_ip (contract §4.5, §4.10).
func (r *ClusterOrderReconciler) clusterReserveInput(n *clusterNetworking) (map[string]any, error) {
	resource, err := networktask.Snapshot(n.co, kindClusterOrder, n.co.Spec)
	if err != nil {
		return nil, err
	}
	vn, err := networktask.Snapshot(n.vn, "VirtualNetwork", n.vn.Spec, "ipv4Cidr")
	if err != nil {
		return nil, err
	}
	subnet, err := networktask.Snapshot(n.subnet, "Subnet", n.subnet.Spec, "ipv4Cidr")
	if err != nil {
		return nil, err
	}
	input := networktask.NewInput(n.co, kindClusterOrder, resource)
	input["cluster_id"] = n.clusterID
	input["virtual_network"] = vn
	input["subnet"] = subnet
	input["ipam"] = networktask.IPAMVars(n.subnet.Status.IPAM)
	return input, nil
}

// reconcileGuestIngress reserves the guest ingress address once the guest API
// is usable and configures guest ingress once the guest has a Ready node
// (CaaS design, Guest ingress reservation).
func (r *ClusterOrderReconciler) reconcileGuestIngress(ctx context.Context, n *clusterNetworking) error {
	co := n.co
	if n.hc == nil || n.subnet == nil || n.vn == nil || n.subnet.Status.IPAM == nil {
		return nil
	}
	reserve := networktask.FindJob(co.Status.Jobs, networktask.ReserveClusterIngressIP, v1alpha1.NetworkJobManagerFabric)
	if reserve == nil {
		if len(n.subnet.Status.IPAM.LBVIPIPv4CIDRs) == 0 {
			return nil
		}
		if err := r.Guest.Usable(ctx, n.hc); err != nil {
			ctrllog.FromContext(ctx).Info("guest API not usable yet", "reason", err.Error())
			n.requeue(clusterNetworkingRecheck)
			return nil
		}
	}
	fabric := networktask.ManagerRef(n.subnet.Status.NetworkManagers, v1alpha1.NetworkJobManagerFabric)
	if reserve == nil || reserve.State != v1alpha1.NetworkJobStateSucceeded {
		if fabric == nil {
			return nil
		}
		input, err := networktask.LoadInput(co)
		if err != nil {
			return err
		}
		var before func() error
		if input == nil {
			input, err = r.clusterReserveInput(n)
			if err != nil {
				return err
			}
			stored := input
			before = func() error { return networktask.StoreInput(co, stored) }
		}
		subnet := n.subnet
		outcome, err := r.Engine.Run(ctx, co, networktask.Request{
			Task:         networktask.ReserveClusterIngressIP,
			Manager:      v1alpha1.NetworkJobManagerFabric,
			ManagerRef:   fabric,
			Kind:         kindClusterOrder,
			Vars:         input,
			BeforeLaunch: before,
			Accept: func(_ context.Context, result networktask.Result) error {
				address, err := networktask.ParseClusterIngress(result, subnet.Spec.IPv4CIDR, subnet.Status.IPAM, co.Status.IngressAllocatedAddress)
				if err != nil {
					return err
				}
				co.Status.IngressAllocatedAddress = address
				return nil
			},
		})
		if err != nil {
			return err
		}
		n.requeue(outcome.RequeueAfter)
		if !outcome.Succeeded() {
			return nil
		}
	}

	if co.Status.IngressEndpoint != "" || co.Status.IngressAllocatedAddress == "" {
		return nil
	}
	if networktask.FindJob(co.Status.Jobs, configureGuestIngressTask, v1alpha1.NetworkJobManagerNone) == nil {
		ready, err := r.Guest.ReadyNodes(ctx, n.hc)
		if err != nil || ready == 0 {
			n.requeue(clusterNetworkingRecheck)
			return nil
		}
	}
	outcome, err := r.Engine.Run(ctx, co, networktask.Request{
		Task:     configureGuestIngressTask,
		Manager:  v1alpha1.NetworkJobManagerNone,
		Kind:     kindClusterOrder,
		Template: r.EndpointsTemplate,
		Vars:     r.endpointJobVars(n, map[string]any{"ingress_allocated_address": co.Status.IngressAllocatedAddress}),
		Accept: func(ctx context.Context, result networktask.Result) error {
			if err := networktask.AcceptNoOutput(ctx, result); err != nil {
				return err
			}
			co.Status.IngressEndpoint = co.Status.IngressAllocatedAddress
			return nil
		},
	})
	if err != nil {
		return err
	}
	n.requeue(outcome.RequeueAfter)
	if outcome.Succeeded() && co.Status.IngressEndpoint != "" &&
		co.Status.IngressEndpoint == co.Status.IngressAllocatedAddress {
		r.event(co, corev1.EventTypeNormal, eventReasonEndpointsDiscovered, "ingress endpoint %s recorded", co.Status.IngressEndpoint)
	}
	return nil
}

// endpointJobVars is the osac_job_vars of a guest endpoint job (without task):
// the ClusterOrder snapshot, the Cluster API ID and the task keys.
func (r *ClusterOrderReconciler) endpointJobVars(n *clusterNetworking, keys map[string]any) map[string]any {
	resource, _ := networktask.Snapshot(n.co, kindClusterOrder, n.co.Spec)
	vars := map[string]any{"resource": resource, "cluster_id": n.clusterID}
	for k, v := range keys {
		vars[k] = v
	}
	return vars
}

// requestAutomaticExternalIPAttachments asks the fulfillment service for the
// automatic ExternalIPAttachment of each ready endpoint (CaaS design,
// ExternalIPAttachment target cluster): API once apiEndpoint is recorded and
// ManagementPublicationReady=True, INGRESS once ingressEndpoint is recorded.
func (r *ClusterOrderReconciler) requestAutomaticExternalIPAttachments(ctx context.Context, n *clusterNetworking) error {
	co := n.co
	if r.ExternalIPAttachmentsClient == nil || r.NetworkingNamespace == "" {
		return nil
	}
	tenant := co.Annotations[osacTenantKey]
	if tenant == "" {
		return nil
	}
	target := automaticExternalIPAttachmentTarget{
		owner:     autoExternalIPOwner{kind: clusterOrderOwner, id: n.clusterID},
		kind:      clusterOrderOwner,
		kindLabel: autoExternalIPKindLabel(clusterOrderOwner),
		tenant:    tenant,
		endpoints: []string{autoCreatedEndpointAPI, autoCreatedEndpointIngress},
		availableEndpoints: map[string]bool{
			autoCreatedEndpointAPI: co.Status.ApiEndpoint != "" &&
				apimeta.IsStatusConditionTrue(co.Status.Conditions, string(v1alpha1.ClusterOrderConditionManagementPublicationReady)),
			autoCreatedEndpointIngress: co.Status.IngressEndpoint != "",
		},
	}
	result, err := reconcileAutomaticExternalIPAttachments(ctx, r.Client, r.NetworkingNamespace,
		r.ExternalIPAttachmentsClient, clusterNetworkingRecheck, target)
	if err != nil {
		return err
	}
	n.result = mergeReconcileResult(n.result, result)
	return nil
}

// automaticAttachments lists the cluster's automatic ExternalIPAttachment CRs.
func (r *ClusterOrderReconciler) automaticAttachments(ctx context.Context, clusterID string) ([]v1alpha1.ExternalIPAttachment, error) {
	if r.NetworkingNamespace == "" {
		return nil, nil
	}
	return listAutoExternalIPAttachments(ctx, r.Client, r.NetworkingNamespace,
		autoExternalIPOwner{kind: clusterOrderOwner, id: clusterID})
}

// externalIPAddress returns the address of the ExternalIP CR with the given API ID.
func (r *ClusterOrderReconciler) externalIPAddress(ctx context.Context, id string) (string, error) {
	list := &v1alpha1.ExternalIPList{}
	if err := r.List(ctx, list, client.InNamespace(r.NetworkingNamespace), client.MatchingLabels{externalIPUUIDLabel: id}); err != nil {
		return "", err
	}
	if len(list.Items) != 1 {
		return "", nil
	}
	return list.Items[0].Status.Address, nil
}

// reconcilePublicDNS publishes and removes the public DNS records of the
// cluster's automatic ExternalIPAttachments (CaaS design, Endpoint feedback
// and DNS). It adds finalizer osac.openshift.io/public-dns to each automatic
// ExternalIPAttachment CR before its publish job, publishes once the
// attachment is READY, and on its deletion waits for a running publish job,
// runs the remove job and removes the finalizer.
func (r *ClusterOrderReconciler) reconcilePublicDNS(ctx context.Context, n *clusterNetworking) error {
	co := n.co
	attachments, err := r.automaticAttachments(ctx, n.clusterID)
	if err != nil {
		return err
	}
	for i := range attachments {
		attachment := &attachments[i]
		tasks, ok := dnsTasks[attachment.Labels[autoCreatedEndpointLabel]]
		if !ok {
			continue
		}
		publishTask, removeTask := tasks[0], tasks[1]
		address, err := r.externalIPAddress(ctx, attachment.Spec.ExternalIP)
		if err != nil {
			return err
		}
		vars := map[string]any{}
		if address != "" {
			vars["external_ip_address"] = address
		}

		if attachment.DeletionTimestamp.IsZero() {
			if !controllerutil.ContainsFinalizer(attachment, publicDNSFinalizer) {
				base := attachment.DeepCopy()
				controllerutil.AddFinalizer(attachment, publicDNSFinalizer)
				if err := r.Patch(ctx, attachment, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
					return client.IgnoreNotFound(err)
				}
			}
			if !co.DeletionTimestamp.IsZero() || address == "" {
				continue
			}
			if state, _ := networkFeedback(attachment, attachment.Status.Conditions); state != networkFeedbackReady {
				n.requeue(clusterNetworkingRecheck)
				continue
			}
			previous := networktask.FindJob(co.Status.Jobs, publishTask, v1alpha1.NetworkJobManagerNone)
			outcome, err := r.Engine.Run(ctx, co, networktask.Request{
				Task:     publishTask,
				Manager:  v1alpha1.NetworkJobManagerNone,
				Kind:     kindClusterOrder,
				Template: r.EndpointsTemplate,
				Vars:     r.endpointJobVars(n, vars),
				Accept: func(ctx context.Context, result networktask.Result) error {
					if err := networktask.AcceptNoOutput(ctx, result); err != nil {
						return err
					}
					if publishTask == publishPublicDNSAPITask && co.Status.APIRecordsPublishedTime == nil {
						now := metav1.Now()
						co.Status.APIRecordsPublishedTime = &now
					}
					return nil
				},
			})
			if err != nil {
				return err
			}
			n.requeue(outcome.RequeueAfter)
			failed := outcome.Entry.State == v1alpha1.NetworkJobStateRetrying || outcome.Failed()
			if failed && previous != nil && previous.State == v1alpha1.NetworkJobStateRunning {
				r.event(co, corev1.EventTypeWarning, eventReasonEndpointDNSFailed, "%s failed in AAP job %s: %s",
					publishTask, previous.JobID, outcome.Entry.Message)
			}
			continue
		}

		if !controllerutil.ContainsFinalizer(attachment, publicDNSFinalizer) {
			continue
		}
		if networktask.Started(co, publishTask, v1alpha1.NetworkJobManagerNone) {
			// No publish job starts any more; a running one is cancelled in AAP
			// and tracked until it ended.
			outcome, err := r.Engine.Run(ctx, co, networktask.Request{
				Task:           publishTask,
				Manager:        v1alpha1.NetworkJobManagerNone,
				Kind:           kindClusterOrder,
				Template:       r.EndpointsTemplate,
				Vars:           r.endpointJobVars(n, vars),
				TargetDeleting: true,
			})
			if err != nil {
				return err
			}
			if outcome.Entry.State == v1alpha1.NetworkJobStateRunning {
				n.requeue(outcome.RequeueAfter)
				continue
			}
			removal, err := r.Engine.Run(ctx, co, networktask.Request{
				Task:     removeTask,
				Manager:  v1alpha1.NetworkJobManagerNone,
				Kind:     kindClusterOrder,
				Template: r.EndpointsTemplate,
				Vars:     r.endpointJobVars(n, vars),
			})
			if err != nil {
				return err
			}
			if !removal.Succeeded() {
				n.requeue(removal.RequeueAfter)
				continue
			}
		}
		base := attachment.DeepCopy()
		controllerutil.RemoveFinalizer(attachment, publicDNSFinalizer)
		if err := r.Patch(ctx, attachment, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return client.IgnoreNotFound(err)
		}
	}
	return nil
}

// reconcileEndpointsReady sets EndpointsReady from the endpoint steps (CaaS
// design, Endpoint status; the first matching row applies).
func (r *ClusterOrderReconciler) reconcileEndpointsReady(ctx context.Context, n *clusterNetworking) error {
	co := n.co
	set := func(status metav1.ConditionStatus, reason, message string) {
		n.setCondition(v1alpha1.ClusterOrderConditionEndpointsReady, status, reason, message)
	}
	ipsByEndpoint, attachmentsByEndpoint, failure, err := r.automaticEndpointResources(ctx, n.clusterID)
	if err != nil {
		return err
	}
	if failure != "" {
		set(metav1.ConditionFalse, v1alpha1.ClusterOrderReasonExternalAccessFailed, failure)
		return nil
	}
	if reason, message := endpointJobFailure(n); reason != "" {
		set(metav1.ConditionFalse, reason, message)
		return nil
	}
	if co.Status.IngressEndpoint == "" {
		set(metav1.ConditionFalse, v1alpha1.ClusterOrderReasonGuestIngressProgressing, guestIngressProgress(co))
		return nil
	}
	if co.Status.ApiEndpoint == "" {
		set(metav1.ConditionFalse, v1alpha1.ClusterOrderReasonEndpointsProgressing, "waiting for the API endpoint")
		return nil
	}
	if message := endpointsProgress(co, ipsByEndpoint, attachmentsByEndpoint); message != "" {
		set(metav1.ConditionFalse, v1alpha1.ClusterOrderReasonEndpointsProgressing, message)
		return nil
	}
	set(metav1.ConditionTrue, v1alpha1.ClusterOrderReasonEndpointsPublished, "")
	return nil
}

// automaticEndpointResources returns the cluster's automatic ExternalIPs and
// ExternalIPAttachments by endpoint, or the failure of the first failed one.
func (r *ClusterOrderReconciler) automaticEndpointResources(ctx context.Context, clusterID string) (
	map[string]*v1alpha1.ExternalIP, map[string]*v1alpha1.ExternalIPAttachment, string, error,
) {
	owner := autoExternalIPOwner{kind: clusterOrderOwner, id: clusterID}
	externalIPs, err := listAutoExternalIPs(ctx, r.Client, r.NetworkingNamespace, owner)
	if err != nil {
		return nil, nil, "", err
	}
	attachments, err := r.automaticAttachments(ctx, clusterID)
	if err != nil {
		return nil, nil, "", err
	}
	ipsByEndpoint := map[string]*v1alpha1.ExternalIP{}
	for i := range externalIPs {
		ip := &externalIPs[i]
		if state, message := networkFeedback(ip, ip.Status.Conditions); state == networkFeedbackFailed {
			return nil, nil, fmt.Sprintf("automatic ExternalIP %s failed: %s", ip.Name, message), nil
		}
		ipsByEndpoint[ip.Labels[autoCreatedEndpointLabel]] = ip
	}
	attachmentsByEndpoint := map[string]*v1alpha1.ExternalIPAttachment{}
	for i := range attachments {
		attachment := &attachments[i]
		if state, message := networkFeedback(attachment, attachment.Status.Conditions); state == networkFeedbackFailed {
			return nil, nil, fmt.Sprintf("automatic ExternalIPAttachment %s failed: %s", attachment.Name, message), nil
		}
		attachmentsByEndpoint[attachment.Labels[autoCreatedEndpointLabel]] = attachment
	}
	return ipsByEndpoint, attachmentsByEndpoint, "", nil
}

// endpointJobFailure returns the reason and message of a failed endpoint
// step: the ingress reservation, the guest ingress configuration or a DNS
// publication. The reason is empty when no step failed.
func endpointJobFailure(n *clusterNetworking) (string, string) {
	co := n.co
	retryingOrFailed := func(entry *v1alpha1.NetworkJobStatus) bool {
		return entry != nil &&
			(entry.State == v1alpha1.NetworkJobStateRetrying || entry.State == v1alpha1.NetworkJobStateFailed)
	}
	reserve := networktask.FindJob(co.Status.Jobs, networktask.ReserveClusterIngressIP, v1alpha1.NetworkJobManagerFabric)
	if reserve != nil && reserve.State == v1alpha1.NetworkJobStateFailed {
		return v1alpha1.ClusterOrderReasonIngressReservationFailed, reserve.Message
	}
	if n.subnet != nil && n.subnet.Status.IPAM != nil && len(n.subnet.Status.IPAM.LBVIPIPv4CIDRs) == 0 {
		return v1alpha1.ClusterOrderReasonIngressReservationFailed,
			fmt.Sprintf("Subnet %s has no LB VIP range", co.Spec.NetworkAttachment.SubnetRef)
	}
	configure := networktask.FindJob(co.Status.Jobs, configureGuestIngressTask, v1alpha1.NetworkJobManagerNone)
	if retryingOrFailed(configure) {
		return v1alpha1.ClusterOrderReasonGuestIngressFailed, configure.Message
	}
	for _, task := range []string{publishPublicDNSAPITask, publishPublicDNSIngressTask} {
		if publish := networktask.FindJob(co.Status.Jobs, task, v1alpha1.NetworkJobManagerNone); retryingOrFailed(publish) {
			return v1alpha1.ClusterOrderReasonDNSPublishFailed,
				fmt.Sprintf("%s (AAP job %s): %s", task, publish.JobID, publish.Message)
		}
	}
	return "", ""
}

// guestIngressProgress is the EndpointsReady message while the guest ingress
// endpoint is not known yet.
func guestIngressProgress(co *v1alpha1.ClusterOrder) string {
	reserve := networktask.FindJob(co.Status.Jobs, networktask.ReserveClusterIngressIP, v1alpha1.NetworkJobManagerFabric)
	switch {
	case reserve != nil && reserve.State != v1alpha1.NetworkJobStateSucceeded:
		return networktask.ProgressMessage(*reserve)
	case co.Status.IngressAllocatedAddress != "":
		configure := networktask.FindJob(co.Status.Jobs, configureGuestIngressTask, v1alpha1.NetworkJobManagerNone)
		if configure != nil {
			return networktask.ProgressMessage(*configure)
		}
		return "waiting for a Ready guest node and the guest ingress configuration"
	}
	return "waiting for the guest API"
}

// endpointsProgress returns what the API and ingress endpoints still wait
// for: the ExternalIP allocation, the ExternalIPAttachment or the DNS
// publication; empty when both are published.
func endpointsProgress(co *v1alpha1.ClusterOrder, ipsByEndpoint map[string]*v1alpha1.ExternalIP,
	attachmentsByEndpoint map[string]*v1alpha1.ExternalIPAttachment,
) string {
	for _, endpoint := range []string{autoCreatedEndpointAPI, autoCreatedEndpointIngress} {
		ip := ipsByEndpoint[endpoint]
		if ip == nil || ip.Status.Address == "" {
			return fmt.Sprintf("waiting for the %s ExternalIP allocation", endpoint)
		}
		attachment := attachmentsByEndpoint[endpoint]
		if attachment == nil || !attachmentReady(attachment) {
			return fmt.Sprintf("waiting for the %s ExternalIPAttachment", endpoint)
		}
		if task := dnsTasks[endpoint][0]; !networktask.Succeeded(co, task, v1alpha1.NetworkJobManagerNone) {
			return fmt.Sprintf("waiting for %s", task)
		}
	}
	return ""
}

func attachmentReady(attachment *v1alpha1.ExternalIPAttachment) bool {
	state, _ := networkFeedback(attachment, attachment.Status.Conditions)
	return state == networkFeedbackReady
}

// workerNetworkingFailureReasons are the BMI condition reasons that fail worker networking.
var workerNetworkingFailureReasons = map[string]bool{
	"NetworkAttachmentFailed": true,
	"NetworkHandoffFailed":    true,
	"IPDiscoveryFailed":       true,
}

// bmiNetworkConditions are the BMI network conditions in the order they are reported.
var bmiNetworkConditions = []string{
	string(bmfov1alpha1.HostConditionNetworkAttachmentsReady),
	string(bmfov1alpha1.HostConditionNetworkHandoffComplete),
	string(bmfov1alpha1.HostConditionIPDiscoveryComplete),
}

// reconcileWorkerNetworking sets WorkerNetworkingReady from the worker BMI
// CRs and the SecurityGroups of the cluster's network attachment (CaaS
// design, Worker networking state).
func (r *ClusterOrderReconciler) reconcileWorkerNetworking(ctx context.Context, n *clusterNetworking) error {
	co := n.co
	set := func(status metav1.ConditionStatus, reason, message string) {
		old := apimeta.FindStatusCondition(co.Status.Conditions, string(v1alpha1.ClusterOrderConditionWorkerNetworkingReady))
		if reason == v1alpha1.ClusterOrderReasonWorkerNetworkingFailed && (old == nil || old.Reason != reason || old.Message != message) {
			r.event(co, corev1.EventTypeWarning, eventReasonWorkerNetworkingFailed, "%s", message)
		}
		n.setCondition(v1alpha1.ClusterOrderConditionWorkerNetworkingReady, status, reason, message)
	}

	failure, err := r.securityGroupFailure(ctx, co)
	if err != nil {
		return err
	}
	if failure != "" {
		set(metav1.ConditionFalse, v1alpha1.ClusterOrderReasonWorkerNetworkingFailed, failure)
		return nil
	}

	waiting := ""
	for _, worker := range co.Status.Workers {
		bmi, err := r.workerBareMetalInstance(ctx, worker)
		if err != nil {
			return err
		}
		if failure := workerNetworkingFailure(worker, bmi); failure != "" {
			set(metav1.ConditionFalse, v1alpha1.ClusterOrderReasonWorkerNetworkingFailed, failure)
			return nil
		}
		if waiting == "" {
			waiting = workerNetworkingWait(worker, bmi)
		}
	}
	if waiting != "" {
		set(metav1.ConditionFalse, v1alpha1.ClusterOrderReasonWorkersProgressing, waiting)
		return nil
	}
	set(metav1.ConditionTrue, v1alpha1.ClusterOrderReasonWorkersReady, "")
	return nil
}

// securityGroupFailure returns the failure of the first failed SecurityGroup
// of the cluster's network attachment, or "".
func (r *ClusterOrderReconciler) securityGroupFailure(ctx context.Context, co *v1alpha1.ClusterOrder) (string, error) {
	attachment := co.Spec.NetworkAttachment
	if attachment == nil || r.NetworkingNamespace == "" {
		return "", nil
	}
	for _, id := range attachment.SecurityGroupRefs {
		groups := &v1alpha1.SecurityGroupList{}
		if err := r.List(ctx, groups, client.InNamespace(r.NetworkingNamespace),
			client.MatchingLabels{networktask.IDLabel(kindSecurityGroup): id}); err != nil {
			return "", err
		}
		for i := range groups.Items {
			group := &groups.Items[i]
			if state, message := networkFeedback(group, group.Status.Conditions); state == networkFeedbackFailed {
				return fmt.Sprintf("SecurityGroup %s failed: %s", id, message), nil
			}
		}
	}
	return "", nil
}

// workerBareMetalInstance returns the BMI CR of a worker, or nil.
func (r *ClusterOrderReconciler) workerBareMetalInstance(ctx context.Context, worker v1alpha1.WorkerStatus) (
	*bmfov1alpha1.BareMetalInstance, error,
) {
	if worker.BareMetalInstance.ID == "" {
		return nil, nil
	}
	list := &bmfov1alpha1.BareMetalInstanceList{}
	if err := r.List(ctx, list, client.InNamespace(r.BareMetalInstanceNamespace),
		client.MatchingLabels{osacBareMetalInstanceIDLabel: worker.BareMetalInstance.ID}); err != nil {
		return nil, err
	}
	if len(list.Items) != 1 {
		return nil, nil
	}
	return &list.Items[0], nil
}

// workerNetworkingFailure returns the networking failure of a worker: a failed
// BMI network condition, or a networking failure recorded on the worker while
// its BMI is not Ready; "" when none.
func workerNetworkingFailure(worker v1alpha1.WorkerStatus, bmi *bmfov1alpha1.BareMetalInstance) string {
	if bmi != nil && bmi.Status.Phase == bmfov1alpha1.BareMetalInstancePhaseFailed {
		for _, conditionType := range bmiNetworkConditions {
			condition := apimeta.FindStatusCondition(bmi.Status.Conditions, conditionType)
			if condition != nil && condition.Status == metav1.ConditionFalse && workerNetworkingFailureReasons[condition.Reason] {
				return fmt.Sprintf("worker BareMetalInstance %s failed: %s: %s",
					worker.BareMetalInstance.Name, condition.Reason, condition.Message)
			}
		}
	}
	if workerNetworkingFailureReasons[worker.LastFailureReason] &&
		(bmi == nil || bmi.Status.Phase != bmfov1alpha1.BareMetalInstancePhaseReady) {
		return fmt.Sprintf("worker %s failed: %s: %s", worker.Name, worker.LastFailureReason, worker.LastFailureMessage)
	}
	return ""
}

// workerNetworkingWait returns what a worker without a Ready BMI waits for,
// or "" when its BMI is Ready.
func workerNetworkingWait(worker v1alpha1.WorkerStatus, bmi *bmfov1alpha1.BareMetalInstance) string {
	if bmi == nil {
		return fmt.Sprintf("worker %s has no Ready BareMetalInstance", worker.Name)
	}
	if bmi.Status.Phase == bmfov1alpha1.BareMetalInstancePhaseReady {
		return ""
	}
	for _, conditionType := range bmiNetworkConditions {
		condition := apimeta.FindStatusCondition(bmi.Status.Conditions, conditionType)
		if condition != nil && condition.Status != metav1.ConditionTrue {
			return fmt.Sprintf("worker BareMetalInstance %s: %s: %s", bmi.Name, condition.Reason, condition.Message)
		}
	}
	return fmt.Sprintf("worker %s has no Ready BareMetalInstance", worker.Name)
}

// requestedWorkers is the number of requested bare-metal workers.
func requestedWorkers(co *v1alpha1.ClusterOrder) int {
	total := 0
	for _, request := range co.Spec.NodeRequests {
		if request.IsBareMetal() && request.NumberOfNodes > 0 {
			total += request.NumberOfNodes
		}
	}
	return total
}

// guestBlocker returns why the guest cluster is not ready: a requested
// worker without a Ready node, or a ClusterOperator that is not Available.
func (r *ClusterOrderReconciler) guestBlocker(ctx context.Context, co *v1alpha1.ClusterOrder, hc *hypershiftv1beta1.HostedCluster) (string, string) {
	if hc == nil {
		return v1alpha1.ReasonControlPlaneStarting, "waiting for the HostedCluster"
	}
	ready, err := r.Guest.ReadyNodes(ctx, hc)
	if err != nil {
		return v1alpha1.ReasonWorkersJoining, fmt.Sprintf("reading the guest nodes: %v", err)
	}
	if requested := requestedWorkers(co); ready < requested {
		return v1alpha1.ReasonWorkersJoining, fmt.Sprintf("%d of %d requested workers have a Ready node", ready, requested)
	}
	operator, err := r.Guest.UnavailableClusterOperator(ctx, hc)
	if err != nil {
		return reasonClusterOperatorsProgressing, fmt.Sprintf("reading the cluster operators: %v", err)
	}
	if operator != "" {
		return reasonClusterOperatorsProgressing, fmt.Sprintf("ClusterOperator %s is not Available", operator)
	}
	return "", ""
}

// clusterReadyBlocker applies the Cluster Ready rule of the CaaS design on top
// of the core readiness: EndpointsReady=True, a Ready node for every requested
// worker and every ClusterOperator Available=True. It returns "" when the
// ClusterOrder may become Ready.
func (r *ClusterOrderReconciler) clusterReadyBlocker(ctx context.Context, co *v1alpha1.ClusterOrder, hc *hypershiftv1beta1.HostedCluster) (string, string) {
	if r.Engine == nil {
		return "", ""
	}
	endpoints := apimeta.FindStatusCondition(co.Status.Conditions, string(v1alpha1.ClusterOrderConditionEndpointsReady))
	if endpoints == nil {
		return v1alpha1.ClusterOrderReasonEndpointsProgressing, "waiting for the cluster endpoints"
	}
	if endpoints.Status != metav1.ConditionTrue {
		return endpoints.Reason, endpoints.Message
	}
	return r.guestBlocker(ctx, co, hc)
}

// setClusterURLs sets the API and console URLs when the ClusterOrder becomes Ready.
func setClusterURLs(co *v1alpha1.ClusterOrder, hc *hypershiftv1beta1.HostedCluster) {
	if hc == nil {
		return
	}
	domain := hc.Spec.DNS.BaseDomain
	if port := hc.Status.ControlPlaneEndpoint.Port; port != 0 {
		co.Status.APIURL = fmt.Sprintf("https://api.%s.%s:%d", hc.Name, domain, port)
	}
	co.Status.ConsoleURL = fmt.Sprintf("https://console-openshift-console.apps.%s.%s", hc.Name, domain)
}

// reconcileClusterHealthy sets ClusterHealthy after the first Ready.
func (r *ClusterOrderReconciler) reconcileClusterHealthy(ctx context.Context, n *clusterNetworking) {
	n.requeue(clusterNetworkingRecheck)
	if _, message := r.guestBlocker(ctx, n.co, n.hc); message != "" {
		n.setCondition(v1alpha1.ClusterOrderConditionClusterHealthy, metav1.ConditionFalse, clusterHealthyReasonUnhealthy, message)
		return
	}
	n.setCondition(v1alpha1.ClusterOrderConditionClusterHealthy, metav1.ConditionTrue, clusterHealthyReasonHealthy, "")
}

// runPostInstall runs the hosted cluster post-install job once the
// ClusterOrder is Ready (it needs the workers). A failure is reported in its
// status.jobs entry and does not change readiness.
func (r *ClusterOrderReconciler) runPostInstall(ctx context.Context, n *clusterNetworking) error {
	if r.PostInstallTemplate == "" || networktask.Succeeded(n.co, postInstallTask, v1alpha1.NetworkJobManagerNone) {
		return nil
	}
	if entry := networktask.FindJob(n.co.Status.Jobs, postInstallTask, v1alpha1.NetworkJobManagerNone); entry != nil &&
		entry.State == v1alpha1.NetworkJobStateFailed {
		return nil
	}
	vars, err := provisioning.JobVars(ctx, n.co)
	if err != nil {
		return err
	}
	outcome, err := r.Engine.Run(ctx, n.co, networktask.Request{
		Task:     postInstallTask,
		Manager:  v1alpha1.NetworkJobManagerNone,
		Kind:     kindClusterOrder,
		Template: r.PostInstallTemplate,
		Vars:     vars,
	})
	if err != nil {
		return err
	}
	n.requeue(outcome.RequeueAfter)
	return nil
}

// reconcileNetworkingDelete cancels the cluster's running jobs and, once
// every worker BMI record is gone, releases the guest ingress address (CaaS
// design, Delete steps 3 to 5).
func (r *ClusterOrderReconciler) reconcileNetworkingDelete(ctx context.Context, n *clusterNetworking) error {
	co := n.co
	done, wait, err := r.Engine.CancelForDelete(ctx, co, kindClusterOrder)
	if err != nil {
		return err
	}
	if !done {
		n.requeue(wait)
		return nil
	}
	if !networktask.Started(co, networktask.ReserveClusterIngressIP, v1alpha1.NetworkJobManagerFabric) ||
		networktask.Succeeded(co, networktask.ReleaseClusterIngressIP, v1alpha1.NetworkJobManagerFabric) {
		return nil
	}
	if controllerutil.ContainsFinalizer(co, bareMetalWorkerFinalizer) {
		n.requeue(clusterNetworkingRecheck)
		return nil
	}
	var fabric *v1alpha1.NetworkManagerRef
	if n.subnet != nil {
		fabric = networktask.ManagerRef(n.subnet.Status.NetworkManagers, v1alpha1.NetworkJobManagerFabric)
	}
	if fabric == nil {
		ctrllog.FromContext(ctx).Info("cannot release the guest ingress address: the Subnet or its Fabric Manager is not found",
			"subnet", co.Spec.NetworkAttachment)
		n.requeue(clusterNetworkingRecheck)
		return nil
	}
	input, err := networktask.LoadInput(co)
	if err != nil {
		return err
	}
	outcome, err := r.Engine.Run(ctx, co, networktask.Request{
		Task:       networktask.ReleaseClusterIngressIP,
		Manager:    v1alpha1.NetworkJobManagerFabric,
		ManagerRef: fabric,
		Kind:       kindClusterOrder,
		Vars:       input,
		Accept: func(ctx context.Context, result networktask.Result) error {
			if err := networktask.AcceptNoOutput(ctx, result); err != nil {
				return err
			}
			co.Status.IngressAllocatedAddress = ""
			return nil
		},
	})
	if err != nil {
		return err
	}
	n.requeue(outcome.RequeueAfter)
	return nil
}

// clusterOrderDeleteBlocker returns the cleanup step that holds the
// ClusterOrder delete before the core delete job (CaaS design, Delete step 4),
// or "".
func (r *ClusterOrderReconciler) clusterOrderDeleteBlocker(ctx context.Context, instance *v1alpha1.ClusterOrder) (string, error) {
	if r.Engine == nil {
		return "", nil
	}
	remains, err := automaticExternalAccessRemains(ctx, r.Client, r.NetworkingNamespace,
		autoExternalIPOwner{kind: clusterOrderOwner, id: instance.Labels[osacClusterOrderIDLabel]})
	if err != nil {
		return "", err
	}
	if remains {
		return clusterOrderDeletingWaitExternal, nil
	}
	for _, entry := range instance.Status.Jobs {
		if !networktask.IsCleanupTask(entry.Task) && (entry.State == v1alpha1.NetworkJobStateRunning ||
			entry.State == v1alpha1.NetworkJobStateRetrying) {
			return clusterOrderDeletingWaitJobs, nil
		}
	}
	return "", nil
}

// clusterOrderReleasePending returns the release step that holds the
// ClusterOrder finalizer, or "".
func (r *ClusterOrderReconciler) clusterOrderReleasePending(instance *v1alpha1.ClusterOrder) string {
	if r.Engine == nil || !networktask.Started(instance, networktask.ReserveClusterIngressIP, v1alpha1.NetworkJobManagerFabric) ||
		networktask.Succeeded(instance, networktask.ReleaseClusterIngressIP, v1alpha1.NetworkJobManagerFabric) {
		return ""
	}
	message := clusterOrderDeletingReleaseIngress
	if entry := networktask.FindJob(instance.Status.Jobs, networktask.ReleaseClusterIngressIP, v1alpha1.NetworkJobManagerFabric); entry != nil && entry.Message != "" {
		message += ": " + entry.Message
	}
	return message
}
