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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"math/big"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	bmfov1alpha1 "github.com/osac-project/osac/bare-metal-fulfillment-operator/api/v1alpha1"
	osacv1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/internal/networktask"
)

// fakeProber is a PublicationProber that fails the listed endpoints.
type fakeProber struct {
	failTLS     map[string]error
	failResolve map[string]error
}

func (p *fakeProber) Resolve(_ context.Context, host string) error { return p.failResolve[host] }
func (p *fakeProber) ProbeTLS(_ context.Context, address, _ string) error {
	return p.failTLS[address]
}

// fakeGuest is a GuestClusterAccess with fixed answers.
type fakeGuest struct {
	usableErr   error
	readyNodes  int
	unavailable string
}

func (g *fakeGuest) Usable(context.Context, *hypershiftv1beta1.HostedCluster) error {
	return g.usableErr
}
func (g *fakeGuest) ReadyNodes(context.Context, *hypershiftv1beta1.HostedCluster) (int, error) {
	return g.readyNodes, nil
}
func (g *fakeGuest) UnavailableClusterOperator(context.Context, *hypershiftv1beta1.HostedCluster) (string, error) {
	return g.unavailable, nil
}

var _ = Describe("ClusterOrder CaaS networking", func() {
	const (
		ns         = "default"
		vip        = "198.51.100.7"
		baseDomain = "example.com"
	)
	ctx := context.Background()

	type coFixture struct {
		r           *ClusterOrderReconciler
		aap         *wlFakeAAP
		attachments *testExternalIPAttachmentsClient
		guest       *fakeGuest
		prober      *fakeProber
		key         types.NamespacedName
		id          string
		subnetID    string
	}

	cleanup := func(obj client.Object) {
		DeferCleanup(func() {
			current := obj.DeepCopyObject().(client.Object)
			if k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), current) == nil {
				current.SetFinalizers(nil)
				_ = k8sClient.Update(ctx, current)
				_ = k8sClient.Delete(ctx, current)
			}
		})
	}

	create := func(obj client.Object) {
		GinkgoHelper()
		Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		cleanup(obj)
	}

	unstructuredObject := func(apiVersion, kind, namespace, name string, content map[string]any) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: content}
		u.SetAPIVersion(apiVersion)
		u.SetKind(kind)
		u.SetNamespace(namespace)
		u.SetName(name)
		return u
	}

	// newCluster creates a ClusterOrder whose hosted control plane passes the
	// publication gate (with the fake prober), its Subnet and VirtualNetwork.
	newCluster := func(name string, enabled bool) *coFixture {
		GinkgoHelper()
		id := name + "-id"
		subnetID := name + "-subnet"
		managers := &osacv1alpha1.NetworkManagers{Fabric: &osacv1alpha1.NetworkManagerRef{Name: "netris", AnsibleRole: "osac.templates.netris"}}

		vn := &osacv1alpha1.VirtualNetwork{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-vn", Namespace: ns,
				Labels: map[string]string{networktask.IDLabel("VirtualNetwork"): name + "-vn"}},
			Spec: osacv1alpha1.VirtualNetworkSpec{IPv4CIDR: "10.0.0.0/16"},
		}
		create(vn)
		subnet := &osacv1alpha1.Subnet{
			ObjectMeta: metav1.ObjectMeta{Name: subnetID, Namespace: ns, Labels: map[string]string{osacSubnetIDLabel: subnetID}},
			Spec: osacv1alpha1.SubnetSpec{VirtualNetwork: name + "-vn", IPv4CIDR: "10.0.1.0/24",
				AddressPlan: osacv1alpha1.SubnetAddressPlan{Overlay: 8, Fabric: 4, LBVIP: 4}},
		}
		create(subnet)
		subnet.Status.NetworkManagers = managers
		subnet.Status.IPAM = &osacv1alpha1.SubnetIPAM{GatewayIPv4: "10.0.1.1",
			OverlayIPv4CIDRs: []string{"10.0.1.0/25"}, PhysicalIPv4CIDRs: []string{"10.0.1.128/26"}, LBVIPIPv4CIDRs: []string{"10.0.1.192/26"}}
		Expect(k8sClient.Status().Update(ctx, subnet)).To(Succeed())

		hc := &hypershiftv1beta1.HostedCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       hypershiftv1beta1.HostedClusterSpec{DNS: hypershiftv1beta1.DNSSpec{BaseDomain: baseDomain}},
		}
		create(hc)
		hc.Status.ControlPlaneEndpoint = hypershiftv1beta1.APIEndpoint{Host: "api." + name + "." + baseDomain, Port: 6443}
		hc.Status.Conditions = []metav1.Condition{{Type: string(hypershiftv1beta1.HostedClusterAvailable), Status: metav1.ConditionTrue,
			Reason: "AsExpected", LastTransitionTime: metav1.Now()}}
		Expect(k8sClient.Status().Update(ctx, hc)).To(Succeed())

		hcpNamespace := ns + "-" + name
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: hcpNamespace}}))).To(Succeed())
		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: kubeAPIServerServiceName, Namespace: hcpNamespace,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "hypershift.openshift.io/v1beta1", Kind: "HostedControlPlane",
					Name: name, UID: "11111111-2222-3333-4444-555555555555"}}},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Name: "client", Port: 6443}}},
		}
		create(service)
		service.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: vip}}
		Expect(k8sClient.Status().Update(ctx, service)).To(Succeed())
		create(unstructuredObject("route.openshift.io/v1", "Route", hcpNamespace, "oauth", map[string]any{
			"spec": map[string]any{"host": "oauth-" + name + ".apps.mgmt.example.com"},
			"status": map[string]any{"ingress": []any{map[string]any{
				"conditions": []any{map[string]any{"type": "Admitted", "status": "True"}},
			}}},
		}))
		create(unstructuredObject("agent-install.openshift.io/v1beta1", "InfraEnv", ns, name+"-infraenv", map[string]any{
			"spec":   map[string]any{},
			"status": map[string]any{"bootArtifacts": map[string]any{"discoveryIgnitionURL": "https://assisted.mgmt.example.com/ignition"}},
		}))

		co := &osacv1alpha1.ClusterOrder{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns,
				Labels:      map[string]string{osacClusterOrderIDLabel: id},
				Annotations: map[string]string{osacTenantKey: "tenant-1"},
				Finalizers:  []string{osacFinalizer},
			},
			Spec: osacv1alpha1.ClusterOrderSpec{TemplateID: "t", NetworkAttachment: &osacv1alpha1.ClusterNetworkAttachment{
				SubnetRef: subnetID, SecurityGroupRefs: []string{name + "-sg"},
			}},
		}
		create(co)
		co.Status.Phase = osacv1alpha1.ClusterOrderPhaseProgressing
		co.Status.ClusterReference = &osacv1alpha1.ClusterOrderClusterReferenceType{Namespace: ns, HostedClusterName: name}
		Expect(k8sClient.Status().Update(ctx, co)).To(Succeed())

		engine, fake := newTestEngine(enabled)
		f := &coFixture{
			aap:         fake,
			attachments: &testExternalIPAttachmentsClient{},
			guest:       &fakeGuest{readyNodes: 3},
			prober:      &fakeProber{},
			key:         client.ObjectKeyFromObject(co),
			id:          id,
			subnetID:    subnetID,
		}
		f.r = &ClusterOrderReconciler{
			Client:                      indexedClient,
			apiReader:                   k8sClient,
			NetworkingNamespace:         ns,
			BareMetalInstanceNamespace:  ns,
			StatusPollInterval:          100 * time.Millisecond,
			Engine:                      engine,
			EndpointsTemplate:           "osac-cluster-endpoints",
			PostInstallTemplate:         "osac-create-hosted-cluster-post-install",
			Prober:                      f.prober,
			Guest:                       f.guest,
			ExternalIPAttachmentsClient: f.attachments,
		}
		return f
	}

	networkingUntil := func(f *coFixture, check func(g Gomega, co *osacv1alpha1.ClusterOrder)) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			_, _ = f.r.reconcileNetworking(ctx, f.key)
			co := &osacv1alpha1.ClusterOrder{}
			g.Expect(k8sClient.Get(ctx, f.key, co)).To(Succeed())
			check(g, co)
		}, 15*time.Second, 100*time.Millisecond).Should(Succeed())
	}

	conditionOf := func(co *osacv1alpha1.ClusterOrder, t osacv1alpha1.ClusterOrderConditionType) *metav1.Condition {
		return apimeta.FindStatusCondition(co.Status.Conditions, string(t))
	}

	automaticIP := func(f *coFixture, endpoint, address string) *osacv1alpha1.ExternalIP {
		ip := &osacv1alpha1.ExternalIP{
			ObjectMeta: metav1.ObjectMeta{Name: "auto-eip-" + f.id + "-" + endpoint, Namespace: ns, Labels: map[string]string{
				autoCreatedLabel: labelValueTrue, autoCreatedForLabel: f.id, autoCreatedKindLabel: autoCreatedKindCluster,
				autoCreatedEndpointLabel: endpoint, externalIPUUIDLabel: f.id + "-eip-" + endpoint,
			}, Annotations: map[string]string{tenantAnnotation: "tenant-1"}},
			Spec: osacv1alpha1.ExternalIPSpec{Pool: "pool"},
		}
		create(ip)
		ip.Status.Address = address
		ip.Status.Conditions = allocatedConditions(address)
		Expect(k8sClient.Status().Update(ctx, ip)).To(Succeed())
		return ip
	}

	automaticAttachment := func(f *coFixture, endpoint string) *osacv1alpha1.ExternalIPAttachment {
		cluster := f.id
		target := osacv1alpha1.ExternalIPAttachmentTargetEndpointAPI
		if endpoint == autoCreatedEndpointIngress {
			target = osacv1alpha1.ExternalIPAttachmentTargetEndpointIngress
		}
		attachment := &osacv1alpha1.ExternalIPAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "auto-eipa-" + f.id + "-" + endpoint, Namespace: ns, Labels: map[string]string{
				autoCreatedLabel: labelValueTrue, autoCreatedForLabel: f.id, autoCreatedEndpointLabel: endpoint,
			}},
			Spec: osacv1alpha1.ExternalIPAttachmentSpec{ExternalIP: f.id + "-eip-" + endpoint, Cluster: &cluster, TargetEndpoint: &target},
		}
		create(attachment)
		attachment.Status.Conditions = []metav1.Condition{{Type: osacv1alpha1.NetworkConditionReady, Status: metav1.ConditionTrue,
			Reason: osacv1alpha1.ReasonSucceeded, LastTransitionTime: metav1.Now()}}
		Expect(k8sClient.Status().Update(ctx, attachment)).To(Succeed())
		return attachment
	}

	It("publishes the management API, reserves and configures guest ingress, and publishes the DNS records", func() {
		f := newCluster("caas-flow", true)

		By("passing the publication gate and reserving the guest ingress address")
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionManagementPublicationReady)
			g.Expect(c).NotTo(BeNil())
			g.Expect(c.Status).To(Equal(metav1.ConditionTrue), c.Message)
			g.Expect(co.Status.ApiEndpoint).To(Equal(vip))
			g.Expect(networktask.FindJob(co.Status.Jobs, networktask.ReserveClusterIngressIP, osacv1alpha1.NetworkJobManagerFabric)).NotTo(BeNil())
			g.Expect(conditionOf(co, osacv1alpha1.ClusterOrderConditionEndpointsReady).Reason).To(
				Equal(osacv1alpha1.ClusterOrderReasonGuestIngressProgressing))
		})
		reserve := f.aap.last(networktask.ReserveClusterIngressIP)
		Expect(reserve.template).To(Equal("osac-reserve-cluster-ingress-ip"))
		Expect(reserve.label).To(Equal("osac-reserve_cluster_ingress_ip-fabric-" + f.id + "-1"))
		Expect(reserve.vars).To(HaveKeyWithValue("cluster_id", f.id))
		Expect(reserve.vars).To(HaveKeyWithValue("tenant", "tenant-1"))
		Expect(reserve.vars["ipam"]).To(HaveKeyWithValue("lb_vip_ipv4_cidrs", []any{"10.0.1.192/26"}))
		Expect(reserve.vars["resource"]).To(HaveKeyWithValue("spec", map[string]any{}))
		Expect(reserve.vars["manager"]).To(Equal(map[string]any{"name": "netris", "type": "fabric", "ansible_role": "osac.templates.netris"}))

		f.aap.finish(networktask.ReserveClusterIngressIP, "successful", `{"osac_result": {"cluster_ingress": {"address": "10.0.1.200"}}}`)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(co.Status.IngressAllocatedAddress).To(Equal("10.0.1.200"))
			g.Expect(networktask.FindJob(co.Status.Jobs, configureGuestIngressTask, osacv1alpha1.NetworkJobManagerNone)).NotTo(BeNil())
			g.Expect(co.Status.TaskInput).NotTo(BeNil())
		})
		configure := f.aap.last(configureGuestIngressTask)
		Expect(configure.template).To(Equal("osac-cluster-endpoints"))
		Expect(configure.label).To(Equal("osac-configure_guest_ingress-none-" + f.id + "-1"))
		Expect(configure.vars).To(HaveKeyWithValue("ingress_allocated_address", "10.0.1.200"))
		Expect(configure.vars).To(HaveKeyWithValue("cluster_id", f.id))

		f.aap.finish(configureGuestIngressTask, "successful", "")
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(co.Status.IngressEndpoint).To(Equal("10.0.1.200"))
		})

		By("requesting the automatic ExternalIPAttachments of both endpoints")
		automaticIP(f, autoCreatedEndpointAPI, "203.0.113.10")
		automaticIP(f, autoCreatedEndpointIngress, "203.0.113.11")
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(f.attachments.created).To(HaveLen(2))
		})

		By("publishing the records once the attachments are READY")
		apiAttachment := automaticAttachment(f, autoCreatedEndpointAPI)
		automaticAttachment(f, autoCreatedEndpointIngress)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(networktask.FindJob(co.Status.Jobs, publishPublicDNSAPITask, osacv1alpha1.NetworkJobManagerNone)).NotTo(BeNil())
			g.Expect(networktask.FindJob(co.Status.Jobs, publishPublicDNSIngressTask, osacv1alpha1.NetworkJobManagerNone)).NotTo(BeNil())
			current := &osacv1alpha1.ExternalIPAttachment{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(apiAttachment), current)).To(Succeed())
			g.Expect(current.Finalizers).To(ContainElement(publicDNSFinalizer))
		})
		Expect(f.aap.last(publishPublicDNSAPITask).vars).To(HaveKeyWithValue("external_ip_address", "203.0.113.10"))
		Expect(f.aap.last(publishPublicDNSIngressTask).vars).To(HaveKeyWithValue("external_ip_address", "203.0.113.11"))

		f.aap.finish(publishPublicDNSAPITask, "successful", "")
		f.aap.finish(publishPublicDNSIngressTask, "successful", "")
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(co.Status.APIRecordsPublishedTime).NotTo(BeNil())
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionEndpointsReady)
			g.Expect(c.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(c.Reason).To(Equal(osacv1alpha1.ClusterOrderReasonEndpointsPublished))
		})

		By("removing the API records while the API attachment is deleted")
		Expect(k8sClient.Delete(ctx, apiAttachment)).To(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(networktask.FindJob(co.Status.Jobs, removePublicDNSAPITask, osacv1alpha1.NetworkJobManagerNone)).NotTo(BeNil())
		})
		current := &osacv1alpha1.ExternalIPAttachment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(apiAttachment), current)).To(Succeed())
		Expect(current.Finalizers).To(ContainElement(publicDNSFinalizer), "the records go before the finalizer")
		f.aap.finish(removePublicDNSAPITask, "successful", "")
		Eventually(func(g Gomega) {
			_, _ = f.r.reconcileNetworking(ctx, f.key)
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(apiAttachment), &osacv1alpha1.ExternalIPAttachment{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}, 15*time.Second, 100*time.Millisecond).Should(Succeed())

		By("releasing the guest ingress address on delete once the workers are gone")
		Expect(k8sClient.Delete(ctx, &osacv1alpha1.ClusterOrder{ObjectMeta: metav1.ObjectMeta{Name: f.key.Name, Namespace: ns}})).To(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(networktask.FindJob(co.Status.Jobs, networktask.ReleaseClusterIngressIP, osacv1alpha1.NetworkJobManagerFabric)).NotTo(BeNil())
		})
		release := f.aap.last(networktask.ReleaseClusterIngressIP)
		Expect(release.vars).To(HaveKeyWithValue("cluster_id", f.id))
		Expect(release.vars["ipam"]).To(Equal(reserve.vars["ipam"]), "the release uses the stored input")
		f.aap.finish(networktask.ReleaseClusterIngressIP, "successful", "")
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(co.Status.IngressAllocatedAddress).To(BeEmpty())
			g.Expect(f.r.clusterOrderReleasePending(co)).To(BeEmpty())
		})
	})

	It("cancels a running publish job when its automatic ExternalIPAttachment is deleted", func() {
		f := newCluster("caas-publish-cancel", true)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(networktask.FindJob(co.Status.Jobs, networktask.ReserveClusterIngressIP, osacv1alpha1.NetworkJobManagerFabric)).NotTo(BeNil())
		})
		f.aap.finish(networktask.ReserveClusterIngressIP, "successful", `{"osac_result": {"cluster_ingress": {"address": "10.0.1.200"}}}`)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(networktask.FindJob(co.Status.Jobs, configureGuestIngressTask, osacv1alpha1.NetworkJobManagerNone)).NotTo(BeNil())
		})
		f.aap.finish(configureGuestIngressTask, "successful", "")
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(co.Status.IngressEndpoint).To(Equal("10.0.1.200"))
		})
		automaticIP(f, autoCreatedEndpointAPI, "203.0.113.10")
		automaticIP(f, autoCreatedEndpointIngress, "203.0.113.11")
		apiAttachment := automaticAttachment(f, autoCreatedEndpointAPI)
		automaticAttachment(f, autoCreatedEndpointIngress)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(networktask.FindJob(co.Status.Jobs, publishPublicDNSAPITask, osacv1alpha1.NetworkJobManagerNone)).NotTo(BeNil())
			current := &osacv1alpha1.ExternalIPAttachment{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(apiAttachment), current)).To(Succeed())
			g.Expect(current.Finalizers).To(ContainElement(publicDNSFinalizer))
		})
		publish := strconv.Itoa(f.aap.last(publishPublicDNSAPITask).id)

		By("deleting the API attachment while its publish job runs")
		Expect(k8sClient.Delete(ctx, apiAttachment)).To(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			f.aap.mu.Lock()
			cancelled := append([]string(nil), f.aap.cancelled...)
			f.aap.mu.Unlock()
			g.Expect(cancelled).To(ContainElement(publish), "the running publish job is cancelled, not awaited")
			entry := networktask.FindJob(co.Status.Jobs, publishPublicDNSAPITask, osacv1alpha1.NetworkJobManagerNone)
			g.Expect(entry.State).To(Equal(osacv1alpha1.NetworkJobStateCancelled))
			g.Expect(networktask.FindJob(co.Status.Jobs, removePublicDNSAPITask, osacv1alpha1.NetworkJobManagerNone)).NotTo(BeNil())
		})
		f.aap.finish(removePublicDNSAPITask, "successful", "")
		Eventually(func(g Gomega) {
			_, _ = f.r.reconcileNetworking(ctx, f.key)
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(apiAttachment), &osacv1alpha1.ExternalIPAttachment{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}, 15*time.Second, 100*time.Millisecond).Should(Succeed())
	})

	It("waits for the workers before the release", func() {
		f := newCluster("caas-release-wait", true)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(networktask.FindJob(co.Status.Jobs, networktask.ReserveClusterIngressIP, osacv1alpha1.NetworkJobManagerFabric)).NotTo(BeNil())
		})
		f.aap.finish(networktask.ReserveClusterIngressIP, "successful", `{"osac_result": {"cluster_ingress": {"address": "10.0.1.201"}}}`)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(co.Status.IngressAllocatedAddress).To(Equal("10.0.1.201"))
		})
		Eventually(func() error {
			co := &osacv1alpha1.ClusterOrder{}
			if err := k8sClient.Get(ctx, f.key, co); err != nil {
				return err
			}
			controllerutil.AddFinalizer(co, bareMetalWorkerFinalizer)
			return k8sClient.Update(ctx, co)
		}).Should(Succeed())
		Expect(k8sClient.Delete(ctx, &osacv1alpha1.ClusterOrder{ObjectMeta: metav1.ObjectMeta{Name: f.key.Name, Namespace: ns}})).To(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(co.DeletionTimestamp).NotTo(BeNil())
			g.Expect(f.r.clusterOrderReleasePending(co)).To(Equal(clusterOrderDeletingReleaseIngress))
		})
		Expect(f.aap.tasks()).NotTo(ContainElement(networktask.ReleaseClusterIngressIP))
	})

	It("keeps the gate False when the VIP is inside the VirtualNetwork or an endpoint check fails", func() {
		f := newCluster("caas-gate", true)
		f.prober.failTLS = map[string]error{"oauth-caas-gate.apps.mgmt.example.com:443": errors.New("certificate expired")}
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionManagementPublicationReady)
			g.Expect(c).NotTo(BeNil())
			g.Expect(c.Reason).To(Equal(osacv1alpha1.ClusterOrderReasonManagementPublicationFailed))
			g.Expect(c.Message).To(ContainSubstring("certificate expired"))
			g.Expect(co.Status.ApiEndpoint).To(BeEmpty())
		})
	})

	It("fails the gate when the management API VIP changed after it was recorded", func() {
		f := newCluster("caas-vip-changed", true)
		Eventually(func() error {
			co := &osacv1alpha1.ClusterOrder{}
			if err := k8sClient.Get(ctx, f.key, co); err != nil {
				return err
			}
			co.Status.ApiEndpoint = "198.51.100.99"
			return k8sClient.Status().Update(ctx, co)
		}).Should(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionManagementPublicationReady)
			g.Expect(c).NotTo(BeNil())
			g.Expect(c.Reason).To(Equal(osacv1alpha1.ClusterOrderReasonManagementPublicationFailed))
			g.Expect(c.Message).To(Equal("management API VIP changed from 198.51.100.99 to " + vip + "; restore it"))
			g.Expect(co.Status.ApiEndpoint).To(Equal("198.51.100.99"))
		})
	})

	It("reports provider networking disabled and runs no networking job", func() {
		f := newCluster("caas-disabled", false)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			publication := conditionOf(co, osacv1alpha1.ClusterOrderConditionManagementPublicationReady)
			g.Expect(publication).NotTo(BeNil())
			g.Expect(publication.Status).To(Equal(metav1.ConditionTrue), publication.Message)
			g.Expect(co.Status.ApiEndpoint).To(Equal(vip))
			for _, t := range []osacv1alpha1.ClusterOrderConditionType{
				osacv1alpha1.ClusterOrderConditionEndpointsReady, osacv1alpha1.ClusterOrderConditionWorkerNetworkingReady,
			} {
				c := conditionOf(co, t)
				g.Expect(c).NotTo(BeNil())
				g.Expect(c.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(c.Reason).To(Equal(osacv1alpha1.ReasonProvisioningDisabled))
			}
		})
		Expect(f.aap.tasks()).To(BeEmpty())
	})

	It("fails EndpointsReady for a failed automatic ExternalIP and a failed reservation", func() {
		f := newCluster("caas-endpoint-failures", true)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			g.Expect(co.Status.Jobs).NotTo(BeEmpty())
		})
		f.aap.finish(networktask.ReserveClusterIngressIP, "failed", `{"osac_error": {"message": "LB VIP range exhausted", "retryable": false}}`)
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionEndpointsReady)
			g.Expect(c.Reason).To(Equal(osacv1alpha1.ClusterOrderReasonIngressReservationFailed))
			g.Expect(c.Message).To(Equal("LB VIP range exhausted"))
		})
		ip := automaticIP(f, autoCreatedEndpointAPI, "")
		ip.Status.Conditions = []metav1.Condition{{Type: osacv1alpha1.NetworkConditionReady, Status: metav1.ConditionFalse,
			Reason: osacv1alpha1.ReasonFailed, Message: "pool exhausted", LastTransitionTime: metav1.Now()}}
		Expect(k8sClient.Status().Update(ctx, ip)).To(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionEndpointsReady)
			g.Expect(c.Reason).To(Equal(osacv1alpha1.ClusterOrderReasonExternalAccessFailed))
			g.Expect(c.Message).To(ContainSubstring("pool exhausted"))
		})
	})

	It("reports worker networking failures from the worker BMIs and the SecurityGroups", func() {
		f := newCluster("caas-workers", true)
		bmi := &bmfov1alpha1.BareMetalInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "caas-workers-w1", Namespace: ns,
				Labels: map[string]string{osacBareMetalInstanceIDLabel: "caas-workers-bmi-1"}},
			Spec: bmfov1alpha1.BareMetalInstanceSpec{
				ExternalHostID: "ext-1",
				TemplateID:     "tpl_1",
				Selector:       bmfov1alpha1.HostSelectorSpec{HostSelector: map[string]string{"osac.openshift.io/host-type": "default"}},
			},
		}
		create(bmi)
		Eventually(func() error {
			co := &osacv1alpha1.ClusterOrder{}
			if err := k8sClient.Get(ctx, f.key, co); err != nil {
				return err
			}
			co.Status.Workers = []osacv1alpha1.WorkerStatus{{
				NodeSet: "workers", Name: "w1", Kind: "BareMetalInstance", Phase: "Provisioning",
				CreationTimestamp: metav1.Now(),
				BareMetalInstance: osacv1alpha1.BareMetalInstanceReference{Name: "caas-workers-w1", ID: "caas-workers-bmi-1"},
			}}
			return k8sClient.Status().Update(ctx, co)
		}).Should(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionWorkerNetworkingReady)
			g.Expect(c).NotTo(BeNil())
			g.Expect(c.Reason).To(Equal(osacv1alpha1.ClusterOrderReasonWorkersProgressing))
		})

		bmi.Status.Phase = bmfov1alpha1.BareMetalInstancePhaseFailed
		bmi.Status.Conditions = []metav1.Condition{{Type: string(bmfov1alpha1.HostConditionNetworkHandoffComplete),
			Status: metav1.ConditionFalse, Reason: "NetworkHandoffFailed", Message: "reboot not confirmed", LastTransitionTime: metav1.Now()}}
		Expect(k8sClient.Status().Update(ctx, bmi)).To(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionWorkerNetworkingReady)
			g.Expect(c.Reason).To(Equal(osacv1alpha1.ClusterOrderReasonWorkerNetworkingFailed))
			g.Expect(c.Message).To(ContainSubstring("NetworkHandoffFailed"))
		})

		group := &osacv1alpha1.SecurityGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "caas-workers-sg", Namespace: ns,
				Labels: map[string]string{networktask.IDLabel("SecurityGroup"): "caas-workers-sg"}},
			Spec: osacv1alpha1.SecurityGroupSpec{VirtualNetwork: "caas-workers-vn"},
		}
		create(group)
		group.Status.Conditions = []metav1.Condition{{Type: osacv1alpha1.NetworkConditionReady, Status: metav1.ConditionFalse,
			Reason: osacv1alpha1.ReasonFailed, Message: "rule rejected", LastTransitionTime: metav1.Now()}}
		Expect(k8sClient.Status().Update(ctx, group)).To(Succeed())
		networkingUntil(f, func(g Gomega, co *osacv1alpha1.ClusterOrder) {
			c := conditionOf(co, osacv1alpha1.ClusterOrderConditionWorkerNetworkingReady)
			g.Expect(c.Message).To(Equal("SecurityGroup caas-workers-sg failed: rule rejected"))
		})
	})
})

var _ = Describe("ClusterOrder Ready rule", func() {
	ctx := context.Background()

	readyOrder := func(endpoints metav1.ConditionStatus) *osacv1alpha1.ClusterOrder {
		co := &osacv1alpha1.ClusterOrder{Spec: osacv1alpha1.ClusterOrderSpec{NodeRequests: []osacv1alpha1.NodeRequest{
			{NodeSet: "workers", NumberOfNodes: 2, BareMetal: &osacv1alpha1.BareMetalNodeSpec{InstanceType: "bm"}},
		}}}
		co.SetStatusCondition(string(osacv1alpha1.ClusterOrderConditionEndpointsReady), endpoints, "waiting for publish_public_dns_api",
			osacv1alpha1.ClusterOrderReasonEndpointsProgressing)
		return co
	}
	hc := &hypershiftv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1"},
		Spec:   hypershiftv1beta1.HostedClusterSpec{DNS: hypershiftv1beta1.DNSSpec{BaseDomain: "example.com"}},
		Status: hypershiftv1beta1.HostedClusterStatus{ControlPlaneEndpoint: hypershiftv1beta1.APIEndpoint{Port: 7443}}}

	It("waits for EndpointsReady, a Ready node per worker and every ClusterOperator", func() {
		engine, _ := newTestEngine(true)
		r := &ClusterOrderReconciler{Engine: engine, Guest: &fakeGuest{readyNodes: 2}}
		reason, message := r.clusterReadyBlocker(ctx, readyOrder(metav1.ConditionFalse), hc)
		Expect(reason).To(Equal(osacv1alpha1.ClusterOrderReasonEndpointsProgressing))
		Expect(message).To(Equal("waiting for publish_public_dns_api"))

		r.Guest = &fakeGuest{readyNodes: 1}
		reason, _ = r.clusterReadyBlocker(ctx, readyOrder(metav1.ConditionTrue), hc)
		Expect(reason).To(Equal(osacv1alpha1.ReasonWorkersJoining))

		r.Guest = &fakeGuest{readyNodes: 2, unavailable: "ingress"}
		_, message = r.clusterReadyBlocker(ctx, readyOrder(metav1.ConditionTrue), hc)
		Expect(message).To(Equal("ClusterOperator ingress is not Available"))

		r.Guest = &fakeGuest{readyNodes: 2}
		reason, _ = r.clusterReadyBlocker(ctx, readyOrder(metav1.ConditionTrue), hc)
		Expect(reason).To(BeEmpty())
	})

	It("sets the URLs from the API frontend port", func() {
		co := &osacv1alpha1.ClusterOrder{}
		setClusterURLs(co, hc)
		Expect(co.Status.APIURL).To(Equal("https://api.c1.example.com:7443"))
		Expect(co.Status.ConsoleURL).To(Equal("https://console-openshift-console.apps.c1.example.com"))
	})
})

var _ = Describe("Publication certificate check", func() {
	newCertificate := func(names []string, notBefore, notAfter time.Time) *x509.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		Expect(err).NotTo(HaveOccurred())
		template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: names, NotBefore: notBefore, NotAfter: notAfter}
		der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
		Expect(err).NotTo(HaveOccurred())
		certificate, err := x509.ParseCertificate(der)
		Expect(err).NotTo(HaveOccurred())
		return certificate
	}
	now := time.Now()

	It("accepts a current certificate for the name", func() {
		Expect(checkServerCertificate(newCertificate([]string{"api.c1.example.com"}, now.Add(-time.Hour), now.Add(time.Hour)),
			"api.c1.example.com", now)).To(Succeed())
	})

	It("rejects another name and an expired certificate", func() {
		Expect(checkServerCertificate(newCertificate([]string{"api.c2.example.com"}, now.Add(-time.Hour), now.Add(time.Hour)),
			"api.c1.example.com", now)).NotTo(Succeed())
		Expect(checkServerCertificate(newCertificate([]string{"api.c1.example.com"}, now.Add(-2*time.Hour), now.Add(-time.Hour)),
			"api.c1.example.com", now)).NotTo(Succeed())
	})
})
