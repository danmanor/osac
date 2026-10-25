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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"time"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

const (
	// publicationTimeout bounds each DNS lookup, TCP connection, TLS
	// handshake and guest API request (CaaS design, Publication gate).
	publicationTimeout = 10 * time.Second

	// kubeAPIServerServiceName is the hosted kube-apiserver Service that
	// management MetalLB gives the management API VIP.
	kubeAPIServerServiceName = "kube-apiserver"
	// adminKubeconfigSecretName is the in-hub admin kubeconfig of a hosted
	// control plane namespace.
	adminKubeconfigSecretName = "service-network-admin-kubeconfig"
	// internalRouteLabel marks HyperShift Routes that are not published.
	internalRouteLabel = "hypershift.openshift.io/internal-route"

	defaultClusterPodCIDR     = "10.128.0.0/14"
	defaultClusterServiceCIDR = "172.30.0.0/16"
)

var (
	routeListGVK           = schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "RouteList"}
	clusterOperatorListGVK = schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "ClusterOperatorList"}
	infraEnvObjectGVK      = schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: "v1beta1", Kind: "InfraEnv"}
)

// hostedControlPlaneNamespace is the namespace of a HostedCluster's control plane.
func hostedControlPlaneNamespace(hc *hypershiftv1beta1.HostedCluster) string {
	return hc.Namespace + "-" + hc.Name
}

// PublicationProber runs the network checks of the management publication
// gate from the hub.
type PublicationProber interface {
	// Resolve resolves host in the provider DNS.
	Resolve(ctx context.Context, host string) error
	// ProbeTLS connects to address (host:port) and checks that the server
	// certificate is valid for serverName.
	ProbeTLS(ctx context.Context, address, serverName string) error
}

// netPublicationProber is the PublicationProber of the operator.
type netPublicationProber struct{}

// NewPublicationProber returns the prober that resolves names and connects
// from the operator pod, each step with a 10-second timeout.
func NewPublicationProber() PublicationProber { return netPublicationProber{} }

func (netPublicationProber) Resolve(ctx context.Context, host string) error {
	ctx, cancel := context.WithTimeout(ctx, publicationTimeout)
	defer cancel()
	addresses, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return err
	}
	if len(addresses) == 0 {
		return fmt.Errorf("%s has no address", host)
	}
	return nil
}

func (netPublicationProber) ProbeTLS(ctx context.Context, address, serverName string) error {
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: publicationTimeout},
		// The certificate chain is the hosted control plane's own CA; the gate
		// checks only that the certificate names the published host and is
		// current (CaaS design, Publication gate).
		Config: &tls.Config{InsecureSkipVerify: true, ServerName: serverName}, //nolint:gosec
	}
	ctx, cancel := context.WithTimeout(ctx, publicationTimeout)
	defer cancel()
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	certificates := conn.(*tls.Conn).ConnectionState().PeerCertificates
	if len(certificates) == 0 {
		return errors.New("no server certificate")
	}
	return checkServerCertificate(certificates[0], serverName, time.Now())
}

// checkServerCertificate checks that a certificate is current and valid for serverName.
func checkServerCertificate(certificate *x509.Certificate, serverName string, now time.Time) error {
	if now.Before(certificate.NotBefore) || now.After(certificate.NotAfter) {
		return fmt.Errorf("certificate is not valid at %s", now.UTC().Format(time.RFC3339))
	}
	return certificate.VerifyHostname(serverName)
}

// publicationResult is the outcome of the management publication gate.
type publicationResult struct {
	ready   bool
	reason  string
	message string
	vip     string
	port    int32
}

func publicationPending(format string, args ...any) publicationResult {
	return publicationResult{reason: v1alpha1.ClusterOrderReasonManagementPublicationPending, message: fmt.Sprintf(format, args...)}
}

func publicationFailed(format string, args ...any) publicationResult {
	return publicationResult{reason: v1alpha1.ClusterOrderReasonManagementPublicationFailed, message: fmt.Sprintf(format, args...)}
}

// checkManagementPublication evaluates the publication gate from current
// resources (CaaS design, Publication gate and Publication field sources).
// excluded are the CIDRs the VIP must stay outside (the VirtualNetwork CIDR is
// added by the caller when known).
func checkManagementPublication(ctx context.Context, reader client.Reader, prober PublicationProber,
	co *v1alpha1.ClusterOrder, hc *hypershiftv1beta1.HostedCluster, excluded []string) publicationResult {
	if hc == nil {
		return publicationPending("waiting for the HostedCluster")
	}
	vip, port, blocked := checkAPIService(ctx, reader, co, hc, excluded)
	if blocked != nil {
		return *blocked
	}
	hosts, blocked := checkControlPlaneRoutes(ctx, reader, hostedControlPlaneNamespace(hc))
	if blocked != nil {
		return *blocked
	}
	discovery, blocked := discoveryIgnitionURL(ctx, reader, co)
	if blocked != nil {
		return *blocked
	}
	if blocked := probePublishedEndpoints(ctx, prober, hc, vip, port, hosts, discovery); blocked != nil {
		return *blocked
	}
	return publicationResult{ready: true, vip: vip, port: port}
}

// checkAPIService checks the hosted control plane's kube-apiserver Service
// and returns its VIP and port, or the result that blocks the gate.
func checkAPIService(ctx context.Context, reader client.Reader, co *v1alpha1.ClusterOrder,
	hc *hypershiftv1beta1.HostedCluster, excluded []string) (string, int32, *publicationResult) {
	blocked := func(result publicationResult) (string, int32, *publicationResult) { return "", 0, &result }
	hcpNamespace := hostedControlPlaneNamespace(hc)
	service := &corev1.Service{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: hcpNamespace, Name: kubeAPIServerServiceName}, service); err != nil {
		if apierrors.IsNotFound(err) {
			return blocked(publicationPending("waiting for Service %s/%s", hcpNamespace, kubeAPIServerServiceName))
		}
		return blocked(publicationPending("reading Service %s/%s: %v", hcpNamespace, kubeAPIServerServiceName, err))
	}
	owned := false
	for _, owner := range service.OwnerReferences {
		if owner.Kind == "HostedControlPlane" && owner.Name == hc.Name {
			owned = true
		}
	}
	if !owned {
		return blocked(publicationFailed("Service %s/%s is not owned by HostedControlPlane %s", hcpNamespace, kubeAPIServerServiceName, hc.Name))
	}
	var vips []string
	for _, ingress := range service.Status.LoadBalancer.Ingress {
		if ingress.IP != "" {
			vips = append(vips, ingress.IP)
		}
	}
	switch {
	case len(vips) == 0:
		return blocked(publicationPending("waiting for the management API VIP of Service %s/%s", hcpNamespace, kubeAPIServerServiceName))
	case len(vips) > 1:
		return blocked(publicationFailed("Service %s/%s has more than one VIP", hcpNamespace, kubeAPIServerServiceName))
	case len(service.Spec.Ports) != 1:
		return blocked(publicationFailed("Service %s/%s must have exactly one port", hcpNamespace, kubeAPIServerServiceName))
	}
	vip := vips[0]
	port := service.Spec.Ports[0].Port
	address, err := netip.ParseAddr(vip)
	if err != nil || !address.Is4() {
		return blocked(publicationFailed("management API VIP %q is not an IPv4 address", vip))
	}
	if recorded := co.Status.ApiEndpoint; recorded != "" && recorded != vip {
		return blocked(publicationFailed("management API VIP changed from %s to %s; restore it", recorded, vip))
	}
	for _, cidr := range excluded {
		if prefix, err := netip.ParsePrefix(cidr); err == nil && prefix.Contains(address) {
			return blocked(publicationFailed("management API VIP %s is inside %s", vip, cidr))
		}
	}
	switch endpointPort := hc.Status.ControlPlaneEndpoint.Port; {
	case endpointPort == 0:
		return blocked(publicationPending("waiting for the HostedCluster control plane endpoint"))
	case endpointPort != port:
		return blocked(publicationFailed("HostedCluster control plane endpoint port %d differs from the API frontend port %d", endpointPort, port))
	}
	return vip, port, nil
}

// checkControlPlaneRoutes returns the hosts of the admitted external hosted
// control plane Routes, or the result that blocks the gate.
func checkControlPlaneRoutes(ctx context.Context, reader client.Reader, hcpNamespace string) ([]string, *publicationResult) {
	blocked := func(result publicationResult) ([]string, *publicationResult) { return nil, &result }
	routes := &unstructured.UnstructuredList{}
	routes.SetGroupVersionKind(routeListGVK)
	if err := reader.List(ctx, routes, client.InNamespace(hcpNamespace)); err != nil {
		return blocked(publicationPending("listing the Routes of %s: %v", hcpNamespace, err))
	}
	var hosts []string
	for i := range routes.Items {
		route := &routes.Items[i]
		if route.GetLabels()[internalRouteLabel] == labelValueTrue {
			continue
		}
		host, _, _ := unstructured.NestedString(route.Object, "spec", "host")
		if host == "" || !routeAdmitted(route) {
			return blocked(publicationFailed("Route %s/%s is not admitted", hcpNamespace, route.GetName()))
		}
		hosts = append(hosts, host)
	}
	if len(hosts) == 0 {
		return blocked(publicationPending("waiting for the hosted control plane Routes in %s", hcpNamespace))
	}
	return hosts, nil
}

// discoveryIgnitionURL returns the discovery ignition URL of the cluster's
// InfraEnv, or the result that blocks the gate.
func discoveryIgnitionURL(ctx context.Context, reader client.Reader, co *v1alpha1.ClusterOrder) (string, *publicationResult) {
	infraEnv := &unstructured.Unstructured{}
	infraEnv.SetGroupVersionKind(infraEnvObjectGVK)
	if err := reader.Get(ctx, client.ObjectKey{Namespace: co.Namespace, Name: co.Name + "-infraenv"}, infraEnv); err != nil {
		result := publicationPending("waiting for the InfraEnv of the cluster")
		return "", &result
	}
	discovery, _, _ := unstructured.NestedString(infraEnv.Object, "status", "bootArtifacts", "discoveryIgnitionURL")
	if discovery == "" {
		result := publicationPending("waiting for the InfraEnv discovery ignition URL")
		return "", &result
	}
	return discovery, nil
}

// probePublishedEndpoints probes the API VIP, the Route hosts and the
// assisted service endpoint; it returns the result that blocks the gate, or nil.
func probePublishedEndpoints(ctx context.Context, prober PublicationProber, hc *hypershiftv1beta1.HostedCluster,
	vip string, port int32, hosts []string, discovery string) *publicationResult {
	failed := func(format string, args ...any) *publicationResult {
		result := publicationFailed(format, args...)
		return &result
	}
	apiName := fmt.Sprintf("api.%s.%s", hc.Name, hc.Spec.DNS.BaseDomain)
	apiAddress := net.JoinHostPort(vip, strconv.Itoa(int(port)))
	if err := prober.ProbeTLS(ctx, apiAddress, apiName); err != nil {
		return failed("API endpoint %s (%s): %v", apiAddress, apiName, err)
	}
	endpoints := make([]string, 0, len(hosts)+1)
	for _, host := range hosts {
		endpoints = append(endpoints, net.JoinHostPort(host, "443"))
	}
	assisted, err := url.Parse(discovery)
	if err != nil || assisted.Hostname() == "" {
		return failed("the InfraEnv discovery ignition URL is not a valid URL")
	}
	assistedPort := assisted.Port()
	if assistedPort == "" {
		assistedPort = "443"
	}
	endpoints = append(endpoints, net.JoinHostPort(assisted.Hostname(), assistedPort))
	for _, endpoint := range endpoints {
		host, _, _ := net.SplitHostPort(endpoint)
		if err := prober.Resolve(ctx, host); err != nil {
			return failed("%s does not resolve: %v", host, err)
		}
		if err := prober.ProbeTLS(ctx, endpoint, host); err != nil {
			return failed("%s: %v", endpoint, err)
		}
	}
	return nil
}

// routeAdmitted reports whether a Route has an Admitted=True ingress condition.
func routeAdmitted(route *unstructured.Unstructured) bool {
	ingresses, _, _ := unstructured.NestedSlice(route.Object, "status", "ingress")
	for _, ingress := range ingresses {
		entry, ok := ingress.(map[string]any)
		if !ok {
			continue
		}
		conditions, _, _ := unstructured.NestedSlice(entry, "conditions")
		for _, condition := range conditions {
			c, ok := condition.(map[string]any)
			if ok && c["type"] == "Admitted" && c["status"] == "True" {
				return true
			}
		}
	}
	return false
}

// GuestClusterAccess reads the hosted (guest) cluster through the in-hub admin
// kubeconfig (CaaS design, Hub access to the guest API).
type GuestClusterAccess interface {
	// Usable returns nil when a guest API request succeeds within 10 seconds.
	Usable(ctx context.Context, hc *hypershiftv1beta1.HostedCluster) error
	// ReadyNodes returns the number of Ready guest nodes.
	ReadyNodes(ctx context.Context, hc *hypershiftv1beta1.HostedCluster) (int, error)
	// UnavailableClusterOperator returns the first ClusterOperator that is not
	// Available=True, or "" when all are.
	UnavailableClusterOperator(ctx context.Context, hc *hypershiftv1beta1.HostedCluster) (string, error)
}

// adminKubeconfigGuestAccess is the GuestClusterAccess of the operator: the
// credentials of Secret service-network-admin-kubeconfig with server
// https://kube-apiserver.<hosted control plane namespace>.svc:<Service port>.
type adminKubeconfigGuestAccess struct {
	hub client.Reader
}

// NewGuestClusterAccess returns the GuestClusterAccess that reads the in-hub
// admin kubeconfig with the hub reader.
func NewGuestClusterAccess(hub client.Reader) GuestClusterAccess {
	return &adminKubeconfigGuestAccess{hub: hub}
}

func (g *adminKubeconfigGuestAccess) client(ctx context.Context, hc *hypershiftv1beta1.HostedCluster) (client.Client, error) {
	namespace := hostedControlPlaneNamespace(hc)
	secret := &corev1.Secret{}
	if err := g.hub.Get(ctx, client.ObjectKey{Namespace: namespace, Name: adminKubeconfigSecretName}, secret); err != nil {
		return nil, fmt.Errorf("reading the in-hub admin kubeconfig: %w", err)
	}
	config, err := clientcmd.RESTConfigFromKubeConfig(secret.Data["kubeconfig"])
	if err != nil {
		return nil, fmt.Errorf("parsing the in-hub admin kubeconfig: %w", err)
	}
	service := &corev1.Service{}
	if err := g.hub.Get(ctx, client.ObjectKey{Namespace: namespace, Name: kubeAPIServerServiceName}, service); err != nil {
		return nil, fmt.Errorf("reading Service %s/%s: %w", namespace, kubeAPIServerServiceName, err)
	}
	if len(service.Spec.Ports) == 0 {
		return nil, fmt.Errorf("service %s/%s has no port", namespace, kubeAPIServerServiceName)
	}
	config.Host = fmt.Sprintf("https://%s.%s.svc:%d", kubeAPIServerServiceName, namespace, service.Spec.Ports[0].Port)
	config.Timeout = publicationTimeout
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return client.New(config, client.Options{Scheme: scheme})
}

func (g *adminKubeconfigGuestAccess) Usable(ctx context.Context, hc *hypershiftv1beta1.HostedCluster) error {
	if !apimeta.IsStatusConditionTrue(hc.Status.Conditions, string(hypershiftv1beta1.HostedClusterAvailable)) {
		return errors.New("the HostedCluster is not Available")
	}
	guest, err := g.client(ctx, hc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, publicationTimeout)
	defer cancel()
	return guest.Get(ctx, client.ObjectKey{Name: "default"}, &corev1.Namespace{})
}

func (g *adminKubeconfigGuestAccess) ReadyNodes(ctx context.Context, hc *hypershiftv1beta1.HostedCluster) (int, error) {
	guest, err := g.client(ctx, hc)
	if err != nil {
		return 0, err
	}
	nodes := &corev1.NodeList{}
	if err := guest.List(ctx, nodes); err != nil {
		return 0, err
	}
	ready := 0
	for _, node := range nodes.Items {
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				ready++
			}
		}
	}
	return ready, nil
}

func (g *adminKubeconfigGuestAccess) UnavailableClusterOperator(ctx context.Context, hc *hypershiftv1beta1.HostedCluster) (string, error) {
	guest, err := g.client(ctx, hc)
	if err != nil {
		return "", err
	}
	operators := &unstructured.UnstructuredList{}
	operators.SetGroupVersionKind(clusterOperatorListGVK)
	if err := guest.List(ctx, operators); err != nil {
		return "", err
	}
	if len(operators.Items) == 0 {
		return "the cluster operators", nil
	}
	for i := range operators.Items {
		conditions, _, _ := unstructured.NestedSlice(operators.Items[i].Object, "status", "conditions")
		available := false
		for _, condition := range conditions {
			c, ok := condition.(map[string]any)
			if ok && c["type"] == "Available" && c["status"] == "True" {
				available = true
			}
		}
		if !available {
			return operators.Items[i].GetName(), nil
		}
	}
	return "", nil
}
