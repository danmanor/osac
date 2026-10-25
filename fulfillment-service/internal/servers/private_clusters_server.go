/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package servers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"

	"github.com/bits-and-blooms/bitset"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/osac-project/osac/fulfillment-service/internal/auth"
	"github.com/osac-project/osac/fulfillment-service/internal/database"
	"github.com/osac-project/osac/fulfillment-service/internal/database/dao"
	"github.com/osac-project/osac/fulfillment-service/internal/utils"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

type PrivateClustersServerBuilder struct {
	logger                       *slog.Logger
	attributionLogic             auth.AttributionLogic
	tenancyLogic                 auth.TenancyLogic
	metricsRegisterer            prometheus.Registerer
	filterDesc                   protoreflect.MessageDescriptor
	addOnOperatorResolverFactory addOnOperatorResolverFactory
	networkingSettings           *NetworkingSettings
}

var _ privatev1.ClustersServer = (*PrivateClustersServer)(nil)

type PrivateClustersServer struct {
	privatev1.UnimplementedClustersServer
	logger                    *slog.Logger
	tenancyLogic              auth.TenancyLogic
	templatesDao              *dao.GenericDAO[*privatev1.ClusterTemplate]
	catalogItemsDao           *dao.GenericDAO[*privatev1.ClusterCatalogItem]
	clusterVersionsDao        *dao.GenericDAO[*privatev1.ClusterVersion]
	networkClassesDao         *dao.GenericDAO[*privatev1.NetworkClass]
	subnetsDao                *dao.GenericDAO[*privatev1.Subnet]
	securityGroupsDao         *dao.GenericDAO[*privatev1.SecurityGroup]
	externalIPPoolDao         *dao.GenericDAO[*privatev1.ExternalIPPool]
	externalIPDao             *dao.GenericDAO[*privatev1.ExternalIP]
	externalIPAttachmentDao   *dao.GenericDAO[*privatev1.ExternalIPAttachment]
	secretsDao                *dao.GenericDAO[*privatev1.Secret]
	addOnOperators            *addOnOperatorResourceResolver
	generic                   *GenericServer[*privatev1.Cluster]
	lifecycle                 *externalIPLifecycle
	bareMetalInstanceTypesDao *dao.GenericDAO[*privatev1.BareMetalInstanceType]
	settings                  NetworkingSettings
	guards                    *networkingGuards
}

func NewPrivateClustersServer() *PrivateClustersServerBuilder {
	return &PrivateClustersServerBuilder{}
}

func (b *PrivateClustersServerBuilder) SetLogger(value *slog.Logger) *PrivateClustersServerBuilder {
	b.logger = value
	return b
}

func (b *PrivateClustersServerBuilder) SetAttributionLogic(value auth.AttributionLogic) *PrivateClustersServerBuilder {
	b.attributionLogic = value
	return b
}

func (b *PrivateClustersServerBuilder) SetTenancyLogic(value auth.TenancyLogic) *PrivateClustersServerBuilder {
	b.tenancyLogic = value
	return b
}

// SetMetricsRegisterer sets the Prometheus registerer used to register the metrics for the underlying database
// access objects. This is optional. If not set, no metrics will be recorded.
func (b *PrivateClustersServerBuilder) SetMetricsRegisterer(value prometheus.Registerer) *PrivateClustersServerBuilder {
	b.metricsRegisterer = value
	return b
}

// SetFilterDesc sets the protobuf message descriptor used to validate and translate CEL filter
// expressions. This is optional. When unset, the descriptor of this server's own private message type is used.
func (b *PrivateClustersServerBuilder) SetFilterDesc(value protoreflect.MessageDescriptor) *PrivateClustersServerBuilder {
	b.filterDesc = value
	return b
}

// SetAddOnOperatorResolverFactory sets the resolver policy used with the server-owned DAO.
func (b *PrivateClustersServerBuilder) SetAddOnOperatorResolverFactory(value addOnOperatorResolverFactory) *PrivateClustersServerBuilder {
	b.addOnOperatorResolverFactory = value
	return b
}

type addOnOperatorResolverFactory func(*dao.GenericDAO[*privatev1.AddOnOperator]) *addOnOperatorResourceResolver

// SetNetworkingSettings sets the networking installation settings. This is optional; the default is
// DefaultNetworkingSettings.
func (b *PrivateClustersServerBuilder) SetNetworkingSettings(value NetworkingSettings) *PrivateClustersServerBuilder {
	b.networkingSettings = &value
	return b
}

func (b *PrivateClustersServerBuilder) Build() (result *PrivateClustersServer, err error) {
	// Check parameters:
	if b.logger == nil {
		err = errors.New("logger is mandatory")
		return
	}
	if b.tenancyLogic == nil {
		err = errors.New("tenancy logic is mandatory")
		return
	}
	networkClassesDao, err := newNetworkClassesDAO(b.logger, b.tenancyLogic, b.metricsRegisterer)
	if err != nil {
		return
	}
	// Create the templates DAO:
	templatesDao, err := dao.NewGenericDAO[*privatev1.ClusterTemplate]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	// Create the catalog items DAO:
	catalogItemsDao, err := dao.NewGenericDAO[*privatev1.ClusterCatalogItem]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	// Create the private add-on operators DAO:
	addOnOperatorsDao, err := dao.NewGenericDAO[*privatev1.AddOnOperator]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}
	var addOnOperators *addOnOperatorResourceResolver
	if b.addOnOperatorResolverFactory != nil {
		addOnOperators = b.addOnOperatorResolverFactory(addOnOperatorsDao)
	} else {
		addOnOperators = newScopedAddOnOperatorResourceResolver(addOnOperatorsDao)
	}

	// Create the bare metal instance types DAO:
	bareMetalInstanceTypesDao, err := dao.NewGenericDAO[*privatev1.BareMetalInstanceType]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	// Create the cluster versions DAO:
	clusterVersionsDao, err := dao.NewGenericDAO[*privatev1.ClusterVersion]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	// Create the subnets DAO:
	subnetsDao, err := dao.NewGenericDAO[*privatev1.Subnet]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	// Create the ExternalIP DAOs:
	externalIPPoolDaoBuilder := dao.NewGenericDAO[*privatev1.ExternalIPPool]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer)
	externalIPPoolDao, err := externalIPPoolDaoBuilder.Build()
	if err != nil {
		return
	}

	// Create the security groups DAO:
	securityGroupsDao, err := dao.NewGenericDAO[*privatev1.SecurityGroup]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	externalIPDaoBuilder := dao.NewGenericDAO[*privatev1.ExternalIP]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer)
	externalIPDao, err := externalIPDaoBuilder.Build()
	if err != nil {
		return
	}

	externalIPAttachmentDaoBuilder := dao.NewGenericDAO[*privatev1.ExternalIPAttachment]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer)
	externalIPAttachmentDao, err := externalIPAttachmentDaoBuilder.Build()
	if err != nil {
		return
	}

	// Create the secrets DAO:
	secretsDao, err := dao.NewGenericDAO[*privatev1.Secret]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	// Create the generic server:
	generic, err := NewGenericServer[*privatev1.Cluster]().
		SetLogger(b.logger).
		SetService(privatev1.Clusters_ServiceDesc.ServiceName).
		SetAttributionLogic(b.attributionLogic).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		SetFilterDesc(b.filterDesc).
		Build()
	if err != nil {
		return
	}

	// Create and populate the object:
	guards, err := newNetworkingGuards(b.logger)
	if err != nil {
		return
	}
	result = &PrivateClustersServer{
		settings:                  resolveNetworkingSettings(b.networkingSettings),
		guards:                    guards,
		logger:                    b.logger,
		tenancyLogic:              b.tenancyLogic,
		templatesDao:              templatesDao,
		catalogItemsDao:           catalogItemsDao,
		bareMetalInstanceTypesDao: bareMetalInstanceTypesDao,
		clusterVersionsDao:        clusterVersionsDao,
		networkClassesDao:         networkClassesDao,
		subnetsDao:                subnetsDao,
		securityGroupsDao:         securityGroupsDao,
		externalIPPoolDao:         externalIPPoolDao,
		externalIPDao:             externalIPDao,
		externalIPAttachmentDao:   externalIPAttachmentDao,
		secretsDao:                secretsDao,
		addOnOperators:            addOnOperators,
		generic:                   generic,
	}
	result.lifecycle = newExternalIPLifecycle(
		externalIPDao,
		externalIPAttachmentDao,
		nil,
		externalIPPoolDao,
		nil,
		generic.dao,
		nil,
		nil,
	)
	return
}

func (s *PrivateClustersServer) List(ctx context.Context,
	request *privatev1.ClustersListRequest) (response *privatev1.ClustersListResponse, err error) {
	err = s.generic.List(ctx, request, &response)
	return
}

func (s *PrivateClustersServer) Get(ctx context.Context,
	request *privatev1.ClustersGetRequest) (response *privatev1.ClustersGetResponse, err error) {
	err = s.generic.Get(ctx, request, &response)
	return
}

func (s *PrivateClustersServer) Create(ctx context.Context, request *privatev1.ClustersCreateRequest) (response *privatev1.ClustersCreateResponse, err error) {
	err = s.generic.CreateWithCandidatePreparation(ctx, request, &response, func(ctx context.Context, _ *privatev1.Cluster, candidate *privatev1.Cluster) error {
		if err := lockTenantForCreate(ctx, candidate.GetMetadata().GetTenant()); err != nil {
			return err
		}
		return s.prepareCreate(ctx, candidate)
	})
	if err != nil {
		return
	}
	if !isDryRun(ctx) {
		err = s.createAutomaticResources(ctx, response.GetObject())
		if err != nil {
			if tx, txErr := database.TxFromContext(ctx); txErr == nil {
				tx.ReportError(&err)
			}
			return
		}
	}
	return
}

// prepareCreate fills the new Cluster before it is stored. It selects either a published
// Catalog Item or a direct Template, applies Catalog rules and Template defaults, then
// checks the resulting network, node sets, and other required inputs.
func (s *PrivateClustersServer) prepareCreate(ctx context.Context, candidate *privatev1.Cluster) (err error) {
	// Ensure sane defaults:
	s.setDefaults(candidate)
	if err = validateClusterEndpointAddresses(candidate.GetStatus(), nil); err != nil {
		return
	}
	// Get the spec:
	spec := candidate.GetSpec()

	// Validate duplicate conditions first:
	err = s.validateNoDuplicateConditions(candidate)
	if err != nil {
		return
	}

	template, err := s.resolveCreationSource(ctx, candidate)
	if err != nil {
		return err
	}
	clusterVersion, err := s.applyClusterTemplate(ctx, candidate, template)
	if err != nil {
		return
	}
	if err := s.validateCreatedNodeSetVersions(ctx, candidate); err != nil {
		return err
	}
	if key := spec.GetSshPublicKey(); key != "" {
		if err = validateOpenSSHPublicKey(key); err != nil {
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "spec.ssh_public_key: %s", err)
		}
	}

	if err = s.validateAndExpandAddOnOperators(ctx, candidate, clusterVersion); err != nil {
		return
	}

	// Automatic external access is mandatory for Clusters: omitted means true, and false is rejected by validation.
	spec.SetAutoExternalIpAttachment(true)

	// The fabric_interface of each node set is server-derived: the first port with role "fabric" of its
	// BareMetalInstanceType. Values set by the caller are replaced. A missing fabric port surfaces first.
	if err = s.resolveFabricInterfaces(ctx, candidate); err != nil {
		return
	}
	// The resolved creation reference is the initial observed baseline until
	// HyperShift reports the installed versions.
	candidate.GetStatus().SetObservedCpVersion(clusterVersion.GetSpec().GetVersion())
	nodeSetStatus := make(map[string]*privatev1.ClusterNodeSet, len(spec.GetNodeSets()))
	for name := range spec.GetNodeSets() {
		status := &privatev1.ClusterNodeSet{}
		status.SetObservedVersion(clusterVersion.GetSpec().GetVersion())
		nodeSetStatus[name] = status
	}
	candidate.GetStatus().SetNodeSets(nodeSetStatus)
	return s.resolveNetworkAttachment(ctx, candidate)
}

// resolveNetworkAttachment resolves the Cluster network attachment field with the shared defaulting rules and checks
// the Cluster gates: the pod and service networks don't overlap the VirtualNetwork, an excluded range or an
// ExternalIPPool (under the shared NetworkClass lock), and with provider networking enabled the VirtualNetwork has a
// READY NATGateway.
func (s *PrivateClustersServer) resolveNetworkAttachment(ctx context.Context, cluster *privatev1.Cluster) error {
	if _, err := lockNetworkClass(ctx, s.guards.networkClasses, false); err != nil {
		return err
	}
	attachment := cluster.GetSpec().GetNetworkAttachment()
	if attachment == nil {
		attachment = &privatev1.ClusterNetworkAttachment{}
	}
	for i, ref := range attachment.GetSecurityGroups() {
		if refKey(ref) == "" {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"spec.network_attachment.security_groups[%d]: reference is empty", i)
		}
	}
	resolution, err := resolveNetworkAttachment(ctx, s.logger, s.guards, s.settings, networkAttachmentRequest{
		tenant:            cluster.GetMetadata().GetTenant(),
		project:           cluster.GetMetadata().GetProject(),
		subnetsDao:        s.subnetsDao,
		securityGroupsDao: s.securityGroupsDao,
		subnet:            attachment.GetSubnet(),
		securityGroups:    attachment.GetSecurityGroups(),
		source:            "spec.network_attachment",
	})
	if err != nil {
		return err
	}
	attachment.SetSubnet(resolution.subnetReference())
	attachment.SetSecurityGroups(resolution.securityGroupReferences())
	cluster.GetSpec().SetNetworkAttachment(attachment)

	virtualNetworkPrefix, _ := netip.ParsePrefix(resolution.virtualNetwork.GetSpec().GetIpv4Cidr())
	for _, cidr := range clusterCIDRs(cluster) {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "spec.network: invalid CIDR '%s'", cidr)
		}
		if virtualNetworkPrefix.IsValid() && prefix.Overlaps(virtualNetworkPrefix) {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"spec.network: CIDR %s overlaps the CIDR %s of VirtualNetwork '%s'",
				cidr, virtualNetworkPrefix, resolution.virtualNetwork.GetId())
		}
		if excluded, overlaps := s.settings.firstExcludedOverlap(prefix); overlaps {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"spec.network: CIDR %s overlaps the excluded range %s", cidr, excluded)
		}
		poolID, err := s.guards.firstPoolOverlap(ctx, prefix, "")
		if err != nil {
			return err
		}
		if poolID != "" {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"spec.network: CIDR %s overlaps ExternalIPPool '%s'", cidr, poolID)
		}
	}

	if s.settings.ProvisioningEnabled {
		if err := requireReadyNATGateway(ctx, s.guards, resolution.virtualNetwork); err != nil {
			return err
		}
	}

	canonicalHubID, err := canonicalNetworkingHubID(ctx, s.logger, s.networkClassesDao)
	if err != nil {
		return err
	}
	return validateNetworkingHubReferences(canonicalHubID, resolution.hubReferences()...)
}

// createAutomaticResources creates, in the transaction that stores the Cluster, its automatic SecurityGroup and, with
// provider networking enabled, its API and ingress ExternalIPs.
func (s *PrivateClustersServer) createAutomaticResources(ctx context.Context, cluster *privatev1.Cluster) error {
	if err := s.createAutomaticSecurityGroup(ctx, cluster); err != nil {
		return err
	}
	return createAutomaticExternalIPs(ctx, s.logger, s.settings, s.externalIPPoolDao, s.externalIPDao,
		cluster.GetMetadata(), cluster.GetId(), "cluster", "api", "ingress")
}

// createAutomaticSecurityGroup creates the SecurityGroup auto-sg-<Cluster ID> in the VirtualNetwork of the Cluster
// Subnet. It allows all traffic from the Subnet, HTTP and HTTPS from anywhere, and all outbound traffic.
func (s *PrivateClustersServer) createAutomaticSecurityGroup(ctx context.Context, cluster *privatev1.Cluster) error {
	subnet, err := lockRecord(ctx, s.guards.subnets, refKey(cluster.GetSpec().GetNetworkAttachment().GetSubnet()))
	if err != nil {
		return err
	}
	tenant := cluster.GetMetadata().GetTenant()
	rule := func(protocol privatev1.Protocol, cidr string, port int32) *privatev1.SecurityRule {
		builder := privatev1.SecurityRule_builder{Protocol: protocol, Ipv4Cidr: new(cidr)}
		if port != 0 {
			builder.PortFrom = new(port)
			builder.PortTo = new(port)
		}
		return builder.Build()
	}
	group := privatev1.SecurityGroup_builder{
		Metadata: privatev1.Metadata_builder{
			Name:    automaticSecurityGroupPrefix + cluster.GetId(),
			Tenant:  tenant,
			Creator: systemCreator,
			Labels: map[string]string{
				autoCreatedLabel:     "true",
				autoCreatedForLabel:  cluster.GetId(),
				autoCreatedKindLabel: "cluster",
			},
			Annotations: map[string]string{
				tenantAnnotation:         tenant,
				ownerReferenceAnnotation: cluster.GetId(),
			},
		}.Build(),
		Spec: privatev1.SecurityGroupSpec_builder{
			VirtualNetwork: subnet.GetSpec().GetVirtualNetwork(),
			Ingress: []*privatev1.SecurityRule{
				rule(privatev1.Protocol_PROTOCOL_ALL, subnet.GetSpec().GetIpv4Cidr(), 0),
				rule(privatev1.Protocol_PROTOCOL_TCP, "0.0.0.0/0", 80),
				rule(privatev1.Protocol_PROTOCOL_TCP, "0.0.0.0/0", 443),
			},
			Egress: []*privatev1.SecurityRule{
				rule(privatev1.Protocol_PROTOCOL_ALL, "0.0.0.0/0", 0),
			},
		}.Build(),
		Status: privatev1.SecurityGroupStatus_builder{
			State: privatev1.SecurityGroupState_SECURITY_GROUP_STATE_PENDING,
		}.Build(),
	}.Build()
	if _, err := s.guards.securityGroups.Create().SetObject(group).Do(ctx); err != nil {
		return ConvertDAOErrorToGRPC(err, "create", "")
	}
	return nil
}

// resolveCreationSource accepts exactly one provisioning source: spec.catalog_item or
// spec.template. For a Catalog Item it finds the item's Template and applies its field rules;
// for a direct Template it resolves that reference under the Cluster's assigned tenant/project.
func (s *PrivateClustersServer) resolveCreationSource(ctx context.Context,
	candidate *privatev1.Cluster) (*privatev1.ClusterTemplate, error) {
	spec := candidate.GetSpec()
	if spec.GetCatalogItem() != nil && spec.GetTemplate() != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"catalog_item and template are mutually exclusive")
	}
	if spec.GetCatalogItem() != nil {
		return s.resolveCatalogItem(ctx, candidate)
	}
	if spec.GetTemplate() == nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "template is mandatory")
	}
	return resolveAndCanonicalizeReference(ctx, s.templatesDao, candidate.GetMetadata(), spec.GetTemplate(),
		"template", grpccodes.InvalidArgument)
}

func (s *PrivateClustersServer) Update(ctx context.Context,
	request *privatev1.ClustersUpdateRequest) (response *privatev1.ClustersUpdateResponse, err error) {
	if err = s.validateNoDuplicateConditions(request.GetObject()); err != nil {
		return
	}
	if err = s.validateClusterStateForSpecUpdate(ctx, request); err != nil {
		return
	}
	// Version validation canonicalizes selected references; keep the caller's request intact.
	request = proto.Clone(request).(*privatev1.ClustersUpdateRequest)
	if err = s.validateClusterUpdateRequest(ctx, request); err != nil {
		return
	}
	// Upgrade-history updates contain only the changed state, time, and attempt ID.
	// Merge the stored audit fields before validating the completed candidate.
	err = s.generic.updateWithCandidatePreparation(ctx, request, &response, func(ctx context.Context,
		current, candidate *privatev1.Cluster) error {
		return s.prepareUpdatedCluster(ctx, current, candidate, request)
	}, true)
	return
}

func (s *PrivateClustersServer) validateClusterUpdateRequest(ctx context.Context,
	request *privatev1.ClustersUpdateRequest) error {
	if err := s.validateVersionUpdate(ctx, request); err != nil {
		return err
	}
	if err := s.validateNodeSetsUpdate(ctx, request); err != nil {
		return err
	}
	if err := s.validateAutoExternalIPImmutability(ctx, request); err != nil {
		return err
	}
	return s.validateSpecUpdateRequest(request)
}

func (s *PrivateClustersServer) prepareUpdatedCluster(ctx context.Context, current, candidate *privatev1.Cluster,
	request *privatev1.ClustersUpdateRequest) error {
	// An unmasked metadata-only request omits spec. Keep the stored spec so its
	// version and node sets are not interpreted as a metering-relevant change.
	if request.GetUpdateMask() == nil && request.GetObject().GetSpec() == nil && current.GetSpec() != nil {
		candidate.SetSpec(proto.Clone(current.GetSpec()).(*privatev1.ClusterSpec))
	}
	if err := validateClusterEndpointAddresses(candidate.GetStatus(), request.GetUpdateMask()); err != nil {
		return err
	}
	if err := validateClusterNetworkAttachmentImmutability(current, candidate, request.GetUpdateMask()); err != nil {
		return err
	}
	if err := validateClusterTemplateImmutability(current, candidate, request.GetUpdateMask()); err != nil {
		return err
	}
	mask := request.GetUpdateMask()
	sizeOnlyNodeSets := len(mask.GetPaths()) > 0 && s.isUpdatingOnlySizes(mask)
	if err := s.prepareUpdatedClusterNodeSets(ctx, current, candidate,
		request.GetObject().GetSpec().GetNodeSets(), mask, sizeOnlyNodeSets); err != nil {
		return err
	}
	// A parent spec mask or a full-object replacement must never replace IDs
	// generated by Fulfillment.
	candidate.GetSpec().SetControlPlaneAttemptId(current.GetSpec().GetControlPlaneAttemptId())
	attemptIDs := make(map[string]string, len(current.GetSpec().GetNodeSetAttemptIds()))
	for name, id := range current.GetSpec().GetNodeSetAttemptIds() {
		if candidate.GetSpec().GetNodeSets()[name] != nil {
			attemptIDs[name] = id
		}
	}
	candidate.GetSpec().SetNodeSetAttemptIds(attemptIDs)
	mergeClusterUpgradeHistory(current, candidate, mask)
	if err := s.prepareClusterVersionChange(ctx, current, candidate, mask); err != nil {
		return err
	}
	if err := s.initializeAddedNodeSetVersions(ctx, current, candidate); err != nil {
		return err
	}
	if err := s.validateUpdatedClusterSpec(current, candidate, mask); err != nil {
		return err
	}
	if err := rejectStateChangeWhileDeleting(current, candidate, current.GetMetadata()); err != nil {
		return err
	}
	// External access is mandatory and set by the server; an update that doesn't set the field keeps it.
	if !request.GetObject().GetSpec().HasAutoExternalIpAttachment() && candidate.HasSpec() {
		if current.GetSpec().GetAutoExternalIpAttachment() {
			candidate.GetSpec().SetAutoExternalIpAttachment(true)
		} else {
			candidate.GetSpec().ClearAutoExternalIpAttachment()
		}
	}
	return s.prepareUpdatedClusterReferences(ctx, current, candidate, mask, sizeOnlyNodeSets)
}

func (s *PrivateClustersServer) initializeAddedNodeSetVersions(ctx context.Context,
	current, candidate *privatev1.Cluster) error {
	for name, nodeSet := range candidate.GetSpec().GetNodeSets() {
		if current.GetSpec().GetNodeSets()[name] != nil || nodeSet == nil {
			continue
		}
		version, err := s.clusterVersionSemver(ctx, nodeSet.GetVersion())
		if err != nil {
			return err
		}
		statuses := candidate.GetStatus().GetNodeSets()
		if statuses == nil {
			statuses = map[string]*privatev1.ClusterNodeSet{}
			candidate.GetStatus().SetNodeSets(statuses)
		}
		status := &privatev1.ClusterNodeSet{}
		status.SetObservedVersion(version)
		statuses[name] = status
	}
	return nil
}

func (s *PrivateClustersServer) validateUpdatedClusterSpec(current, candidate *privatev1.Cluster,
	mask *fieldmaskpb.FieldMask) error {
	if updateIncludesField(mask, "spec.ssh_public_key") {
		if key := candidate.GetSpec().GetSshPublicKey(); key != "" {
			if err := validateOpenSSHPublicKey(key); err != nil {
				return grpcstatus.Errorf(grpccodes.InvalidArgument, "spec.ssh_public_key: %s", err)
			}
		}
	}
	if err := s.validateAddOnOperatorImmutability(current, candidate, mask); err != nil {
		return err
	}
	if err := utils.ValidateClusterSpecFields(candidate.GetSpec()); err != nil {
		return err
	}
	return nil
}

func (s *PrivateClustersServer) prepareUpdatedClusterReferences(ctx context.Context,
	current, candidate *privatev1.Cluster, mask *fieldmaskpb.FieldMask, sizeOnlyNodeSets bool) error {
	// An explicit size-only update keeps the existing hardware type and fabric interface.
	// Re-resolving here would prevent scaling legacy clusters with tenant-scoped BMITs.
	if s.updateAffectsNodeSets(mask) && !sizeOnlyNodeSets {
		if err := s.resolveFabricInterfaces(ctx, candidate); err != nil {
			return err
		}
	}
	if !proto.Equal(current.GetSpec().GetPullSecretSecret(), candidate.GetSpec().GetPullSecretSecret()) {
		ref := candidate.GetSpec().GetPullSecretSecret()
		allowShared := ref.GetId() != "" && ref.GetId() == current.GetSpec().GetPullSecretSecret().GetId()
		if err := s.validatePullSecretSecret(ctx, candidate, allowShared); err != nil {
			return err
		}
	}
	return nil
}

// prepareUpdatedClusterNodeSets preserves stored fields for nested node-set
// updates and checks that newly selected hardware types are not being deleted.
func (s *PrivateClustersServer) prepareUpdatedClusterNodeSets(
	ctx context.Context, current, candidate *privatev1.Cluster,
	requestedNodeSets map[string]*privatev1.ClusterNodeSet, mask *fieldmaskpb.FieldMask, sizeOnlyNodeSets bool,
) error {
	if !s.updateAffectsNodeSets(mask) {
		return nil
	}
	partialSizeOrVersionUpdate := isUpdatingOnlyNodeSizeOrVersionFields(mask) && !sizeOnlyNodeSets
	for name, nodeSet := range candidate.GetSpec().GetNodeSets() {
		existing := current.GetSpec().GetNodeSets()[name]
		if partialSizeOrVersionUpdate {
			var err error
			nodeSet, err = restoreMaskedNodeSetFields(name, existing, requestedNodeSets[name], mask)
			if err != nil {
				return err
			}
			candidate.GetSpec().GetNodeSets()[name] = nodeSet
		}
		retainOmittedNodeSetVersion(current.GetSpec().GetVersion(), existing, nodeSet)
		if err := s.prepareUpdatedNodeSetHardware(ctx, name, existing, nodeSet, mask, sizeOnlyNodeSets); err != nil {
			return err
		}
	}
	return nil
}

func (s *PrivateClustersServer) validateSpecUpdateRequest(request *privatev1.ClustersUpdateRequest) error {
	updateMask := request.GetUpdateMask()
	if updateMask != nil && len(updateMask.GetPaths()) > 0 &&
		updateIncludesField(updateMask, "spec") && request.GetObject().GetSpec() == nil {
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "object and spec are required")
	}
	return nil
}

func (s *PrivateClustersServer) Delete(ctx context.Context,
	request *privatev1.ClustersDeleteRequest) (response *privatev1.ClustersDeleteResponse, err error) {
	id := request.GetId()
	if id == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "identifier is mandatory")
	}
	cluster, err := lockLifecycleResource(ctx, s.generic.dao, id, "Cluster")
	if err != nil {
		return
	}
	if deleteAlreadyAccepted(cluster.GetMetadata()) {
		return &privatev1.ClustersDeleteResponse{}, nil
	}
	if err = s.lifecycle.rejectManualAttachmentDelete(ctx, "Cluster", id, cluster.GetMetadata()); err != nil {
		return
	}
	// The automatic ExternalIPAttachments, ExternalIPs and SecurityGroup are deleted by the reconciler, in order,
	// after this.
	if err = markDeleting(ctx, s.generic.dao, cluster); err != nil {
		return
	}
	err = s.generic.Delete(ctx, request, &response)
	return
}

func (s *PrivateClustersServer) Signal(ctx context.Context,
	request *privatev1.ClustersSignalRequest) (response *privatev1.ClustersSignalResponse, err error) {
	err = s.generic.Signal(ctx, request, &response)
	return
}

func (s *PrivateClustersServer) setDefaults(cluster *privatev1.Cluster) {
	if !cluster.HasSpec() {
		cluster.SetSpec(&privatev1.ClusterSpec{})
	}
	if !cluster.HasStatus() {
		cluster.SetStatus(&privatev1.ClusterStatus{})
	}
}

func validateClusterEndpointAddresses(status *privatev1.ClusterStatus, updateMask *fieldmaskpb.FieldMask) error {
	for _, field := range []struct {
		path  string
		value string
	}{
		{path: "status.api_endpoint", value: status.GetApiEndpoint()},
		{path: "status.ingress_endpoint", value: status.GetIngressEndpoint()},
	} {
		if updateIncludesField(updateMask, field.path) {
			if err := validateCanonicalIPv4Address(field.path, field.value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *PrivateClustersServer) validatePullSecretSecret(
	ctx context.Context, cluster *privatev1.Cluster, allowShared bool,
) error {
	ref := cluster.GetSpec().GetPullSecretSecret()
	if ref == nil {
		return nil
	}
	if ref.GetId() == "" && ref.GetName() == "" {
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "pull_secret_secret must specify id or name")
	}
	identifier := refKey(ref)
	metadata := cluster.GetMetadata()
	var secret *privatev1.Secret
	var err error
	if allowShared {
		sharedMetadata := privatev1.Metadata_builder{Tenant: auth.SharedTenant}.Build()
		secret, err = resolveAndCanonicalizeReference(ctx, s.secretsDao, sharedMetadata, ref, "pull_secret_secret", grpccodes.NotFound)
		if err != nil && grpcstatus.Code(err) != grpccodes.NotFound {
			return err
		}
	}
	if secret == nil {
		secret, err = resolveAndCanonicalizeReference(ctx, s.secretsDao, metadata, ref, "pull_secret_secret", grpccodes.InvalidArgument)
		if err != nil {
			return err
		}
	}
	return validateResolvedSecretLifecycleAndType(secret, identifier, "pull_secret_secret",
		privatev1.SecretType_SECRET_TYPE_PULL_SECRET)
}

// ensureClusterVersion makes sure the cluster spec has a usable version reference: if the user didn't provide one, it
// resolves the system default.
func (s *PrivateClustersServer) ensureClusterVersion(
	ctx context.Context,
	cluster *privatev1.Cluster,
) (*privatev1.ClusterVersion, error) {
	versionRef := cluster.GetSpec().GetVersion()
	if versionRef != nil {
		version, err := resolveAndCanonicalizeReference(ctx, s.clusterVersionsDao, cluster.GetMetadata(), versionRef, "version", grpccodes.InvalidArgument)
		if err != nil {
			return nil, err
		}
		if err := validateResolvedClusterVersion(version, version.GetMetadata().GetName(), ""); err != nil {
			return nil, err
		}
		return version, nil
	}
	version, err := resolveDefaultClusterVersion(ctx, s.logger, s.clusterVersionsDao)
	if err != nil {
		return nil, err
	}
	cluster.GetSpec().SetVersion(buildClusterVersionReference(version))
	return version, nil
}

func (s *PrivateClustersServer) validateNoDuplicateConditions(object *privatev1.Cluster) error {
	conditions := object.GetStatus().GetConditions()
	if conditions == nil {
		return nil
	}
	conditionTypes := &bitset.BitSet{}
	for _, condition := range conditions {
		conditionType := condition.GetType()
		if conditionTypes.Test(uint(conditionType)) { // #nosec G115 -- proto enum, non-negative
			return grpcstatus.Errorf(
				grpccodes.InvalidArgument,
				"condition '%s' is duplicated",
				conditionType.String(),
			)
		}
		conditionTypes.Set(uint(conditionType)) // #nosec G115 -- proto enum, non-negative
	}
	return nil
}

// validateNodeSetsUpdate validates that changes to node_sets are allowed.
// It delegates to specific validators for different aspects of the validation.
func (s *PrivateClustersServer) validateNodeSetsUpdate(ctx context.Context,
	request *privatev1.ClustersUpdateRequest) error {
	// Check if the update affects node_sets at all:
	if !s.updateAffectsNodeSets(request.GetUpdateMask()) {
		// Update doesn't touch node_sets, no validation needed
		return nil
	}

	// Full-object updates and nested size/version masks are checked against the
	// prepared candidate instead of treating a partial node-set map as a replacement.
	if s.isUpdatingOnlySizes(request.GetUpdateMask()) ||
		isUpdatingOnlyNodeSizeOrVersionFields(request.GetUpdateMask()) {
		return nil
	}

	// Fetch the existing cluster from the database:
	existingCluster, found, err := s.getExistingCluster(ctx, request)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	// Get the node sets from both clusters:
	existingNodeSets := existingCluster.GetSpec().GetNodeSets()
	newNodeSets := request.GetObject().GetSpec().GetNodeSets()

	// Run specific validations:
	if err := s.validateAtLeastOneNodeSet(newNodeSets); err != nil {
		return err
	}
	if err := validateNodeSetNames(newNodeSets); err != nil {
		return err
	}
	if err := s.validateNodeSetBareMetalInstanceTypeImmutability(existingNodeSets, newNodeSets); err != nil {
		return err
	}

	return nil
}

// getExistingCluster fetches the existing cluster from the database.
// Returns the cluster, a boolean indicating if it was found, and any error that occurred.
func (s *PrivateClustersServer) getExistingCluster(ctx context.Context,
	request *privatev1.ClustersUpdateRequest) (*privatev1.Cluster, bool, error) {
	cluster := request.GetObject()
	if cluster == nil {
		return nil, false, nil
	}
	id := cluster.GetId()
	if id == "" {
		return nil, false, nil
	}
	getResponse, err := s.generic.dao.Get().
		SetId(id).
		Do(ctx)
	if err != nil {
		return nil, false, err
	}
	existingCluster := getResponse.GetObject()
	return existingCluster, true, nil
}

// validateClusterStateForSpecUpdate rejects spec modifications when the cluster is in a
// terminal or non-reconcilable state. The reconciler processes UNSPECIFIED (by transitioning
// to PROGRESSING via setDefaults), PROGRESSING, and READY clusters — it returns nil for
// every other state — so spec changes outside those states would be silently ignored.
//
// This uses SetLock(true) (SELECT ... FOR UPDATE) so the row lock is held for the
// remainder of the transaction, preventing a concurrent status transition from
// bypassing the check before GenericServer.Update writes the spec changes.
func (s *PrivateClustersServer) validateClusterStateForSpecUpdate(ctx context.Context,
	request *privatev1.ClustersUpdateRequest) error {
	if !updateIncludesField(request.GetUpdateMask(), "spec") {
		return nil
	}
	cluster := request.GetObject()
	if cluster == nil {
		return nil
	}
	id := cluster.GetId()
	if id == "" {
		return nil
	}
	getResponse, err := s.generic.dao.Get().
		SetId(id).
		SetLock(true).
		Do(ctx)
	if err != nil {
		return err
	}
	existingCluster := getResponse.GetObject()
	state := existingCluster.GetStatus().GetState()
	if state != privatev1.ClusterState_CLUSTER_STATE_UNSPECIFIED &&
		state != privatev1.ClusterState_CLUSTER_STATE_PROGRESSING &&
		state != privatev1.ClusterState_CLUSTER_STATE_READY {
		return grpcstatus.Errorf(
			grpccodes.InvalidArgument,
			"cannot update cluster spec when cluster state is %s",
			state,
		)
	}
	return nil
}

// updateAffectsNodeSets checks if the update mask indicates that node_sets are being modified.
func (s *PrivateClustersServer) updateAffectsNodeSets(updateMask *fieldmaskpb.FieldMask) bool {
	return updateIncludesField(updateMask, "spec.node_sets")
}

// isUpdatingOnlySizes checks if the update mask is only modifying size fields of node sets.
func (s *PrivateClustersServer) isUpdatingOnlySizes(updateMask *fieldmaskpb.FieldMask) bool {
	if len(updateMask.GetPaths()) == 0 {
		return true
	}
	hasSize := false
	for _, path := range updateMask.GetPaths() {
		if path == "spec" || strings.HasPrefix(path, "spec.node_sets") {
			if !strings.HasSuffix(path, ".size") {
				return false
			}
			hasSize = true
		}
	}
	return hasSize
}

// validateAtLeastOneNodeSet ensures that clusters always have at least one node set.
func (s *PrivateClustersServer) validateAtLeastOneNodeSet(nodeSets map[string]*privatev1.ClusterNodeSet) error {
	if len(nodeSets) == 0 {
		return grpcstatus.Errorf(
			grpccodes.InvalidArgument,
			"cannot remove the last node set: clusters must have at least one node set",
		)
	}
	return nil
}

// validateClusterNodeSetMap checks that a node-set map has at least one entry and that
// every entry has the fields required after defaults and references have been resolved.
func validateClusterNodeSetMap(nodeSets map[string]*privatev1.ClusterNodeSet) error {
	if len(nodeSets) == 0 {
		return fmt.Errorf("must contain at least one node set")
	}
	for name, nodeSet := range nodeSets {
		if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
			return fmt.Errorf("node set name '%s' is not a valid DNS label: %s", name, strings.Join(errs, ", "))
		}
		if nodeSet == nil {
			return fmt.Errorf("node set '%s' must not be null", name)
		}
		if !nodeSet.HasSize() {
			return fmt.Errorf("size for node set '%s' is required", name)
		}
		if nodeSet.GetSize() <= 0 {
			return fmt.Errorf("size for node set '%s' should be greater than zero, but it is %d", name, nodeSet.GetSize())
		}
		if refKey(nodeSet.GetBaremetalInstanceType()) == "" {
			return fmt.Errorf("bare metal instance type for node set '%s' is required", name)
		}
	}
	return nil
}

// validateNodeSetNames checks that every key in the node-set map is a valid RFC 1123 DNS label.
func validateNodeSetNames[V any](nodeSets map[string]V) error {
	for name := range nodeSets {
		if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
			return grpcstatus.Errorf(
				grpccodes.InvalidArgument,
				"node set name '%s' is not a valid DNS label: %s", name, strings.Join(errs, ", "),
			)
		}
	}
	return nil
}

func (s *PrivateClustersServer) validateNodeSetBareMetalInstanceTypeImmutability(
	existingNodeSets map[string]*privatev1.ClusterNodeSet,
	newNodeSets map[string]*privatev1.ClusterNodeSet) error {
	for nodeSetName, existingNodeSet := range existingNodeSets {
		newNodeSet, exists := newNodeSets[nodeSetName]
		if !exists {
			continue
		}
		existingBmit := existingNodeSet.GetBaremetalInstanceType()
		newBmit := newNodeSet.GetBaremetalInstanceType()
		if refKey(existingBmit) != refKey(newBmit) {
			return grpcstatus.Errorf(
				grpccodes.InvalidArgument,
				"cannot change baremetal_instance_type for node set '%s' from '%s' to '%s': baremetal_instance_type is immutable",
				nodeSetName,
				refKey(existingBmit),
				refKey(newBmit),
			)
		}
	}
	return nil
}

// validateClusterTemplateImmutability checks that Update keeps the Cluster's original Template
// and parameters. It also preserves the stored spec.catalog_item as history, so a normal Update
// works after that Catalog Item is deleted without reading the deleted item.
func validateClusterTemplateImmutability(current, candidate *privatev1.Cluster, mask *fieldmaskpb.FieldMask) error {
	oldSpec, newSpec := current.GetSpec(), candidate.GetSpec()
	// Preserve the legacy unmasked Update behavior for omitted immutable inputs.
	// An explicit mask, including a parent mask, still makes clearing an error.
	if mask == nil {
		if newSpec == nil {
			newSpec = &privatev1.ClusterSpec{}
			candidate.SetSpec(newSpec)
		}
		if newSpec.GetTemplate() == nil {
			newSpec.SetTemplate(cloneMessage(oldSpec.GetTemplate()))
		}
		if len(newSpec.GetTemplateParameters()) == 0 {
			newSpec.SetTemplateParameters(cloneMessage(oldSpec).GetTemplateParameters())
		}
	}
	if updateIncludesField(mask, "spec.template") && refKey(oldSpec.GetTemplate()) != refKey(newSpec.GetTemplate()) {
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "cannot change spec.template from '%s' to '%s': template is immutable", refKey(oldSpec.GetTemplate()), refKey(newSpec.GetTemplate()))
	}
	if updateIncludesField(mask, "spec.template_parameters") && !maps.EqualFunc(oldSpec.GetTemplateParameters(), newSpec.GetTemplateParameters(), func(a, b *anypb.Any) bool { return proto.Equal(a, b) }) { //nolint:govet // inline: type parameter inference is not supported yet
		return grpcstatus.Error(grpccodes.InvalidArgument, "cannot change spec.template_parameters: template parameters are immutable")
	}
	if updateIncludesField(mask, "spec.catalog_item") {
		ref, err := preserveCatalogItemProvenance(oldSpec.GetCatalogItem(), newSpec.GetCatalogItem(), mask)
		if err != nil {
			return err
		}
		if newSpec != nil {
			newSpec.SetCatalogItem(ref)
		}
	}

	return nil
}

// validateAddOnOperatorImmutability validates and preserves the canonical operator references during updates.
func (s *PrivateClustersServer) validateAddOnOperatorImmutability(
	current, candidate *privatev1.Cluster, mask *fieldmaskpb.FieldMask,
) error {
	currentOperators := current.GetSpec().GetAddOnOperators()
	candidateOperators := candidate.GetSpec().GetAddOnOperators()
	cloneCurrent := func() []*privatev1.AddOnOperatorReference {
		result := make([]*privatev1.AddOnOperatorReference, len(currentOperators))
		for i, ref := range currentOperators {
			result[i] = cloneMessage(ref)
		}
		return result
	}

	if !updateIncludesField(mask, "spec.add_on_operators") || (mask == nil && len(candidateOperators) == 0) {
		candidate.GetSpec().SetAddOnOperators(cloneCurrent())
		return nil
	}
	if len(currentOperators) != len(candidateOperators) {
		return grpcstatus.Errorf(
			grpccodes.InvalidArgument,
			"cannot change spec.add_on_operators: add-on operators are immutable",
		)
	}

	for i, ref := range candidateOperators {
		stored := currentOperators[i]
		if err := validateImmutableReferenceIdentity(
			stored, ref, fmt.Sprintf("spec.add_on_operators[%d]", i), "add-on operators", true,
		); err != nil {
			return err
		}
	}
	candidate.GetSpec().SetAddOnOperators(cloneCurrent())
	return nil
}

// validateClusterNetworkAttachmentImmutability rejects changes to the complete
// network attachment after applying the update mask.
func validateClusterNetworkAttachmentImmutability(current, candidate *privatev1.Cluster,
	updateMask *fieldmaskpb.FieldMask) error {
	if !updateIncludesField(updateMask, "spec.network_attachment") {
		return nil
	}
	// The create resolves the attachment, so a maskless update that omits it keeps the stored one.
	if updateMask == nil && !candidate.GetSpec().HasNetworkAttachment() {
		if candidate.HasSpec() && current.GetSpec().HasNetworkAttachment() {
			candidate.GetSpec().SetNetworkAttachment(cloneMessage(current.GetSpec().GetNetworkAttachment()))
		}
		return nil
	}
	existingAttachment := current.GetSpec().GetNetworkAttachment()
	newAttachment := candidate.GetSpec().GetNetworkAttachment()
	existingSubnet := existingAttachment.GetSubnet()
	newSubnet := newAttachment.GetSubnet()

	if refKey(existingSubnet) != refKey(newSubnet) {
		return grpcstatus.Errorf(
			grpccodes.InvalidArgument,
			"cannot change spec.network_attachment.subnet from '%s' to '%s': subnet is immutable",
			refKey(existingSubnet), refKey(newSubnet),
		)
	}
	if err := validateImmutableSecurityGroups(
		existingAttachment.GetSecurityGroups(),
		newAttachment.GetSecurityGroups(),
		"spec.network_attachment.security_groups",
	); err != nil {
		return err
	}

	return nil
}

// validateAutoExternalIPImmutability prevents changing auto_external_ip_attachment after creation.
func (s *PrivateClustersServer) validateAutoExternalIPImmutability(ctx context.Context,
	request *privatev1.ClustersUpdateRequest) error {
	if !updateIncludesField(request.GetUpdateMask(), "spec.auto_external_ip_attachment") ||
		!request.GetObject().GetSpec().HasAutoExternalIpAttachment() {
		return nil
	}

	existing, found, err := s.getExistingCluster(ctx, request)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	oldVal := existing.GetSpec().GetAutoExternalIpAttachment()
	newVal := request.GetObject().GetSpec().GetAutoExternalIpAttachment()
	if oldVal != newVal {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change spec.auto_external_ip_attachment: auto_external_ip_attachment is immutable after creation")
	}
	return nil
}

// resolveFabricInterfaces populates fabric_interface on each node set by
// loading its BareMetalInstanceType by the already-resolved ID and selecting the first interface with role "fabric".
func (s *PrivateClustersServer) resolveFabricInterfaces(ctx context.Context, cluster *privatev1.Cluster) error {
	for name, nodeSet := range cluster.GetSpec().GetNodeSets() {
		if refKey(nodeSet.GetBaremetalInstanceType()) == "" {
			continue
		}
		ref := nodeSet.GetBaremetalInstanceType()
		kind := "node_sets." + name + ".baremetal_instance_type"
		bmit, err := getReferenceResource(ctx, s.bareMetalInstanceTypesDao, ref.GetId())
		if err != nil {
			return resourceLookupError(err, kind, ref.GetId(), "", grpccodes.InvalidArgument)
		}
		if err := validateResourceNotDeleted(kind, refKey(ref), "", bmit.GetMetadata()); err != nil {
			return err
		}
		fabricInterface, err := selectClusterFabricInterface(bmit)
		if err != nil {
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "node_sets[%s]: %s", name, err)
		}
		nodeSet.SetFabricInterface(fabricInterface)
	}
	return nil
}

func selectClusterFabricInterface(instanceType *privatev1.BareMetalInstanceType) (string, error) {
	for _, port := range instanceType.GetSpec().GetHardware().GetNetworkPorts() {
		if strings.EqualFold(port.GetRole(), "fabric") {
			return port.GetName(), nil
		}
	}
	return "", fmt.Errorf("bare metal instance type '%s' has no network port with role 'fabric'", instanceType.GetId())
}

func (s *PrivateClustersServer) applyClusterTemplate(
	ctx context.Context,
	cluster *privatev1.Cluster,
	template *privatev1.ClusterTemplate,
) (*privatev1.ClusterVersion, error) {
	actualClusterParameters, err := utils.ApplyTemplateParameterDefaultsAndValidate(
		utils.ClusterTemplateAdapter{ClusterTemplate: template}, cluster.GetSpec().GetTemplateParameters(),
	)
	if err != nil {
		return nil, err
	}
	cluster.GetSpec().SetTemplateParameters(actualClusterParameters)

	// Only a pull secret inherited from a shared template may cross the tenant boundary. Direct
	// shared SecretLocalReference values on tenant clusters stay local.
	inheritsPullSecretSecret := cluster.GetSpec().GetPullSecretSecret() == nil &&
		template.GetMetadata().GetTenant() == auth.SharedTenant &&
		template.GetSpecDefaults().GetPullSecretSecret() != nil

	// Apply spec defaults from the template (user values take precedence):
	inheritVersion := cluster.GetSpec().GetVersion() == nil
	utils.ApplyClusterSpecDefaults(cluster.GetSpec(), template.GetSpecDefaults())
	if inheritVersion && cluster.GetSpec().GetVersion() != nil {
		inheritReferenceScope(cluster.GetSpec().GetVersion(), template.GetMetadata())
	}

	// Validate pull_secret_secret reference exists:
	if err := s.validatePullSecretSecret(ctx, cluster, inheritsPullSecretSecret); err != nil {
		return nil, err
	}

	clusterVersion, err := s.ensureClusterVersion(ctx, cluster)
	if err != nil {
		return nil, err
	}

	// Validate cluster spec fields (CIDR format, etc.) after defaults have been applied:
	if err := utils.ValidateClusterSpecFields(cluster.GetSpec()); err != nil {
		return nil, err
	}

	if err := s.resolveClusterNodeSets(ctx, cluster); err != nil {
		return nil, err
	}

	// Store the template reference with both ID and name. NodeSet hardware references
	// already have their IDs and names from the interceptor or Catalog Item authoring.
	cluster.GetSpec().SetTemplate(canonicalClusterTemplateReference(template))

	return clusterVersion, nil
}

// resolveClusterNodeSets validates the effective Cluster NodeSets after Catalog Item
// policies have been applied. References are already resolved by the interceptor or
// Catalog Item authoring; only the hardware types' current deletion state is checked here.
func (s *PrivateClustersServer) resolveClusterNodeSets(ctx context.Context, cluster *privatev1.Cluster) error {
	if err := validateClusterNodeSetMap(cluster.GetSpec().GetNodeSets()); err != nil {
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "node_sets: %s", err)
	}
	for name, nodeSet := range cluster.GetSpec().GetNodeSets() {
		ref := nodeSet.GetBaremetalInstanceType()
		kind := "node_sets." + name + ".baremetal_instance_type"
		resolved, err := getReferenceResource(ctx, s.bareMetalInstanceTypesDao, ref.GetId())
		if err != nil {
			return resourceLookupError(err, kind, ref.GetId(), "", grpccodes.InvalidArgument)
		}
		if err := validateResourceNotDeleted(kind, refKey(ref), "", resolved.GetMetadata()); err != nil {
			return err
		}
	}
	return nil
}

// resolveCatalogItem finds the Cluster's published Catalog Item in the selected tenant/project
// or shared scope, then finds the item's Template under the item's ownership. It applies locked
// and editable field and parameter rules and returns that Template for defaults.
func (s *PrivateClustersServer) resolveCatalogItem(ctx context.Context,
	cluster *privatev1.Cluster) (*privatev1.ClusterTemplate, error) {
	if cluster == nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "object is mandatory")
	}
	catalogItemRef := cluster.GetSpec().GetCatalogItem()
	if catalogItemRef == nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "catalog_item is mandatory")
	}
	catalogItemRefStr := refKey(catalogItemRef)

	catalogItem, err := resolveAndCanonicalizeLockedReference(ctx, s.catalogItemsDao, cluster.GetMetadata(), catalogItemRef, "catalog item", grpccodes.NotFound)
	if err != nil {
		return nil, err
	}

	if err := validateCatalogItemForCreation(catalogItem, catalogItemRefStr); err != nil {
		return nil, err
	}

	templateRef := catalogItem.GetTemplate()
	if templateRef == nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "catalog item '%s' has no template", catalogItemRefStr)
	}
	templateRef = cloneMessage(templateRef)
	resolvedTemplate, resolveErr := resolveAndCanonicalizeLockedReference(ctx, s.templatesDao, catalogItem.GetMetadata(), templateRef, "template", grpccodes.InvalidArgument)
	if resolveErr != nil {
		return nil, resolveErr
	}

	if err := applyClusterCatalogItemPolicies(cluster.GetSpec(), catalogItem.GetFields()); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%s", err)
	}

	parameters, err := applyCatalogItemTemplateParameterPolicies(utils.ClusterTemplateAdapter{ClusterTemplate: resolvedTemplate}, catalogItem.GetTemplateParameters(), cluster.GetSpec().GetTemplateParameters())
	if err != nil {
		return nil, err
	}
	cluster.GetSpec().SetTemplateParameters(parameters)
	return resolvedTemplate, nil
}
