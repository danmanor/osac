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
	"maps"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/osac-project/osac/fulfillment-service/internal/auth"
	"github.com/osac-project/osac/fulfillment-service/internal/database"
	"github.com/osac-project/osac/fulfillment-service/internal/database/dao"
	"github.com/osac-project/osac/fulfillment-service/internal/utils"
	"github.com/osac-project/osac/fulfillment-service/internal/validation"
	"github.com/osac-project/osac/fulfillment-service/internal/vault"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

const bareMetalInstanceUserDataMaxBytes = 64 * 1024

func validateBareMetalUserData(userData []byte) error {
	if len(userData) > bareMetalInstanceUserDataMaxBytes {
		return fmt.Errorf("size %d exceeds the maximum of %d bytes", len(userData), bareMetalInstanceUserDataMaxBytes)
	}
	return nil
}

type PrivateBareMetalInstancesServerBuilder struct {
	logger             *slog.Logger
	attributionLogic   auth.AttributionLogic
	tenancyLogic       auth.TenancyLogic
	metricsRegisterer  prometheus.Registerer
	filterDesc         protoreflect.MessageDescriptor
	secretStore        vault.SecretStore
	networkingSettings *NetworkingSettings
}

var _ privatev1.BareMetalInstancesServer = (*PrivateBareMetalInstancesServer)(nil)

type PrivateBareMetalInstancesServer struct {
	privatev1.UnimplementedBareMetalInstancesServer
	logger                  *slog.Logger
	tenancyLogic            auth.TenancyLogic
	generic                 *GenericServer[*privatev1.BareMetalInstance]
	catalogItemsDao         *dao.GenericDAO[*privatev1.BareMetalInstanceCatalogItem]
	templatesDao            *dao.GenericDAO[*privatev1.BareMetalInstanceTemplate]
	instanceTypesDao        *dao.GenericDAO[*privatev1.BareMetalInstanceType]
	subnetsDao              *dao.GenericDAO[*privatev1.Subnet]
	virtualNetworksDao      *dao.GenericDAO[*privatev1.VirtualNetwork]
	networkClassesDao       *dao.GenericDAO[*privatev1.NetworkClass]
	securityGroupsDao       *dao.GenericDAO[*privatev1.SecurityGroup]
	diskImagesDao           *dao.GenericDAO[*privatev1.DiskImage]
	externalIPPoolDao       *dao.GenericDAO[*privatev1.ExternalIPPool]
	externalIPDao           *dao.GenericDAO[*privatev1.ExternalIP]
	externalIPAttachmentDao *dao.GenericDAO[*privatev1.ExternalIPAttachment]
	secretsDao              *dao.GenericDAO[*privatev1.Secret]
	secretStore             vault.SecretStore
	lifecycle               *externalIPLifecycle
	settings                NetworkingSettings
	guards                  *networkingGuards
}

func NewPrivateBareMetalInstancesServer() *PrivateBareMetalInstancesServerBuilder {
	return &PrivateBareMetalInstancesServerBuilder{}
}

func (b *PrivateBareMetalInstancesServerBuilder) SetLogger(value *slog.Logger) *PrivateBareMetalInstancesServerBuilder {
	b.logger = value
	return b
}

func (b *PrivateBareMetalInstancesServerBuilder) SetAttributionLogic(value auth.AttributionLogic) *PrivateBareMetalInstancesServerBuilder {
	b.attributionLogic = value
	return b
}

func (b *PrivateBareMetalInstancesServerBuilder) SetTenancyLogic(value auth.TenancyLogic) *PrivateBareMetalInstancesServerBuilder {
	b.tenancyLogic = value
	return b
}

func (b *PrivateBareMetalInstancesServerBuilder) SetMetricsRegisterer(value prometheus.Registerer) *PrivateBareMetalInstancesServerBuilder {
	b.metricsRegisterer = value
	return b
}

// SetFilterDesc sets the protobuf message descriptor used to validate and translate CEL filter
// expressions. This is optional. When unset, the descriptor of this server's own private message type is used.
func (b *PrivateBareMetalInstancesServerBuilder) SetFilterDesc(value protoreflect.MessageDescriptor) *PrivateBareMetalInstancesServerBuilder {
	b.filterDesc = value
	return b
}

func (b *PrivateBareMetalInstancesServerBuilder) SetSecretStore(value vault.SecretStore) *PrivateBareMetalInstancesServerBuilder {
	b.secretStore = value
	return b
}

// SetNetworkingSettings sets the networking installation settings. This is optional; the default is
// DefaultNetworkingSettings.
func (b *PrivateBareMetalInstancesServerBuilder) SetNetworkingSettings(value NetworkingSettings) *PrivateBareMetalInstancesServerBuilder {
	b.networkingSettings = &value
	return b
}

func (b *PrivateBareMetalInstancesServerBuilder) Build() (result *PrivateBareMetalInstancesServer, err error) {
	if b.logger == nil {
		err = errors.New("logger is mandatory")
		return
	}
	if b.tenancyLogic == nil {
		err = errors.New("tenancy logic is mandatory")
		return
	}
	if b.attributionLogic == nil {
		err = errors.New("attribution logic is mandatory")
		return
	}

	catalogItemsDao, err := dao.NewGenericDAO[*privatev1.BareMetalInstanceCatalogItem]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	templatesDao, err := dao.NewGenericDAO[*privatev1.BareMetalInstanceTemplate]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	instanceTypesDao, err := dao.NewGenericDAO[*privatev1.BareMetalInstanceType]().SetLogger(b.logger).SetTenancyLogic(b.tenancyLogic).SetMetricsRegisterer(b.metricsRegisterer).Build()
	if err != nil {
		return
	}

	subnetsDao, err := dao.NewGenericDAO[*privatev1.Subnet]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	virtualNetworksDao, err := dao.NewGenericDAO[*privatev1.VirtualNetwork]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	networkClassesDao, err := dao.NewGenericDAO[*privatev1.NetworkClass]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	securityGroupsDao, err := dao.NewGenericDAO[*privatev1.SecurityGroup]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	diskImagesDao, err := dao.NewGenericDAO[*privatev1.DiskImage]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	externalIPPoolDaoBuilder := dao.NewGenericDAO[*privatev1.ExternalIPPool]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer)
	externalIPPoolDao, err := externalIPPoolDaoBuilder.Build()
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

	secretsDao, err := dao.NewGenericDAO[*privatev1.Secret]().
		SetLogger(b.logger).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		Build()
	if err != nil {
		return
	}

	generic, err := NewGenericServer[*privatev1.BareMetalInstance]().
		SetLogger(b.logger).
		SetService(privatev1.BareMetalInstances_ServiceDesc.ServiceName).
		SetAttributionLogic(b.attributionLogic).
		SetTenancyLogic(b.tenancyLogic).
		SetMetricsRegisterer(b.metricsRegisterer).
		SetFilterDesc(b.filterDesc).
		Build()
	if err != nil {
		return
	}

	guards, err := newNetworkingGuards(b.logger)
	if err != nil {
		return
	}
	result = &PrivateBareMetalInstancesServer{
		settings:                resolveNetworkingSettings(b.networkingSettings),
		guards:                  guards,
		logger:                  b.logger,
		tenancyLogic:            b.tenancyLogic,
		generic:                 generic,
		catalogItemsDao:         catalogItemsDao,
		templatesDao:            templatesDao,
		instanceTypesDao:        instanceTypesDao,
		subnetsDao:              subnetsDao,
		virtualNetworksDao:      virtualNetworksDao,
		networkClassesDao:       networkClassesDao,
		securityGroupsDao:       securityGroupsDao,
		diskImagesDao:           diskImagesDao,
		externalIPPoolDao:       externalIPPoolDao,
		externalIPDao:           externalIPDao,
		externalIPAttachmentDao: externalIPAttachmentDao,
		secretsDao:              secretsDao,
		secretStore:             b.secretStore,
	}
	result.lifecycle = newExternalIPLifecycle(
		externalIPDao,
		externalIPAttachmentDao,
		nil,
		externalIPPoolDao,
		nil,
		nil,
		generic.dao,
		nil,
	)
	return
}

func (s *PrivateBareMetalInstancesServer) List(ctx context.Context,
	request *privatev1.BareMetalInstancesListRequest) (response *privatev1.BareMetalInstancesListResponse, err error) {
	err = s.generic.List(ctx, request, &response)
	return
}

func (s *PrivateBareMetalInstancesServer) Get(ctx context.Context,
	request *privatev1.BareMetalInstancesGetRequest) (response *privatev1.BareMetalInstancesGetResponse, err error) {
	err = s.generic.Get(ctx, request, &response)
	return
}

func (s *PrivateBareMetalInstancesServer) Create(ctx context.Context, request *privatev1.BareMetalInstancesCreateRequest) (response *privatev1.BareMetalInstancesCreateResponse, err error) {
	var warnings []string
	err = s.generic.CreateWithCandidatePreparation(ctx, request, &response, func(ctx context.Context, _ *privatev1.BareMetalInstance, candidate *privatev1.BareMetalInstance) error {
		if err := lockTenantForCreate(ctx, candidate.GetMetadata().GetTenant()); err != nil {
			return err
		}
		warnings, err = s.prepareCreate(ctx, candidate)
		return err
	})
	if err != nil {
		return
	}
	if !isDryRun(ctx) && response.GetObject().GetSpec().GetAutoExternalIpAttachment() {
		created := response.GetObject()
		err = createAutomaticExternalIPs(ctx, s.logger, s.settings, s.externalIPPoolDao, s.externalIPDao,
			created.GetMetadata(), created.GetId(), "baremetal_instance")
		if err != nil {
			if tx, txErr := database.TxFromContext(ctx); txErr == nil {
				tx.ReportError(&err)
			}
			return
		}
	}
	response.SetWarnings(warnings)
	return
}

// prepareCreate fills the new bare metal instance before it is stored. It selects either a
// published Catalog Item or a direct Template, applies Catalog rules and Template defaults,
// then checks the resulting image, network, and other required inputs.
func (s *PrivateBareMetalInstancesServer) prepareCreate(ctx context.Context, candidate *privatev1.BareMetalInstance) (warnings []string, err error) {
	template, err := s.resolveCreationSource(ctx, candidate)
	if err != nil {
		return nil, err
	}
	if err = s.applyBareMetalTemplate(candidate, template); err != nil {
		return
	}
	if err = s.validateAndResolveUserDataSecret(ctx, candidate.GetSpec(), true); err != nil {
		return
	}

	if err = s.validateSpec(candidate); err != nil {
		return
	}
	ref := candidate.GetSpec().GetInstanceType()
	if ref == nil {
		err = grpcstatus.Errorf(grpccodes.InvalidArgument, "spec.instance_type is mandatory")
		return
	}
	if err = validatePlatformReference(ref, "bare metal instance type", " in spec.instance_type"); err != nil {
		return
	}
	instanceType, err := resolveAndCanonicalizeLockedReference(ctx, s.instanceTypesDao, candidate.GetMetadata(), ref,
		"bare metal instance type", grpccodes.NotFound)
	if err != nil {
		return
	}
	if !candidate.GetSpec().HasRunStrategy() ||
		candidate.GetSpec().GetRunStrategy() == privatev1.BareMetalInstanceRunStrategy_BARE_METAL_INSTANCE_RUN_STRATEGY_UNSPECIFIED {
		candidate.GetSpec().SetRunStrategy(privatev1.BareMetalInstanceRunStrategy_BARE_METAL_INSTANCE_RUN_STRATEGY_ALWAYS)
	}
	if err = s.resolveNetworkAttachments(ctx, candidate, instanceType); err != nil {
		return
	}

	if ref := candidate.GetSpec().GetDiskImage(); ref != nil {
		if refKey(ref) == "" {
			return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "disk_image is mandatory")
		}
		resolved, err := resolveLockedDiskImageReference(ctx, s.diskImagesDao, referenceScope{tenant: candidate.GetMetadata().GetTenant(), project: candidate.GetMetadata().GetProject()}, ref, "")
		if err != nil {
			return nil, err
		}
		candidate.GetSpec().SetDiskImage(canonicalDiskImageReference(resolved))
		warnings, err := validateResolvedDiskImage(resolved, refKey(ref), "")
		if err != nil {
			return nil, err
		}
		if err := validation.ValidateBareMetalDiskImageArchitecture(instanceType, resolved); err != nil {
			return nil, grpcstatus.Error(grpccodes.InvalidArgument, err.Error())
		}
		return warnings, nil
	}

	return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "disk_image is mandatory")
}

// resolveCreationSource accepts exactly one provisioning source: spec.catalog_item or
// spec.template. For a Catalog Item it finds the item's Template and applies its field rules;
// for a direct Template it resolves that reference under the instance's assigned tenant/project.
func (s *PrivateBareMetalInstancesServer) resolveCreationSource(ctx context.Context,
	candidate *privatev1.BareMetalInstance) (*privatev1.BareMetalInstanceTemplate, error) {
	spec := candidate.GetSpec()
	if spec.GetCatalogItem() != nil && spec.GetTemplate() != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"catalog_item and template are mutually exclusive")
	}
	if spec.GetCatalogItem() != nil {
		return s.resolveCatalogItem(ctx, candidate)
	}
	if spec.GetTemplate() == nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"either catalog_item or template is required")
	}
	return resolveAndCanonicalizeReference(ctx, s.templatesDao, candidate.GetMetadata(), spec.GetTemplate(),
		"template", grpccodes.NotFound)
}

func (s *PrivateBareMetalInstancesServer) Update(ctx context.Context,
	request *privatev1.BareMetalInstancesUpdateRequest) (response *privatev1.BareMetalInstancesUpdateResponse, err error) {
	err = s.generic.UpdateWithCandidatePreparation(ctx, request, &response, func(ctx context.Context, current, candidate *privatev1.BareMetalInstance) error {
		if err := s.validateAndResolveUserDataSecret(
			ctx,
			candidate.GetSpec(),
			updateIncludesField(request.GetUpdateMask(), "spec.user_data_source"),
		); err != nil {
			return err
		}
		if err := validateBareMetalImmutability(current, candidate, request.GetUpdateMask()); err != nil {
			return err
		}
		return rejectStateChangeWhileDeleting(current, candidate, current.GetMetadata())
	})
	return
}

func (s *PrivateBareMetalInstancesServer) validateAndResolveUserDataSecret(
	ctx context.Context,
	spec *privatev1.BareMetalInstanceSpec,
	resolve bool,
) error {
	if !resolve || spec.GetUserDataSource().GetSecret() == nil {
		return nil
	}
	resolved, err := validateUserDataSecret(
		ctx, s.logger, s.secretsDao, s.secretStore, spec.GetUserDataSource().GetSecret(),
	)
	if err != nil {
		return err
	}
	spec.GetUserDataSource().SetSecret(resolved)
	return nil
}

func (s *PrivateBareMetalInstancesServer) Delete(ctx context.Context,
	request *privatev1.BareMetalInstancesDeleteRequest) (response *privatev1.BareMetalInstancesDeleteResponse, err error) {
	id := request.GetId()
	if id == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "identifier is mandatory")
	}
	instance, err := lockLifecycleResource(ctx, s.generic.dao, id, "BareMetalInstance")
	if err != nil {
		return
	}
	if deleteAlreadyAccepted(instance.GetMetadata()) {
		return &privatev1.BareMetalInstancesDeleteResponse{}, nil
	}
	if err = s.lifecycle.rejectManualAttachmentDelete(ctx, "BareMetalInstance", id, instance.GetMetadata()); err != nil {
		return
	}
	// The automatic ExternalIPAttachment and ExternalIP are deleted by the reconciler, in order, after this.
	if err = markDeleting(ctx, s.generic.dao, instance); err != nil {
		return
	}
	err = s.generic.Delete(ctx, request, &response)
	return
}

func (s *PrivateBareMetalInstancesServer) Signal(ctx context.Context,
	request *privatev1.BareMetalInstancesSignalRequest) (response *privatev1.BareMetalInstancesSignalResponse, err error) {
	err = s.generic.Signal(ctx, request, &response)
	return
}

// validateSpec validates fields on the bare metal instance spec that are checked at create time.
func (s *PrivateBareMetalInstancesServer) validateSpec(bmi *privatev1.BareMetalInstance) error {
	if bmi == nil {
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "bare metal instance is mandatory")
	}
	spec := bmi.GetSpec()
	if spec == nil {
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "bare metal instance spec is mandatory")
	}

	if key := spec.GetSshPublicKey(); key != "" {
		if err := validateOpenSSHPublicKey(key); err != nil {
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "spec.ssh_public_key: %s", err)
		}
	}

	if spec.GetUserDataSource().HasInline() {
		if err := validateBareMetalUserData([]byte(spec.GetUserDataSource().GetInline())); err != nil {
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "spec.user_data_source.inline: %s", err)
		}
	}

	// If none of the authentication inputs are set, the server cannot be accessed after deployment.
	if spec.GetSshPublicKey() == "" && spec.GetUserDataSource().GetInline() == "" && spec.GetUserDataSource().GetSecret() == nil {
		return grpcstatus.Error(grpccodes.InvalidArgument,
			"at least one authentication method must be provided: spec.ssh_public_key, spec.user_data_source.inline or spec.user_data_source.secret")
	}

	return nil
}

// resolveDefaultInterface returns the first fabric-role port on the selected instance type.
func (s *PrivateBareMetalInstancesServer) resolveDefaultInterface(instanceType *privatev1.BareMetalInstanceType) (string, error) {
	for _, port := range instanceType.GetSpec().GetHardware().GetNetworkPorts() {
		if port != nil && strings.EqualFold(port.GetRole(), "fabric") {
			return port.GetName(), nil
		}
	}
	return "", grpcstatus.Errorf(grpccodes.InvalidArgument,
		"bare metal instance type '%s' has no fabric-role network port for default network attachment", instanceType.GetId())
}

// resolveCatalogItem finds the instance's published Catalog Item in the selected tenant/project
// or shared scope, then finds the item's Template under the item's ownership. It applies locked
// and editable field and parameter rules and returns that Template for defaults.
func (s *PrivateBareMetalInstancesServer) resolveCatalogItem(ctx context.Context,
	bmi *privatev1.BareMetalInstance) (*privatev1.BareMetalInstanceTemplate, error) {
	if bmi == nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "bare metal instance is mandatory")
	}
	catalogItemRef := bmi.GetSpec().GetCatalogItem()
	if catalogItemRef == nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "catalog_item is mandatory")
	}
	catalogItemRefStr := refKey(catalogItemRef)

	item, err := resolveAndCanonicalizeLockedReference(ctx, s.catalogItemsDao, bmi.GetMetadata(), catalogItemRef, "catalog item", grpccodes.NotFound)
	if err != nil {
		return nil, err
	}

	if err := validateCatalogItemForCreation(item, catalogItemRefStr); err != nil {
		return nil, err
	}

	templateRef := item.GetTemplate()
	if templateRef == nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"catalog item '%s' does not reference a template", catalogItemRefStr)
	}
	templateRef = cloneMessage(templateRef)
	resolvedTemplate, resolveErr := resolveAndCanonicalizeLockedReference(ctx, s.templatesDao, item.GetMetadata(), templateRef, "template", grpccodes.NotFound)
	if resolveErr != nil {
		return nil, resolveErr
	}

	if err := applyBareMetalInstanceCatalogItemPolicies(bmi.GetSpec(), item.GetFields()); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%s", err)
	}
	parameters, parameterErr := applyCatalogItemTemplateParameterPolicies(
		utils.BareMetalInstanceTemplateAdapter{BareMetalInstanceTemplate: resolvedTemplate},
		item.GetTemplateParameters(), bmi.GetSpec().GetTemplateParameters())
	if parameterErr != nil {
		return nil, parameterErr
	}
	bmi.GetSpec().SetTemplateParameters(parameters)
	return resolvedTemplate, nil
}

// applyBareMetalTemplate validates the instance's Template parameters, fills omitted parameter
// values from the Template, and stores the Template's actual ID, name, and scope.
func (s *PrivateBareMetalInstancesServer) applyBareMetalTemplate(bmi *privatev1.BareMetalInstance, template *privatev1.BareMetalInstanceTemplate) error {
	if bmi.GetSpec().GetInstanceType() == nil && template.GetInstanceType() != nil {
		bmi.GetSpec().SetInstanceType(cloneMessage(template.GetInstanceType()))
	}

	providedParams := bmi.GetSpec().GetTemplateParameters()
	if len(template.GetParameters()) != 0 || len(providedParams) != 0 {
		actualParams, err := utils.ApplyTemplateParameterDefaultsAndValidate(
			utils.BareMetalInstanceTemplateAdapter{BareMetalInstanceTemplate: template}, providedParams,
		)
		if err != nil {
			return err
		}
		bmi.GetSpec().SetTemplateParameters(actualParams)
	}

	bmi.GetSpec().SetTemplate(canonicalBareMetalInstanceTemplateReference(template))
	return nil
}

// validateBareMetalImmutability ensures template, catalog_item, instance_type, disk_image, ssh_public_key, user_data_source.inline, template_parameters,
// auto_external_ip_attachment, and network_attachments cannot be changed after creation.
func validateBareMetalImmutability(
	current, candidate *privatev1.BareMetalInstance,
	mask *fieldmaskpb.FieldMask,
) error {
	updatingTemplate := updateIncludesField(mask, "spec.template")
	updatingCatalogItem := updateIncludesField(mask, "spec.catalog_item")
	updatingInstanceType := updateIncludesField(mask, "spec.instance_type")
	updatingDiskImage := updateIncludesField(mask, "spec.disk_image")
	updatingSshKey := updateIncludesField(mask, "spec.ssh_public_key")
	updatingUserDataSource := updateIncludesField(mask, "spec.user_data_source")
	updatingTemplateParams := updateIncludesField(mask, "spec.template_parameters")
	updatingAutoExternalIP := updateIncludesField(mask, "spec.auto_external_ip_attachment")
	updatingNetworkAttachments := updateIncludesField(mask, "spec.network_attachments")

	newSpec := candidate.GetSpec()
	existingSpec := current.GetSpec()
	if existingSpec == nil {
		return grpcstatus.Errorf(grpccodes.Internal, "stored bare metal instance is missing spec")
	}

	if updatingTemplate && refKey(existingSpec.GetTemplate()) != refKey(newSpec.GetTemplate()) {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change spec.template from '%s' to '%s': template is immutable",
			refKey(existingSpec.GetTemplate()), refKey(newSpec.GetTemplate()))
	}

	if updatingCatalogItem {
		ref, err := preserveCatalogItemProvenance(existingSpec.GetCatalogItem(), newSpec.GetCatalogItem(), mask)
		if err != nil {
			return err
		}
		newSpec.SetCatalogItem(ref)
	}
	if updatingInstanceType && !proto.Equal(existingSpec.GetInstanceType(), newSpec.GetInstanceType()) {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change spec.instance_type: instance type is immutable after creation")
	}
	if updatingDiskImage && !proto.Equal(existingSpec.GetDiskImage(), newSpec.GetDiskImage()) {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change spec.disk_image from '%s' to '%s': disk image is immutable",
			refKey(existingSpec.GetDiskImage()), refKey(newSpec.GetDiskImage()))
	}

	if updatingSshKey && existingSpec.GetSshPublicKey() != newSpec.GetSshPublicKey() {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change spec.ssh_public_key: ssh_public_key is immutable after creation")
	}

	if err := validateBareMetalUserDataImmutability(
		existingSpec, newSpec, updatingUserDataSource,
	); err != nil {
		return err
	}

	if updatingTemplateParams {
		templateParamsEqual := func(first, second *anypb.Any) bool {
			return proto.Equal(first, second)
		}
		if !maps.EqualFunc(existingSpec.GetTemplateParameters(), newSpec.GetTemplateParameters(), templateParamsEqual) {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"cannot change spec.template_parameters: template parameters are immutable")
		}
	}

	if updatingAutoExternalIP && existingSpec.GetAutoExternalIpAttachment() != newSpec.GetAutoExternalIpAttachment() {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change spec.auto_external_ip_attachment: auto_external_ip_attachment is immutable after creation")
	}

	if updatingNetworkAttachments {
		if err := compareNetworkAttachmentsImmutability(existingSpec.GetNetworkAttachments(), newSpec.GetNetworkAttachments()); err != nil {
			return err
		}
	}

	return nil
}

func validateBareMetalUserDataImmutability(
	existingSpec, newSpec *privatev1.BareMetalInstanceSpec,
	updatingUserDataSource bool,
) error {
	isAtomicMigration := updatingUserDataSource &&
		existingSpec.GetUserDataSource().HasInline() && newSpec.GetUserDataSource().GetSecret() != nil
	if updatingUserDataSource && existingSpec.GetUserDataSource().GetInline() != newSpec.GetUserDataSource().GetInline() && !isAtomicMigration {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change spec.user_data_source.inline: user_data_source.inline is immutable after creation")
	}
	if updatingUserDataSource && existingSpec.GetUserDataSource().GetSecret() != nil &&
		!sameSecretReference(existingSpec.GetUserDataSource().GetSecret(), newSpec.GetUserDataSource().GetSecret()) {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change spec.user_data_source.secret: user_data_source.secret is immutable after creation")
	}
	return nil
}

func compareNetworkAttachmentsImmutability(existing, updated []*privatev1.BareMetalNetworkAttachment) error {
	if len(existing) != len(updated) {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"cannot change number of network attachments from %d to %d: network_attachments structure is immutable",
			len(existing), len(updated))
	}
	for i := range existing {
		if refKey(existing[i].GetSubnet()) != refKey(updated[i].GetSubnet()) {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"cannot change network_attachments[%d].subnet from '%s' to '%s': subnet is immutable",
				i, refKey(existing[i].GetSubnet()), refKey(updated[i].GetSubnet()))
		}
		if existing[i].GetInterface() != updated[i].GetInterface() {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"cannot change network_attachments[%d].interface from '%s' to '%s': interface is immutable",
				i, existing[i].GetInterface(), updated[i].GetInterface())
		}
		if existing[i].GetPrimary() != updated[i].GetPrimary() {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"cannot change network_attachments[%d].primary: primary is immutable after creation", i)
		}
		if err := validateImmutableSecurityGroups(
			existing[i].GetSecurityGroups(),
			updated[i].GetSecurityGroups(),
			fmt.Sprintf("network_attachments[%d].security_groups", i),
		); err != nil {
			return err
		}
	}
	return nil
}

func validateBareMetalNetworkAttachmentStructure(source string, attachments []*privatev1.BareMetalNetworkAttachment) error {
	prefix := ""
	if source != "" {
		prefix = fmt.Sprintf("field '%s': ", source)
	}

	if len(attachments) > 1 {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"%sat most one network attachment is supported", prefix)
	}

	for i, a := range attachments {
		if a == nil {
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "%snetwork_attachments[%d]: attachment cannot be null", prefix, i)
		}
		if a.HasPrimary() && !a.GetPrimary() {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"%snetwork_attachments[%d]: primary: false is not supported; omit primary or set primary: true", prefix, i)
		}
	}
	return nil
}

func validateBareMetalAttachmentsForInstanceType(
	source string,
	attachments []*privatev1.BareMetalNetworkAttachment,
	instanceType *privatev1.BareMetalInstanceType,
) error {
	prefix := ""
	if source != "" {
		prefix = fmt.Sprintf("field '%s': ", source)
	}
	if instanceType == nil {
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "%sinstance_type is mandatory", prefix)
	}

	instanceTypeID := instanceType.GetId()
	interfaceRoles := make(map[string]string)
	validInterfaces := make(map[string]bool)
	for _, port := range instanceType.GetSpec().GetHardware().GetNetworkPorts() {
		if port == nil {
			continue
		}
		interfaceRoles[port.GetName()] = port.GetRole()
		if !strings.EqualFold(port.GetRole(), "lifecycle") {
			validInterfaces[port.GetName()] = true
		}
	}

	if len(attachments) > len(validInterfaces) {
		return grpcstatus.Errorf(grpccodes.InvalidArgument,
			"%snumber of network attachments (%d) exceeds available interfaces (%d) on instance type '%s'",
			prefix, len(attachments), len(validInterfaces), instanceTypeID)
	}

	for i, attachment := range attachments {
		iface := attachment.GetInterface()
		if iface == "" {
			continue
		}
		role, found := interfaceRoles[iface]
		switch {
		case found && strings.EqualFold(role, "lifecycle"):
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"%snetwork_attachments[%d]: interface '%s' has role 'lifecycle' and cannot be used for tenant networking", prefix, i, iface)
		case !validInterfaces[iface]:
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"%snetwork_attachments[%d]: interface '%s' not found in instance type '%s'", prefix, i, iface, instanceTypeID)
		}
	}

	return nil
}

// resolveNetworkAttachments resolves the network attachment field of a new bare metal instance: at most one
// attachment, primary, defaulted from the tenant defaults, on a fabric port of the instance type. A CaaS worker (tenant
// system) is checked against its Cluster instead.
func (s *PrivateBareMetalInstancesServer) resolveNetworkAttachments(ctx context.Context,
	bmi *privatev1.BareMetalInstance, instanceType *privatev1.BareMetalInstanceType) error {
	spec := bmi.GetSpec()
	attachments := spec.GetNetworkAttachments()
	if err := validateBareMetalNetworkAttachmentStructure("", attachments); err != nil {
		return err
	}
	attachment := &privatev1.BareMetalNetworkAttachment{}
	if len(attachments) == 1 {
		attachment = attachments[0]
	}
	request := networkAttachmentRequest{
		tenant:            bmi.GetMetadata().GetTenant(),
		project:           bmi.GetMetadata().GetProject(),
		subnetsDao:        s.subnetsDao,
		securityGroupsDao: s.securityGroupsDao,
		subnet:            attachment.GetSubnet(),
		securityGroups:    attachment.GetSecurityGroups(),
		source:            "spec.network_attachments[0]",
	}
	var cluster *privatev1.Cluster
	if bmi.GetMetadata().GetTenant() == auth.SystemTenant {
		var err error
		cluster, err = s.authorizeWorker(ctx, bmi, attachment)
		if err != nil {
			return err
		}
		// The worker (tenant system) references the Cluster tenant's records: resolve them with the guard DAOs
		// in the Cluster's tenant and project.
		request.tenant = cluster.GetMetadata().GetTenant()
		request.project = cluster.GetMetadata().GetProject()
		request.subnetsDao = s.guards.subnets
		request.securityGroupsDao = s.guards.securityGroups
		request.automaticGroupOwner = cluster.GetId()
	} else {
		if attachment.GetInterface() == "" {
			name, err := s.resolveDefaultInterface(instanceType)
			if err != nil {
				return err
			}
			attachment.SetInterface(name)
		}
		if err := validateBareMetalAttachmentsForInstanceType("",
			[]*privatev1.BareMetalNetworkAttachment{attachment}, instanceType); err != nil {
			return err
		}
	}
	resolution, err := resolveNetworkAttachment(ctx, s.logger, s.guards, s.settings, request)
	if err != nil {
		return err
	}
	if cluster != nil && s.settings.ProvisioningEnabled {
		if err := requireReadyNATGateway(ctx, s.guards, resolution.virtualNetwork); err != nil {
			return err
		}
	}
	attachment.SetSubnet(resolution.subnetReference())
	attachment.SetSecurityGroups(resolution.securityGroupReferences())
	attachment.SetPrimary(true)
	spec.SetNetworkAttachments([]*privatev1.BareMetalNetworkAttachment{attachment})

	canonicalHubID, err := canonicalNetworkingHubID(ctx, s.logger, s.networkClassesDao)
	if err != nil {
		return err
	}
	return validateNetworkingHubReferences(canonicalHubID, resolution.hubReferences()...)
}

// authorizeWorker checks the create of a CaaS worker bare metal instance (tenant system): status.cluster names a
// Cluster that isn't being deleted, and the network attachment field is exactly the Cluster's: its Subnet, its
// SecurityGroups plus the automatic Cluster SecurityGroup, and the fabric port of a node set of the instance type.
func (s *PrivateBareMetalInstancesServer) authorizeWorker(ctx context.Context, bmi *privatev1.BareMetalInstance,
	attachment *privatev1.BareMetalNetworkAttachment) (*privatev1.Cluster, error) {
	clusterID := bmi.GetStatus().GetCluster().GetId()
	if clusterID == "" {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"a bare metal instance in tenant system needs status.cluster")
	}
	clusters, err := s.guards.clusters.List().SetFilter(fmt.Sprintf("this.id == %q", clusterID)).SetLimit(1).Do(ctx)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, "failed to find the Cluster of the worker")
	}
	if len(clusters.GetItems()) == 0 {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "Cluster '%s' does not exist", clusterID)
	}
	cluster, err := lockRecord(ctx, s.guards.clusters, clusterID)
	if err != nil {
		return nil, err
	}
	if cluster.GetMetadata().HasDeletionTimestamp() {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "Cluster '%s' is being deleted", clusterID)
	}
	clusterAttachment := cluster.GetSpec().GetNetworkAttachment()
	if refKey(attachment.GetSubnet()) != refKey(clusterAttachment.GetSubnet()) {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"spec.network_attachments[0].subnet must be the Subnet '%s' of Cluster '%s'",
			refKey(clusterAttachment.GetSubnet()), clusterID)
	}
	expected := map[string]bool{automaticSecurityGroupPrefix + clusterID: true}
	for _, ref := range clusterAttachment.GetSecurityGroups() {
		expected[refKey(ref)] = true
	}
	automaticGroups, err := s.guards.securityGroups.List().
		SetFilter(fmt.Sprintf("this.metadata.name == %q && this.metadata.labels[\"%s\"] == %q",
			automaticSecurityGroupPrefix+clusterID, autoCreatedForLabel, clusterID)).
		Do(ctx)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, "failed to find the automatic Cluster SecurityGroup")
	}
	automaticID := ""
	for _, group := range automaticGroups.GetItems() {
		automaticID = group.GetId()
	}
	actual := map[string]bool{}
	for _, ref := range attachment.GetSecurityGroups() {
		key := refKey(ref)
		if key == automaticID {
			key = automaticSecurityGroupPrefix + clusterID
		}
		actual[key] = true
	}
	if len(actual) != len(attachment.GetSecurityGroups()) || !maps.Equal(actual, expected) {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"spec.network_attachments[0].security_groups must be the SecurityGroups of Cluster '%s' and its "+
				"automatic SecurityGroup", clusterID)
	}
	instanceTypeID := refKey(bmi.GetSpec().GetInstanceType())
	for _, nodeSet := range cluster.GetSpec().GetNodeSets() {
		if refKey(nodeSet.GetBaremetalInstanceType()) == instanceTypeID && nodeSet.GetFabricInterface() != "" &&
			nodeSet.GetFabricInterface() == attachment.GetInterface() {
			return cluster, nil
		}
	}
	return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
		"spec.network_attachments[0].interface '%s' is not the fabric port of a node set of Cluster '%s' with "+
			"instance type '%s'", attachment.GetInterface(), clusterID, instanceTypeID)
}

// requireReadyNATGateway checks the Cluster gate: with provider networking enabled, the VirtualNetwork has a READY
// NATGateway, through which the workers reach the management endpoints. It takes a share lock on the READY NATGateway
// and rechecks it, so a concurrent NATGateway delete (which locks the row for update and then checks the Clusters)
// runs either before this check, which then sees the gateway DELETING, or after this transaction, and then sees the
// new Cluster.
func requireReadyNATGateway(ctx context.Context, guards *networkingGuards,
	virtualNetwork *privatev1.VirtualNetwork) error {
	response, err := guards.natGateways.List().
		SetFilter(fmt.Sprintf("this.spec.virtual_network.id == %q", virtualNetwork.GetId())).
		Do(ctx)
	if err != nil {
		return grpcstatus.Error(grpccodes.Internal, "failed to find the NATGateway of the VirtualNetwork")
	}
	for _, gateway := range response.GetItems() {
		if !isReadyNATGateway(gateway) {
			continue
		}
		locked, err := guards.natGateways.Get().SetId(gateway.GetId()).SetShareLock(true).Do(ctx)
		if err != nil {
			var notFound *dao.ErrNotFound
			if errors.As(err, &notFound) {
				continue
			}
			return translateLifecycleError(err)
		}
		if isReadyNATGateway(locked.GetObject()) {
			return nil
		}
	}
	return grpcstatus.Errorf(grpccodes.FailedPrecondition,
		"VirtualNetwork '%s' has no READY NATGateway; Cluster workers reach the management endpoints through it",
		virtualNetwork.GetId())
}

func isReadyNATGateway(gateway *privatev1.NATGateway) bool {
	return !gateway.GetMetadata().HasDeletionTimestamp() &&
		gateway.GetStatus().GetState() == privatev1.NATGatewayState_NAT_GATEWAY_STATE_READY
}
