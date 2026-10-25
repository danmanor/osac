/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package baremetalinstance

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log/slog"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	"github.com/osac-project/osac/fulfillment-service/internal/cmd/cli/create/fieldutil"
	"github.com/osac-project/osac/fulfillment-service/internal/cmd/cli/create/metautil"
	"github.com/osac-project/osac/fulfillment-service/internal/cmd/cli/create/netutil"
	"github.com/osac-project/osac/fulfillment-service/internal/cmd/cli/lookup"
	"github.com/osac-project/osac/fulfillment-service/internal/config"
	"github.com/osac-project/osac/fulfillment-service/internal/logging"
	"github.com/osac-project/osac/fulfillment-service/internal/terminal"
	publicv1 "github.com/osac-project/osac/proto/gen/osac/public/v1"
)

var runStrategyMap = map[string]publicv1.BareMetalInstanceRunStrategy{
	"always": publicv1.BareMetalInstanceRunStrategy_BARE_METAL_INSTANCE_RUN_STRATEGY_ALWAYS,
	"halted": publicv1.BareMetalInstanceRunStrategy_BARE_METAL_INSTANCE_RUN_STRATEGY_HALTED,
}

func Cmd() *cobra.Command {
	runner := &runnerContext{}
	result := &cobra.Command{
		Use:                   "baremetalinstance [FLAG...]",
		Aliases:               []string{string(proto.MessageName((*publicv1.BareMetalInstance)(nil)))},
		Short:                 shortHelp,
		Long:                  longHelp,
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  runner.run,
	}
	flags := result.Flags()
	flags.StringVarP(
		&runner.args.name,
		"name",
		"n",
		"",
		nameFlagHelp,
	)
	flags.StringVar(
		&runner.args.catalogItem,
		"catalog-item",
		"",
		catalogItemFlagHelp,
	)
	flags.StringVar(
		&runner.args.sshKey,
		"ssh-key",
		"",
		sshKeyFlagHelp,
	)
	flags.StringVar(
		&runner.args.userData,
		"user-data",
		"",
		userDataFlagHelp,
	)
	flags.StringVar(
		&runner.args.userDataSecret,
		"user-data-secret",
		"",
		userDataSecretFlagHelp,
	)
	flags.StringVar(
		&runner.args.runStrategy,
		"run-strategy",
		"",
		runStrategyFlagHelp,
	)
	flags.StringVar(
		&runner.args.diskImage,
		"disk-image",
		"",
		diskImageFlagHelp,
	)
	flags.BoolVar(
		&runner.args.externalIPAttachment,
		"external-ip-attachment",
		false,
		externalIPAttachmentFlagHelp,
	)
	flags.StringArrayVar(
		&runner.args.networkAttachments,
		"network-attachment",
		nil,
		networkAttachmentFlagHelp,
	)
	flags.StringArrayVar(
		&runner.args.setFields,
		"set",
		nil,
		setFlagHelp,
	)
	flags.StringVar(
		&runner.args.instanceType,
		"instance-type",
		"",
		instanceTypeFlagHelp,
	)

	if err := result.MarkFlagRequired("catalog-item"); err != nil {
		panic(fmt.Sprintf("failed to mark catalog-item flag as required: %v", err))
	}
	result.MarkFlagsMutuallyExclusive("user-data", "user-data-secret")
	return result
}

type runnerContext struct {
	networkAttachment *publicv1.BareMetalNetworkAttachment
	args              struct {
		name                 string
		catalogItem          string
		setFields            []string
		networkAttachments   []string
		sshKey               string
		userData             string
		userDataSecret       string
		runStrategy          string
		diskImage            string
		instanceType         string
		externalIPAttachment bool
	}
	logger *slog.Logger
}

func (c *runnerContext) run(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	c.logger = logging.LoggerFromContext(ctx)
	console := terminal.ConsoleFromContext(ctx)

	cfg := config.SettingsFromContext(ctx)
	if !cfg.Armed() {
		return fmt.Errorf("there is no configuration, run the 'login' command")
	}

	conn, err := cfg.Connect(ctx, cmd.Flags())
	if err != nil {
		return fmt.Errorf("failed to create gRPC connection: %w", err)
	}
	defer conn.Close()

	// Resolve the names of the network attachment before anything is created:
	if err := c.resolveNetworkAttachment(ctx, conn); err != nil {
		return err
	}

	catalogItemsClient := publicv1.NewBareMetalInstanceCatalogItemsClient(conn)
	catalogItem, err := lookup.Find(c.args.catalogItem, "bare metal instance catalog item",
		func(filter string, limit int32) ([]*publicv1.BareMetalInstanceCatalogItem, error) {
			resp, err := catalogItemsClient.List(ctx, publicv1.BareMetalInstanceCatalogItemsListRequest_builder{
				Filter: proto.String(filter),
				Limit:  proto.Int32(limit),
			}.Build())
			if err != nil {
				return nil, fmt.Errorf("failed to list catalog items: %w", err)
			}
			return resp.GetItems(), nil
		})
	if err != nil {
		return err
	}

	builtSpec, err := c.buildSpec(catalogItem.GetId(), cmd.Flags().Changed("external-ip-attachment"))
	if err != nil {
		return err
	}

	bmi := publicv1.BareMetalInstance_builder{
		Metadata: metautil.ForCreate(ctx, c.args.name).Build(),
		Spec:     builtSpec,
	}.Build()

	client := publicv1.NewBareMetalInstancesClient(conn)
	response, err := client.Create(ctx, publicv1.BareMetalInstancesCreateRequest_builder{
		Object: bmi,
	}.Build())
	if err != nil {
		return fmt.Errorf("failed to create bare metal instance: %w", err)
	}

	for _, warning := range response.GetWarnings() {
		console.Errorf(ctx, "Warning: %s\n", warning)
	}
	console.Infof(ctx, "Created bare metal instance '%s'.\n", response.GetObject().GetId())
	return nil
}

func (c *runnerContext) buildSpec(catalogItemID string, externalIPAttachmentSet bool) (*publicv1.BareMetalInstanceSpec, error) {
	spec := publicv1.BareMetalInstanceSpec_builder{
		CatalogItem: &publicv1.BareMetalInstanceCatalogItemReference{Id: catalogItemID},
	}
	if c.args.sshKey != "" {
		sshKey := c.args.sshKey
		spec.SshPublicKey = &sshKey
	}
	c.applyUserDataFlags(&spec)
	if c.args.diskImage != "" {
		spec.DiskImage = &publicv1.DiskImageReference{Name: c.args.diskImage}
	}
	if c.args.instanceType != "" {
		spec.InstanceType = &publicv1.BareMetalInstanceTypeReference{Name: c.args.instanceType, Shared: true}
	}
	if c.args.runStrategy != "" {
		rs, err := fieldutil.ParseEnum(c.args.runStrategy, runStrategyMap, "run-strategy")
		if err != nil {
			return nil, err
		}
		spec.RunStrategy = &rs
	}
	if externalIPAttachmentSet {
		spec.AutoExternalIpAttachment = proto.Bool(c.args.externalIPAttachment)
	}

	if err := c.applyNetworkingFlags(&spec); err != nil {
		return nil, err
	}

	builtSpec := spec.Build()
	if err := fieldutil.ApplyFields(builtSpec, c.args.setFields); err != nil {
		return nil, err
	}

	return builtSpec, nil
}

const shortHelp = `Create a bare metal instance`

const longHelp = `
Create a bare metal instance.
`

const nameFlagHelp = `
_NAME_ - Name of the bare metal instance.
`

const catalogItemFlagHelp = `
_ID_OR_NAME_ - Catalog item identifier or name. If a name matches more than
one visible item, use its identifier. Required.
`

const sshKeyFlagHelp = `
_KEY_ - SSH public key injected into the OS at provisioning time. Must be a
valid OpenSSH public key. Immutable after creation.
`

const userDataFlagHelp = `
_DATA_ - User data passed to the OS at first boot (e.g. cloud-init).
Maximum 64 KB. Immutable after creation.
`

const userDataSecretFlagHelp = `
_NAME_ - Name of a Secret resource containing user data passed to the OS at
first boot. The secret must exist in the same tenant. See also
{{ bt }}osac create secret{{ bt }}. Mutually exclusive with
{{ bt }}--user-data{{ bt }}.
`

const runStrategyFlagHelp = `
_STRATEGY_ - Run strategy controlling the power state. Valid values are
{{ bt }}Always{{ bt }} (keep powered on) and {{ bt }}Halted{{ bt }}
(power off).
`

const diskImageFlagHelp = `
_NAME_ - DiskImage resource name to use for this bare metal instance. When
omitted, the catalog item must provide a default.
`

const instanceTypeFlagHelp = `
_NAME_ - InstanceType resource name to use for this bare metal instance. When
omitted, the catalog item must provide a default.
`

const externalIPAttachmentFlagHelp = `
_[BOOLEAN]_ - When set, the system auto-selects an ExternalIPPool and
creates an ExternalIP with an ExternalIPAttachment for this instance
atomically during creation. Immutable after creation.
`

const networkAttachmentFlagHelp = `
_SPEC_ - Network attachment of the instance, at most one. The value can be a
plain subnet name or ID, or a comma-separated specification in the format
{{ bt }}[subnet=NAME][,interface=NAME][,security-groups=NAME,NAME...]{{ bt }};
names and IDs are accepted. The {{ bt }}interface{{ bt }} field selects a
fabric port of the bare metal instance type; without it the first fabric port
is used. Without a subnet the tenant's default subnet is used, and without
security groups the tenant's default security group is used when the subnet
is in the tenant's default virtual network.
`

const setFlagHelp = `
_KEY=VALUE_ - Set a spec field or template parameter on the resource.
Use dot notation for nested fields (e.g.
{{ bt }}template_parameters.vpc_id=vpc-123{{ bt }}). Can be specified
multiple times.
`

// resolveNetworkAttachment parses the --network-attachment flag, at most once, and resolves its names to IDs.
func (c *runnerContext) resolveNetworkAttachment(ctx context.Context, conn grpc.ClientConnInterface) error {
	if len(c.args.networkAttachments) == 0 {
		return nil
	}
	if len(c.args.networkAttachments) > 1 {
		return fmt.Errorf("at most one --network-attachment is supported")
	}
	flag, err := netutil.ParseNetworkAttachmentFlag(c.args.networkAttachments[0], true)
	if err != nil {
		return err
	}
	subnet, groups, err := netutil.ResolveNetworkAttachment(ctx, conn, flag)
	if err != nil {
		return err
	}
	builder := publicv1.BareMetalNetworkAttachment_builder{
		Subnet:         subnet,
		SecurityGroups: groups,
	}
	if flag.Interface != "" {
		builder.Interface = &flag.Interface
	}
	c.networkAttachment = builder.Build()
	return nil
}

func (c *runnerContext) applyNetworkingFlags(spec *publicv1.BareMetalInstanceSpec_builder) error {
	if c.networkAttachment != nil {
		spec.NetworkAttachments = []*publicv1.BareMetalNetworkAttachment{c.networkAttachment}
	}
	return nil
}

func (c *runnerContext) applyUserDataFlags(spec *publicv1.BareMetalInstanceSpec_builder) {
	if c.args.userData != "" {
		spec.UserDataSource = publicv1.UserDataSource_builder{Inline: proto.String(c.args.userData)}.Build()
	}
	if c.args.userDataSecret != "" {
		spec.UserDataSource = publicv1.UserDataSource_builder{Secret: publicv1.SecretLocalReference_builder{Name: c.args.userDataSecret}.Build()}.Build()
	}
}
