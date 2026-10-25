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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/osac-project/osac/fulfillment-service/internal/config"
	"github.com/osac-project/osac/fulfillment-service/internal/logging"
	"github.com/osac-project/osac/fulfillment-service/internal/terminal"
	"github.com/osac-project/osac/fulfillment-service/internal/testing"
	publicv1 "github.com/osac-project/osac/proto/gen/osac/public/v1"
)

type catalogItemsServer struct {
	publicv1.UnimplementedBareMetalInstanceCatalogItemsServer
}

func (catalogItemsServer) List(context.Context, *publicv1.BareMetalInstanceCatalogItemsListRequest) (*publicv1.BareMetalInstanceCatalogItemsListResponse, error) {
	return &publicv1.BareMetalInstanceCatalogItemsListResponse{
		Items: []*publicv1.BareMetalInstanceCatalogItem{{Id: "catalog-123"}},
	}, nil
}

type bareMetalInstancesServer struct {
	publicv1.UnimplementedBareMetalInstancesServer
	warnings  []string
	createErr error
}

func (s bareMetalInstancesServer) Create(context.Context, *publicv1.BareMetalInstancesCreateRequest) (*publicv1.BareMetalInstancesCreateResponse, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}
	return &publicv1.BareMetalInstancesCreateResponse{
		Object:   &publicv1.BareMetalInstance{Id: "bmi-123"},
		Warnings: s.warnings,
	}, nil
}

var _ = Describe("Create baremetalinstance response output", func() {
	DescribeTable("renders successful creation and server warnings",
		func(warnings []string, createErr error, wantStderr string) {
			server := testing.NewServer()
			DeferCleanup(server.Stop)
			publicv1.RegisterBareMetalInstanceCatalogItemsServer(server.Registrar(), catalogItemsServer{})
			publicv1.RegisterBareMetalInstancesServer(server.Registrar(), bareMetalInstancesServer{
				warnings: warnings, createErr: createErr,
			})
			server.Start()

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			settings, err := config.NewSettings().SetLogger(logger).SetDir(GinkgoT().TempDir()).Build()
			Expect(err).NotTo(HaveOccurred())
			settings.SetAddress(server.Address())
			settings.SetPlaintext(true)

			var stdout, stderr bytes.Buffer
			console, err := terminal.NewConsole().SetLogger(logger).SetStdout(&stdout).SetStderr(&stderr).Build()
			Expect(err).NotTo(HaveOccurred())
			ctx := logging.LoggerIntoContext(context.Background(), logger)
			ctx = config.SettingsIntoContext(ctx, settings)
			ctx = terminal.ConsoleIntoContext(ctx, console)

			cmd := Cmd()
			cmd.SetContext(ctx)
			cmd.SetArgs([]string{"--catalog-item", "catalog-123", "--name", "example"})
			err = cmd.Execute()

			if createErr != nil {
				Expect(err).To(MatchError(ContainSubstring(createErr.Error())))
				Expect(stdout.String()).To(BeEmpty())
				Expect(stderr.String()).NotTo(ContainSubstring("Warning:"))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(stdout.String()).To(Equal("Created bare metal instance 'bmi-123'.\n"))
				Expect(stderr.String()).To(Equal(wantStderr))
			}
		},
		Entry("without warnings", nil, nil, ""),
		Entry("with one warning", []string{"image is deprecated"}, nil, "Warning: image is deprecated\n"),
		Entry("with multiple warnings in server order", []string{"first", "second"}, nil, "Warning: first\nWarning: second\n"),
		Entry("when create fails", nil, errors.New("create failed"), ""),
	)
})

var _ = Describe("Create baremetalinstance flag registration", func() {
	It("should register --disk-image flag", func() {
		cmd := Cmd()
		cmd.SetOut(GinkgoWriter)
		cmd.SetErr(GinkgoWriter)

		flag := cmd.Flags().Lookup("disk-image")
		Expect(flag).NotTo(BeNil())
		Expect(flag.Usage).To(ContainSubstring("DiskImage"))
		Expect(cmd.ParseFlags([]string{"--disk-image", "rhel-9"})).To(Succeed())
	})

	It("should register --instance-type flag", func() {
		cmd := Cmd()
		cmd.SetOut(GinkgoWriter)
		cmd.SetErr(GinkgoWriter)

		flag := cmd.Flags().Lookup("instance-type")
		Expect(flag).NotTo(BeNil())
		Expect(flag.Usage).To(ContainSubstring("InstanceType"))
		Expect(cmd.ParseFlags([]string{"--instance-type", "standard-bm"})).To(Succeed())
	})

	It("should not register legacy image flags", func() {
		cmd := Cmd()
		Expect(cmd.Flags().Lookup("image")).To(BeNil())
		Expect(cmd.Flags().Lookup("image-source-type")).To(BeNil())
	})

	It("should register --network-attachment flag", func() {
		cmd := Cmd()
		cmd.SetOut(GinkgoWriter)
		cmd.SetErr(GinkgoWriter)
		flag := cmd.Flags().Lookup("network-attachment")
		Expect(flag).NotTo(BeNil())
		Expect(flag.Usage).To(ContainSubstring("Network attachment"))
	})

	It("should register --user-data-secret flag", func() {
		cmd := Cmd()
		flag := cmd.Flags().Lookup("user-data-secret")
		Expect(flag).NotTo(BeNil())
		Expect(flag.Usage).To(ContainSubstring("Secret resource"))
	})

	It("should reject user data and a user data secret together", func() {
		cmd := Cmd()
		cmd.SetOut(GinkgoWriter)
		cmd.SetErr(GinkgoWriter)
		cmd.SetArgs([]string{"--catalog-item", "cat-001", "--user-data", "data", "--user-data-secret", "cloud-init"})
		err := cmd.Execute()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("user-data"))
		Expect(err.Error()).To(ContainSubstring("user-data-secret"))
	})
})

var _ = Describe("applyUserDataFlags", func() {
	It("should set a secret reference", func() {
		c := &runnerContext{}
		c.args.userDataSecret = "cloud-init"
		spec := publicv1.BareMetalInstanceSpec_builder{}

		c.applyUserDataFlags(&spec)

		Expect(spec.Build().GetUserDataSource().GetSecret().GetName()).To(Equal("cloud-init"))
	})
})

var _ = Describe("buildSpec", func() {
	It("should set disk_image from the disk-image flag", func() {
		c := &runnerContext{}
		c.args.diskImage = "rhel-9"

		spec, err := c.buildSpec("catalog-item-id", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(spec.GetDiskImage().GetName()).To(Equal("rhel-9"))
	})

	It("should leave disk_image unset when the disk-image flag is empty", func() {
		c := &runnerContext{}

		spec, err := c.buildSpec("catalog-item-id", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(spec.HasDiskImage()).To(BeFalse())
	})

	It("should apply an instance type reference through the set flag", func() {
		c := &runnerContext{}
		c.args.setFields = []string{"instance_type.name=bare-metal.large", "instance_type.shared=true"}

		spec, err := c.buildSpec("catalog-item-id", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(spec.GetInstanceType().GetName()).To(Equal("bare-metal.large"))
		Expect(spec.GetInstanceType().GetShared()).To(BeTrue())
	})

	It("should set instance_type from the instance-type flag", func() {
		c := &runnerContext{}
		c.args.instanceType = "standard-bm"

		spec, err := c.buildSpec("catalog-item-id", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(spec.GetInstanceType().GetName()).To(Equal("standard-bm"))
	})

	It("should leave instance_type unset when the instance-type flag is empty", func() {
		c := &runnerContext{}

		spec, err := c.buildSpec("catalog-item-id", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(spec.HasInstanceType()).To(BeFalse())
	})
})

var _ = Describe("applyNetworkingFlags", func() {
	It("should set the resolved attachment", func() {
		iface := "data-0"
		c := &runnerContext{
			networkAttachment: publicv1.BareMetalNetworkAttachment_builder{
				Subnet:    &publicv1.SubnetLocalReference{Id: "n1"},
				Interface: &iface,
			}.Build(),
		}
		spec := publicv1.BareMetalInstanceSpec_builder{}
		Expect(c.applyNetworkingFlags(&spec)).To(Succeed())
		Expect(spec.Build().GetNetworkAttachments()).To(HaveLen(1))
		Expect(spec.Build().GetNetworkAttachments()[0].GetInterface()).To(Equal("data-0"))
	})

	It("should leave attachments empty when no network flag is set", func() {
		c := &runnerContext{}
		spec := publicv1.BareMetalInstanceSpec_builder{}
		Expect(c.applyNetworkingFlags(&spec)).To(Succeed())
		Expect(spec.Build().GetNetworkAttachments()).To(BeEmpty())
	})

	It("should reject more than one network-attachment flag", func() {
		c := &runnerContext{}
		c.args.networkAttachments = []string{"subnet=n1", "subnet=n2"}
		err := c.resolveNetworkAttachment(context.Background(), nil)
		Expect(err).To(MatchError(ContainSubstring("at most one --network-attachment")))
	})

	It("should reject the removed primary keyword", func() {
		c := &runnerContext{}
		c.args.networkAttachments = []string{"subnet=n1,primary"}
		Expect(c.resolveNetworkAttachment(context.Background(), nil)).NotTo(Succeed())
	})
})
