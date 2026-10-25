/*
Copyright (c) 2026 Red Hat Inc.

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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/osac-project/osac/fulfillment-service/internal/database"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

// These specs check that the Cluster NATGateway gate and the NATGateway delete serialize on the NATGateway row: the
// gate takes a share lock on the READY gateway, the delete locks it for update and then checks the Clusters.
var _ = Describe("Cluster create and NATGateway delete race", func() {
	var (
		natServer      *PrivateNATGatewaysServer
		guards         *networkingGuards
		virtualNetwork *privatev1.VirtualNetwork
		subnet         *privatev1.Subnet
		natGatewayID   string
	)

	// begin starts an independent transaction and returns its context and backend PID.
	begin := func() (database.Tx, context.Context, int32) {
		tx, err := tm.Begin(context.Background())
		Expect(err).ToNot(HaveOccurred())
		txCtx := database.TxIntoContext(context.Background(), tx)
		var pid int32
		Expect(tx.QueryRow(txCtx, "select pg_backend_pid()").Scan(&pid)).To(Succeed())
		DeferCleanup(func() {
			_ = tx.End(context.Background())
		})
		return tx, txCtx, pid
	}

	// waitBlockedBy waits until PostgreSQL reports a query blocked by the transaction with the given PID.
	waitBlockedBy := func(pid int32) {
		Eventually(func() (bool, error) {
			var waiting bool
			err := testPool.QueryRow(context.Background(), `
				select exists (
					select 1
					from pg_stat_activity
					where datname = current_database()
					  and wait_event_type = 'Lock'
					  and $1 = any(pg_blocking_pids(pid))
				)
			`, pid).Scan(&waiting)
			return waiting, err
		}, 10*time.Second, 10*time.Millisecond).Should(BeTrue())
	}

	// storeCluster stores a Cluster whose network attachment is on the Subnet, as the Cluster create does after its
	// gates pass.
	storeCluster := func(txCtx context.Context) {
		_, err := fixtureDAO[*privatev1.Cluster]().Create().SetObject(privatev1.Cluster_builder{
			Metadata: privatev1.Metadata_builder{
				Name:   "race-cluster",
				Tenant: testTenant,
			}.Build(),
			Spec: privatev1.ClusterSpec_builder{
				NetworkAttachment: privatev1.ClusterNetworkAttachment_builder{
					Subnet: privatev1.SubnetLocalReference_builder{Id: subnet.GetId()}.Build(),
				}.Build(),
			}.Build(),
		}.Build()).Do(txCtx)
		Expect(err).ToNot(HaveOccurred())
	}

	deleteGateway := func(txCtx context.Context) error {
		_, err := natServer.Delete(txCtx, privatev1.NATGatewaysDeleteRequest_builder{Id: natGatewayID}.Build())
		return err
	}

	BeforeEach(func() {
		var err error
		natServer, err = NewPrivateNATGatewaysServer().
			SetLogger(logger).
			SetAttributionLogic(attribution).
			SetTenancyLogic(tenancy).
			Build()
		Expect(err).ToNot(HaveOccurred())
		guards, err = newNetworkingGuards(logger)
		Expect(err).ToNot(HaveOccurred())

		virtualNetwork = seedVirtualNetwork(testTenant, "10.60.0.0/16",
			privatev1.VirtualNetworkState_VIRTUAL_NETWORK_STATE_READY, nil)
		subnet = seedSubnet(virtualNetwork, "", "10.60.1.0/24", privatev1.SubnetState_SUBNET_STATE_READY, nil)
		natGatewayID = seedReadyNATGateway(testTenant, virtualNetwork.GetId(), false)

		// Commit the fixtures so independent transactions contend on the same rows:
		Expect(suiteTx.End(ctx)).To(Succeed())
		suiteTx = nil
	})

	It("rejects the NATGateway delete when the Cluster gate locked the gateway first", func() {
		createTx, createCtx, createPID := begin()
		Expect(requireReadyNATGateway(createCtx, guards, virtualNetwork)).To(Succeed())

		_, deleteCtx, _ := begin()
		deleteDone := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			deleteDone <- deleteGateway(deleteCtx)
		}()
		waitBlockedBy(createPID)

		storeCluster(createCtx)
		Expect(createTx.End(context.Background())).To(Succeed())

		var err error
		Eventually(deleteDone, 10*time.Second).Should(Receive(&err))
		Expect(grpcstatus.Code(err)).To(Equal(grpccodes.FailedPrecondition))
		Expect(err.Error()).To(ContainSubstring("Cluster(s) have a network attachment"))
	})

	It("rejects the Cluster gate when the NATGateway delete locked the gateway first", func() {
		deleteTx, deleteCtx, deletePID := begin()
		Expect(deleteGateway(deleteCtx)).To(Succeed())

		_, createCtx, _ := begin()
		gateDone := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			gateDone <- requireReadyNATGateway(createCtx, guards, virtualNetwork)
		}()
		waitBlockedBy(deletePID)
		Expect(deleteTx.End(context.Background())).To(Succeed())

		var err error
		Eventually(gateDone, 10*time.Second).Should(Receive(&err))
		Expect(grpcstatus.Code(err)).To(Equal(grpccodes.FailedPrecondition))
		Expect(err.Error()).To(ContainSubstring("has no READY NATGateway"))
	})
})
