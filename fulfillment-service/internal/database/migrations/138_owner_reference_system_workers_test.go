/*
Copyright (c) 2026 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/

package migrations

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2/dsl/core"
	. "github.com/onsi/gomega"
)

var _ = DescribeMigration("Owner reference checks for system workers", func() {
	BeforeEach(func(ctx context.Context) {
		Expect(tool.Migrate(ctx, 138)).To(Succeed())
		_, err := conn.Exec(ctx, `
			insert into tenants (id, name, tenant, creator, data)
			values ('tenant-a', 'tenant-a', 'tenant-a', 'system', '{}'),
			       ('tenant-b', 'tenant-b', 'tenant-b', 'system', '{}');
			insert into clusters (id, name, tenant, data)
			values ('cluster-1', 'cluster-1', 'tenant-a', '{}')`)
		Expect(err).ToNot(HaveOccurred())
	})

	insertWorker := func(ctx context.Context, tenant string) error {
		_, err := conn.Exec(ctx, `
			insert into bare_metal_instances (id, name, tenant, data)
			values ('worker-1', 'worker-1', $1, '{"status":{"cluster":{"id":"cluster-1"},"node_set":"compute"}}')`,
			tenant)
		return err
	}

	It("accepts a worker in tenant system that references a Cluster of another tenant", func(ctx context.Context) {
		Expect(insertWorker(ctx, "system")).To(Succeed())
	})

	It("still rejects a worker of another tenant", func(ctx context.Context) {
		err := insertWorker(ctx, "tenant-b")
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue())
		Expect(pgErr.Code).To(Equal("Z0005"))
	})
})
