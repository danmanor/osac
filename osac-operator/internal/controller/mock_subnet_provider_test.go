/*
Copyright 2025.

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

	"sigs.k8s.io/controller-runtime/pkg/client"

	osacv1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/pkg/provisioning"
)

// mockSubnetProvider implements ProvisioningProvider for controller tests.
type mockSubnetProvider struct {
	triggerProvisionFunc                func(ctx context.Context, resource client.Object) (*provisioning.ProvisionResult, error)
	triggerProvisionWithExtraVarsFunc   func(ctx context.Context, resource client.Object, extraVars map[string]any) (*provisioning.ProvisionResult, error)
	getProvisionStatusFunc              func(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatus, error)
	getProvisionStatusWithExtraVarsFunc func(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatusWithExtraVars, error)
	triggerDeprovisionFunc              func(ctx context.Context, resource client.Object, provisionJobs []osacv1alpha1.JobStatus) (*provisioning.DeprovisionResult, error)
	getDeprovisionStatusFunc            func(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatus, error)
}

func (m *mockSubnetProvider) TriggerProvision(ctx context.Context, resource client.Object) (*provisioning.ProvisionResult, error) {
	if m.triggerProvisionFunc != nil {
		return m.triggerProvisionFunc(ctx, resource)
	}
	return &provisioning.ProvisionResult{
		JobID:        "mock-job-id",
		InitialState: osacv1alpha1.JobStatePending,
		Message:      "Provisioning job triggered",
	}, nil
}

func (m *mockSubnetProvider) TriggerProvisionWithExtraVars(ctx context.Context, resource client.Object, extraVars map[string]any) (*provisioning.ProvisionResult, error) {
	if m.triggerProvisionWithExtraVarsFunc != nil {
		return m.triggerProvisionWithExtraVarsFunc(ctx, resource, extraVars)
	}
	return m.TriggerProvision(ctx, resource)
}

func (m *mockSubnetProvider) GetProvisionStatus(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatus, error) {
	if m.getProvisionStatusFunc != nil {
		return m.getProvisionStatusFunc(ctx, resource, jobID)
	}
	return provisioning.ProvisionStatus{
		JobID:   jobID,
		State:   osacv1alpha1.JobStateSucceeded,
		Message: "Job completed successfully",
	}, nil
}

func (m *mockSubnetProvider) GetProvisionStatusWithExtraVars(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatusWithExtraVars, error) {
	if m.getProvisionStatusWithExtraVarsFunc != nil {
		return m.getProvisionStatusWithExtraVarsFunc(ctx, resource, jobID)
	}
	status, err := m.GetProvisionStatus(ctx, resource, jobID)
	return provisioning.ProvisionStatusWithExtraVars{ProvisionStatus: status}, err
}

func (m *mockSubnetProvider) TriggerDeprovision(ctx context.Context, resource client.Object, provisionJobs []osacv1alpha1.JobStatus) (*provisioning.DeprovisionResult, error) {
	if m.triggerDeprovisionFunc != nil {
		return m.triggerDeprovisionFunc(ctx, resource, provisionJobs)
	}
	return &provisioning.DeprovisionResult{
		Action:                 provisioning.DeprovisionTriggered,
		JobID:                  "mock-deprovision-job-id",
		BlockDeletionOnFailure: true,
	}, nil
}

func (m *mockSubnetProvider) GetDeprovisionStatus(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatus, error) {
	if m.getDeprovisionStatusFunc != nil {
		return m.getDeprovisionStatusFunc(ctx, resource, jobID)
	}
	return provisioning.ProvisionStatus{
		JobID:   jobID,
		State:   osacv1alpha1.JobStateSucceeded,
		Message: "Deprovision completed successfully",
	}, nil
}

func (m *mockSubnetProvider) Name() string {
	return "mock-subnet-provider"
}
