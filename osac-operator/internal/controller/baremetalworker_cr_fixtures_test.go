// Copyright (c) 2026 Red Hat Inc.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

// createWorkerOrder creates a bare-metal ClusterOrder with the worker creation
// gate open: the HostedCluster exists and ManagementPublicationReady is True.
func createWorkerOrder(ctx context.Context, co *api.ClusterOrder) error {
	if err := k8sClient.Create(ctx, co); err != nil {
		return err
	}
	co.SetStatusCondition(api.ConditionControlPlaneCreated, metav1.ConditionTrue, "", api.ReasonAsExpected)
	co.SetStatusCondition(string(api.ClusterOrderConditionManagementPublicationReady), metav1.ConditionTrue, "", api.ReasonAsExpected)
	// The gate opened long before any worker attempt, so it never moves a
	// registration deadline in these fixtures.
	for i := range co.Status.Conditions {
		co.Status.Conditions[i].LastTransitionTime = metav1.NewTime(time.Now().Add(-24 * time.Hour))
	}
	return k8sClient.Status().Update(ctx, co)
}

// These fixtures write only the external evidence consumed by the worker.
// They do not run Assisted Service, HyperShift or CAP-Agent controllers. The
// shared controller envtest suite supplies the real API server and etcd.
type workerAgentFixture struct {
	Name                  string
	Namespace             string
	MAC                   string
	ClusterDeploymentName string
}

func workerFixtureObject(gvk schema.GroupVersionKind, name, namespace string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetName(name)
	u.SetNamespace(namespace)
	return u
}

func setWorkerIgnitionURL(ctx context.Context, name, namespace, url string) error {
	u := workerFixtureObject(infraEnvGVK, name, namespace)
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(u), u); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(u.Object, url, "status", "bootArtifacts", "discoveryIgnitionURL"); err != nil {
		return err
	}
	return k8sClient.Update(ctx, u)
}

func createWorkerAgent(ctx context.Context, fixture workerAgentFixture) error {
	agent := workerFixtureObject(agentGVK, fixture.Name, fixture.Namespace)
	spec := map[string]interface{}{"approved": true}
	if fixture.ClusterDeploymentName != "" {
		spec["clusterDeploymentName"] = map[string]interface{}{
			"name": fixture.ClusterDeploymentName, "namespace": fixture.Namespace,
		}
	}
	if err := unstructured.SetNestedMap(agent.Object, spec, "spec"); err != nil {
		return fmt.Errorf("setting Agent spec: %w", err)
	}
	nics := []interface{}{map[string]interface{}{"macAddress": fixture.MAC}}
	if err := unstructured.SetNestedSlice(agent.Object, nics, "status", "inventory", "interfaces"); err != nil {
		return fmt.Errorf("setting Agent inventory: %w", err)
	}
	return k8sClient.Create(ctx, agent)
}

func clearWorkerAgentBinding(ctx context.Context, name, namespace string) error {
	agent := workerFixtureObject(agentGVK, name, namespace)
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), agent); err != nil {
		return err
	}
	unstructured.RemoveNestedField(agent.Object, "spec", "clusterDeploymentName")
	if err := unstructured.SetNestedField(agent.Object, "unbinding-pending-user-action", "status", "debugInfo", "state"); err != nil {
		return err
	}
	return k8sClient.Update(ctx, agent)
}

func ensureWorkerClusterDeployment(ctx context.Context, name, namespace string) error {
	gvk := schema.GroupVersionKind{Group: "hive.openshift.io", Version: "v1", Kind: "ClusterDeployment"}
	cd := workerFixtureObject(gvk, name, namespace)
	err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cd), cd)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	if err := unstructured.SetNestedField(cd.Object, false, "spec", "installed"); err != nil {
		return err
	}
	return k8sClient.Create(ctx, cd)
}
