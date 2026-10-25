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
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

type autoExternalIPOwnerKind int

const (
	clusterOrderOwner autoExternalIPOwnerKind = iota
	computeInstanceOwner
	bareMetalInstanceOwner
)

type autoExternalIPOwner struct {
	kind autoExternalIPOwnerKind
	id   string
}

const (
	autoCreatedLabel                        = osacPrefix + "/auto-created"
	autoCreatedForLabel                     = osacPrefix + "/auto-created-for"
	autoExternalIPAttachmentOwnerIndexField = "networking.osac.openshift.io/auto-external-ip-owner"
)

// RegisterAutoExternalIPAttachmentOwnerIndex indexes attachments by their target owner.
// Call before the manager cache starts; cleanup reconciliations use this index to avoid
// listing every automatically managed attachment in the networking namespace.
func RegisterAutoExternalIPAttachmentOwnerIndex(indexer client.FieldIndexer) error {
	return indexer.IndexField(context.Background(), &v1alpha1.ExternalIPAttachment{}, autoExternalIPAttachmentOwnerIndexField,
		func(obj client.Object) []string {
			attachment, ok := obj.(*v1alpha1.ExternalIPAttachment)
			if !ok {
				return nil
			}
			return autoExternalIPAttachmentOwnerIndexValues(attachment)
		})
}

func autoExternalIPAttachmentOwnerIndexValues(attachment *v1alpha1.ExternalIPAttachment) []string {
	if attachment == nil {
		return nil
	}
	values := make([]string, 0, 3)
	if attachment.Spec.Cluster != nil && *attachment.Spec.Cluster != "" {
		values = append(values, autoExternalIPOwnerIndexKey(autoExternalIPOwner{kind: clusterOrderOwner, id: *attachment.Spec.Cluster}))
	}
	if attachment.Spec.ComputeInstance != nil && *attachment.Spec.ComputeInstance != "" {
		values = append(values, autoExternalIPOwnerIndexKey(autoExternalIPOwner{kind: computeInstanceOwner, id: *attachment.Spec.ComputeInstance}))
	}
	if attachment.Spec.BaremetalInstance != nil && *attachment.Spec.BaremetalInstance != "" {
		values = append(values, autoExternalIPOwnerIndexKey(autoExternalIPOwner{kind: bareMetalInstanceOwner, id: *attachment.Spec.BaremetalInstance}))
	}
	return values
}

func autoExternalIPOwnerIndexKey(owner autoExternalIPOwner) string {
	return fmt.Sprintf("%d/%s", owner.kind, owner.id)
}

func (owner autoExternalIPOwner) matchesAttachment(attachment *v1alpha1.ExternalIPAttachment) bool {
	if attachment == nil || owner.id == "" {
		return false
	}

	switch owner.kind {
	case clusterOrderOwner:
		return attachment.Spec.Cluster != nil && *attachment.Spec.Cluster == owner.id
	case computeInstanceOwner:
		return attachment.Spec.ComputeInstance != nil && *attachment.Spec.ComputeInstance == owner.id
	case bareMetalInstanceOwner:
		return attachment.Spec.BaremetalInstance != nil && *attachment.Spec.BaremetalInstance == owner.id
	default:
		return false
	}
}

// automaticExternalAccessRemains reports whether an automatic
// ExternalIPAttachment or ExternalIP CR of the workload still exists (Unified
// design, Automatic external access). The workload's cleanup finalizer holds
// the workload CR until none remains; only the fulfillment service deletes
// them, so this deletes nothing.
func automaticExternalAccessRemains(
	ctx context.Context,
	c client.Client,
	networkingNamespace string,
	owner autoExternalIPOwner,
) (bool, error) {
	if networkingNamespace == "" || owner.id == "" {
		return false, nil
	}
	attachments, err := listAutoExternalIPAttachments(ctx, c, networkingNamespace, owner)
	if err != nil {
		return false, err
	}
	if len(attachments) > 0 {
		return true, nil
	}
	externalIPs, err := listAutoExternalIPs(ctx, c, networkingNamespace, owner)
	if err != nil {
		return false, err
	}
	return len(externalIPs) > 0, nil
}

func listAutoExternalIPAttachments(
	ctx context.Context,
	c client.Client,
	namespace string,
	owner autoExternalIPOwner,
) ([]v1alpha1.ExternalIPAttachment, error) {
	list := &v1alpha1.ExternalIPAttachmentList{}
	if err := c.List(ctx, list,
		client.InNamespace(namespace),
		client.MatchingFields{autoExternalIPAttachmentOwnerIndexField: autoExternalIPOwnerIndexKey(owner)},
	); err != nil {
		return nil, fmt.Errorf("list ExternalIPAttachments for owner %q: %w", owner.id, err)
	}

	attachments := make([]v1alpha1.ExternalIPAttachment, 0, len(list.Items))
	for i := range list.Items {
		attachment := &list.Items[i]
		if !owner.matchesAttachment(attachment) || attachment.Labels[autoCreatedLabel] != labelValueTrue {
			continue
		}
		attachments = append(attachments, *attachment.DeepCopy())
	}
	return attachments, nil
}

func listAutoExternalIPs(
	ctx context.Context,
	c client.Client,
	namespace string,
	owner autoExternalIPOwner,
) ([]v1alpha1.ExternalIP, error) {
	list := &v1alpha1.ExternalIPList{}
	if err := c.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		autoCreatedLabel:    labelValueTrue,
		autoCreatedForLabel: owner.id,
	}); err != nil {
		return nil, fmt.Errorf("list automatically created ExternalIPs: %w", err)
	}
	return list.Items, nil
}
