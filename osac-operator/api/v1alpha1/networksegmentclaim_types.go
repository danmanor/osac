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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// SegmentClaimOwner identifies a Subnet incarnation, including its namespace.
type SegmentClaimOwner struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
}

// NetworkSegmentClaimSpec is operator-owned deployment coordination state.
type NetworkSegmentClaimSpec struct {
	// VNI is the numeric segment identifier, shared across both roles.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=16777215
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="vni is immutable"
	VNI int32 `json:"vni"`
	// L2Owner exclusively reserves the L2 role.
	// +optional
	L2Owner *SegmentClaimOwner `json:"l2Owner,omitempty"`
	// L3VirtualNetwork identifies the network whose Subnets share the L3 role.
	// +optional
	L3VirtualNetwork string `json:"l3VirtualNetwork,omitempty"`
	// L3Owners records each sharing Subnet incarnation.
	// +optional
	// +listType=map
	// +listMapKey=uid
	L3Owners []SegmentClaimOwner `json:"l3Owners,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced

// NetworkSegmentClaim is internal operator state; it is not a tenant resource.
type NetworkSegmentClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NetworkSegmentClaimSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// NetworkSegmentClaimList contains NetworkSegmentClaim objects.
type NetworkSegmentClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NetworkSegmentClaim `json:"items"`
}
