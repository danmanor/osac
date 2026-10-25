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

package networktask

import (
	"context"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

// Segment rejection messages (contract §4.9). They name only the broken rule.
const (
	SegmentVNIInUse         = "VNI already in use"
	SegmentL3InUse          = "L3 VNI already in use by another network"
	SegmentL3DiffersSibling = "L3 VNI differs from the other Subnets of this VirtualNetwork"
)

// segmentFreed reports whether a Subnet's segment no longer counts: its
// Fabric delete_subnet succeeded.
func segmentFreed(s *v1alpha1.Subnet) bool {
	return Succeeded(s, DeleteSubnet, v1alpha1.NetworkJobManagerFabric)
}

// CheckSegment checks a segment returned for subnet against the other Subnets
// (contract §4.9). It returns a RejectError for a conflict, ErrWaitForDelete
// when every conflict is with a Subnet whose Fabric delete_subnet started and
// has not succeeded, or nil to accept. The details of a conflict go only to
// the operator log.
func CheckSegment(ctx context.Context, subnet *v1alpha1.Subnet, segment *v1alpha1.NetworkSegment, others []v1alpha1.Subnet) error {
	log := ctrllog.FromContext(ctx)
	if subnet.Status.NetworkSegment != nil {
		if *subnet.Status.NetworkSegment == *segment {
			return nil
		}
		return Reject("network segment differs from the segment already accepted")
	}
	waiting := false
	for i := range others {
		other := &others[i]
		if other.UID == subnet.UID {
			continue
		}
		rule := segmentConflictRule(subnet, segment, other)
		if rule == "" {
			continue
		}
		otherSegment := other.Status.NetworkSegment
		if Started(other, DeleteSubnet, v1alpha1.NetworkJobManagerFabric) {
			log.Info("Network segment conflicts with a Subnet being deleted; waiting",
				"subnet", subnet.Name, "other", other.Name, "l2Vni", segment.L2VNI, "l3Vni", segment.L3VNI,
				"otherL2Vni", otherSegment.L2VNI, "otherL3Vni", otherSegment.L3VNI)
			waiting = true
			continue
		}
		log.Info("Rejecting network segment", "subnet", subnet.Name, "rule", rule, "other", other.Name,
			"l2Vni", segment.L2VNI, "l3Vni", segment.L3VNI,
			"otherL2Vni", otherSegment.L2VNI, "otherL3Vni", otherSegment.L3VNI)
		return Reject("%s", rule)
	}
	if waiting {
		return ErrWaitForDelete
	}
	return nil
}

func segmentConflictRule(subnet *v1alpha1.Subnet, segment *v1alpha1.NetworkSegment, other *v1alpha1.Subnet) string {
	if other.Status.NetworkSegment == nil || segmentFreed(other) {
		return ""
	}
	otherSegment := other.Status.NetworkSegment
	sibling := other.Spec.VirtualNetwork == subnet.Spec.VirtualNetwork
	switch {
	case segment.L2VNI == otherSegment.L2VNI || segment.L2VNI == otherSegment.L3VNI:
		return SegmentVNIInUse
	case segment.L3VNI == otherSegment.L2VNI:
		return SegmentL3InUse
	case !sibling && segment.L3VNI == otherSegment.L3VNI:
		return SegmentL3InUse
	case sibling && segment.L3VNI != otherSegment.L3VNI:
		return SegmentL3DiffersSibling
	default:
		return ""
	}
}

// AcceptSegment is the AcceptFunc helper of the Fabric create_subnet: it
// parses the segment, reserves its VNIs through the shared claim index, and
// records status.networkSegment. Claims survive a failed status write.
func AcceptSegment(ctx context.Context, index *SegmentClaimIndex, subnet *v1alpha1.Subnet, networkType string, result Result) error {
	segment, err := ParseNetworkSegment(result, networkType)
	if err != nil || segment == nil {
		return err
	}
	if err := index.Acquire(ctx, subnet, segment); err != nil {
		return err
	}
	subnet.Status.NetworkSegment = segment
	return nil
}

// keyedMutex retains a key only while it has a holder or waiters.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedMutexEntry
}

type keyedMutexEntry struct {
	mu   sync.Mutex
	refs int
}

func (k *keyedMutex) acquire(key string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*keyedMutexEntry{}
	}
	m, ok := k.locks[key]
	if !ok {
		m = &keyedMutexEntry{}
		k.locks[key] = m
	}
	// Count the reference before waiting, so unlock cannot remove a key that
	// another goroutine is about to acquire.
	m.refs++
	k.mu.Unlock()
	m.mu.Lock()
	return func() {
		m.mu.Unlock()
		k.mu.Lock()
		defer k.mu.Unlock()
		m.refs--
		if m.refs == 0 {
			delete(k.locks, key)
		}
	}
}

// SubnetSlot serializes Subnet tasks per VirtualNetwork (contract §4.7
// Concurrency). Set it on a Subnet task's Request: before the launch the
// engine takes the VirtualNetwork's lock, lists the Subnets from the API
// server with its namespace and VirtualNetwork label. The Running status
// reserves the slot after its conditional write releases the lock, before AAP
// calls. The same key serializes acceptance through its resulting status write.
type SubnetSlot struct {
	// VirtualNetworkID is the API ID of the Subnet's VirtualNetwork.
	VirtualNetworkID string
}

// slotHeld reports whether another Subnet of the VirtualNetwork holds the
// slot: it has a Running entry, also with an empty jobID, except a
// create_subnet entry that waits for a deleted Subnet's segment.
func slotHeld(subnet client.Object, others []v1alpha1.Subnet, vnID string) bool {
	for i := range others {
		other := &others[i]
		if other.UID == subnet.GetUID() || other.Spec.VirtualNetwork != vnID {
			continue
		}
		for _, job := range other.Status.Jobs {
			if job.State != v1alpha1.NetworkJobStateRunning {
				continue
			}
			if job.Task == CreateSubnet && job.Message == ErrWaitForDelete.Error() {
				continue
			}
			return true
		}
	}
	return false
}
