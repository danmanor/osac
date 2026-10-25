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

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

// AcceptSegment records the network segment returned by the Fabric Manager.
// The Fabric Manager owns VNI allocation and uniqueness; OSAC validates only
// the result shape before passing it to the Kubernetes Manager.
func AcceptSegment(_ context.Context, subnet *v1alpha1.Subnet, networkType string, result Result) error {
	segment, err := ParseNetworkSegment(result, networkType)
	if err != nil || segment == nil {
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
// slot through a Running task.
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
			return true
		}
	}
	return false
}
