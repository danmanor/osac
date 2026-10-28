/*
Copyright (c) 2026 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package controller

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
	clnt "sigs.k8s.io/controller-runtime/pkg/client"
)

// stateTransitionTimeOf returns the time a status change happened. A CR deleted before any
// controller status write carries no transition time; the deletion started at the deletion
// timestamp, so that is then the time of the change.
func stateTransitionTimeOf(at, deleted *metav1.Time) *timestamppb.Timestamp {
	if at == nil {
		at = deleted
	}
	if at == nil {
		return nil
	}
	return timestamppb.New(at.Time)
}

// laterTransitionTimeOf returns the later of the two times.
func laterTransitionTimeOf(a, b *metav1.Time) *timestamppb.Timestamp {
	at := stateTransitionTimeOf(a, b)
	if a != nil && b != nil && b.Time.After(a.Time) {
		at = timestamppb.New(b.Time)
	}
	return at
}

// backfillAttachmentStateTransitionTime stamps a first-observed time on
// attachments that settled before StateTransitionTime existed (upgrade path),
// so settlement does not reject their feedback. The stamp is persisted once,
// subsequent reocnciles observe a non nil time and skip
func backfillAttachmentStateTransitionTime(ctx context.Context, hubClient clnt.Client, obj *v1alpha1.ExternalIPAttachment) error {
	if obj.Status.StateTransitionTime != nil {
		return nil
	}
	now := metav1.Now()
	obj.Status.StateTransitionTime = &now
	return hubClient.Status().Update(ctx, obj)
}
