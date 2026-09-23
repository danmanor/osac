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
	"fmt"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/osac-project/osac/fulfillment-service/internal/auth"
	"github.com/osac-project/osac/fulfillment-service/internal/database/dao"
	"github.com/osac-project/osac/fulfillment-service/internal/events"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

const defaultLabel = "osac.openshift.io/default"
const ownerReferenceAnnotation = "osac.openshift.io/owner-reference"

// makeNotifyCallback returns the event callback shared by server-side
// lifecycle helpers. It is kept with the server infrastructure rather than
// with default networking because external-IP lifecycle events use it too.
func makeNotifyCallback[O dao.Object](notifier events.Notifier) dao.EventCallback {
	var zero O
	objDesc := zero.ProtoReflect().Descriptor()
	eventDesc := (&privatev1.Event{}).ProtoReflect().Descriptor()
	var payloadField protoreflect.FieldDescriptor
	fields := eventDesc.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if fd.Kind() == protoreflect.MessageKind && fd.Message().FullName() == objDesc.FullName() {
			payloadField = fd
			break
		}
	}
	return func(ctx context.Context, e dao.Event) error {
		var eventType privatev1.EventType
		switch e.Type {
		case dao.EventTypeCreated:
			eventType = privatev1.EventType_EVENT_TYPE_OBJECT_CREATED
		case dao.EventTypeUpdated:
			eventType = privatev1.EventType_EVENT_TYPE_OBJECT_UPDATED
		case dao.EventTypeDeleted:
			eventType = privatev1.EventType_EVENT_TYPE_OBJECT_DELETED
		default:
			return fmt.Errorf("unknown event type '%s'", e.Type)
		}
		event := newEvent(eventType)
		if payloadField != nil {
			event.ProtoReflect().Set(payloadField, protoreflect.ValueOfMessage(e.Object.ProtoReflect()))
		}
		return notifier.Notify(ctx, event)
	}
}

// validateNotDefault prevents direct deletion of system-managed networking.
// Only the authenticated fulfillment controller may delete these objects as
// part of root-project cleanup.
func validateNotDefault(ctx context.Context, labels map[string]string, resourceType string) error {
	if labels[defaultLabel] == "true" && !auth.IsControllerServiceAccount(ctx) {
		return grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"cannot delete default %s: default networking resources are system-managed", resourceType)
	}
	return nil
}
