/*
Copyright 2026 The KubeEdge Authors.

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

package imitator

import (
	"sync"
	"testing"

	"github.com/beego/beego/v2/client/orm"
	"github.com/golang/mock/gomock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/mocks/beego"
	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	edgemodules "github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	v2 "github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/v2"
)

func newTestImitator() *imitator {
	return &imitator{
		lock:      sync.RWMutex{},
		versioner: Versioner,
		codec:     unstructured.UnstructuredJSONScheme,
	}
}

// deleteMsg builds the message synccontroller's orphaned-sync GC produces: a
// delete whose body carries only name/namespace/uid and no apiVersion or kind.
func deleteMsg(source, resource, body string) *model.Message {
	msg := model.NewMessage("")
	msg.BuildRouter(source, "resource", resource, model.DeleteOperation)
	msg.FillBody([]byte(body))
	return msg
}

// mockRows makes the next RawMetaByNN call return rows.
func mockRows(t *testing.T, rows []v2.MetaV2) *gomock.Controller {
	t.Helper()
	mockCtrl := gomock.NewController(t)
	ormerMock := beego.NewMockOrmer(mockCtrl)
	querySeterMock := beego.NewMockQuerySeter(mockCtrl)
	dbm.DBAccess = ormerMock

	ormerMock.EXPECT().QueryTable(gomock.Any()).Return(orm.QuerySeter(querySeterMock)).Times(1)
	querySeterMock.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(orm.QuerySeter(querySeterMock)).MinTimes(1)
	querySeterMock.EXPECT().All(gomock.Any()).SetArg(0, rows).Return(int64(len(rows)), nil).Times(1)
	return mockCtrl
}

// A GVK-less delete stub must be recovered from the stored row rather than
// dropped, which is what strands the object in meta_v2 forever.
func TestEventRecoversGVKLessDelete(t *testing.T) {
	tests := []struct {
		name           string
		resource       string
		body           string
		storedKey      string
		storedValue    string
		storedRV       uint64
		wantAPIVersion string
		wantKind       string
		wantNamespace  string
		wantName       string
	}{
		{
			name:           "core group pod",
			resource:       "default/pod/ghost-probe",
			body:           `{"metadata":{"name":"ghost-probe","namespace":"default","uid":"6e2572f6-3dcc-400b-a0fd-81c92838e444"}}`,
			storedKey:      "/core/v1/pods/default/ghost-probe",
			storedValue:    `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"ghost-probe","namespace":"default","uid":"6e2572f6-3dcc-400b-a0fd-81c92838e444","resourceVersion":"6812489"},"spec":{"nodeName":"edge-44-200-60-94"}}`,
			storedRV:       6812489,
			wantAPIVersion: "v1",
			wantKind:       "Pod",
			wantNamespace:  "default",
			wantName:       "ghost-probe",
		},
		{
			name:           "crd serviceaccountaccess",
			resource:       "default/serviceaccountaccess/default",
			body:           `{"metadata":{"name":"default","namespace":"default","uid":"d122199c-96d8-473f-94bb-2c0b02375199"}}`,
			storedKey:      "/policy.kubeedge.io/v1alpha1/serviceaccountaccesses/default/default",
			storedValue:    `{"apiVersion":"policy.kubeedge.io/v1alpha1","kind":"ServiceAccountAccess","metadata":{"name":"default","namespace":"default","uid":"d122199c-96d8-473f-94bb-2c0b02375199"}}`,
			storedRV:       6812481,
			wantAPIVersion: "policy.kubeedge.io/v1alpha1",
			wantKind:       "ServiceAccountAccess",
			wantNamespace:  "default",
			wantName:       "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := mockRows(t, []v2.MetaV2{{
				Key:             tt.storedKey,
				Namespace:       tt.wantNamespace,
				Name:            tt.wantName,
				ResourceVersion: tt.storedRV,
				Value:           tt.storedValue,
			}})
			defer mockCtrl.Finish()

			s := newTestImitator()
			events := s.Event(deleteMsg("edgecontroller", tt.resource, tt.body))

			if len(events) != 1 {
				t.Fatalf("got %d events, want 1", len(events))
			}
			if events[0].Type != watch.Deleted {
				t.Errorf("event type = %v, want %v", events[0].Type, watch.Deleted)
			}
			obj, ok := events[0].Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("event object is %T, want *unstructured.Unstructured", events[0].Object)
			}
			if got := obj.GetAPIVersion(); got != tt.wantAPIVersion {
				t.Errorf("apiVersion = %q, want %q", got, tt.wantAPIVersion)
			}
			if got := obj.GetKind(); got != tt.wantKind {
				t.Errorf("kind = %q, want %q", got, tt.wantKind)
			}
			if got := obj.GetName(); got != tt.wantName {
				t.Errorf("name = %q, want %q", got, tt.wantName)
			}
			// the event must carry the last known state, not a skeleton, so that
			// watchers see a normal deletion
			if tt.wantKind == "Pod" {
				nodeName, found, _ := unstructured.NestedString(obj.Object, "spec", "nodeName")
				if !found || nodeName != "edge-44-200-60-94" {
					t.Errorf("recovered object lost the stored spec: %v", obj.Object)
				}
			}
		})
	}
}

// If the recovered GVK does not address exactly one stored row, the recovery
// must decline rather than delete something else.
func TestEventRefusesAmbiguousRecovery(t *testing.T) {
	tests := []struct {
		name string
		rows []v2.MetaV2
	}{
		{
			name: "no matching row",
			rows: []v2.MetaV2{},
		},
		{
			name: "row of a different resource",
			rows: []v2.MetaV2{{
				Key:   "/core/v1/configmaps/default/ghost-probe",
				Name:  "ghost-probe",
				Value: `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"ghost-probe","namespace":"default"}}`,
			}},
		},
		{
			name: "two rows of the same resource",
			rows: []v2.MetaV2{
				{Key: "/core/v1/pods/default/ghost-probe", Name: "ghost-probe"},
				{Key: "/core/v1/pods/other/ghost-probe", Name: "ghost-probe"},
			},
		},
		{
			name: "stored row carries no TypeMeta",
			rows: []v2.MetaV2{{
				Key:   "/core/v1/pods/default/ghost-probe",
				Name:  "ghost-probe",
				Value: `{"metadata":{"name":"ghost-probe","namespace":"default"}}`,
			}},
		},
		{
			name: "stored row belongs to another incarnation",
			rows: []v2.MetaV2{{
				Key:   "/core/v1/pods/default/ghost-probe",
				Name:  "ghost-probe",
				Value: `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"ghost-probe","namespace":"default","uid":"11111111-1111-1111-1111-111111111111"}}`,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := mockRows(t, tt.rows)
			defer mockCtrl.Finish()

			s := newTestImitator()
			events := s.Event(deleteMsg("edgecontroller", "default/pod/ghost-probe",
				`{"metadata":{"name":"ghost-probe","namespace":"default","uid":"6e2572f6-3dcc-400b-a0fd-81c92838e444"}}`))

			if len(events) != 0 {
				t.Errorf("got %d events, want 0", len(events))
			}
		})
	}
}

// Edge-originated bodies never decoded and never belonged in meta_v2; they must
// not reach the store lookup at all.
func TestEventSkipsEdgeOriginatedUndecodableBody(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	// no EXPECT() calls: any DB access here is a failure
	dbm.DBAccess = beego.NewMockOrmer(mockCtrl)

	s := newTestImitator()
	events := s.Event(deleteMsg(edgemodules.EdgedModuleName, "default/node/edge-44-200-60-94",
		`{"metadata":{"name":"edge-44-200-60-94","creationTimestamp":null}}`))

	if len(events) != 0 {
		t.Errorf("got %d events, want 0", len(events))
	}
}

// A body that already carries its GVK must keep working untouched.
func TestEventTypedDeleteNeedsNoRecovery(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	dbm.DBAccess = beego.NewMockOrmer(mockCtrl)

	s := newTestImitator()
	events := s.Event(deleteMsg("edgecontroller", "default/pod/ghost-probe",
		`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"ghost-probe","namespace":"default"}}`))

	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	obj := events[0].Object.(*unstructured.Unstructured)
	if got := obj.GetKind(); got != "Pod" {
		t.Errorf("kind = %q, want Pod", got)
	}
}
