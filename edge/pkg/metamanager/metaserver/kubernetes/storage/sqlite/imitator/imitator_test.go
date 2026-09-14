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

	"github.com/golang/mock/gomock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/mocks/beego"
	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	edgemodules "github.com/kubeedge/kubeedge/edge/pkg/common/modules"
)

func newTestImitator() *imitator {
	return &imitator{
		lock:      sync.RWMutex{},
		versioner: Versioner,
		codec:     unstructured.UnstructuredJSONScheme,
	}
}

func deleteMsg(source, resource, body string) *model.Message {
	msg := model.NewMessage("")
	msg.BuildRouter(source, "resource", resource, model.DeleteOperation)
	msg.FillBody([]byte(body))
	return msg
}

// Edge-originated bodies never decoded and never belonged in meta_v2; they must
// not reach the store at all.
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
