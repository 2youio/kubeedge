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

package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/beego/beego/v2/client/orm"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "stored-test")
	if err != nil {
		panic(err)
	}
	orm.RegisterModel(new(dao.Meta))
	dbm.InitDBConfig("sqlite3", "default", filepath.Join(dir, "edgecore.db"))
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func testPod() *unstructured.Unstructured {
	pod := &unstructured.Unstructured{}
	pod.SetAPIVersion("v1")
	pod.SetKind("Pod")
	pod.SetNamespace("default")
	pod.SetName("web-0")
	pod.SetLabels(map[string]string{"app": "web"})
	_ = unstructured.SetNestedField(pod.Object, "Pending", "status", "phase")
	return pod
}

// The patch lands in the object the caller passed, and the fields it does not touch stay.
func TestMergePatchedResource(t *testing.T) {
	tests := []struct {
		name      string
		patch     string
		wantPhase string
	}{
		{"podpatch stored", `{"status":{"phase":"Running"}}`, "Running"},
		{"no podpatch", "", "Pending"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.patch != "" {
				patch := &dao.Meta{Key: "default/podpatch/web-0", Type: "podpatch", Value: test.patch}
				if err := dao.InsertOrUpdate(patch); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = dao.DeleteMetaByKey(patch.Key) }()
			}
			pod := testPod()
			if err := MergePatchedResource(pod, model.ResourceTypePodPatch); err != nil {
				t.Fatal(err)
			}
			phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase")
			if phase != test.wantPhase {
				t.Errorf("status.phase = %q, want %q", phase, test.wantPhase)
			}
			if pod.GetLabels()["app"] != "web" || pod.GetName() != "web-0" {
				t.Errorf("merge lost fields outside the patch: labels %v, name %q", pod.GetLabels(), pod.GetName())
			}
		})
	}
}
