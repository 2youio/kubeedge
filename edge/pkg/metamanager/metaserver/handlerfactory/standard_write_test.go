/*
Copyright 2025 The KubeEdge Authors.

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

package handlerfactory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	kubescheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/kubernetes/storage"
)

// What client-go sends for a built-in kind when it is configured for protobuf.
const clientGoProtobufAccept = "application/vnd.kubernetes.protobuf, application/json"

// fakeStorage replaces the storage's Create and Update and records the object
// each one received, which is what MetaServer sends to the cloud.
type fakeStorage struct {
	patches *gomonkey.Patches
	got     runtime.Object
	calls   int
	err     error
}

func newFakeStorage(t *testing.T) *fakeStorage {
	fs := &fakeStorage{patches: gomonkey.NewPatches()}
	t.Cleanup(fs.patches.Reset)

	fs.patches.ApplyFunc(storage.NewREST, func() (*storage.REST, error) {
		return &storage.REST{Store: &genericregistry.Store{
			NewFunc: func() runtime.Object { return &unstructured.Unstructured{} },
		}}, nil
	})
	fs.patches.ApplyMethod((*storage.REST)(nil), "Create",
		func(_ *storage.REST, _ context.Context, obj runtime.Object, _ rest.ValidateObjectFunc, _ *metav1.CreateOptions) (runtime.Object, error) {
			fs.calls++
			fs.got = obj
			if fs.err != nil {
				return nil, fs.err
			}
			return obj, nil
		})
	fs.patches.ApplyMethod((*storage.REST)(nil), "Update",
		func(_ *storage.REST, ctx context.Context, _ string, objInfo rest.UpdatedObjectInfo, _ rest.ValidateObjectFunc, _ rest.ValidateObjectUpdateFunc, _ bool, _ *metav1.UpdateOptions) (runtime.Object, bool, error) {
			fs.calls++
			obj, err := objInfo.UpdatedObject(ctx, nil)
			if err != nil {
				return nil, false, err
			}
			fs.got = obj
			if fs.err != nil {
				return nil, false, fs.err
			}
			return obj, false, nil
		})
	return fs
}

// gotContent returns the object the storage received as a map, as it goes to the cloud.
func (fs *fakeStorage) gotContent(t *testing.T) map[string]interface{} {
	t.Helper()
	require.Equal(t, 1, fs.calls, "storage was not called exactly once")
	u, ok := fs.got.(*unstructured.Unstructured)
	require.True(t, ok, "storage got %T, want *unstructured.Unstructured", fs.got)
	return u.UnstructuredContent()
}

// protobufBody encodes obj as client-go does, with its apiVersion and kind in the envelope.
func protobufBody(t *testing.T, obj runtime.Object) []byte {
	t.Helper()
	gvks, _, err := kubescheme.Scheme.ObjectKinds(obj)
	require.NoError(t, err)
	obj.GetObjectKind().SetGroupVersionKind(gvks[0])
	body := &bytes.Buffer{}
	require.NoError(t, protobuf.NewSerializer(kubescheme.Scheme, kubescheme.Scheme).Encode(obj, body))
	return body.Bytes()
}

func serve(h http.Handler, method, path string, info *request.RequestInfo, contentType string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", clientGoProtobufAccept)
	req = req.WithContext(request.WithRequestInfo(req.Context(), info))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func nodeUpdateInfo(name string) *request.RequestInfo {
	return &request.RequestInfo{IsResourceRequest: true, Verb: "update", APIVersion: "v1", Resource: "nodes", Name: name}
}

func eventCreateInfo() *request.RequestInfo {
	return &request.RequestInfo{IsResourceRequest: true, Verb: "create", APIVersion: "v1", Resource: "events", Namespace: "default"}
}

func calicoCreateInfo(resource string) *request.RequestInfo {
	return &request.RequestInfo{IsResourceRequest: true, Verb: "create", APIGroup: "crd.projectcalico.org", APIVersion: "v1", Resource: resource}
}

// The case this change exists for: calico-node PUTs its v1.Node as protobuf.
// The cloud gets it as an object with apiVersion and kind, and the answer is JSON.
func TestUpdateProtobufNode(t *testing.T) {
	fs := newFakeStorage(t)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "edge-34-237-223-165",
			Annotations: map[string]string{"projectcalico.org/IPv4Address": "10.100.120.155/24"},
		},
		Spec: corev1.NodeSpec{PodCIDR: "10.4.3.0/24"},
	}

	w := serve(NewFactory().Update(nodeUpdateInfo(node.Name)), http.MethodPut, "/api/v1/nodes/"+node.Name,
		nodeUpdateInfo(node.Name), runtime.ContentTypeProtobuf, protobufBody(t, node))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, runtime.ContentTypeJSON, w.Header().Get("Content-Type"))
	got := fs.gotContent(t)
	assert.Equal(t, "v1", got["apiVersion"])
	assert.Equal(t, "Node", got["kind"])
	podCIDR, _, _ := unstructured.NestedString(got, "spec", "podCIDR")
	assert.Equal(t, "10.4.3.0/24", podCIDR)
	address, _, _ := unstructured.NestedString(got, "metadata", "annotations", "projectcalico.org/IPv4Address")
	assert.Equal(t, "10.100.120.155/24", address)
}

// kube-proxy posts its startup Event as protobuf.
func TestCreateProtobufEvent(t *testing.T) {
	fs := newFakeStorage(t)
	event := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "kube-proxy.1", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "edge-1"},
		Reason:         "Starting",
	}

	w := serve(NewFactory().Create(eventCreateInfo()), http.MethodPost, "/api/v1/namespaces/default/events",
		eventCreateInfo(), runtime.ContentTypeProtobuf, protobufBody(t, event))

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	got := fs.gotContent(t)
	assert.Equal(t, "Event", got["kind"])
	assert.Equal(t, "Starting", got["reason"])
}

// A JSON body is not decoded into edgecore's types, so a field those types do
// not have, such as one added by a newer Kubernetes, still reaches the cloud.
func TestCreateJSONKeepsFieldsEdgecoreDoesNotKnow(t *testing.T) {
	fs := newFakeStorage(t)
	body := `{"apiVersion":"v1","kind":"Event","metadata":{"name":"e","namespace":"default"},"reason":"Starting","fieldFromANewerRelease":"kept"}`

	w := serve(NewFactory().Create(eventCreateInfo()), http.MethodPost, "/api/v1/namespaces/default/events",
		eventCreateInfo(), runtime.ContentTypeJSON, []byte(body))

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	got := fs.gotContent(t)
	assert.Equal(t, "kept", got["fieldFromANewerRelease"])
	assert.NotContains(t, got, "source", "JSON bodies must not gain the zero-value fields of a typed object")
}

// Calico's own writes are custom resources, which are never protobuf. Their
// JSON bodies reach the cloud as they were sent.
func TestCreateJSONCustomResource(t *testing.T) {
	fs := newFakeStorage(t)
	const block = `{"apiVersion":"crd.projectcalico.org/v1","kind":"IPAMBlock",` +
		`"metadata":{"name":"10-4-4-0-26"},"spec":{"cidr":"10.4.4.0/26","affinity":"host:edge-52-21-33-192"}}`

	w := serve(NewFactory().Create(calicoCreateInfo("ipamblocks")), http.MethodPost, "/apis/crd.projectcalico.org/v1/ipamblocks",
		calicoCreateInfo("ipamblocks"), runtime.ContentTypeJSON, []byte(block))

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	got := fs.gotContent(t)
	spec, _, _ := unstructured.NestedStringMap(got, "spec")
	assert.Equal(t, map[string]string{"cidr": "10.4.4.0/26", "affinity": "host:edge-52-21-33-192"}, spec)
}

func TestCreateYAMLCustomResource(t *testing.T) {
	fs := newFakeStorage(t)
	const pool = "apiVersion: crd.projectcalico.org/v1\nkind: IPPool\nmetadata:\n  name: default-ipv4\nspec:\n  cidr: 10.4.0.0/16\n"

	w := serve(NewFactory().Create(calicoCreateInfo("ippools")), http.MethodPost, "/apis/crd.projectcalico.org/v1/ippools",
		calicoCreateInfo("ippools"), "application/yaml", []byte(pool))

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	got := fs.gotContent(t)
	assert.Equal(t, "IPPool", got["kind"])
	cidr, _, _ := unstructured.NestedString(got, "spec", "cidr")
	assert.Equal(t, "10.4.0.0/16", cidr)
}

// The cloud updates the object named in the body, so a body that names another
// object than the URL must be refused here.
func TestUpdateRefusesNameMismatch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		contentType string
		body        []byte
	}{
		{"json", runtime.ContentTypeJSON, []byte(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-b"}}`)},
		{"protobuf", runtime.ContentTypeProtobuf, protobufBody(t, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeStorage(t)
			w := serve(NewFactory().Update(nodeUpdateInfo("node-a")), http.MethodPut, "/api/v1/nodes/node-a",
				nodeUpdateInfo("node-a"), tc.contentType, tc.body)

			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Equal(t, 0, fs.calls)
		})
	}
}

func TestCreateRefusesBadBodies(t *testing.T) {
	unknownKind := &runtime.Unknown{
		TypeMeta: runtime.TypeMeta{APIVersion: "example.com/v1", Kind: "Widget"},
		Raw:      []byte{},
	}
	unknownKindBody := &bytes.Buffer{}
	require.NoError(t, protobuf.NewSerializer(kubescheme.Scheme, kubescheme.Scheme).Encode(unknownKind, unknownKindBody))

	for _, tc := range []struct {
		name        string
		contentType string
		body        []byte
		wantCode    int
	}{
		{"protobuf of a kind that is not built in", runtime.ContentTypeProtobuf, unknownKindBody.Bytes(), http.StatusUnsupportedMediaType},
		{"protobuf content type, not protobuf", runtime.ContentTypeProtobuf, []byte(`{"kind":"Event"}`), http.StatusBadRequest},
		{"truncated protobuf", runtime.ContentTypeProtobuf, []byte("k8s\x00\x0a\x05"), http.StatusBadRequest},
		{"malformed JSON", runtime.ContentTypeJSON, []byte(`{"kind":`), http.StatusBadRequest},
		{"JSON without kind", runtime.ContentTypeJSON, []byte(`{"metadata":{"name":"e"}}`), http.StatusBadRequest},
		{"unsupported content type", "text/plain", []byte("hello"), http.StatusUnsupportedMediaType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeStorage(t)
			w := serve(NewFactory().Create(eventCreateInfo()), http.MethodPost, "/api/v1/namespaces/default/events",
				eventCreateInfo(), tc.contentType, tc.body)

			assert.Equal(t, tc.wantCode, w.Code, w.Body.String())
			assert.Equal(t, 0, fs.calls)
		})
	}
}

func TestCreateRefusesTooLargeProtobufBody(t *testing.T) {
	fs := newFakeStorage(t)
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "default"},
		Message:    strings.Repeat("x", 4*1024*1024),
	}

	w := serve(NewFactory().Create(eventCreateInfo()), http.MethodPost, "/api/v1/namespaces/default/events",
		eventCreateInfo(), runtime.ContentTypeProtobuf, protobufBody(t, event))

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	assert.Equal(t, 0, fs.calls)
}

func TestWriteStorageError(t *testing.T) {
	fs := newFakeStorage(t)
	fs.err = errors.New("cloud unreachable")

	w := serve(NewFactory().Create(eventCreateInfo()), http.MethodPost, "/api/v1/namespaces/default/events",
		eventCreateInfo(), runtime.ContentTypeProtobuf, protobufBody(t, &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "e"}}))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "cloud unreachable")

	w = serve(NewFactory().Update(nodeUpdateInfo("n")), http.MethodPut, "/api/v1/nodes/n",
		nodeUpdateInfo("n"), runtime.ContentTypeProtobuf, protobufBody(t, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}}))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "cloud unreachable")
}

// Kinds of every built-in group decode, not only the core group's.
func TestProtobufBodyToJSONKnowsAllBuiltInGroups(t *testing.T) {
	for _, obj := range []runtime.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}},
		&policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}},
		&authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{Audiences: []string{"a"}}},
	} {
		gvks, _, err := kubescheme.Scheme.ObjectKinds(obj)
		require.NoError(t, err)
		want := gvks[0]

		out, err := protobufBodyToJSON(protobufBody(t, obj))
		require.NoError(t, err, want.String())

		var m map[string]interface{}
		require.NoError(t, json.Unmarshal(out, &m))
		assert.Equal(t, want.GroupVersion().String(), m["apiVersion"])
		assert.Equal(t, want.Kind, m["kind"])
	}
}
