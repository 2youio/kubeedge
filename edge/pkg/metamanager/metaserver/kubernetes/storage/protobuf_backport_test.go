package storage

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/watch"
	kubescheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/kubernetes/serializer"
)

// encodeWith serializes obj the way metaserver's response path does, for one media type.
func encodeWith(t *testing.T, mediaType string, obj runtime.Object, gvk schema.GroupVersionKind) ([]byte, error) {
	t.Helper()
	ns := serializer.NewNegotiatedSerializer()
	for _, info := range ns.SupportedMediaTypes() {
		if info.MediaType != mediaType {
			continue
		}
		buf := &bytes.Buffer{}
		err := ns.EncoderForVersion(info.Serializer, serializer.NewWithKindGroupVersioner(gvk)).Encode(obj, buf)
		return buf.Bytes(), err
	}
	t.Fatalf("no serializer registered for media type %q", mediaType)
	return nil, nil
}

// The case this backport exists for: calico-node PUTs a v1.Node with
// Content-Type: application/vnd.kubernetes.protobuf. Before protobuf support the
// request was rejected outright. It has to decode, and the body forwarded to
// cloudcore must keep its TypeMeta -- cloudcore feeds it to a dynamic client,
// which rejects an object with no apiVersion/kind.
func TestProtobufNodeUpdateReachesCloudWithTypeMeta(t *testing.T) {
	node := &corev1.Node{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "edge-34-237-223-165",
			Annotations: map[string]string{"projectcalico.org/IPv4Address": "10.100.120.155/24"},
		},
		Spec: corev1.NodeSpec{PodCIDR: "10.4.3.0/24"},
	}
	body := &bytes.Buffer{}
	require.NoError(t, protobuf.NewSerializer(kubescheme.Scheme, kubescheme.Scheme).Encode(node, body))

	obj, err := DecodeAndConvert(body.Bytes(), "")
	require.NoError(t, err)

	got, ok := obj.(*corev1.Node)
	require.True(t, ok, "expected *corev1.Node, got %T", obj)
	assert.Equal(t, "edge-34-237-223-165", got.Name)
	assert.Equal(t, "10.4.3.0/24", got.Spec.PodCIDR)
	assert.Equal(t, "10.100.120.155/24", got.Annotations["projectcalico.org/IPv4Address"])

	// This is verbatim what becomes Application.ReqBody on the way to cloudcore.
	reqBody, err := json.Marshal(obj)
	require.NoError(t, err)
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(reqBody, &m))
	assert.Equal(t, "Node", m["kind"], "TypeMeta must survive; cloudcore's dynamic client rejects an object without it")
	assert.Equal(t, "v1", m["apiVersion"])
}

// Offering protobuf must not change what a JSON client sees for a custom
// resource. Unregistered kinds come back as runtime.Unknown, which carries the
// original bytes -- if that ever stopped round-tripping, every
// crd.projectcalico.org read on an edge node would silently return an empty
// object.
func TestCustomResourceStillServedIntactOverJSON(t *testing.T) {
	const ipPoolList = `{"apiVersion":"crd.projectcalico.org/v1","kind":"IPPoolList",` +
		`"metadata":{"resourceVersion":"123"},"items":[{"apiVersion":"crd.projectcalico.org/v1",` +
		`"kind":"IPPool","metadata":{"name":"site-edge-pods"},"spec":{"cidr":"10.4.0.0/16"}}]}`

	obj, err := DecodeAndConvert([]byte(ipPoolList), "crd.projectcalico.org")
	require.NoError(t, err)
	require.IsType(t, &runtime.Unknown{}, obj)

	out, err := encodeWith(t, "application/json", obj,
		schema.GroupVersionKind{Group: "crd.projectcalico.org", Version: "v1", Kind: "IPPoolList"})
	require.NoError(t, err)
	assert.JSONEq(t, ipPoolList, string(out))
}

// A decode failure that carries no GroupVersionKind must fall back, not panic.
func TestDecodeAndConvertUnparseableBodyDoesNotPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		_, _ = DecodeAndConvert([]byte("not an object at all"), "crd.projectcalico.org")
		_, _ = DecodeAndConvert([]byte("not an object at all"), "")
		_, _ = DecodeAndConvert(nil, "")
	})
}

type erroringWatcher struct{ ch chan watch.Event }

func (e *erroringWatcher) Stop()                          { close(e.ch) }
func (e *erroringWatcher) ResultChan() <-chan watch.Event { return e.ch }

// newTypedWatcher starts a goroutine that immediately reads from the wrapped
// watcher, so wrapping a nil watch.Interface -- which is what Store.Watch
// returns alongside an error -- crashes edgecore rather than surfacing the
// error. REST.Watch must return the error instead of wrapping.
func TestTypedWatcherIsNotGivenANilWatcher(t *testing.T) {
	assert.NotPanics(t, func() {
		w := newTypedWatcher(&erroringWatcher{ch: make(chan watch.Event)}, "")
		w.Stop()
		time.Sleep(50 * time.Millisecond)
	})
}

// Watch events are converted to typed objects for protobuf encodability.
func TestTypedWatcherConvertsCoreObjects(t *testing.T) {
	src := make(chan watch.Event, 2)
	w := newTypedWatcher(&erroringWatcher{ch: src}, "")
	defer w.Stop()

	src <- watch.Event{Type: watch.Added, Object: &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]interface{}{"name": "p", "namespace": "d"},
	}}}
	select {
	case ev := <-w.ResultChan():
		_, isPod := ev.Object.(*corev1.Pod)
		assert.True(t, isPod, "core objects must be converted to their typed form, got %T", ev.Object)
	case <-time.After(2 * time.Second):
		t.Fatal("no event delivered")
	}

	// A custom resource has no typed form; it must still be delivered.
	src <- watch.Event{Type: watch.Added, Object: &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "crd.projectcalico.org/v1", "kind": "IPPool",
		"metadata": map[string]interface{}{"name": "site-edge-pods"},
	}}}
	select {
	case ev := <-w.ResultChan():
		assert.NotNil(t, ev.Object)
	case <-time.After(2 * time.Second):
		t.Fatal("custom resource event dropped")
	}
}
