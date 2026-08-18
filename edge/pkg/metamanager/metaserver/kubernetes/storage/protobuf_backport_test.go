package storage

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
)

// The case this backport exists for: calico-node PUTs a v1.Node with
// Content-Type: application/vnd.kubernetes.protobuf. Before protobuf support
// the request was refused with "the body of the request was in an unknown
// format". It has to decode, and the body forwarded to cloudcore must keep its
// TypeMeta -- cloudcore feeds it to a dynamic client, which rejects an object
// with no apiVersion/kind.
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

	// Verbatim what becomes Application.ReqBody on the way to cloudcore.
	reqBody, err := json.Marshal(obj)
	require.NoError(t, err)
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(reqBody, &m))
	assert.Equal(t, "Node", m["kind"],
		"TypeMeta must survive; cloudcore's dynamic client rejects an object without it")
	assert.Equal(t, "v1", m["apiVersion"])
}

// A custom resource has no typed form, so DecodeAndConvert cannot convert one --
// it hands the bytes back untouched instead. That is not a degraded path, it is
// the one Calico's own writes take: IPAMBlocks, BlockAffinities and IPAMHandles
// are all crd.projectcalico.org. The body has to reach cloudcore byte-for-byte,
// which is what runtime.Unknown's MarshalJSON gives.
func TestCustomResourceWriteBodyPassesThroughVerbatim(t *testing.T) {
	const block = `{"apiVersion":"crd.projectcalico.org/v1","kind":"IPAMBlock",` +
		`"metadata":{"name":"10-4-4-0-26"},"spec":{"cidr":"10.4.4.0/26","affinity":"host:edge-52-21-33-192"}}`

	obj, err := DecodeAndConvert([]byte(block), "crd.projectcalico.org")
	require.NoError(t, err)

	out, err := json.Marshal(obj)
	require.NoError(t, err)
	assert.JSONEq(t, block, string(out))
}

// A body that fails to decode before a kind is recognised leaves a nil
// GroupVersionKind, which the not-registered fallback used to dereference.
func TestDecodeAndConvertUnparseableBodyDoesNotPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		_, _ = DecodeAndConvert([]byte("not an object at all"), "crd.projectcalico.org")
		_, _ = DecodeAndConvert([]byte("not an object at all"), "")
		_, _ = DecodeAndConvert(nil, "")
	})
}
