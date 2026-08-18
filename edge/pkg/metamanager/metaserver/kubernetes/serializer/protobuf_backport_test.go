package serializer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
)

// MetaServer accepts protobuf REQUEST bodies -- that is what unblocks
// calico-node, and the write handlers get there by decoding the body themselves
// rather than by negotiating against this list. Responses are a separate
// question, and the answer is no.
//
// Offering protobuf here changes OUTPUT negotiation for every client whose
// AcceptContentTypes prefers it, which is every modern client-go, and those
// clients then stop falling back to JSON. For a built-in resource that is fine.
// For a custom resource it is not: MetaServer is schema-less end to end, an
// unstructured object cannot be a protobuf message, and the best the encoder can
// do is wrap the JSON in a protobuf envelope the client cannot unmarshal.
//
// Measured on dev-cluster edge-52-21-33-192, 2026-08-18, with the protobuf media
// type offered: every crd.projectcalico.org read from calico-node failed with
//
//	Unable to query IP pool configuration
//	  error=no kind "IppoolList" is registered for version "crd.projectcalico.org/v1"
//
// -- the envelope's kind coming from decorateList's UnsafeResourceToKind
// title-casing rather than from the body. Removing this entry restored it. The
// mangled kind was luck; a resource whose name survives title-casing would have
// come back undecodable with no clue as to why.
//
// So: if a future change adds protobuf to this list, it also has to make the
// read path build a per-request scope and offer protobuf only for kinds
// registered in the scheme, the way a real API server does for CRDs.
func TestNegotiatedSerializerDoesNotOfferProtobufForResponses(t *testing.T) {
	var media []string
	for _, info := range NewNegotiatedSerializer().SupportedMediaTypes() {
		media = append(media, info.MediaType)
	}

	assert.Equal(t, []string{"application/json", "application/yaml"}, media)
	assert.NotContains(t, media, runtime.ContentTypeProtobuf,
		"see this test's comment: offering protobuf on responses breaks every custom-resource read")
}
