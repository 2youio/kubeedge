package filter

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	"github.com/kubeedge/kubeedge/cloud/pkg/controllermanager/nodegroup"
)

func TestSameGroupAs(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	nodes := map[string]string{
		"edge-a1": "site-a",
		"edge-a2": "site-a",
		"edge-b1": "site-b",
		"edge-x":  "",
		"cloud-1": "",
	}
	for name, group := range nodes {
		node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if group != "" {
			node.Labels = map[string]string{nodegroup.LabelBelongingTo: group}
		}
		assert.NoError(t, indexer.Add(node))
	}
	lister := cache.NewGenericLister(indexer, v1.Resource("nodes"))
	listerCalls := 0
	patches := gomonkey.ApplyFunc(GetSyncedResourceLister, func(schema.GroupVersionResource) (cache.GenericLister, error) {
		listerCalls++
		return lister, nil
	})
	defer patches.Reset()

	cases := []struct {
		target, ep string
		want       bool
	}{
		{"edge-a1", "edge-a2", true},
		{"edge-a1", "edge-b1", false},
		{"edge-a1", "cloud-1", false},
		// A node in no group keeps only its own endpoints, not every
		// ungrouped cloud node's.
		{"edge-x", "cloud-1", false},
		{"edge-x", "edge-a1", false},
		{"edge-x", "edge-x", true},
		{"edge-a1", "missing", false},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, SameGroupAs(c.target)(c.ep), "%s -> %s", c.target, c.ep)
	}

	// The lister is looked up once per SameGroupAs call, not once per endpoint.
	listerCalls = 0
	inGroup := SameGroupAs("edge-a1")
	for _, ep := range []string{"edge-a2", "edge-b1", "cloud-1"} {
		inGroup(ep)
	}
	assert.Equal(t, 1, listerCalls)
}
