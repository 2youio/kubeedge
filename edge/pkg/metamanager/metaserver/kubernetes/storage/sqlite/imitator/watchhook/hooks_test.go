package watchhook

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

type recorder struct {
	events []watch.Event
}

func (r *recorder) Receive(e watch.Event) error {
	r.events = append(r.events, e)
	return nil
}

func newPod(name, rv string) *unstructured.Unstructured {
	pod := &unstructured.Unstructured{}
	pod.SetAPIVersion("v1")
	pod.SetKind("Pod")
	pod.SetNamespace("default")
	pod.SetName(name)
	pod.SetResourceVersion(rv)
	return pod
}

func TestTriggerResourceVersion(t *testing.T) {
	tests := []struct {
		name      string
		eventType watch.EventType
		rv        string
		delivered bool
	}{
		{"modified above the watch revision", watch.Modified, "150", true},
		{"modified below the watch revision", watch.Modified, "50", false},
		{"deleted above the watch revision", watch.Deleted, "150", true},
		{"deleted below the watch revision", watch.Deleted, "50", true},
		{"deleted without resourceVersion (orphan-GC stub)", watch.Deleted, "", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := &recorder{}
			hook, err := NewWatchHook("/core/v1/pods/default", 100, r)
			if err != nil {
				t.Fatal(err)
			}
			defer hook.Stop()

			Trigger(watch.Event{Type: test.eventType, Object: newPod("p", test.rv)})

			if got := len(r.events) == 1; got != test.delivered {
				t.Errorf("delivered = %v, want %v", got, test.delivered)
			}
		})
	}
}
