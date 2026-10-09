package imitator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	v2 "github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/v2"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "imitator-test")
	if err != nil {
		panic(err)
	}
	orm.RegisterModel(new(v2.MetaV2))
	dbm.InitDBConfig("sqlite3", "default", filepath.Join(dir, "edgecore.db"))
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// newTestImitator returns an imitator on an empty meta_v2 table.
func newTestImitator(t *testing.T) *imitator {
	if _, err := dbm.DBAccess.Raw("DELETE FROM meta_v2").Exec(); err != nil {
		t.Fatal(err)
	}
	return newV2Client().(*imitator)
}

func podMessage(op string, pod *unstructured.Unstructured) model.Message {
	return *model.NewMessage("").
		BuildRouter("edgecontroller", "resource", pod.GetNamespace()+"/pod/"+pod.GetName(), op).
		FillBody(pod.Object)
}

func fullPod(uid, rv string) *unstructured.Unstructured {
	pod := stubPod(uid)
	pod.SetResourceVersion(rv)
	pod.SetLabels(map[string]string{"app": "web"})
	_ = unstructured.SetNestedField(pod.Object, "edge-node", "spec", "nodeName")
	return pod
}

// stubPod is the body synccontroller's orphan GC sends: no resourceVersion, labels or spec.
func stubPod(uid string) *unstructured.Unstructured {
	pod := &unstructured.Unstructured{}
	pod.SetAPIVersion("v1")
	pod.SetKind("Pod")
	pod.SetNamespace("default")
	pod.SetName("web-0")
	pod.SetUID(k8stypes.UID("uid-" + uid))
	return pod
}

func storedPods(t *testing.T, s *imitator) []v2.MetaV2 {
	resp, err := s.List(context.TODO(), "/core/v1/pods/default")
	if err != nil {
		t.Fatal(err)
	}
	return *resp.Kvs
}

func nextEvent(ch <-chan watch.Event) (watch.Event, bool) {
	select {
	case e := <-ch:
		return e, true
	case <-time.After(time.Second):
		return watch.Event{}, false
	}
}

// The watch opened after the pod's last update, so its revision is above the pod's
// resourceVersion: the KEDEV-770 case, where the stub DELETE never reached promtail.
func TestInjectDeleteStub(t *testing.T) {
	s := newTestImitator(t)
	s.Inject(podMessage(model.InsertOperation, fullPod("a", "10")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx, "/core/v1/pods/default", 20)

	go s.Inject(podMessage(model.DeleteOperation, stubPod("a")))

	e, ok := nextEvent(ch)
	if !ok {
		t.Fatal("watch got no event for the delete stub")
	}
	if e.Type != watch.Deleted {
		t.Fatalf("event type = %s, want DELETED", e.Type)
	}
	obj := e.Object.(*unstructured.Unstructured)
	if obj.GetLabels()["app"] != "web" || obj.GetResourceVersion() != "10" {
		t.Errorf("DELETED object is not the stored pod: labels %v, resourceVersion %q", obj.GetLabels(), obj.GetResourceVersion())
	}
	if n := len(storedPods(t, s)); n != 0 {
		t.Errorf("meta_v2 holds %d pods after the delete, want 0", n)
	}
}

// A DELETE for an older object under the same key must not remove the current one.
func TestInjectDeleteOtherUID(t *testing.T) {
	s := newTestImitator(t)
	s.Inject(podMessage(model.InsertOperation, fullPod("b", "30")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx, "/core/v1/pods/default", 0)

	for _, msg := range []model.Message{
		podMessage(model.DeleteOperation, stubPod("a")),
		podMessage(model.DeleteOperation, fullPod("a", "25")),
	} {
		go s.Inject(msg)
		if e, ok := nextEvent(ch); ok {
			t.Errorf("watch got %s for a delete of another uid", e.Type)
		}
	}
	if pods := storedPods(t, s); len(pods) != 1 {
		t.Errorf("meta_v2 holds %d pods, want the current one", len(pods))
	}
}
