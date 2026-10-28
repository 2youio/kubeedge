package imitator

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
	v2 "github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/v2"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "imitator-test")
	if err != nil {
		panic(err)
	}
	orm.RegisterModel(new(v2.MetaV2), new(dao.Meta))
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
	pod := podWithoutRV(uid)
	pod.SetResourceVersion(rv)
	pod.SetLabels(map[string]string{"app": "web"})
	_ = unstructured.SetNestedField(pod.Object, "edge-node", "spec", "nodeName")
	return pod
}

// podWithoutRV is the body of the DELETE synccontroller.gcOrphanedObjectSync sends: no
// resourceVersion, labels or spec.
func podWithoutRV(uid string) *unstructured.Unstructured {
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
// resourceVersion: the KEDEV-770 case, where the DELETE without resourceVersion never reached promtail.
func TestInjectDeleteWithoutRV(t *testing.T) {
	s := newTestImitator(t)
	s.Inject(podMessage(model.InsertOperation, fullPod("a", "10")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx, "/core/v1/pods/default", 20)

	go s.Inject(podMessage(model.DeleteOperation, podWithoutRV("a")))

	e, ok := nextEvent(ch)
	if !ok {
		t.Fatal("watch got no event for the delete without resourceVersion")
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
		podMessage(model.DeleteOperation, podWithoutRV("a")),
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

// A DELETE that waited in cloudhub behind an un-ACKed update (the KEDEV-770 window) carries
// a resourceVersion below a watch opened meanwhile. The watch listed the pod, so it must get
// the DELETED.
func TestInjectDeleteBelowWatchRevision(t *testing.T) {
	s := newTestImitator(t)
	s.Inject(podMessage(model.InsertOperation, fullPod("a", "10")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx, "/core/v1/pods/default", 20)

	go s.Inject(podMessage(model.DeleteOperation, fullPod("a", "15")))

	e, ok := nextEvent(ch)
	if !ok {
		t.Fatal("watch got no event for the delete below its revision")
	}
	if e.Type != watch.Deleted {
		t.Fatalf("event type = %s, want DELETED", e.Type)
	}
	if rv := e.Object.(*unstructured.Unstructured).GetResourceVersion(); rv != "15" {
		t.Errorf("DELETED object has resourceVersion %q, want the message's 15", rv)
	}
	if n := len(storedPods(t, s)); n != 0 {
		t.Errorf("meta_v2 holds %d pods after the delete, want 0", n)
	}
}

// A DELETE that finds no row is filtered by resourceVersion, as before the UID check: a client
// may hold the object from a list served by the cloud, whose insert never reached the edge.
// A DELETE without resourceVersion that finds no row reaches no watch.
func TestInjectDeleteWithoutRow(t *testing.T) {
	tests := []struct {
		name  string
		first *unstructured.Unstructured
		then  *unstructured.Unstructured
		event bool
	}{
		{"delete without resourceVersion after one with", fullPod("a", "30"), podWithoutRV("a"), false},
		{"delete with resourceVersion after one without", podWithoutRV("a"), fullPod("a", "30"), true},
		{"delete with resourceVersion repeated", fullPod("a", "30"), fullPod("a", "30"), true},
		{"never stored", nil, fullPod("a", "30"), true},
		{"never stored, below the watch's revision", nil, fullPod("a", "15"), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newTestImitator(t)
			if test.first != nil {
				s.Inject(podMessage(model.InsertOperation, fullPod("a", "10")))
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch := s.Watch(ctx, "/core/v1/pods/default", 20)

			if test.first != nil {
				go s.Inject(podMessage(model.DeleteOperation, test.first))
				if e, ok := nextEvent(ch); !ok || e.Type != watch.Deleted {
					t.Fatalf("watch got %v (%v) for the first delete, want DELETED", e.Type, ok)
				}
			}
			go s.Inject(podMessage(model.DeleteOperation, test.then))
			e, ok := nextEvent(ch)
			if ok != test.event || ok && e.Type != watch.Deleted {
				t.Errorf("watch got %v (%v) for a delete that found no row, want DELETED: %v", e.Type, ok, test.event)
			}
		})
	}
}

// A DELETE that fails in the database still goes out, as before the UID check, rather than
// leaving the object in every watch client.
func TestInjectDeleteDBError(t *testing.T) {
	s := newTestImitator(t)
	s.Inject(podMessage(model.InsertOperation, fullPod("a", "10")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx, "/core/v1/pods/default", 20)

	if _, err := dbm.DBAccess.Raw("ALTER TABLE meta_v2 RENAME TO meta_v2_off").Exec(); err != nil {
		t.Fatal(err)
	}
	go s.Inject(podMessage(model.DeleteOperation, fullPod("a", "30")))
	e, ok := nextEvent(ch)
	if _, err := dbm.DBAccess.Raw("ALTER TABLE meta_v2_off RENAME TO meta_v2").Exec(); err != nil {
		t.Fatal(err)
	}
	if !ok || e.Type != watch.Deleted {
		t.Errorf("watch got %v (%v) for a delete that failed, want DELETED", e.Type, ok)
	}
}

// edgecontroller deletes a pod by name only, with no UID or resourceVersion, when the edge
// reports the status of a pod that the cloud no longer has under that UID. The row may hold
// the recreated pod, so the watch must not get it as DELETED. The row goes, as before.
func TestInjectDeleteWithoutUID(t *testing.T) {
	s := newTestImitator(t)
	s.Inject(podMessage(model.InsertOperation, fullPod("b", "10")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx, "/core/v1/pods/default", 20)

	byName := podWithoutRV("")
	byName.SetUID("")
	go s.Inject(podMessage(model.DeleteOperation, byName))
	if e, ok := nextEvent(ch); ok {
		t.Errorf("watch got %s for a delete without UID", e.Type)
	}
	if n := len(storedPods(t, s)); n != 0 {
		t.Errorf("meta_v2 holds %d pods after the delete, want 0", n)
	}
}

// A pod recreated under the same key while a DELETE of the old UID is injected must stay:
// the UID check and the delete are one step, whichever of the two runs first.
func TestInjectDeleteOtherUIDConcurrent(t *testing.T) {
	s := newTestImitator(t)
	for i := 0; i < 300; i++ {
		if err := s.InsertOrUpdateObj(context.TODO(), fullPod("a", "10")); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			s.Inject(podMessage(model.DeleteOperation, podWithoutRV("a")))
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = s.InsertOrUpdateObj(context.TODO(), fullPod("b", "20"))
		}()
		close(start)
		wg.Wait()
		if len(storedPods(t, s)) != 1 {
			t.Fatalf("round %d: the delete of uid-a removed the recreated pod", i)
		}
		s.lock.Lock()
		s.deleteKey("/core/v1/pods/default/web-0")
		s.lock.Unlock()
	}
}

// The stored pod swapped in for a DELETE without resourceVersion carries its podpatch, as
// Get serves it.
func TestInjectDeleteWithoutRVMergesPodPatch(t *testing.T) {
	s := newTestImitator(t)
	pod := fullPod("a", "10")
	_ = unstructured.SetNestedField(pod.Object, "Pending", "status", "phase")
	s.Inject(podMessage(model.InsertOperation, pod))
	patch := &dao.Meta{Key: "default/podpatch/web-0", Type: "podpatch", Value: `{"status":{"phase":"Running"}}`}
	if err := dao.InsertOrUpdate(patch); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dao.DeleteMetaByKey(patch.Key) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx, "/core/v1/pods/default", 20)

	go s.Inject(podMessage(model.DeleteOperation, podWithoutRV("a")))
	e, ok := nextEvent(ch)
	if !ok {
		t.Fatal("watch got no event for the delete without resourceVersion")
	}
	phase, _, _ := unstructured.NestedString(e.Object.(*unstructured.Unstructured).Object, "status", "phase")
	if phase != "Running" {
		t.Errorf("DELETED pod has status.phase %q, want the podpatch's Running", phase)
	}
}

// The UID check reads the stored row without its podpatch: kubelet puts the pod's UID in
// every status patch, so an older pod's patch names the old UID, and a patch that fails to
// merge must not skip the check.
func TestInjectDeleteOtherUIDWithPodPatch(t *testing.T) {
	for name, value := range map[string]string{
		"older pod's patch":         `{"metadata":{"uid":"uid-a"},"status":{"phase":"Running"}}`,
		"patch that fails to merge": `{"$patch":"bogus"}`,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestImitator(t)
			s.Inject(podMessage(model.InsertOperation, fullPod("b", "30")))
			patch := &dao.Meta{Key: "default/podpatch/web-0", Type: "podpatch", Value: value}
			if err := dao.InsertOrUpdate(patch); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = dao.DeleteMetaByKey(patch.Key) }()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch := s.Watch(ctx, "/core/v1/pods/default", 0)

			for _, msg := range []model.Message{
				podMessage(model.DeleteOperation, podWithoutRV("a")),
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
		})
	}
}

// A DELETE without resourceVersion for the stored pod removes it even when its podpatch
// fails to merge, and the DELETED then carries the DELETE's own body.
func TestInjectDeleteWithoutRVPodPatchFails(t *testing.T) {
	s := newTestImitator(t)
	s.Inject(podMessage(model.InsertOperation, fullPod("a", "10")))
	patch := &dao.Meta{Key: "default/podpatch/web-0", Type: "podpatch", Value: `{"$patch":"bogus"}`}
	if err := dao.InsertOrUpdate(patch); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dao.DeleteMetaByKey(patch.Key) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx, "/core/v1/pods/default", 20)

	go s.Inject(podMessage(model.DeleteOperation, podWithoutRV("a")))
	e, ok := nextEvent(ch)
	if !ok || e.Type != watch.Deleted {
		t.Fatalf("watch got %v (%v) for the delete without resourceVersion, want DELETED", e.Type, ok)
	}
	if uid := e.Object.(*unstructured.Unstructured).GetUID(); uid != "uid-a" {
		t.Errorf("DELETED object has uid %q, want the delete's uid-a", uid)
	}
	if n := len(storedPods(t, s)); n != 0 {
		t.Errorf("meta_v2 holds %d pods after the delete, want 0", n)
	}
}

func objMessage(op string, obj *unstructured.Unstructured) model.Message {
	ns := obj.GetNamespace()
	if ns == "" {
		ns = "null"
	}
	return *model.NewMessage("").
		BuildRouter("dynamiccontroller", "resource", ns+"/"+strings.ToLower(obj.GetKind())+"/"+obj.GetName(), op).
		FillBody(obj.Object)
}

func testObj(apiVersion, kind, ns, name, uid, rv string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	obj.SetNamespace(ns)
	obj.SetName(name)
	obj.SetUID(k8stypes.UID(uid))
	obj.SetResourceVersion(rv)
	return obj
}

// namespaceStub is the body of the Namespace DELETE that synccontroller resends: no resourceVersion.
func namespaceStub(name, uid string) *unstructured.Unstructured {
	return testObj("v1", "Namespace", "", name, uid, "")
}

func storedKeys(t *testing.T, s *imitator) []string {
	var keys []string
	if _, err := dbm.DBAccess.Raw("SELECT key FROM meta_v2 ORDER BY key").QueryRows(&keys); err != nil {
		t.Fatal(err)
	}
	return keys
}

// fillNamespaces stores namespace ns1 with a slice, a Service and a pod, and a slice in ns2.
func fillNamespaces(s *imitator, ns1UID string) {
	for _, obj := range []*unstructured.Unstructured{
		testObj("v1", "Namespace", "", "ns1", ns1UID, "5"),
		testObj("discovery.k8s.io/v1", "EndpointSlice", "ns1", "echo-abc", "uid-s1", "10"),
		testObj("v1", "Service", "ns1", "echo", "uid-svc1", "11"),
		testObj("discovery.k8s.io/v1", "EndpointSlice", "ns2", "echo-xyz", "uid-s2", "12"),
	} {
		s.Inject(objMessage(model.InsertOperation, obj))
	}
	s.Inject(podMessage(model.InsertOperation, fullPod("p", "13")))
	pod := fullPod("q", "14")
	pod.SetNamespace("ns1")
	s.Inject(podMessage(model.InsertOperation, pod))
}

// The Namespace DELETE removes the rows the cloud never deleted, except pods, and every watch
// of them gets DELETED, whatever its revision (KEDEV-834).
func TestInjectNamespaceDeleteRemovesContents(t *testing.T) {
	s := newTestImitator(t)
	fillNamespaces(s, "uid-ns1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slices := s.Watch(ctx, "/discovery.k8s.io/v1/endpointslices", 100)
	services := s.Watch(ctx, "/core/v1/services/ns1", 100)

	go s.Inject(objMessage(model.DeleteOperation, namespaceStub("ns1", "uid-ns1")))
	// The watches are unbuffered and get their events in row order, so read both at once.
	deleted := map[string]string{}
	for len(deleted) < 2 {
		var e watch.Event
		select {
		case e = <-slices:
		case e = <-services:
		case <-time.After(time.Second):
			t.Fatalf("watches got only %v, want DELETED for echo-abc and echo", deleted)
		}
		obj := e.Object.(*unstructured.Unstructured)
		if e.Type != watch.Deleted || obj.GetResourceVersion() == "" {
			t.Errorf("watch got %s %s rv %q, want DELETED of the stored object", e.Type, obj.GetName(), obj.GetResourceVersion())
		}
		deleted[obj.GetName()] = obj.GetKind()
	}
	if want := map[string]string{"echo-abc": "EndpointSlice", "echo": "Service"}; !reflect.DeepEqual(deleted, want) {
		t.Errorf("watches got DELETED for %v, want %v", deleted, want)
	}
	if e, ok := nextEvent(slices); ok {
		t.Errorf("slice watch got a second event %s %s", e.Type, e.Object.(*unstructured.Unstructured).GetName())
	}
	want := []string{
		"/core/v1/pods/default/web-0",
		"/core/v1/pods/ns1/web-0",
		"/discovery.k8s.io/v1/endpointslices/ns2/echo-xyz",
	}
	if got := storedKeys(t, s); !reflect.DeepEqual(got, want) {
		t.Errorf("meta_v2 keys = %v, want %v", got, want)
	}
}

// A late DELETE of an older namespace must not empty the namespace recreated under its name,
// nor may a DELETE of a namespace that the edge never stored.
func TestInjectNamespaceDeleteKeepsContents(t *testing.T) {
	tests := []struct {
		name    string
		storeNS bool
	}{
		{"recreated namespace", true},
		{"namespace not stored", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newTestImitator(t)
			fillNamespaces(s, "uid-ns1-new")
			if !test.storeNS {
				if _, err := dbm.DBAccess.Raw("DELETE FROM meta_v2 WHERE key = '/core/v1/namespaces/null/ns1'").Exec(); err != nil {
					t.Fatal(err)
				}
			}
			before := storedKeys(t, s)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			slices := s.Watch(ctx, "/discovery.k8s.io/v1/endpointslices", 0)

			go s.Inject(objMessage(model.DeleteOperation, namespaceStub("ns1", "uid-ns1-old")))
			if e, ok := nextEvent(slices); ok {
				t.Errorf("slice watch got %s for the delete of another namespace", e.Type)
			}
			if got := storedKeys(t, s); !reflect.DeepEqual(got, before) {
				t.Errorf("meta_v2 keys = %v, want %v", got, before)
			}
		})
	}
}
