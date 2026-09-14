package imitator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/common/constants"
	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	v2 "github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/v2"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/kubernetes/storage/sqlite/imitator/watchhook"
	"github.com/kubeedge/kubeedge/pkg/metaserver"
	"github.com/kubeedge/kubeedge/pkg/metaserver/util"
)

// imitator is a storage based on metav2 that imitate the behavior of etcd
type imitator struct {
	lock sync.RWMutex
	// The Revision is the current revision of client
	// It is set when client inits or a bigger resourceversion obj was saved into meta_v2
	revision uint64
	// to parse obj resource version from string to int64
	versioner storage.Versioner
	// to co/decoder obj
	codec runtime.Codec
}

// Inject transform the message to watch.event, save internal obj/objs to table meta_v2
// and trigger the corresponding hook to serve watch
func (s *imitator) Inject(msg model.Message) {
	for _, e := range s.Event(&msg) {
		// save to meta_v2
		var err error
		switch e.Type {
		case watch.Added, watch.Modified:
			err = s.InsertOrUpdateObj(context.TODO(), e.Object)
		case watch.Deleted:
			err = s.DeleteObj(context.TODO(), e.Object)
		}
		if err != nil {
			key := metaserver.KeyFunc(e.Object)
			klog.Errorf("failed to serve event {type:%v,key:%v}", e.Type, key)
			continue
		}
		// TODO: move Trigger inside InsertOrUpdateObj and DeleteObj
		watchhook.Trigger(e)
	}
}

// TODO: filter out insert or update req that the obj's rev is smaller than the stored
func (s *imitator) InsertOrUpdateObj(_ context.Context, obj runtime.Object) error {
	key, err := metaserver.KeyFuncObj(obj)
	if err != nil {
		return err
	}
	gvr, ns, name := metaserver.ParseKey(key)
	unstr, isUnstr := obj.(*unstructured.Unstructured)
	if !isUnstr {
		return fmt.Errorf("obj is not unstructured type")
	}
	buf := bytes.NewBuffer(nil)
	err = s.codec.Encode(unstr, buf)
	if err != nil {
		return err
	}
	objRv, err := s.versioner.ObjectResourceVersion(obj)
	m := v2.MetaV2{
		Key:                  key,
		GroupVersionResource: gvr.String(),
		Namespace:            ns,
		Name:                 name,
		ResourceVersion:      objRv,
		Value:                buf.String(),
	}
	return s.insertOrReplaceMetaV2(m, objRv)
}

func (s *imitator) insertOrReplaceMetaV2(m v2.MetaV2, objRv uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, err := dbm.DBAccess.Raw("INSERT OR REPLACE INTO meta_v2 (key, groupversionresource, namespace,name,resourceversion,value) VALUES (?,?,?,?,?,?)", m.Key, m.GroupVersionResource, m.Namespace, m.Name, m.ResourceVersion, m.Value).Exec()
	var maxRetryTimes = 3
	for i := 1; err != nil; i++ {
		klog.Errorf("failed to access database:%v", err)
		if i == maxRetryTimes {
			return fmt.Errorf("failed to access database after %v times try", i)
		}
		_, err = dbm.DBAccess.Raw("INSERT OR REPLACE INTO meta_v2 (key, groupversionresource, namespace,name,resourceversion,value) VALUES (?,?,?,?,?,?)", m.Key, m.GroupVersionResource, m.Namespace, m.Name, m.ResourceVersion, m.Value).Exec()
	}
	if objRv > s.GetRevision() {
		s.SetRevision(objRv)
	}
	klog.V(4).Infof("[metaserver]successfully insert or update obj:%v", m.Key)
	return nil
}

func (s *imitator) InsertOrUpdatePassThroughObj(_ context.Context, obj []byte, key string) error {
	m := v2.MetaV2{
		Key:   key,
		Value: string(obj),
	}
	return s.insertOrReplaceMetaV2(m, 0)
}

func (s *imitator) GetPassThroughObj(_ context.Context, key string) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	results := new([]v2.MetaV2)
	_, err := dbm.DBAccess.QueryTable(v2.NewMetaTableName).Filter(v2.KEY, key).All(results)
	if err != nil {
		return nil, err
	}

	switch {
	case len(*results) == 1:
		klog.V(4).Infof("[metaserver]successfully insert or update obj:%v", key)
		return []byte((*results)[0].Value), nil
	default:
		return nil, fmt.Errorf("the server could not find the requested resource")
	}
}

func (s *imitator) Delete(_ context.Context, key string) error {
	m := v2.MetaV2{
		Key: key,
	}
	s.lock.Lock()
	_, err := dbm.DBAccess.Delete(&m)
	if err != nil {
		klog.Errorf("[imitator] delete error: %v", err)
	}
	s.lock.Unlock()
	return nil
}
func (s *imitator) DeleteObj(_ context.Context, obj runtime.Object) error {
	key, err := metaserver.KeyFuncObj(obj)
	if err != nil {
		return err
	}
	err = s.Delete(context.TODO(), key)
	return err
}
func (s *imitator) Get(_ context.Context, key string) (Resp, error) {
	var resp Resp
	s.lock.RLock()
	results, err := v2.RawMetaByGVRNN(metaserver.ParseKey(key))
	resp.Revision = s.revision
	s.lock.RUnlock()
	if err != nil {
		return Resp{}, err
	}
	switch {
	case len(*results) == 1:
		resp.Kvs = results
		return resp, nil
	default:
		return Resp{}, fmt.Errorf("the server could not find the requested resource")
	}
}
func (s *imitator) List(_ context.Context, key string) (Resp, error) {
	gvr, ns, name := metaserver.ParseKey(key)
	//if name != NullName {
	//	return Resp{}, fmt.Errorf("dao client list must not have resource name")
	//}
	klog.Infof("%v,%v,%v", gvr, ns, name)
	var resp Resp
	s.lock.RLock()
	results, err := v2.RawMetaByGVRNN(gvr, ns, name)
	resp.Revision = s.revision

	s.lock.RUnlock()
	if err != nil {
		return Resp{}, err
	}
	resp.Kvs = results
	return resp, nil
}

func (s *imitator) GetRevision() uint64 {
	return s.revision
}

func (s *imitator) SetRevision(version interface{}) {
	switch v := version.(type) {
	case int64:
		s.revision = uint64(v)
	case uint64:
		s.revision = v
	case string:
		rv, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			klog.Error(err)
			return
		}
		s.revision = rv
	default:
		klog.Error("unsupported type when parse version")
	}
}

func (s *imitator) Watch(ctx context.Context, key string, rev uint64) <-chan watch.Event {
	wch := make(chan watch.Event)
	receiver := watchhook.NewChanReceiver(wch)
	wh, err := watchhook.NewWatchHook(key, rev, receiver)
	if err != nil {
		klog.Errorf("add hook for %s failed, %v", key, err)
		return nil
	}

	go func() {
		<-ctx.Done()
		wh.Stop()
		close(wch)
	}()
	return wch
}

// Event transform the message to watch.event
func (s *imitator) Event(msg *model.Message) []watch.Event {
	klog.V(4).Infof("[metaserver] get a message from metamanager: %+v", msg)
	var ret []watch.Event
	_, resType, _ := parseResource(msg.Router.Resource)
	//skip nodestatus, podstatus and node-lease
	if strings.Contains(resType, "status") || (strings.Contains(resType, "lease") && msg.GetSource() == modules.EdgedModuleName) {
		klog.V(4).Infof("skip status or node-lease messages")
		return []watch.Event{}
	}
	var bytes []byte
	var err error
	var body = msg.GetContent()
	// convert body to bytes
	switch body := body.(type) {
	case []byte:
		bytes = body
	default:
		bytes, err = json.Marshal(body)
		if err != nil {
			klog.Errorf("failed to marshal msg content, err: %+v", err)
			return ret
		}
	}
	var op watch.EventType
	switch msg.Router.Operation {
	case model.InsertOperation:
		op = watch.Added
	case model.UpdateOperation:
		op = watch.Modified
	case model.DeleteOperation:
		op = watch.Deleted
	}
	//TODO: support array List like []obj
	obj := new(unstructured.Unstructured)
	err = runtime.DecodeInto(s.codec, bytes, obj)
	if err != nil {
		// Edge-originated bodies (DeleteOptions, patches, TypeMeta-less node
		// objects) never decode and never belonged in meta_v2. Keep them out of
		// the error log so real failures stand out, and never let them reach the
		// store lookup below.
		if msg.GetSource() == modules.EdgedModuleName {
			klog.V(4).Infof("skip undecodable edge-originated message content: %v", err)
			return ret
		}
		// A delete built by synccontroller's orphaned-sync GC carries a fabricated
		// body with no apiVersion/kind, which this codec rejects. Recover the GVK
		// from the row already stored for the object, so that an edge running
		// against a cloudcore that does not stamp it still applies the delete
		// instead of stranding the object in meta_v2 forever.
		recovered, rerr := s.recoverDeletedObj(msg, bytes, op)
		if rerr != nil {
			klog.Errorf("failed to unmarshal message content to unstructured obj: %+v (gvk recovery: %v)", err, rerr)
			return ret
		}
		klog.V(2).Infof("[metaserver] recovered GVK %s for delete of %s from local storage",
			recovered.GroupVersionKind(), msg.Router.Resource)
		obj = recovered
	}
	if obj.IsList() {
		fn := func(object runtime.Object) error {
			event := watch.Event{
				Type:   op,
				Object: object,
			}
			ret = append(ret, event)
			return nil
		}
		err := obj.EachListItem(fn)
		if err != nil {
			klog.Errorf("failed to get ret list, err: %+v", err)
			return ret
		}
	} else {
		ret = append(ret, watch.Event{Type: op, Object: obj})
	}
	return ret
}

// recoverDeletedObj resolves a delete whose body carries no GVK into the object
// already stored for it in meta_v2.
//
// synccontroller's orphaned-sync GC cannot send the real object (it is gone from
// K8s) and older cloudcores stamp no apiVersion/kind on the stub they fabricate,
// which this codec rejects. The row we hold is both the missing GVK and the last
// known state of the object, so the stored object is returned as the delete
// event: watchers then see a normal deletion instead of nothing at all.
//
// Only deletes are recovered; every other operation carries a real object whose
// GVK the edgecontroller has already stamped via util.SetMetaType. The result is
// accepted only if it addresses exactly the row that was matched, so a bad match
// is a no-op rather than a delete of some other object.
func (s *imitator) recoverDeletedObj(msg *model.Message, body []byte, op watch.EventType) (*unstructured.Unstructured, error) {
	if op != watch.Deleted {
		return nil, fmt.Errorf("operation %q is not a delete", msg.Router.Operation)
	}

	var stub struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
			UID       string `json:"uid"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &stub); err != nil {
		return nil, err
	}
	if stub.Metadata.Name == "" {
		return nil, fmt.Errorf("message content carries no metadata.name")
	}

	_, resType, _ := parseResource(msg.Router.Resource)
	if resType == "" {
		return nil, fmt.Errorf("no resource type in message resource %q", msg.Router.Resource)
	}

	namespace := stub.Metadata.Namespace
	if namespace == v2.NullNamespace {
		namespace = ""
	}
	rows, err := v2.RawMetaByNN(namespace, stub.Metadata.Name)
	if err != nil {
		return nil, err
	}

	// The message resource type is the lower-cased kind, so compare against the
	// kind implied by each stored row rather than pluralising resType: that
	// direction is lossy for names such as serviceaccountaccess.
	var matched *v2.MetaV2
	for i := range *rows {
		row := &(*rows)[i]
		gvr, _, _ := metaserver.ParseKey(row.Key)
		if gvr.Resource == "" || !strings.EqualFold(util.UnsafeResourceToKind(gvr.Resource), resType) {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("more than one meta_v2 row matches %s/%s type %s, refusing to guess",
				namespace, stub.Metadata.Name, resType)
		}
		matched = row
	}
	if matched == nil {
		return nil, fmt.Errorf("no meta_v2 row for %s/%s type %s", namespace, stub.Metadata.Name, resType)
	}

	stored := new(unstructured.Unstructured)
	if err := runtime.DecodeInto(s.codec, []byte(matched.Value), stored); err != nil {
		return nil, fmt.Errorf("stored row %s does not decode: %v", matched.Key, err)
	}

	// Same guard the v1 path applies when it deletes by key and UID: never drop a
	// row that belongs to a different incarnation of the same name.
	if storedUID := string(stored.GetUID()); stub.Metadata.UID != "" && storedUID != "" && storedUID != stub.Metadata.UID {
		return nil, fmt.Errorf("uid mismatch for %s: message has %s, stored row has %s",
			matched.Key, stub.Metadata.UID, storedUID)
	}

	key, err := metaserver.KeyFuncObj(stored)
	if err != nil {
		return nil, err
	}
	if key != matched.Key {
		return nil, fmt.Errorf("stored object addresses key %q, not the matched row %q", key, matched.Key)
	}
	return stored, nil
}

// Resource format: <namespace>/<restype>[/resid]
// return <reskey, restype, resid>
func parseResource(resource string) (string, string, string) {
	tokens := strings.Split(resource, constants.ResourceSep)
	resType := ""
	resID := ""
	switch len(tokens) {
	case 2:
		resType = tokens[len(tokens)-1]
	case 3:
		resType = tokens[len(tokens)-2]
		resID = tokens[len(tokens)-1]
	default:
	}
	return resource, resType, resID
}
