package metamanager

import (
	"encoding/json"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/message"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/client"
)

// kubelet asks for a projected token without audiences; the API server answers with
// its default audiences. The row must be keyed so that kubelet's next request finds it.
func TestTokenRowForRequestKeysByTheRequest(t *testing.T) {
	exp := int64(172800)
	ref := &authenticationv1.BoundObjectReference{Kind: "Pod", APIVersion: "v1", Name: "coredns-edge-9srsb", UID: "92070d8a"}
	asked := &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &exp, BoundObjectRef: ref},
	}
	reply := asked.DeepCopy()
	reply.Spec.Audiences = []string{"https://kubernetes.default.svc"}
	reply.Status = authenticationv1.TokenRequestStatus{
		Token:               "tok",
		ExpirationTimestamp: metav1.NewTime(time.Now().Add(48 * time.Hour).Truncate(time.Second)),
	}

	resource := "kube-system/" + model.ResourceTypeServiceAccountToken + "/coredns-edge"
	req := message.BuildMsg(modules.MetaGroup, "", modules.EdgedModuleName, resource, model.QueryOperation, asked)
	resp := model.NewMessage(req.GetID()).BuildRouter("edgecontroller", "resource", resource, model.ResponseOperation).FillBody(reply)

	row := tokenRowForRequest(req, resp)
	key, resType, _ := parseResource(row)
	if resType != model.ResourceTypeServiceAccountToken {
		t.Fatalf("resource type = %q", resType)
	}

	// What kubelet's next request looks up, with the reply's token and expiry.
	want := asked.DeepCopy()
	want.Status = reply.Status
	if wantKey := client.KeyFunc("coredns-edge", "kube-system", want); key != wantKey {
		t.Errorf("row key\n got %s\nwant %s", key, wantKey)
	}

	var stored authenticationv1.TokenRequest
	data, err := row.GetContentData()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Token != "tok" || len(stored.Spec.Audiences) != 0 {
		t.Errorf("stored token %q with audiences %v, want the reply's token under the request's spec", stored.Status.Token, stored.Spec.Audiences)
	}

	// The reply that goes back to edged is not changed.
	if got := resp.Content.(*authenticationv1.TokenRequest).Spec.Audiences; len(got) != 1 {
		t.Errorf("reply audiences = %v, want the API server's", got)
	}
}

func TestTokenRowForRequestLeavesOtherResources(t *testing.T) {
	req := message.BuildMsg(modules.MetaGroup, "", modules.EdgedModuleName, "default/configmap/cm", model.QueryOperation, nil)
	resp := model.NewMessage(req.GetID()).FillBody(map[string]string{"a": "b"})
	if got := tokenRowForRequest(req, resp); got != resp {
		t.Error("a configmap reply was changed")
	}
}
