/*
Copyright 2026 The KubeEdge Authors.

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

package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

type fakeRequestAuth struct {
	resp *authenticator.Response
	ok   bool
	err  error
}

func (f *fakeRequestAuth) AuthenticateRequest(*http.Request) (*authenticator.Response, bool, error) {
	return f.resp, f.ok, f.err
}

func requestWithToken(t *testing.T, token string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://127.0.0.1:10550/api/v1/pods", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

func newQuiet(inner authenticator.Request) (*quietRefusals, *[]string) {
	var logged []string
	q := QuietRefusals(inner).(*quietRefusals)
	q.log = func(err error, subject string) { logged = append(logged, err.Error()+" | "+subject) }
	return q, &logged
}

func TestQuietRefusalsLogsOncePerTokenAndReason(t *testing.T) {
	inner := &fakeRequestAuth{err: errors.New("tokenData not found when authenticating")}
	q, logged := newQuiet(inner)

	for i := 0; i < 3; i++ {
		resp, ok, err := q.AuthenticateRequest(requestWithToken(t, "a.b.c"))
		if resp != nil || ok || err != nil {
			t.Fatalf("refused request = (%v, %v, %v), want (nil, false, nil)", resp, ok, err)
		}
	}
	if len(*logged) != 1 {
		t.Fatalf("logged %d times for one token and reason, want 1: %v", len(*logged), *logged)
	}

	q.AuthenticateRequest(requestWithToken(t, "d.e.f"))
	if len(*logged) != 2 {
		t.Fatalf("another token: logged %d times in total, want 2", len(*logged))
	}

	inner.err = errors.New("service account token has expired")
	q.AuthenticateRequest(requestWithToken(t, "a.b.c"))
	if len(*logged) != 3 {
		t.Fatalf("same token, new reason: logged %d times in total, want 3", len(*logged))
	}
}

func TestQuietRefusalsPassesSuccessThrough(t *testing.T) {
	want := &authenticator.Response{User: &user.DefaultInfo{Name: "system:serviceaccount:kube-system:coredns-edge"}}
	q, logged := newQuiet(&fakeRequestAuth{resp: want, ok: true})

	resp, ok, err := q.AuthenticateRequest(requestWithToken(t, "a.b.c"))
	if resp != want || !ok || err != nil {
		t.Fatalf("authenticated request = (%v, %v, %v), want (%v, true, nil)", resp, ok, err, want)
	}
	if len(*logged) != 0 {
		t.Fatalf("logged %v for an authenticated request", *logged)
	}
}

func TestQuietRefusalsForgetsWhenFull(t *testing.T) {
	q, logged := newQuiet(&fakeRequestAuth{err: errors.New("refused")})
	for i := 0; i <= maxLoggedRefusals; i++ {
		q.AuthenticateRequest(requestWithToken(t, fmt.Sprintf("t%d.x.y", i)))
	}
	if len(q.logged) > maxLoggedRefusals {
		t.Fatalf("remembered %d refusals, want at most %d", len(q.logged), maxLoggedRefusals)
	}
	if len(*logged) != maxLoggedRefusals+1 {
		t.Fatalf("logged %d refusals of distinct tokens, want %d", len(*logged), maxLoggedRefusals+1)
	}
}

func TestUnverifiedSubject(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString([]byte(
		`{"sub":"system:serviceaccount:kube-system:calico-node-edge","kubernetes.io":{"pod":{"name":"calico-node-edge-x7k2p"}}}`))
	got := unverifiedSubject("Bearer header." + payload + ".sig")
	want := "system:serviceaccount:kube-system:calico-node-edge pod=calico-node-edge-x7k2p"
	if got != want {
		t.Errorf("unverifiedSubject = %q, want %q", got, want)
	}
	if got := unverifiedSubject("Bearer not-a-jwt"); got != "" {
		t.Errorf("unverifiedSubject of a non-JWT = %q, want empty", got)
	}
}
