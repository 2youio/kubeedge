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
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/klog/v2"
)

// maxLoggedRefusals bounds the memory of refusals already logged. When it is
// full, it is emptied, so a refusal may then be logged once more.
const maxLoggedRefusals = 4096

type quietRefusals struct {
	auth authenticator.Request
	log  func(err error, subject string)

	mu     sync.Mutex
	logged map[[sha256.Size]byte]struct{}
}

// QuietRefusals wraps a request authenticator so that a refused request is
// logged once per token and reason instead of on every request. A node cut
// off from cloudcore otherwise logs each retry of every pod at error level.
func QuietRefusals(auth authenticator.Request) authenticator.Request {
	return &quietRefusals{
		auth: auth,
		log: func(err error, subject string) {
			klog.ErrorS(err, "Unable to authenticate the request; later refusals of this token for the same reason are not logged",
				"unverifiedSubject", subject)
		},
		logged: make(map[[sha256.Size]byte]struct{}),
	}
}

// AuthenticateRequest returns no error for a refused request, so that the
// authentication filter does not log it again; the request still gets 401.
func (q *quietRefusals) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	header := req.Header.Get("Authorization")
	resp, ok, err := q.auth.AuthenticateRequest(req)
	if err == nil {
		return resp, ok, nil
	}

	key := sha256.Sum256([]byte(header + "\x00" + err.Error()))
	q.mu.Lock()
	_, seen := q.logged[key]
	if !seen {
		if len(q.logged) >= maxLoggedRefusals {
			clear(q.logged)
		}
		q.logged[key] = struct{}{}
	}
	q.mu.Unlock()

	if !seen {
		q.log(err, unverifiedSubject(header))
	}
	return nil, false, nil
}

// unverifiedSubject names the service account and pod a bearer token claims,
// for the log only. The claims are not verified.
func unverifiedSubject(header string) string {
	token := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(header), "Bearer"))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Subject    string     `json:"sub"`
		Kubernetes kubernetes `json:"kubernetes.io"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return ""
	}
	if claims.Kubernetes.Pod != nil {
		return claims.Subject + " pod=" + claims.Kubernetes.Pod.Name
	}
	return claims.Subject
}
