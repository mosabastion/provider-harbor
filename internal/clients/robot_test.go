package clients

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	harbormodels "github.com/goharbor/go-client/pkg/sdk/v2.0/models"
	"k8s.io/utils/ptr"
)

func observedPerm(kind, ns string, actions ...string) *harbormodels.RobotPermission {
	p := &harbormodels.RobotPermission{Kind: kind, Namespace: ns}
	for _, a := range actions {
		// effect is Harbor-assigned and must never count as drift.
		p.Access = append(p.Access, &harbormodels.Access{Resource: "repository", Action: a, Effect: "allow"})
	}
	return p
}

func TestRobotPermissionsDrifted(t *testing.T) {
	projectSpec := func(actions ...string) *RobotSpec {
		return &RobotSpec{ProjectID: ptr.To("16"), Permissions: []RobotPermission{{Namespace: "repository", Access: actions}}}
	}
	systemSpec := func(perms ...RobotPermission) *RobotSpec {
		return &RobotSpec{Level: "system", Permissions: perms}
	}
	tests := map[string]struct {
		spec     *RobotSpec
		observed *RobotStatus
		want     bool
	}{
		"project unchanged": {
			projectSpec("pull", "push"),
			&RobotStatus{Level: "project", Permissions: []*harbormodels.RobotPermission{observedPerm("project", "tenant-acme", "pull", "push")}},
			false,
		},
		"project reordered": {
			projectSpec("push", "pull"),
			&RobotStatus{Level: "project", Permissions: []*harbormodels.RobotPermission{observedPerm("project", "tenant-acme", "pull", "push")}},
			false,
		},
		"project added": {
			projectSpec("pull", "push"),
			&RobotStatus{Level: "project", Permissions: []*harbormodels.RobotPermission{observedPerm("project", "tenant-acme", "pull")}},
			true,
		},
		"project removed access": {
			projectSpec("pull"),
			&RobotStatus{Level: "project", Permissions: []*harbormodels.RobotPermission{observedPerm("project", "tenant-acme", "pull", "push")}},
			true,
		},
		"system unchanged with scope star": {
			systemSpec(RobotPermission{Namespace: "repository", Access: []string{"pull"}, Kind: ptr.To("project"), Scope: ptr.To("*")}),
			&RobotStatus{Level: "system", Permissions: []*harbormodels.RobotPermission{observedPerm("project", "*", "pull")}},
			false,
		},
		"system default kind round-trips to system /": {
			systemSpec(RobotPermission{Namespace: "repository", Access: []string{"pull"}}),
			&RobotStatus{Level: "system", Permissions: []*harbormodels.RobotPermission{observedPerm("system", "/", "pull")}},
			false,
		},
		"system kind changed": {
			systemSpec(RobotPermission{Namespace: "repository", Access: []string{"pull"}, Kind: ptr.To("project"), Scope: ptr.To("*")}),
			&RobotStatus{Level: "system", Permissions: []*harbormodels.RobotPermission{observedPerm("system", "*", "pull")}},
			true,
		},
		"system scope changed": {
			systemSpec(RobotPermission{Namespace: "repository", Access: []string{"pull"}, Kind: ptr.To("project"), Scope: ptr.To("a")}),
			&RobotStatus{Level: "system", Permissions: []*harbormodels.RobotPermission{observedPerm("project", "b", "pull")}},
			true,
		},
		"system added": {
			systemSpec(RobotPermission{Namespace: "repository", Access: []string{"pull", "push"}}),
			&RobotStatus{Level: "system", Permissions: []*harbormodels.RobotPermission{observedPerm("system", "/", "pull")}},
			true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := RobotPermissionsDrifted(tc.spec, tc.observed); got != tc.want {
				t.Errorf("RobotPermissionsDrifted = %v, want %v", got, tc.want)
			}
		})
	}
}

// Update must PUT the new permissions without a secret field, and return no
// secret, so the connection secret is never rotated by a permission change.
func TestRobotClient_UpdateSendsPermissionsNoSecret(t *testing.T) {
	var putBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2.0/robots/7", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &putBody)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"id":7,"name":"robot$x","level":"system","permissions":[{"kind":"project","namespace":"*","access":[{"resource":"repository","action":"pull","effect":"allow"}]}]}`))
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := newTestClient(t, srv.URL)

	st, err := c.UpdateRobot(context.Background(), "7", &RobotSpec{
		Name:        "x",
		Level:       "system",
		Permissions: []RobotPermission{{Namespace: "repository", Access: []string{"pull"}, Kind: ptr.To("project"), Scope: ptr.To("*")}},
	})
	if err != nil {
		t.Fatalf("UpdateRobot: %v", err)
	}
	if _, has := putBody["secret"]; has {
		t.Errorf("PUT body must not carry a secret, got %v", putBody)
	}
	if perms, _ := putBody["permissions"].([]any); len(perms) != 1 {
		t.Errorf("PUT body must carry the permissions, got %v", putBody["permissions"])
	}
	if st.Secret != "" {
		t.Errorf("Update must not return a secret, got %q", st.Secret)
	}
	if st.Level != "system" || len(st.Permissions) != 1 || st.Permissions[0].Namespace != "*" {
		t.Errorf("GetRobot must return Level and Permissions, got level=%q perms=%v", st.Level, st.Permissions)
	}
}
