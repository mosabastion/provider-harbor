/*
Proof test for the real Replication client implementation.

Runs the actual HarborClient.{Create,List,Get,Update,Delete}ReplicationPolicy methods
against a stateful in-memory fake of Harbor's /api/v2.0 replication policy API (httptest).
This exercises the real goharbor request/response path — it does NOT hit a live Harbor
and uses no real credentials. It demonstrates the methods are genuinely implemented
(issue the right HTTP verbs/paths, parse the real policy ID, and map 404 -> not-found)
rather than returning hardcoded stubs.
*/
package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"k8s.io/utils/ptr"
)

// fakeReplicationServer serves a stateful in-memory Harbor replication policy API.
func fakeReplicationServer(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	type policy struct {
		ID      int64
		Name    string
		Enabled bool
		DestReg string
	}
	policies := map[int64]*policy{}
	nextID := int64(10)

	mux := http.NewServeMux()

	// GET /api/v2.0/registries?name=... -> registry lookup for dest-registry id
	// resolution (replication policies reference registries by numeric id).
	mux.HandleFunc("/api/v2.0/registries", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":7,"name":"my-dest-registry","url":"https://dst.example.com"}]`))
	})

	// POST /api/v2.0/replication/policies -> 201 with Location header
	// GET  /api/v2.0/replication/policies -> 200 list
	mux.HandleFunc("/api/v2.0/replication/policies", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			var body struct {
				Name     string `json:"name"`
				Enabled  bool   `json:"enabled"`
				DestName string `json:"-"`
			}
			// parse body to get name and dest_registry
			var raw map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&raw)
			if v, ok := raw["name"].(string); ok {
				body.Name = v
			}
			if v, ok := raw["enabled"].(bool); ok {
				body.Enabled = v
			}
			nextID++
			id := nextID
			destReg := ""
			if dr, ok := raw["dest_registry"].(map[string]interface{}); ok {
				if n, ok := dr["name"].(string); ok {
					destReg = n
				}
			}
			policies[id] = &policy{ID: id, Name: body.Name, Enabled: body.Enabled, DestReg: destReg}
			w.Header().Set("Location", "/api/v2.0/replication/policies/"+strconv.FormatInt(id, 10))
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			list := make([]map[string]interface{}, 0, len(policies))
			for _, p := range policies {
				list = append(list, map[string]interface{}{
					"id":      p.ID,
					"name":    p.Name,
					"enabled": p.Enabled,
				})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(list)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	// GET/PUT/DELETE /api/v2.0/replication/policies/{id}
	mux.HandleFunc("/api/v2.0/replication/policies/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		idStr := strings.TrimPrefix(r.URL.Path, "/api/v2.0/replication/policies/")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		p, ok := policies[id]
		switch r.Method {
		case http.MethodGet:
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"errors":[{"code":"NOT_FOUND","message":"policy not found"}]}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"id":      p.ID,
				"name":    p.Name,
				"enabled": p.Enabled,
			})
		case http.MethodPut:
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			var raw map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&raw)
			if v, ok := raw["name"].(string); ok {
				p.Name = v
			}
			if v, ok := raw["enabled"].(bool); ok {
				p.Enabled = v
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			delete(policies, id)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	return httptest.NewServer(mux)
}

func TestReplicationPolicyClient_RealCRUD(t *testing.T) {
	srv := fakeReplicationServer(t)
	defer srv.Close()
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	spec := &ReplicationPolicySpec{
		Name:    "proof-replication",
		Enabled: boolPtr(true),
		Trigger: "manual",
		DestinationReg: &ReplicationPolicyDestination{
			Name: "my-dest-registry",
			URL:  "https://registry.example.com",
		},
	}

	// Create -> returns real ID, not "1".
	st, err := c.CreateReplicationPolicy(ctx, spec)
	if err != nil {
		t.Fatalf("CreateReplicationPolicy: %v", err)
	}
	if st.ID == "1" {
		t.Error("expected real policy ID from API, got stub value '1'")
	}
	if st.ID == "" {
		t.Error("expected non-empty policy ID")
	}
	if st.Name != "proof-replication" {
		t.Errorf("expected name proof-replication, got %q", st.Name)
	}
	savedID := st.ID
	t.Logf("created policy ID: %s", savedID)

	// List -> policy appears.
	list, err := c.ListReplicationPolicies(ctx)
	if err != nil {
		t.Fatalf("ListReplicationPolicies: %v", err)
	}
	found := false
	for _, p := range list {
		if p.Name == "proof-replication" {
			found = true
		}
	}
	if !found {
		t.Error("expected policy to appear in list")
	}

	// GetReplicationPolicy by ID.
	got, err := c.GetReplicationPolicy(ctx, savedID)
	if err != nil {
		t.Fatalf("GetReplicationPolicy: %v", err)
	}
	if got == nil {
		t.Fatal("expected policy to exist")
	}
	if got.Name != "proof-replication" {
		t.Errorf("expected name proof-replication, got %q", got.Name)
	}

	// Update -> change name (a non-omitempty field), verify round-trip.
	// Note: SDK model has enabled bool with omitempty so false is never sent;
	// we verify the update path succeeds (200 OK) and re-read returns the
	// policy (showing GET after PUT still works and ID is stable).
	updSpec := &ReplicationPolicySpec{
		Name:    "proof-replication-updated",
		Enabled: boolPtr(true),
		Trigger: "manual",
		DestinationReg: &ReplicationPolicyDestination{
			Name: "my-dest-registry",
			URL:  "https://registry.example.com",
		},
	}
	if _, err := c.UpdateReplicationPolicy(ctx, savedID, updSpec); err != nil {
		t.Fatalf("UpdateReplicationPolicy: %v", err)
	}
	got2, err := c.GetReplicationPolicy(ctx, savedID)
	if err != nil {
		t.Fatalf("GetReplicationPolicy after update: %v", err)
	}
	if got2.Name != "proof-replication-updated" {
		t.Errorf("expected name proof-replication-updated after update, got %q", got2.Name)
	}

	// Delete -> gone.
	if err := c.DeleteReplicationPolicy(ctx, savedID); err != nil {
		t.Fatalf("DeleteReplicationPolicy: %v", err)
	}
	gone, err := c.GetReplicationPolicy(ctx, savedID)
	if err != nil || gone != nil {
		t.Fatalf("expected (nil,nil) after delete, got gone=%v err=%v", gone, err)
	}

	// Idempotent delete -> no error.
	if err := c.DeleteReplicationPolicy(ctx, savedID); err != nil {
		t.Errorf("second delete should be idempotent, got %v", err)
	}
}

func boolPtr(b bool) *bool { return &b }

// fakePullReplicationServer serves a two-registry Harbor fake (source +
// destination, name-filtered lookup) and captures the raw create-policy
// request body it receives, so tests can assert on the exact JSON shape sent
// to Harbor (src_registry/dest_registry/trigger_settings.cron).
func fakePullReplicationServer(t *testing.T, lastBody *map[string]interface{}) *httptest.Server {
	t.Helper()
	registries := map[string]int64{
		"docker-hub":       5,
		"my-dest-registry": 7,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2.0/registries", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		name := r.URL.Query().Get("name")
		if id, ok := registries[name]; ok {
			_, _ = fmt.Fprintf(w, `[{"id":%d,"name":%q}]`, id, name)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	})
	mux.HandleFunc("/api/v2.0/replication/policies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var raw map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&raw)
		*lastBody = raw
		w.Header().Set("Location", "/api/v2.0/replication/policies/42")
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("/api/v2.0/replication/policies/42", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": 42, "name": "pull-policy"})
	})
	return httptest.NewServer(mux)
}

// TestCreateReplicationPolicy_PullMode_SendsSourceRegistryNoDestRegistry proves
// pull-mode's core request shape: the source registry resolves to its numeric
// id, a local (name-less) destination sends no dest_registry at all, and
// dest_namespace still carries the destination project.
func TestCreateReplicationPolicy_PullMode_SendsSourceRegistryNoDestRegistry(t *testing.T) {
	var body map[string]interface{}
	srv := fakePullReplicationServer(t, &body)
	defer srv.Close()
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	spec := &ReplicationPolicySpec{
		Name:           "pull-policy",
		SourceRegistry: ptr.To("docker-hub"),
		Trigger:        "manual",
		Enabled:        boolPtr(true),
		DestinationReg: &ReplicationPolicyDestination{Namespace: "my-project"},
	}

	if _, err := c.CreateReplicationPolicy(ctx, spec); err != nil {
		t.Fatalf("CreateReplicationPolicy: %v", err)
	}

	src, ok := body["src_registry"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected src_registry in request body, got %v", body)
	}
	if id, _ := src["id"].(float64); int64(id) != 5 {
		t.Errorf("expected src_registry.id 5, got %v", src["id"])
	}
	if _, ok := body["dest_registry"]; ok {
		t.Errorf("expected no dest_registry for a local-Harbor destination, got %v", body["dest_registry"])
	}
	if ns, _ := body["dest_namespace"].(string); ns != "my-project" {
		t.Errorf("expected dest_namespace 'my-project', got %q", ns)
	}
}

// TestCreateReplicationPolicy_NilDestinationDoesNotError proves omitting
// destinationReg entirely (not just an empty name) is a valid local-Harbor
// pull request, not the old hard "destination registry is required" error.
func TestCreateReplicationPolicy_NilDestinationDoesNotError(t *testing.T) {
	var body map[string]interface{}
	srv := fakePullReplicationServer(t, &body)
	defer srv.Close()
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	spec := &ReplicationPolicySpec{
		Name:    "pull-policy-no-dest",
		Trigger: "manual",
		Enabled: boolPtr(true),
	}

	if _, err := c.CreateReplicationPolicy(ctx, spec); err != nil {
		t.Fatalf("CreateReplicationPolicy with nil destination should not error, got %v", err)
	}
	if _, ok := body["dest_registry"]; ok {
		t.Errorf("expected no dest_registry, got %v", body["dest_registry"])
	}
}

// TestCreateReplicationPolicy_ScheduledTrigger_CronReachesTriggerSettings
// proves the CR's cron field actually lands on Harbor's
// trigger.trigger_settings.cron, not just the bare trigger type.
func TestCreateReplicationPolicy_ScheduledTrigger_CronReachesTriggerSettings(t *testing.T) {
	var body map[string]interface{}
	srv := fakePullReplicationServer(t, &body)
	defer srv.Close()
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	cron := "0 0 2 * * *"
	spec := &ReplicationPolicySpec{
		Name:    "scheduled-policy",
		Trigger: "scheduled",
		Cron:    &cron,
		Enabled: boolPtr(true),
	}

	if _, err := c.CreateReplicationPolicy(ctx, spec); err != nil {
		t.Fatalf("CreateReplicationPolicy: %v", err)
	}

	trigger, ok := body["trigger"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected trigger in request body, got %v", body)
	}
	settings, ok := trigger["trigger_settings"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected trigger_settings, got %v", trigger)
	}
	if settings["cron"] != cron {
		t.Errorf("expected cron %q, got %v", cron, settings["cron"])
	}
}

// TestCreateReplicationPolicy_ScheduledTriggerWithoutCronErrors proves the
// client enforces cron as required when trigger is scheduled, since the CRD
// itself can't (Harbor accepts several cron dialects).
func TestCreateReplicationPolicy_ScheduledTriggerWithoutCronErrors(t *testing.T) {
	var body map[string]interface{}
	srv := fakePullReplicationServer(t, &body)
	defer srv.Close()
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	spec := &ReplicationPolicySpec{Name: "no-cron", Trigger: "scheduled"}
	if _, err := c.CreateReplicationPolicy(ctx, spec); err == nil {
		t.Error("expected an error when trigger is scheduled without a cron")
	}
}

// TestCreateReplicationPolicy_UnknownSourceRegistryErrors proves a source
// registry name that doesn't resolve returns a clear, actionable error
// instead of silently falling back to local (or a raw Harbor 400).
func TestCreateReplicationPolicy_UnknownSourceRegistryErrors(t *testing.T) {
	var body map[string]interface{}
	srv := fakePullReplicationServer(t, &body)
	defer srv.Close()
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	spec := &ReplicationPolicySpec{
		Name:           "bad-source",
		SourceRegistry: ptr.To("does-not-exist"),
		Trigger:        "manual",
	}
	_, err := c.CreateReplicationPolicy(ctx, spec)
	if err == nil {
		t.Fatal("expected an error for an unknown source registry")
	}
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Errorf("expected error to name the missing registry, got %v", err)
	}
}
