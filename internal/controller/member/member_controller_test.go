/*
Copyright 2024 Crossplane Harbor Provider.
*/

package member

import (
	"context"
	"errors"
	"testing"
	"time"

	xpv1 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rossigee/provider-harbor/apis/member/v1beta1"
	harborclients "github.com/rossigee/provider-harbor/internal/clients"
)

// ptrString returns a pointer to the given string value.
func ptrString(s string) *string { return &s }

// ptrInt64 returns a pointer to the given int64 value.
func ptrInt64(i int64) *int64 { return &i }

// newUserMember returns a Member CR with type=user and the given username.
func newUserMember(username, role string) *v1beta1.Member {
	return &v1beta1.Member{
		ObjectMeta: metav1.ObjectMeta{Name: "test-member", Namespace: "default"},
		Spec: v1beta1.MemberSpec{
			ForProvider: v1beta1.MemberParameters{
				ProjectID: "project-1",
				Type:      "user",
				Username:  ptrString(username),
				Role:      role,
			},
		},
	}
}

// ---- nil-check guards -------------------------------------------------------

func TestConnectNotMember(t *testing.T) {
	ctx := context.Background()
	_, err := (&connector{}).Connect(ctx, nil)
	if err == nil || err.Error() != errNotMember {
		t.Errorf("expected %q error, got %v", errNotMember, err)
	}
}

func TestObserveNotMember(t *testing.T) {
	ctx := context.Background()
	_, err := (&external{}).Observe(ctx, nil)
	if err == nil || err.Error() != errNotMember {
		t.Errorf("expected %q error, got %v", errNotMember, err)
	}
}

func TestCreateNotMember(t *testing.T) {
	ctx := context.Background()
	_, err := (&external{}).Create(ctx, nil)
	if err == nil || err.Error() != errNotMember {
		t.Errorf("expected %q error, got %v", errNotMember, err)
	}
}

func TestUpdateNotMember(t *testing.T) {
	ctx := context.Background()
	_, err := (&external{}).Update(ctx, nil)
	if err == nil || err.Error() != errNotMember {
		t.Errorf("expected %q error, got %v", errNotMember, err)
	}
}

func TestDeleteNotMember(t *testing.T) {
	ctx := context.Background()
	_, err := (&external{}).Delete(ctx, nil)
	if err == nil || err.Error() != errNotMember {
		t.Errorf("expected %q error, got %v", errNotMember, err)
	}
}

// ---- Observe (name-based path: no external-name set) -----------------------

func TestObserveMemberNotFound(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")

	ext := &external{service: &mockMemberClient{
		findProjectMemberFunc: func(_ context.Context, _, _, _ string) (*harborclients.MemberStatus, error) {
			return nil, nil
		},
	}}

	obs, err := ext.Observe(ctx, cr)
	if err != nil {
		t.Fatalf("Observe returned error on not-found: %v", err)
	}
	if obs.ResourceExists {
		t.Error("ResourceExists should be false when member not found")
	}
}

func TestObserveMemberError(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")

	ext := &external{service: &mockMemberClient{
		findProjectMemberFunc: func(_ context.Context, _, _, _ string) (*harborclients.MemberStatus, error) {
			return nil, errors.New("boom")
		},
	}}

	if _, err := ext.Observe(ctx, cr); err == nil {
		t.Error("Observe should surface a real client error")
	}
}

func TestObserveMemberExistsByName(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")

	ext := &external{service: &mockMemberClient{
		findProjectMemberFunc: func(_ context.Context, _, _, _ string) (*harborclients.MemberStatus, error) {
			return &harborclients.MemberStatus{
				ID:           "42",
				MemberName:   "testuser",
				MemberType:   "u",
				Role:         "developer",
				CreationTime: time.Now(),
			}, nil
		},
	}}

	obs, err := ext.Observe(ctx, cr)
	if err != nil {
		t.Fatalf("Observe error: %v", err)
	}
	if !obs.ResourceExists {
		t.Error("ResourceExists should be true")
	}
	if !obs.ResourceUpToDate {
		t.Error("ResourceUpToDate should be true when roles match")
	}
	// crossplane-runtime v2 requires the controller to call Available() itself.
	if cr.GetCondition(xpv1.TypeReady).Status != corev1.ConditionTrue {
		t.Error("Ready condition should be True (Available) after Observe")
	}
}

func TestObserveMemberNotUpToDate(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "admin")

	ext := &external{service: &mockMemberClient{
		findProjectMemberFunc: func(_ context.Context, _, _, _ string) (*harborclients.MemberStatus, error) {
			return &harborclients.MemberStatus{
				ID: "42", MemberName: "testuser", MemberType: "u", Role: "developer",
			}, nil
		},
	}}

	obs, err := ext.Observe(ctx, cr)
	if err != nil {
		t.Fatalf("Observe error: %v", err)
	}
	if !obs.ResourceExists {
		t.Error("ResourceExists should be true")
	}
	if obs.ResourceUpToDate {
		t.Error("ResourceUpToDate should be false when roles differ")
	}
}

// ---- Observe (id-based path: external-name already set) --------------------

func TestObserveMemberByID(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")
	cr.Annotations = map[string]string{"crossplane.io/external-name": "42"}

	ext := &external{service: &mockMemberClient{
		getProjectMemberByIDFunc: func(_ context.Context, _, _ string) (*harborclients.MemberStatus, error) {
			return &harborclients.MemberStatus{
				ID: "42", MemberName: "testuser", MemberType: "u", Role: "developer",
			}, nil
		},
	}}

	obs, err := ext.Observe(ctx, cr)
	if err != nil {
		t.Fatalf("Observe error: %v", err)
	}
	if !obs.ResourceExists {
		t.Error("ResourceExists should be true")
	}
}

func TestObserveMemberByIDNotFound(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")
	cr.Annotations = map[string]string{"crossplane.io/external-name": "42"}

	ext := &external{service: &mockMemberClient{
		getProjectMemberByIDFunc: func(_ context.Context, _, _ string) (*harborclients.MemberStatus, error) {
			return nil, nil
		},
	}}

	obs, err := ext.Observe(ctx, cr)
	if err != nil {
		t.Fatalf("Observe error: %v", err)
	}
	if obs.ResourceExists {
		t.Error("ResourceExists should be false when member not found by ID")
	}
}

// ---- Create ----------------------------------------------------------------

func TestCreateUserMemberSuccess(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")

	ext := &external{service: &mockMemberClient{
		addProjectUserMemberFunc: func(_ context.Context, _, _, _ string) (string, error) {
			return "42", nil
		},
	}}

	if _, err := ext.Create(ctx, cr); err != nil {
		t.Errorf("Create should not fail, got %v", err)
	}
}

func TestCreateUserMemberError(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")

	ext := &external{service: &mockMemberClient{
		addProjectUserMemberFunc: func(_ context.Context, _, _, _ string) (string, error) {
			return "", errors.New("create failed")
		},
	}}

	if _, err := ext.Create(ctx, cr); err == nil {
		t.Error("Create should fail when client fails")
	}
}

func TestCreateGroupMemberSuccess(t *testing.T) {
	ctx := context.Background()
	cr := &v1beta1.Member{
		ObjectMeta: metav1.ObjectMeta{Name: "test-member", Namespace: "default"},
		Spec: v1beta1.MemberSpec{
			ForProvider: v1beta1.MemberParameters{
				ProjectID: "project-1",
				Type:      "group",
				GroupName: ptrString("devs"),
				GroupType: ptrInt64(2),
				Role:      "developer",
			},
		},
	}

	ext := &external{service: &mockMemberClient{
		addProjectGroupMemberFunc: func(_ context.Context, _, _ string, _ int64, _ string) (string, error) {
			return "43", nil
		},
	}}

	if _, err := ext.Create(ctx, cr); err != nil {
		t.Errorf("Create group member should not fail, got %v", err)
	}
}

func TestCreateUnknownTypeError(t *testing.T) {
	ctx := context.Background()
	cr := &v1beta1.Member{
		ObjectMeta: metav1.ObjectMeta{Name: "test-member"},
		Spec: v1beta1.MemberSpec{
			ForProvider: v1beta1.MemberParameters{
				ProjectID: "project-1",
				Type:      "robot",
				Role:      "developer",
			},
		},
	}

	if _, err := (&external{service: &mockMemberClient{}}).Create(ctx, cr); err == nil {
		t.Error("Create with unknown type should return error")
	}
}

// ---- Update ----------------------------------------------------------------

func TestUpdateMemberSuccess(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "admin")
	cr.Annotations = map[string]string{"crossplane.io/external-name": "42"}

	ext := &external{service: &mockMemberClient{
		updateProjectMemberByIDFunc: func(_ context.Context, _, _, _ string) error { return nil },
	}}

	if _, err := ext.Update(ctx, cr); err != nil {
		t.Errorf("Update should not fail, got %v", err)
	}
}

func TestUpdateMemberError(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "admin")
	cr.Annotations = map[string]string{"crossplane.io/external-name": "42"}

	ext := &external{service: &mockMemberClient{
		updateProjectMemberByIDFunc: func(_ context.Context, _, _, _ string) error {
			return errors.New("update failed")
		},
	}}

	if _, err := ext.Update(ctx, cr); err == nil {
		t.Error("Update should fail when client fails")
	}
}

func TestUpdateMemberMissingID(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "admin")
	// No external-name annotation → memberID returns ""

	if _, err := (&external{service: &mockMemberClient{}}).Update(ctx, cr); err == nil {
		t.Error("Update should fail when member id is not known yet")
	}
}

// ---- Delete ----------------------------------------------------------------

func TestDeleteMemberSuccess(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")
	cr.Annotations = map[string]string{"crossplane.io/external-name": "42"}

	ext := &external{service: &mockMemberClient{
		deleteProjectMemberByIDFunc: func(_ context.Context, _, _ string) error { return nil },
	}}

	if _, err := ext.Delete(ctx, cr); err != nil {
		t.Errorf("Delete should not fail, got %v", err)
	}
}

func TestDeleteMemberError(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")
	cr.Annotations = map[string]string{"crossplane.io/external-name": "42"}

	ext := &external{service: &mockMemberClient{
		deleteProjectMemberByIDFunc: func(_ context.Context, _, _ string) error {
			return errors.New("delete failed")
		},
	}}

	if _, err := ext.Delete(ctx, cr); err == nil {
		t.Error("Delete should fail when client fails")
	}
}

func TestDeleteMemberNoIDIsNoOp(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("testuser", "developer")
	// No external-name → memberID returns "" → Delete is a no-op

	if _, err := (&external{service: &mockMemberClient{}}).Delete(ctx, cr); err != nil {
		t.Errorf("Delete with unknown id should be a no-op, got %v", err)
	}
}

// ---- Disconnect ------------------------------------------------------------

func TestDisconnect(t *testing.T) {
	ctx := context.Background()
	if err := (&external{service: &mockMemberClient{}}).Disconnect(ctx); err != nil {
		t.Errorf("Disconnect should not fail, got %v", err)
	}
}

// ---- Field validation (struct-level) ---------------------------------------

func TestMemberHasRequiredFields(t *testing.T) {
	cr := newUserMember("testuser", "admin")

	if cr.Spec.ForProvider.ProjectID == "" {
		t.Error("ProjectID should not be empty")
	}
	if cr.Spec.ForProvider.Type == "" {
		t.Error("Type should not be empty")
	}
	if cr.Spec.ForProvider.Username == nil || *cr.Spec.ForProvider.Username == "" {
		t.Error("Username should be set")
	}
	if cr.Spec.ForProvider.Role == "" {
		t.Error("Role should not be empty")
	}
}

func TestMemberStatusFields(t *testing.T) {
	cr := &v1beta1.Member{
		Status: v1beta1.MemberStatus{
			AtProvider: v1beta1.MemberObservation{
				ID:         ptrString("member-123"),
				MemberName: ptrString("testuser"),
				MemberType: ptrString("u"),
				Role:       ptrString("admin"),
			},
		},
	}

	if cr.Status.AtProvider.ID == nil || *cr.Status.AtProvider.ID != "member-123" {
		t.Errorf("Status ID should be 'member-123', got %v", cr.Status.AtProvider.ID)
	}
}

// ---- entityKey helper ------------------------------------------------------

func TestEntityKeyUser(t *testing.T) {
	cr := newUserMember("alice", "admin")
	eType, eName, err := entityKey(cr)
	if err != nil {
		t.Fatalf("entityKey error: %v", err)
	}
	if eType != "u" || eName != "alice" {
		t.Errorf("expected u/alice, got %s/%s", eType, eName)
	}
}

func TestEntityKeyGroup(t *testing.T) {
	cr := &v1beta1.Member{
		Spec: v1beta1.MemberSpec{
			ForProvider: v1beta1.MemberParameters{
				Type:      "group",
				GroupName: ptrString("devs"),
			},
		},
	}
	eType, eName, err := entityKey(cr)
	if err != nil {
		t.Fatalf("entityKey error: %v", err)
	}
	if eType != "g" || eName != "devs" {
		t.Errorf("expected g/devs, got %s/%s", eType, eName)
	}
}

func TestEntityKeyUserMissingUsername(t *testing.T) {
	cr := &v1beta1.Member{
		Spec: v1beta1.MemberSpec{
			ForProvider: v1beta1.MemberParameters{Type: "user"},
		},
	}
	if _, _, err := entityKey(cr); err == nil {
		t.Error("entityKey should error when type=user and Username is nil")
	}
}

func TestEntityKeyUnknownType(t *testing.T) {
	cr := &v1beta1.Member{
		Spec: v1beta1.MemberSpec{
			ForProvider: v1beta1.MemberParameters{Type: "robot"},
		},
	}
	if _, _, err := entityKey(cr); err == nil {
		t.Error("entityKey should error on unknown type")
	}
}

// ---- resolvedGroupType helper -----------------------------------------------

func TestResolvedGroupTypeDefault(t *testing.T) {
	cr := &v1beta1.Member{
		Spec: v1beta1.MemberSpec{
			ForProvider: v1beta1.MemberParameters{Type: "group", GroupName: ptrString("x")},
		},
	}
	if gt := resolvedGroupType(cr); gt != defaultGroupType {
		t.Errorf("expected default %d, got %d", defaultGroupType, gt)
	}
}

func TestResolvedGroupTypeOverride(t *testing.T) {
	cr := &v1beta1.Member{
		Spec: v1beta1.MemberSpec{
			ForProvider: v1beta1.MemberParameters{
				Type: "group", GroupName: ptrString("x"), GroupType: ptrInt64(1),
			},
		},
	}
	if gt := resolvedGroupType(cr); gt != 1 {
		t.Errorf("expected 1, got %d", gt)
	}
}

// ---- mock ------------------------------------------------------------------

type mockMemberClient struct {
	harborclients.HarborClienter
	findProjectMemberFunc       func(ctx context.Context, projectID, entityType, entityName string) (*harborclients.MemberStatus, error)
	getProjectMemberByIDFunc    func(ctx context.Context, projectID, memberID string) (*harborclients.MemberStatus, error)
	addProjectUserMemberFunc    func(ctx context.Context, projectID, username, role string) (string, error)
	addProjectGroupMemberFunc   func(ctx context.Context, projectID, groupName string, groupType int64, role string) (string, error)
	updateProjectMemberByIDFunc func(ctx context.Context, projectID, memberID, role string) error
	deleteProjectMemberByIDFunc func(ctx context.Context, projectID, memberID string) error
}

func (m *mockMemberClient) FindProjectMember(ctx context.Context, projectID, entityType, entityName string) (*harborclients.MemberStatus, error) {
	if m.findProjectMemberFunc != nil {
		return m.findProjectMemberFunc(ctx, projectID, entityType, entityName)
	}
	return nil, nil
}

func (m *mockMemberClient) GetProjectMemberByID(ctx context.Context, projectID, memberID string) (*harborclients.MemberStatus, error) {
	if m.getProjectMemberByIDFunc != nil {
		return m.getProjectMemberByIDFunc(ctx, projectID, memberID)
	}
	return nil, nil
}

func (m *mockMemberClient) AddProjectUserMember(ctx context.Context, projectID, username, role string) (string, error) {
	if m.addProjectUserMemberFunc != nil {
		return m.addProjectUserMemberFunc(ctx, projectID, username, role)
	}
	return "", nil
}

func (m *mockMemberClient) AddProjectGroupMember(ctx context.Context, projectID, groupName string, groupType int64, role string) (string, error) {
	if m.addProjectGroupMemberFunc != nil {
		return m.addProjectGroupMemberFunc(ctx, projectID, groupName, groupType, role)
	}
	return "", nil
}

func (m *mockMemberClient) UpdateProjectMemberByID(ctx context.Context, projectID, memberID, role string) error {
	if m.updateProjectMemberByIDFunc != nil {
		return m.updateProjectMemberByIDFunc(ctx, projectID, memberID, role)
	}
	return nil
}

func (m *mockMemberClient) DeleteProjectMemberByID(ctx context.Context, projectID, memberID string) error {
	if m.deleteProjectMemberByIDFunc != nil {
		return m.deleteProjectMemberByIDFunc(ctx, projectID, memberID)
	}
	return nil
}

func (m *mockMemberClient) Close() error           { return nil }
func (m *mockMemberClient) GetBaseURL() string     { return "https://harbor.example.com" }
