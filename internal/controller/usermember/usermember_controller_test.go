/*
Copyright 2024 Crossplane Harbor Provider.
*/

package usermember

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rossigee/provider-harbor/apis/member/v1beta1"
	harborclients "github.com/rossigee/provider-harbor/internal/clients"
	ctrlutil "github.com/rossigee/provider-harbor/internal/controller"
)

func newUserMember(name, project, username, role string) *v1beta1.UserMember {
	return &v1beta1.UserMember{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.UserMemberSpec{
			ForProvider: v1beta1.UserMemberParameters{
				ProjectID: project,
				Username:  username,
				Role:      role,
			},
		},
	}
}

func TestConnectNotUserMember(t *testing.T) {
	if _, err := (&connector{}).Connect(context.Background(), nil); err == nil || err.Error() != errNotUserMember {
		t.Errorf("Connect(nil) should return %s", errNotUserMember)
	}
}

func TestObserveNotUserMember(t *testing.T) {
	if _, err := (&external{}).Observe(context.Background(), nil); err == nil || err.Error() != errNotUserMember {
		t.Errorf("Observe(nil) should return %s", errNotUserMember)
	}
}

func TestObserveUserMemberNotFoundAdopts(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("um", "1", "alice", "developer")
	ext := &external{service: &harborclients.MockHarborClient{
		FindProjectMemberFunc: func(ctx context.Context, projectID, entityType, entityName string) (*harborclients.MemberStatus, error) {
			if entityType != "u" {
				t.Errorf("expected entity type u, got %q", entityType)
			}
			return nil, nil
		},
	}}
	obs, err := ext.Observe(ctx, cr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if obs.ResourceExists {
		t.Errorf("expected ResourceExists=false when member absent")
	}
}

func TestObserveUserMemberByID(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("um", "1", "alice", "developer")
	ctrlutil.SetExternalName(cr, "7")
	ext := &external{service: &harborclients.MockHarborClient{
		GetProjectMemberByIDFunc: func(ctx context.Context, projectID, memberID string) (*harborclients.MemberStatus, error) {
			if memberID != "7" {
				t.Errorf("expected get by id 7, got %q", memberID)
			}
			return &harborclients.MemberStatus{ID: "7", MemberName: "alice", MemberType: "user", Role: "developer"}, nil
		},
	}}
	obs, err := ext.Observe(ctx, cr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		t.Errorf("expected exists+uptodate, got %+v", obs)
	}
	if got := ctrlutil.GetExternalName(cr); got != "7" {
		t.Errorf("expected external name 7, got %q", got)
	}
}

func TestCreateUserMemberSetsExternalName(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("um", "1", "alice", "developer")
	ext := &external{service: &harborclients.MockHarborClient{
		AddProjectUserMemberFunc: func(ctx context.Context, projectID, username, role string) (string, error) {
			return "42", nil
		},
	}}
	if _, err := ext.Create(ctx, cr); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := ctrlutil.GetExternalName(cr); got != "42" {
		t.Errorf("expected external name 42 after create, got %q", got)
	}
}

func TestDeleteUserMemberByID(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("um", "1", "alice", "developer")
	ctrlutil.SetExternalName(cr, "9")
	called := false
	ext := &external{service: &harborclients.MockHarborClient{
		DeleteProjectMemberByIDFunc: func(ctx context.Context, projectID, memberID string) error {
			called = true
			if memberID != "9" {
				t.Errorf("expected delete id 9, got %q", memberID)
			}
			return nil
		},
	}}
	if _, err := ext.Delete(ctx, cr); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !called {
		t.Errorf("expected DeleteProjectMemberByID to be called")
	}
}

func TestUpdateUserMemberError(t *testing.T) {
	ctx := context.Background()
	cr := newUserMember("um", "1", "alice", "developer")
	ctrlutil.SetExternalName(cr, "3")
	ext := &external{service: &harborclients.MockHarborClient{
		UpdateProjectMemberByIDFunc: func(ctx context.Context, projectID, memberID, role string) error {
			return errors.New("boom")
		},
	}}
	if _, err := ext.Update(ctx, cr); err == nil {
		t.Errorf("expected error from Update")
	}
}
