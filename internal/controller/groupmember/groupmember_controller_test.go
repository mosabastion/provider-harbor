/*
Copyright 2024 Crossplane Harbor Provider.
*/

package groupmember

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/rossigee/provider-harbor/apis/member/v1beta1"
	harborclients "github.com/rossigee/provider-harbor/internal/clients"
	ctrlutil "github.com/rossigee/provider-harbor/internal/controller"
)

func newGroupMember(name, project, group, role string, gt *int64) *v1beta1.GroupMember {
	return &v1beta1.GroupMember{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.GroupMemberSpec{
			ForProvider: v1beta1.GroupMemberParameters{
				ProjectID: project,
				GroupName: group,
				Role:      role,
				GroupType: gt,
			},
		},
	}
}

func TestConnectNotGroupMember(t *testing.T) {
	if _, err := (&connector{}).Connect(context.Background(), nil); err == nil || err.Error() != errNotGroupMember {
		t.Errorf("Connect(nil) should return %s", errNotGroupMember)
	}
}

func TestGroupTypeDefaultsToOIDC(t *testing.T) {
	if gt := groupType(newGroupMember("gm", "1", "g", "guest", nil)); gt != defaultGroupType {
		t.Errorf("expected default group type %d, got %d", defaultGroupType, gt)
	}
	if gt := groupType(newGroupMember("gm", "1", "g", "guest", ptr.To(int64(1)))); gt != 1 {
		t.Errorf("expected explicit group type 1, got %d", gt)
	}
}

func TestObserveGroupMemberAdoptsByGroupEntity(t *testing.T) {
	ctx := context.Background()
	cr := newGroupMember("gm", "1", "platform-admins", "guest", nil)
	ext := &external{service: &harborclients.MockHarborClient{
		FindProjectMemberFunc: func(ctx context.Context, projectID, entityType, entityName string) (*harborclients.MemberStatus, error) {
			if entityType != "g" {
				t.Errorf("expected entity type g, got %q", entityType)
			}
			if entityName != "platform-admins" {
				t.Errorf("expected entity name platform-admins, got %q", entityName)
			}
			return &harborclients.MemberStatus{ID: "5", MemberName: "platform-admins", MemberType: "group", Role: "guest"}, nil
		},
	}}
	obs, err := ext.Observe(ctx, cr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		t.Errorf("expected exists+uptodate, got %+v", obs)
	}
	if got := ctrlutil.GetExternalName(cr); got != "5" {
		t.Errorf("expected external name 5 after adoption, got %q", got)
	}
}

func TestCreateGroupMemberPassesGroupType(t *testing.T) {
	ctx := context.Background()
	cr := newGroupMember("gm", "1", "platform-admins", "guest", nil)
	var gotType int64
	ext := &external{service: &harborclients.MockHarborClient{
		AddProjectGroupMemberFunc: func(ctx context.Context, projectID, groupName string, groupType int64, role string) (string, error) {
			gotType = groupType
			return "11", nil
		},
	}}
	if _, err := ext.Create(ctx, cr); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if gotType != defaultGroupType {
		t.Errorf("expected group type %d passed to client, got %d", defaultGroupType, gotType)
	}
	if got := ctrlutil.GetExternalName(cr); got != "11" {
		t.Errorf("expected external name 11 after create, got %q", got)
	}
}
