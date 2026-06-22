/*
Copyright 2025 The Crossplane Authors.

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

package clients

import (
	"context"
	"strconv"
	"time"

	harbormember "github.com/goharbor/go-client/pkg/sdk/v2.0/client/member"
	harbormodels "github.com/goharbor/go-client/pkg/sdk/v2.0/models"
	"github.com/pkg/errors"
)

// MemberStatus represents a Harbor project member
type MemberStatus struct {
	ID           string
	MemberName   string
	MemberType   string
	Role         string
	CreationTime time.Time
}

// memberRoleIDByName maps Harbor's project role names to their numeric IDs.
// Harbor: 1 projectAdmin, 2 developer, 3 guest, 4 maintainer.
var memberRoleIDByName = map[string]int64{
	"projectAdmin": 1,
	"developer":    2,
	"guest":        3,
	"maintainer":   4,
}

var memberRoleNameByID = map[int64]string{
	1: "projectAdmin",
	2: "developer",
	3: "guest",
	4: "maintainer",
}

func memberRoleID(role string) (int64, error) {
	if id, ok := memberRoleIDByName[role]; ok {
		return id, nil
	}
	// Also accept the numeric id directly.
	if id, err := strconv.ParseInt(role, 10, 64); err == nil {
		return id, nil
	}
	return 0, errors.Errorf("unknown Harbor project role %q (want projectAdmin|developer|guest|maintainer)", role)
}

func memberStatusFromEntity(m *harbormodels.ProjectMemberEntity) *MemberStatus {
	st := &MemberStatus{
		ID:         strconv.FormatInt(m.ID, 10),
		MemberName: m.EntityName,
		Role:       memberRoleNameByID[m.RoleID],
	}
	switch m.EntityType {
	case "u":
		st.MemberType = "user"
	case "g":
		st.MemberType = "group"
	default:
		st.MemberType = m.EntityType
	}
	if st.Role == "" {
		st.Role = strconv.FormatInt(m.RoleID, 10)
	}
	return st
}

// findProjectMember returns the Harbor user member entity for username, or nil
// if the project has no such member. Harbor exposes no get-member-by-name, so we
// list and match — the numeric member id this yields is required by
// update/delete.
func (c *HarborClient) findProjectMember(ctx context.Context, projectID, username string) (*harbormodels.ProjectMemberEntity, error) {
	return c.findProjectMemberByEntity(ctx, projectID, username, "u")
}

// findProjectMemberByEntity returns the member entity matching entityName and
// entityType ("u" user, "g" group), or nil if absent. Matching on entity_type
// keeps a user member and a group member that happen to share a name distinct.
func (c *HarborClient) findProjectMemberByEntity(ctx context.Context, projectID, entityName, entityType string) (*harbormodels.ProjectMemberEntity, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	ref, isName := projectRef(projectID)
	params := harbormember.NewListProjectMembersParams().WithContext(ctx).WithProjectNameOrID(ref)
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	resp, err := v2Client.Member.ListProjectMembers(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot list Harbor project members")
	}
	for _, m := range resp.Payload {
		if m != nil && m.EntityName == entityName && m.EntityType == entityType {
			return m, nil
		}
	}
	return nil, nil
}

// AddProjectMember adds a user member to a Harbor project with the given role.
func (c *HarborClient) AddProjectMember(ctx context.Context, projectID, username, role string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if username == "" {
		return errors.New("username is required")
	}
	roleID, err := memberRoleID(role)
	if err != nil {
		return err
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Adding Harbor project member", "projectId", projectID, "username", username, "role", role)

	ref, isName := projectRef(projectID)
	params := harbormember.NewCreateProjectMemberParams().WithContext(ctx).
		WithProjectNameOrID(ref).
		WithProjectMember(&harbormodels.ProjectMember{
			RoleID:     roleID,
			MemberUser: &harbormodels.UserEntity{Username: username},
		})
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Member.CreateProjectMember(ctx, params); err != nil {
		return errors.Wrap(err, "cannot add Harbor project member")
	}
	return nil
}

// ListProjectMembers lists members of a Harbor project
func (c *HarborClient) ListProjectMembers(ctx context.Context, projectID string) ([]*MemberStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor project members", "projectId", projectID)

	ref, isName := projectRef(projectID)
	params := harbormember.NewListProjectMembersParams().WithContext(ctx).WithProjectNameOrID(ref)
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	resp, err := v2Client.Member.ListProjectMembers(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list Harbor project members")
	}

	members := make([]*MemberStatus, 0, len(resp.Payload))
	for _, m := range resp.Payload {
		if m != nil {
			members = append(members, memberStatusFromEntity(m))
		}
	}
	return members, nil
}

// GetProjectMember retrieves a specific project member by username, returning
// (nil, nil) when the project has no such member.
func (c *HarborClient) GetProjectMember(ctx context.Context, projectID, username string) (*MemberStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if username == "" {
		return nil, errors.New("username is required")
	}

	m, err := c.findProjectMember(ctx, projectID, username)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, nil
	}
	return memberStatusFromEntity(m), nil
}

// UpdateProjectMember updates a project member's role.
func (c *HarborClient) UpdateProjectMember(ctx context.Context, projectID, username, role string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if username == "" {
		return errors.New("username is required")
	}
	roleID, err := memberRoleID(role)
	if err != nil {
		return err
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	m, err := c.findProjectMember(ctx, projectID, username)
	if err != nil {
		return err
	}
	if m == nil {
		return errors.Errorf("Harbor project member %q not found", username)
	}

	c.logger.Info("Updating Harbor project member", "projectId", projectID, "username", username, "role", role)

	ref, isName := projectRef(projectID)
	params := harbormember.NewUpdateProjectMemberParams().WithContext(ctx).
		WithProjectNameOrID(ref).
		WithMid(m.ID).
		WithRole(&harbormodels.RoleRequest{RoleID: roleID})
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Member.UpdateProjectMember(ctx, params); err != nil {
		return errors.Wrap(err, "cannot update Harbor project member")
	}
	return nil
}

// DeleteProjectMember removes a member from a project (idempotent).
func (c *HarborClient) DeleteProjectMember(ctx context.Context, projectID, username string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if username == "" {
		return errors.New("username is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	m, err := c.findProjectMember(ctx, projectID, username)
	if err != nil {
		return err
	}
	if m == nil {
		return nil
	}

	c.logger.Info("Deleting Harbor project member", "projectId", projectID, "username", username)

	ref, isName := projectRef(projectID)
	params := harbormember.NewDeleteProjectMemberParams().WithContext(ctx).
		WithProjectNameOrID(ref).
		WithMid(m.ID)
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Member.DeleteProjectMember(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor project member")
	}
	return nil
}

// defaultMemberGroupType is Harbor's group_type for OIDC groups; the platform
// binds Keycloak/OIDC groups so this is the default when unset.
const defaultMemberGroupType int64 = 3

func resolveGroupType(groupType *int64) int64 {
	if groupType != nil && *groupType != 0 {
		return *groupType
	}
	return defaultMemberGroupType
}

// AddProjectGroupMember adds a group member to a Harbor project with the given
// role. groupType is Harbor's group_type (1 LDAP, 2 HTTP, 3 OIDC); when nil/0 it
// defaults to OIDC.
func (c *HarborClient) AddProjectGroupMember(ctx context.Context, projectID, groupName string, groupType *int64, role string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if groupName == "" {
		return errors.New("group name is required")
	}
	roleID, err := memberRoleID(role)
	if err != nil {
		return err
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	gt := resolveGroupType(groupType)
	c.logger.Info("Adding Harbor project group member", "projectId", projectID, "groupName", groupName, "groupType", gt, "role", role)

	ref, isName := projectRef(projectID)
	params := harbormember.NewCreateProjectMemberParams().WithContext(ctx).
		WithProjectNameOrID(ref).
		WithProjectMember(&harbormodels.ProjectMember{
			RoleID:      roleID,
			MemberGroup: &harbormodels.UserGroup{GroupName: groupName, GroupType: gt},
		})
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Member.CreateProjectMember(ctx, params); err != nil {
		return errors.Wrap(err, "cannot add Harbor project group member")
	}
	return nil
}

// GetProjectGroupMember retrieves a specific group member by name, returning
// (nil, nil) when the project has no such group member.
func (c *HarborClient) GetProjectGroupMember(ctx context.Context, projectID, groupName string) (*MemberStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if groupName == "" {
		return nil, errors.New("group name is required")
	}

	m, err := c.findProjectMemberByEntity(ctx, projectID, groupName, "g")
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, nil
	}
	return memberStatusFromEntity(m), nil
}

// UpdateProjectGroupMember updates a group member's role.
func (c *HarborClient) UpdateProjectGroupMember(ctx context.Context, projectID, groupName, role string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if groupName == "" {
		return errors.New("group name is required")
	}
	roleID, err := memberRoleID(role)
	if err != nil {
		return err
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	m, err := c.findProjectMemberByEntity(ctx, projectID, groupName, "g")
	if err != nil {
		return err
	}
	if m == nil {
		return errors.Errorf("Harbor project group member %q not found", groupName)
	}

	c.logger.Info("Updating Harbor project group member", "projectId", projectID, "groupName", groupName, "role", role)

	ref, isName := projectRef(projectID)
	params := harbormember.NewUpdateProjectMemberParams().WithContext(ctx).
		WithProjectNameOrID(ref).
		WithMid(m.ID).
		WithRole(&harbormodels.RoleRequest{RoleID: roleID})
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Member.UpdateProjectMember(ctx, params); err != nil {
		return errors.Wrap(err, "cannot update Harbor project group member")
	}
	return nil
}

// DeleteProjectGroupMember removes a group member from a project (idempotent).
func (c *HarborClient) DeleteProjectGroupMember(ctx context.Context, projectID, groupName string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if groupName == "" {
		return errors.New("group name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	m, err := c.findProjectMemberByEntity(ctx, projectID, groupName, "g")
	if err != nil {
		return err
	}
	if m == nil {
		return nil
	}

	c.logger.Info("Deleting Harbor project group member", "projectId", projectID, "groupName", groupName)

	ref, isName := projectRef(projectID)
	params := harbormember.NewDeleteProjectMemberParams().WithContext(ctx).
		WithProjectNameOrID(ref).
		WithMid(m.ID)
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Member.DeleteProjectMember(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor project group member")
	}
	return nil
}
