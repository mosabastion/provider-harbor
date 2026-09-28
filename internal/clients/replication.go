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

	harborreplication "github.com/goharbor/go-client/pkg/sdk/v2.0/client/replication"
	harbormodels "github.com/goharbor/go-client/pkg/sdk/v2.0/models"
	"github.com/pkg/errors"
	"k8s.io/utils/ptr"
)

// ReplicationPolicyFilter defines filter rules for replication
type ReplicationPolicyFilter struct {
	Type  string // repository, tag, label, resource
	Value string
}

// ReplicationPolicyDestination defines where to replicate
type ReplicationPolicyDestination struct {
	Name      string
	Namespace string
	URL       string
}

// ReplicationPolicySpec defines the desired state of a replication policy
type ReplicationPolicySpec struct {
	Name            string
	Description     *string
	SourceRegistry  *string
	DestinationReg  *ReplicationPolicyDestination
	Filters         []ReplicationPolicyFilter
	Trigger         string // manual, scheduled, event_based
	Cron            *string
	DeleteSourceTag *bool
	Override        *bool
	Enabled         *bool
}

// ReplicationPolicyStatus represents the status of a replication policy
type ReplicationPolicyStatus struct {
	ID                 string
	Name               string
	Description        *string
	Enabled            bool
	SourceRegistryName string
	DestRegistryName   string
	DestNamespace      string
	Filters            []ReplicationPolicyFilter
	Trigger            string
	Cron               string
	CreationTime       time.Time
	UpdateTime         time.Time
}

// ReplicationExecution represents a replication execution
type ReplicationExecution struct {
	ID           string
	PolicyID     string
	Status       string
	StatusText   string
	StartTime    time.Time
	EndTime      time.Time
	SuccessCount int64
	FailedCount  int64
}

// LatestReplicationExecution returns the newest execution (by start time,
// falling back to numeric execution ID on a tie), or nil if execs is empty.
func LatestReplicationExecution(execs []*ReplicationExecution) *ReplicationExecution {
	var latest *ReplicationExecution
	for _, e := range execs {
		if e == nil {
			continue
		}
		if latest == nil || e.StartTime.After(latest.StartTime) {
			latest = e
			continue
		}
		if e.StartTime.Equal(latest.StartTime) {
			eID, _ := strconv.ParseInt(e.ID, 10, 64)
			lID, _ := strconv.ParseInt(latest.ID, 10, 64)
			if eID > lID {
				latest = e
			}
		}
	}
	return latest
}

// replicationPolicyModel converts a ReplicationPolicySpec to the Harbor SDK
// ReplicationPolicy model used for create and update calls.
// Caveats:
//   - Both DestinationReg and SourceRegistry carry only a human-readable name on
//     the CR; Harbor's replication API references registries by numeric id, so
//     the caller resolves each name to an id (or nil, for the local Harbor) via
//     resolveDestRegistryID/resolveSourceRegistryID before calling this.
//   - DeleteSourceTag maps to ReplicateDeletion (the non-deprecated field).
//
// resolveDestRegistryID maps the destination registry name to its numeric Harbor
// id. A nil/empty name means pull-mode replication into the local Harbor, so no
// error and no id — DestNamespace still carries the destination project.
func (c *HarborClient) resolveDestRegistryID(ctx context.Context, spec *ReplicationPolicySpec) (*int64, error) {
	if spec.DestinationReg == nil || spec.DestinationReg.Name == "" {
		return nil, nil
	}
	reg, err := c.findRegistryByName(ctx, spec.DestinationReg.Name)
	if err != nil {
		return nil, err
	}
	if reg == nil {
		return nil, errors.Errorf("destination registry %q not found", spec.DestinationReg.Name)
	}
	return &reg.ID, nil
}

// resolveSourceRegistryID maps the source registry name to its numeric Harbor
// id. A nil/empty name means the local Harbor (no SrcRegistry set on create).
func (c *HarborClient) resolveSourceRegistryID(ctx context.Context, spec *ReplicationPolicySpec) (*int64, error) {
	if spec.SourceRegistry == nil || *spec.SourceRegistry == "" {
		return nil, nil
	}
	reg, err := c.findRegistryByName(ctx, *spec.SourceRegistry)
	if err != nil {
		return nil, err
	}
	if reg == nil {
		return nil, errors.Errorf("source registry %q not found", *spec.SourceRegistry)
	}
	return &reg.ID, nil
}

func replicationPolicyModel(spec *ReplicationPolicySpec, destRegID, srcRegID *int64) *harbormodels.ReplicationPolicy {
	p := &harbormodels.ReplicationPolicy{
		Name:     spec.Name,
		Enabled:  spec.Enabled != nil && *spec.Enabled,
		Override: spec.Override != nil && *spec.Override,
	}
	if spec.Description != nil {
		p.Description = *spec.Description
	}
	if spec.DeleteSourceTag != nil {
		p.ReplicateDeletion = *spec.DeleteSourceTag
	}
	if spec.Trigger != "" {
		trigger := &harbormodels.ReplicationTrigger{Type: spec.Trigger}
		if spec.Trigger == "scheduled" && spec.Cron != nil && *spec.Cron != "" {
			trigger.TriggerSettings = &harbormodels.ReplicationTriggerSettings{Cron: *spec.Cron}
		}
		p.Trigger = trigger
	}
	if srcRegID != nil {
		p.SrcRegistry = &harbormodels.Registry{ID: *srcRegID}
	}
	if destRegID != nil {
		// Harbor references the registry by its numeric id; name/url alone yield a
		// 400. The id is resolved by the caller via resolveDestRegistryID.
		p.DestRegistry = &harbormodels.Registry{ID: *destRegID}
	}
	if spec.DestinationReg != nil {
		// Set regardless of whether the destination is local: pull-mode still
		// needs to target a specific project in this Harbor.
		p.DestNamespace = spec.DestinationReg.Namespace
	}
	if len(spec.Filters) > 0 {
		p.Filters = make([]*harbormodels.ReplicationFilter, len(spec.Filters))
		for i, f := range spec.Filters {
			p.Filters[i] = &harbormodels.ReplicationFilter{Type: ReplicationFilterType(f.Type), Value: f.Value}
		}
	}
	return p
}

// ReplicationFilterType maps the CR's filter type to Harbor's. Harbor names the
// repository filter "name" (its valid types are name/tag/label/resource); the CR
// exposes it as the friendlier "repository". Exported so the controller can use
// the same mapping when comparing desired vs. observed filters.
func ReplicationFilterType(t string) string {
	if t == "repository" {
		return "name"
	}
	return t
}

// replicationPolicyStatusFromModel converts a Harbor SDK ReplicationPolicy to our
// internal ReplicationPolicyStatus.
func replicationPolicyStatusFromModel(p *harbormodels.ReplicationPolicy) *ReplicationPolicyStatus {
	if p == nil {
		return &ReplicationPolicyStatus{}
	}
	st := &ReplicationPolicyStatus{
		ID:            strconv.FormatInt(p.ID, 10),
		Name:          p.Name,
		Enabled:       p.Enabled,
		DestNamespace: p.DestNamespace,
	}
	if p.Description != "" {
		st.Description = ptr.To(p.Description)
	}
	if p.SrcRegistry != nil {
		st.SourceRegistryName = p.SrcRegistry.Name
	}
	if p.DestRegistry != nil {
		st.DestRegistryName = p.DestRegistry.Name
	}
	if p.Trigger != nil {
		st.Trigger = p.Trigger.Type
		if p.Trigger.TriggerSettings != nil {
			st.Cron = p.Trigger.TriggerSettings.Cron
		}
	}
	if len(p.Filters) > 0 {
		st.Filters = make([]ReplicationPolicyFilter, len(p.Filters))
		for i, f := range p.Filters {
			if f == nil {
				continue
			}
			v, _ := f.Value.(string)
			st.Filters[i] = ReplicationPolicyFilter{Type: f.Type, Value: v}
		}
	}
	if t := time.Time(p.CreationTime); !t.IsZero() {
		st.CreationTime = t
	}
	if t := time.Time(p.UpdateTime); !t.IsZero() {
		st.UpdateTime = t
	}
	return st
}

// getReplicationPolicyByID fetches a replication policy by its numeric Harbor ID.
// Returns (nil, nil) on 404.
func (c *HarborClient) getReplicationPolicyByID(ctx context.Context, id int64) (*ReplicationPolicyStatus, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	params := harborreplication.NewGetReplicationPolicyParams().WithContext(ctx).WithID(id)
	resp, err := v2Client.Replication.GetReplicationPolicy(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor replication policy")
	}
	return replicationPolicyStatusFromModel(resp.Payload), nil
}

// CreateReplicationPolicy creates a new replication policy using the Harbor SDK.
// The created policy is re-read via the Location header to capture the real numeric ID.
func (c *HarborClient) CreateReplicationPolicy(ctx context.Context, spec *ReplicationPolicySpec) (*ReplicationPolicyStatus, error) {
	if spec == nil {
		return nil, errors.New("spec is required")
	}
	if spec.Name == "" {
		return nil, errors.New("policy name is required")
	}
	if spec.Trigger == "scheduled" && (spec.Cron == nil || *spec.Cron == "") {
		return nil, errors.New("cron is required when trigger is scheduled")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor replication policy",
		"name", spec.Name,
		"trigger", spec.Trigger)

	destRegID, err := c.resolveDestRegistryID(ctx, spec)
	if err != nil {
		return nil, err
	}
	srcRegID, err := c.resolveSourceRegistryID(ctx, spec)
	if err != nil {
		return nil, err
	}
	params := harborreplication.NewCreateReplicationPolicyParams().
		WithContext(ctx).
		WithPolicy(replicationPolicyModel(spec, destRegID, srcRegID))
	resp, err := v2Client.Replication.CreateReplicationPolicy(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor replication policy")
	}

	id, err := idFromLocation(resp.Location)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse replication policy ID from location header")
	}
	st, err := c.getReplicationPolicyByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("replication policy created but not yet observable")
	}
	return st, nil
}

// ListReplicationPolicies lists all replication policies using the Harbor SDK.
func (c *HarborClient) ListReplicationPolicies(ctx context.Context) ([]*ReplicationPolicyStatus, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor replication policies")

	params := harborreplication.NewListReplicationPoliciesParams().WithContext(ctx)
	resp, err := v2Client.Replication.ListReplicationPolicies(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list Harbor replication policies")
	}

	out := make([]*ReplicationPolicyStatus, 0, len(resp.Payload))
	for _, p := range resp.Payload {
		if p != nil {
			out = append(out, replicationPolicyStatusFromModel(p))
		}
	}
	return out, nil
}

// GetReplicationPolicy retrieves a replication policy by numeric ID string.
// Returns (nil, nil) on 404.
func (c *HarborClient) GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicyStatus, error) {
	if policyID == "" {
		return nil, errors.New("policy ID is required")
	}

	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid replication policy ID")
	}
	return c.getReplicationPolicyByID(ctx, id)
}

// UpdateReplicationPolicy updates an existing replication policy. Re-reads and
// returns the updated observed state.
func (c *HarborClient) UpdateReplicationPolicy(ctx context.Context, policyID string, spec *ReplicationPolicySpec) (*ReplicationPolicyStatus, error) {
	if policyID == "" {
		return nil, errors.New("policy ID is required")
	}
	if spec == nil {
		return nil, errors.New("spec is required")
	}

	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid replication policy ID")
	}
	if spec.Trigger == "scheduled" && (spec.Cron == nil || *spec.Cron == "") {
		return nil, errors.New("cron is required when trigger is scheduled")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor replication policy", "policyId", policyID, "name", spec.Name)

	destRegID, err := c.resolveDestRegistryID(ctx, spec)
	if err != nil {
		return nil, err
	}
	srcRegID, err := c.resolveSourceRegistryID(ctx, spec)
	if err != nil {
		return nil, err
	}
	model := replicationPolicyModel(spec, destRegID, srcRegID)
	model.ID = id
	params := harborreplication.NewUpdateReplicationPolicyParams().
		WithContext(ctx).
		WithID(id).
		WithPolicy(model)
	if _, err := v2Client.Replication.UpdateReplicationPolicy(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor replication policy")
	}

	return c.getReplicationPolicyByID(ctx, id)
}

// DeleteReplicationPolicy deletes a replication policy. Idempotent: 404 is success.
func (c *HarborClient) DeleteReplicationPolicy(ctx context.Context, policyID string) error {
	if policyID == "" {
		return errors.New("policy ID is required")
	}

	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid replication policy ID")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor replication policy", "policyId", policyID)

	params := harborreplication.NewDeleteReplicationPolicyParams().WithContext(ctx).WithID(id)
	if _, err := v2Client.Replication.DeleteReplicationPolicy(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor replication policy")
	}
	return nil
}

// TriggerReplication triggers a manual replication for the given policy.
func (c *HarborClient) TriggerReplication(ctx context.Context, policyID string) (*ReplicationExecution, error) {
	if policyID == "" {
		return nil, errors.New("policy ID is required")
	}

	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid replication policy ID")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Triggering Harbor replication", "policyId", policyID)

	params := harborreplication.NewStartReplicationParams().
		WithContext(ctx).
		WithExecution(&harbormodels.StartReplicationExecution{PolicyID: id})
	resp, err := v2Client.Replication.StartReplication(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot trigger Harbor replication")
	}

	execID, _ := idFromLocation(resp.Location)
	return &ReplicationExecution{
		ID:        strconv.FormatInt(execID, 10),
		PolicyID:  policyID,
		Status:    "pending",
		StartTime: time.Now(),
	}, nil
}

// ListReplicationExecutions lists replication executions for a policy.
func (c *HarborClient) ListReplicationExecutions(ctx context.Context, policyID string) ([]*ReplicationExecution, error) {
	if policyID == "" {
		return nil, errors.New("policy ID is required")
	}

	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid replication policy ID")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor replication executions", "policyId", policyID)

	params := harborreplication.NewListReplicationExecutionsParams().WithContext(ctx).WithPolicyID(&id)
	resp, err := v2Client.Replication.ListReplicationExecutions(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list Harbor replication executions")
	}

	out := make([]*ReplicationExecution, 0, len(resp.Payload))
	for _, e := range resp.Payload {
		if e == nil {
			continue
		}
		ex := &ReplicationExecution{
			ID:           strconv.FormatInt(e.ID, 10),
			PolicyID:     policyID,
			Status:       e.Status,
			StatusText:   e.StatusText,
			StartTime:    time.Time(e.StartTime),
			EndTime:      time.Time(e.EndTime),
			SuccessCount: int64(e.Succeed),
			FailedCount:  int64(e.Failed),
		}
		out = append(out, ex)
	}
	return out, nil
}
