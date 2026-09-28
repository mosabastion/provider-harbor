/*
Copyright 2024 Crossplane Harbor Provider.
*/

package replication

import (
	"context"

	"github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	xpcontroller "github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv1 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1beta1 "github.com/rossigee/provider-harbor/apis/v1beta1"
	"github.com/rossigee/provider-harbor/apis/replication/v1beta1"
	harborclients "github.com/rossigee/provider-harbor/internal/clients"
	controllerpkg "github.com/rossigee/provider-harbor/internal/controller"
)

const (
	errNotReplication    = "managed resource is not a Replication custom resource"
	errTrackPCUsage      = "cannot track ProviderConfig usage"
	errReplicationDelete = "cannot delete Harbor replication policy"
	errNewClient         = "cannot create new Harbor client"
)

func Setup(mgr ctrl.Manager, o xpcontroller.Options) error {
	name := managed.ControllerName(v1beta1.ReplicationGroupVersionKind.Kind)

	reconcilerOpts := []managed.ReconcilerOption{
		managed.WithExternalConnector(&connector{
			kube:         mgr.GetClient(),
			usage:        resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apiv1beta1.ProviderConfigUsage{}),
			newServiceFn: harborclients.NewHarborClientFromProviderConfig,
		}),
		managed.WithLogger(logging.NewLogrLogger(mgr.GetLogger().WithValues("controller", name))),
		managed.WithPollInterval(o.PollInterval),
		managed.WithPollJitterHook(o.PollInterval / 10),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorder(name))),
	}
	// Feature-gated options (e.g. Management Policies) appended when enabled.
	reconcilerOpts = append(reconcilerOpts, controllerpkg.ReconcilerOptions(o)...)

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1beta1.ReplicationGroupVersionKind),
		reconcilerOpts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1beta1.Replication{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube         client.Client
	usage        resource.ModernTracker
	newServiceFn func(context.Context, client.Client, resource.Managed) (harborclients.HarborClienter, error)
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1beta1.Replication)
	if !ok {
		return nil, errors.New(errNotReplication)
	}

	if c.usage != nil {
		if err := c.usage.Track(ctx, cr); err != nil {
			return nil, errors.Wrap(err, errTrackPCUsage)
		}
	}

	svc, err := c.newServiceFn(ctx, c.kube, mg)
	if err != nil {
		return nil, errors.Wrap(err, errNewClient)
	}

	return &external{service: svc}, nil
}

type external struct {
	service harborclients.HarborClienter
}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1beta1.Replication)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotReplication)
	}

	policies, err := c.service.ListReplicationPolicies(ctx)
	if err != nil {
		return managed.ExternalObservation{}, err
	}

	for _, policy := range policies {
		if policy.Name == cr.Spec.ForProvider.Name {
			cr.Status.AtProvider.ID = &policy.ID
			cr.Status.AtProvider.Enabled = &policy.Enabled
			t := metav1.NewTime(policy.CreationTime)
			cr.Status.AtProvider.CreationTime = &t
			ut := metav1.NewTime(policy.UpdateTime)
			cr.Status.AtProvider.UpdateTime = &ut

			execs, err := c.service.ListReplicationExecutions(ctx, policy.ID)
			if err != nil {
				return managed.ExternalObservation{}, err
			}
			cr.Status.AtProvider.LastExecution = lastExecutionStatus(harborclients.LatestReplicationExecution(execs))

			upToDate := replicationUpToDate(cr, policy)

			cr.SetConditions(xpv1.Available())

			return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: upToDate}, nil
		}
	}

	return managed.ExternalObservation{ResourceExists: false}, nil
}

// lastExecutionStatus converts the newest replication execution into the CR's
// status shape. Returns nil (not an empty struct) when the policy has never run,
// so atProvider.lastExecution stays absent rather than a zero-valued object.
func lastExecutionStatus(e *harborclients.ReplicationExecution) *v1beta1.ReplicationExecutionStatus {
	if e == nil {
		return nil
	}
	out := &v1beta1.ReplicationExecutionStatus{
		Status:     e.Status,
		StatusText: e.StatusText,
		Succeed:    e.SuccessCount,
		Failed:     e.FailedCount,
	}
	if !e.StartTime.IsZero() {
		t := metav1.NewTime(e.StartTime)
		out.Start = &t
	}
	if !e.EndTime.IsZero() {
		t := metav1.NewTime(e.EndTime)
		out.End = &t
	}
	return out
}

// replicationUpToDate compares the CR's desired state against the observed
// Harbor policy across every field Update can change: source/destination
// registry, destination namespace, filters, trigger type + cron, description
// and enabled. Description/enabled were the only fields compared before this;
// a drift in any of the others previously went unnoticed forever.
func replicationUpToDate(cr *v1beta1.Replication, policy *harborclients.ReplicationPolicyStatus) bool {
	p := &cr.Spec.ForProvider

	if p.Description != nil && policy.Description != nil && *p.Description != *policy.Description {
		return false
	}
	if p.Enabled != nil && *p.Enabled != policy.Enabled {
		return false
	}

	wantSrc := ""
	if p.SourceRegistry != nil {
		wantSrc = *p.SourceRegistry
	}
	if wantSrc != policy.SourceRegistryName {
		return false
	}

	wantDestName, wantDestNS := "", ""
	if p.DestinationReg != nil {
		wantDestName = p.DestinationReg.Name
		wantDestNS = p.DestinationReg.Namespace
	}
	if wantDestName != policy.DestRegistryName || wantDestNS != policy.DestNamespace {
		return false
	}

	if p.Trigger != "" && p.Trigger != policy.Trigger {
		return false
	}
	wantCron := ""
	if p.Trigger == "scheduled" && p.Cron != nil {
		wantCron = *p.Cron
	}
	if wantCron != policy.Cron {
		return false
	}

	return filtersUpToDate(p.Filters, policy.Filters)
}

func filtersUpToDate(want []v1beta1.ReplicationFilter, got []harborclients.ReplicationPolicyFilter) bool {
	if len(want) != len(got) {
		return false
	}
	for i, w := range want {
		if harborclients.ReplicationFilterType(w.Type) != got[i].Type || w.Value != got[i].Value {
			return false
		}
	}
	return true
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1beta1.Replication)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotReplication)
	}

	cr.SetConditions(xpv1.Creating())

	_, err := c.service.CreateReplicationPolicy(ctx, replicationSpecFromCR(cr))
	if err != nil {
		return managed.ExternalCreation{}, err
	}

	return managed.ExternalCreation{}, nil
}

// replicationSpecFromCR builds the full client spec from the CR. Used by both
// Create and Update so an update carries the complete desired state (destination
// registry, filters, source) rather than a partial patch.
func replicationSpecFromCR(cr *v1beta1.Replication) *harborclients.ReplicationPolicySpec {
	spec := &harborclients.ReplicationPolicySpec{
		Name:            cr.Spec.ForProvider.Name,
		Description:     cr.Spec.ForProvider.Description,
		SourceRegistry:  cr.Spec.ForProvider.SourceRegistry,
		Trigger:         cr.Spec.ForProvider.Trigger,
		Cron:            cr.Spec.ForProvider.Cron,
		DeleteSourceTag: cr.Spec.ForProvider.DeleteSourceTag,
		Override:        cr.Spec.ForProvider.Override,
		Enabled:         cr.Spec.ForProvider.Enabled,
	}
	if len(cr.Spec.ForProvider.Filters) > 0 {
		spec.Filters = make([]harborclients.ReplicationPolicyFilter, len(cr.Spec.ForProvider.Filters))
		for i, f := range cr.Spec.ForProvider.Filters {
			spec.Filters[i] = harborclients.ReplicationPolicyFilter{Type: f.Type, Value: f.Value}
		}
	}
	if d := cr.Spec.ForProvider.DestinationReg; d != nil {
		spec.DestinationReg = &harborclients.ReplicationPolicyDestination{
			Name:      d.Name,
			Namespace: d.Namespace,
			URL:       d.URL,
		}
	}
	return spec
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1beta1.Replication)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotReplication)
	}

	if cr.Status.AtProvider.ID == nil {
		return managed.ExternalUpdate{}, errors.New("policy ID not set")
	}

	_, err := c.service.UpdateReplicationPolicy(ctx, *cr.Status.AtProvider.ID, replicationSpecFromCR(cr))
	if err != nil {
		return managed.ExternalUpdate{}, err
	}

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) (managed.ExternalDelete, error) {
	cr, ok := mg.(*v1beta1.Replication)
	if !ok {
		return managed.ExternalDelete{}, errors.New(errNotReplication)
	}

	if cr.Status.AtProvider.ID == nil {
		return managed.ExternalDelete{}, nil
	}

	cr.SetConditions(xpv1.Deleting())

	err := c.service.DeleteReplicationPolicy(ctx, *cr.Status.AtProvider.ID)
	if err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, errReplicationDelete)
	}

	return managed.ExternalDelete{}, nil
}

func (c *external) Disconnect(ctx context.Context) error {
	return c.service.Close()
}
