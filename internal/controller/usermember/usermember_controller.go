/*
Copyright 2024 Crossplane Harbor Provider.
*/

package usermember

import (
	"context"
	"time"

	"github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	xpv1 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"

	"github.com/rossigee/provider-harbor/apis/member/v1beta1"
	harborclients "github.com/rossigee/provider-harbor/internal/clients"
	ctrlutil "github.com/rossigee/provider-harbor/internal/controller"
)

const (
	errNotUserMember    = "managed resource is not a UserMember custom resource"
	errUserMemberCreate = "cannot create Harbor user member"
	errUserMemberUpdate = "cannot update Harbor user member"
	errUserMemberDelete = "cannot delete Harbor user member"
	errNewClient        = "cannot create new Harbor client"
)

// Setup adds a controller that reconciles UserMember managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1beta1.UserMemberGroupVersionKind.Kind)

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1beta1.UserMemberGroupVersionKind),
		managed.WithExternalConnector(&connector{
			kube:         mgr.GetClient(),
			newServiceFn: harborclients.NewHarborClientFromProviderConfig,
		}),
		managed.WithLogger(logging.NewLogrLogger(mgr.GetLogger().WithValues("controller", name))),
		managed.WithPollInterval(1*time.Minute),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorder(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1beta1.UserMember{}).
		Complete(ratelimiter.NewReconciler(name, r, nil))
}

type connector struct {
	kube         client.Client
	newServiceFn func(context.Context, client.Client, resource.Managed) (harborclients.HarborClienter, error)
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	_, ok := mg.(*v1beta1.UserMember)
	if !ok {
		return nil, errors.New(errNotUserMember)
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
	cr, ok := mg.(*v1beta1.UserMember)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotUserMember)
	}

	projectID := cr.Spec.ForProvider.ProjectID

	// Prefer the recorded Harbor member id (external name); fall back to adoption
	// by entity type ("u") + username when the id is not yet known.
	var status *harborclients.MemberStatus
	var err error
	if id := ctrlutil.GetExternalName(cr); id != "" {
		status, err = c.service.GetProjectMemberByID(ctx, projectID, id)
	} else {
		status, err = c.service.FindProjectMember(ctx, projectID, "u", cr.Spec.ForProvider.Username)
	}
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	if status == nil {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	ctrlutil.SetExternalName(cr, status.ID)

	cr.Status.AtProvider.ID = &status.ID
	cr.Status.AtProvider.MemberName = &status.MemberName
	cr.Status.AtProvider.MemberType = &status.MemberType
	cr.Status.AtProvider.Role = &status.Role
	cr.SetConditions(xpv1.Available())

	upToDate := cr.Spec.ForProvider.Role == "" || status.Role == "" || cr.Spec.ForProvider.Role == status.Role

	return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: upToDate}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1beta1.UserMember)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotUserMember)
	}

	cr.SetConditions(xpv1.Creating())

	id, err := c.service.AddProjectUserMember(ctx, cr.Spec.ForProvider.ProjectID, cr.Spec.ForProvider.Username, cr.Spec.ForProvider.Role)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errUserMemberCreate)
	}

	ctrlutil.SetExternalName(cr, id)

	return managed.ExternalCreation{}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1beta1.UserMember)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotUserMember)
	}

	id := ctrlutil.GetExternalName(cr)
	if id == "" {
		return managed.ExternalUpdate{}, errors.New(errUserMemberUpdate + ": external name (member id) is empty")
	}

	if err := c.service.UpdateProjectMemberByID(ctx, cr.Spec.ForProvider.ProjectID, id, cr.Spec.ForProvider.Role); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUserMemberUpdate)
	}

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) (managed.ExternalDelete, error) {
	cr, ok := mg.(*v1beta1.UserMember)
	if !ok {
		return managed.ExternalDelete{}, errors.New(errNotUserMember)
	}

	cr.SetConditions(xpv1.Deleting())

	id := ctrlutil.GetExternalName(cr)
	if id == "" {
		return managed.ExternalDelete{}, nil
	}

	if err := c.service.DeleteProjectMemberByID(ctx, cr.Spec.ForProvider.ProjectID, id); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, errUserMemberDelete)
	}

	return managed.ExternalDelete{}, nil
}

func (c *external) Disconnect(ctx context.Context) error {
	return c.service.Close()
}
