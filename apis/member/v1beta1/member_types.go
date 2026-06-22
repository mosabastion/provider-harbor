/*
Copyright 2024 Crossplane Harbor Provider.
*/

package v1beta1

import (
	xpv1 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MemberParameters defines a Harbor project member. A member is EITHER a user
// (Username set) OR a group (GroupName set) — exactly one of the two.
// +kubebuilder:validation:XValidation:rule="(has(self.username) && self.username != '') != (has(self.groupName) && self.groupName != '')",message="exactly one of username or groupName must be set"
type MemberParameters struct {
	ProjectID string `json:"projectId"`
	// Username of the user member. Mutually exclusive with groupName.
	// +optional
	Username string `json:"username,omitempty"`
	// GroupName of the group member (e.g. a Keycloak/OIDC group). Mutually
	// exclusive with username.
	// +optional
	GroupName string `json:"groupName,omitempty"`
	// MemberGroupType is Harbor's group_type for a group member: 1 LDAP, 2 HTTP,
	// 3 OIDC. Defaults to 3 (OIDC). Only used when groupName is set.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Enum=1;2;3
	MemberGroupType *int64 `json:"memberGroupType,omitempty"`
	Role            string `json:"role"`
}

type MemberObservation struct {
	ID           *string      `json:"id,omitempty"`
	MemberName   *string      `json:"memberName,omitempty"`
	MemberType   *string      `json:"memberType,omitempty"`
	Role         *string      `json:"role,omitempty"`
	CreationTime *metav1.Time `json:"creationTime,omitempty"`
}

type MemberSpec struct {
	xpv1.ManagedResourceSpec `json:",inline"`
	ForProvider              MemberParameters `json:"forProvider"`
}

type MemberStatus struct {
	xpv1.ConditionedStatus `json:",inline"`
	AtProvider             MemberObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="USERNAME",type="string",JSONPath=".spec.forProvider.username"
// +kubebuilder:printcolumn:name="ROLE",type="string",JSONPath=".spec.forProvider.role"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,harbor}

type Member struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MemberSpec   `json:"spec"`
	Status            MemberStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type MemberList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Member `json:"items"`
}

// GetCondition of this Member.
func (mg *Member) GetCondition(ct xpv1.ConditionType) xpv1.Condition {
	return mg.Status.GetCondition(ct)
}

// GetManagementPolicies of this Member.
func (mg *Member) GetManagementPolicies() xpv1.ManagementPolicies {
	return mg.Spec.ManagementPolicies
}

// GetProviderConfigReference of this Member.
func (mg *Member) GetProviderConfigReference() *xpv1.ProviderConfigReference {
	return mg.Spec.ProviderConfigReference
}

// GetWriteConnectionSecretToReference of this Member.
func (mg *Member) GetWriteConnectionSecretToReference() *xpv1.LocalSecretReference {
	return mg.Spec.WriteConnectionSecretToReference
}

// SetConditions of this Member.
func (mg *Member) SetConditions(c ...xpv1.Condition) {
	mg.Status.SetConditions(c...)
}

// SetManagementPolicies of this Member.
func (mg *Member) SetManagementPolicies(r xpv1.ManagementPolicies) {
	mg.Spec.ManagementPolicies = r
}

// SetProviderConfigReference of this Member.
func (mg *Member) SetProviderConfigReference(r *xpv1.ProviderConfigReference) {
	mg.Spec.ProviderConfigReference = r
}

// SetWriteConnectionSecretToReference of this Member.
func (mg *Member) SetWriteConnectionSecretToReference(r *xpv1.LocalSecretReference) {
	mg.Spec.WriteConnectionSecretToReference = r
}
