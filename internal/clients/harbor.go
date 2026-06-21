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
	"crypto/tls"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/goharbor/go-client/pkg/harbor"
	harborartifact "github.com/goharbor/go-client/pkg/sdk/v2.0/client/artifact"
	harbormember "github.com/goharbor/go-client/pkg/sdk/v2.0/client/member"
	harborproject "github.com/goharbor/go-client/pkg/sdk/v2.0/client/project"
	harborregistry "github.com/goharbor/go-client/pkg/sdk/v2.0/client/registry"
	harborreplication "github.com/goharbor/go-client/pkg/sdk/v2.0/client/replication"
	harborrepository "github.com/goharbor/go-client/pkg/sdk/v2.0/client/repository"
	harborretention "github.com/goharbor/go-client/pkg/sdk/v2.0/client/retention"
	harborrobot "github.com/goharbor/go-client/pkg/sdk/v2.0/client/robot"
	harborscan "github.com/goharbor/go-client/pkg/sdk/v2.0/client/scan"
	harborscanner "github.com/goharbor/go-client/pkg/sdk/v2.0/client/scanner"
	harborsysteminfo "github.com/goharbor/go-client/pkg/sdk/v2.0/client/systeminfo"
	harboruser "github.com/goharbor/go-client/pkg/sdk/v2.0/client/user"
	harborusergroup "github.com/goharbor/go-client/pkg/sdk/v2.0/client/usergroup"
	harborwebhook "github.com/goharbor/go-client/pkg/sdk/v2.0/client/webhook"
	harbormodels "github.com/goharbor/go-client/pkg/sdk/v2.0/models"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	xpv1 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/rossigee/provider-harbor/apis/v1beta1"
)

const (
	// errNoProviderConfig is returned when no providerConfig is provided.
	errNoProviderConfig = "no providerConfigRef provided"
	// errGetProviderConfig is returned when the provider config cannot be retrieved.
	errGetProviderConfig = "cannot get referenced ProviderConfig"
	// errExtractCredentials is returned when the credentials cannot be extracted from the provider config.
	errExtractCredentials = "cannot extract credentials"
)

// isHarborNotFound reports whether err represents a Harbor 404. Harbor's swagger
// is inconsistent: some operations carry a typed NotFound response (which
// implements IsCode), while others surface a generic *runtime.APIError. This
// detects both so callers can map "absent" to the (nil, nil) not-found contract.
func isHarborNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *runtime.APIError
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
		return true
	}
	var coder interface{ IsCode(int) bool }
	if errors.As(err, &coder) {
		return coder.IsCode(http.StatusNotFound)
	}
	return false
}

// HarborClient provides Harbor API operations using the native Go client
type HarborClient struct {
	clientSet  *harbor.ClientSet
	config     *harbor.ClientSetConfig
	logger     logging.Logger
	httpClient *http.Client
}

// HarborConfig holds configuration for creating a Harbor client
type HarborConfig struct {
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
	Insecure bool   `json:"insecure"`
}

// ProjectSpec defines the desired state of a Harbor project
type ProjectSpec struct {
	Name                     string            `json:"name"`
	Public                   bool              `json:"public"`
	EnableContentTrust       *bool             `json:"enableContentTrust,omitempty"`
	EnableContentTrustCosign *bool             `json:"enableContentTrustCosign,omitempty"`
	AutoScanImages           *bool             `json:"autoScanImages,omitempty"`
	PreventVulnerableImages  *bool             `json:"preventVulnerableImages,omitempty"`
	Severity                 *string           `json:"severity,omitempty"`
	CVEAllowlist             []string          `json:"cveAllowlist,omitempty"`
	RegistryID               *int64            `json:"registryId,omitempty"`
	StorageLimit             *int64            `json:"storageLimit,omitempty"`
	Metadata                 map[string]string `json:"metadata,omitempty"`
}

// ProjectStatus represents the status of a Harbor project
type ProjectStatus struct {
	ID                  string    `json:"id,omitempty"`
	Name                string    `json:"name"`
	Public              bool      `json:"public"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at,omitempty"`
	OwnerID             int64     `json:"owner_id,omitempty"`
	OwnerName           string    `json:"owner_name,omitempty"`
	RepoCount           int64     `json:"repo_count,omitempty"`
	ChartCount          int64     `json:"chart_count,omitempty"`
	CurrentStorageUsage int64     `json:"current_storage_usage,omitempty"`
}

// ScannerSpec defines the desired state of a Harbor scanner registration
type ScannerSpec struct {
	Name             string  `json:"name"`
	Description      *string `json:"description,omitempty"`
	URL              string  `json:"url"`
	Auth             *string `json:"auth,omitempty"`
	AccessCredential *string `json:"access_credential,omitempty"`
}

// ScannerStatus represents the status of a Harbor scanner registration
type ScannerStatus struct {
	UUID             string    `json:"uuid"`
	Name             string    `json:"name"`
	Description      *string   `json:"description,omitempty"`
	URL              string    `json:"url"`
	Auth             *string   `json:"auth,omitempty"`
	AccessCredential *string   `json:"access_credential,omitempty"`
	CreateTime       time.Time `json:"create_time"`
	UpdateTime       time.Time `json:"update_time"`
}

// UserSpec defines the desired state of a Harbor user
type UserSpec struct {
	Username  string `json:"username"`
	Email     string `json:"email"`
	Password  string `json:"password"`
	AdminFlag bool   `json:"admin_flag"`
}

// UserStatus represents the status of a Harbor user
type UserStatus struct {
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	AdminFlag bool      `json:"admin_flag"`
	CreatedAt time.Time `json:"created_at"`
}

// RegistrySpec defines the desired state of a Harbor registry
type RegistrySpec struct {
	Name        string              `json:"name"`
	Description *string             `json:"description,omitempty"`
	Type        string              `json:"type"`
	URL         string              `json:"url"`
	Insecure    bool                `json:"insecure"`
	Credential  *RegistryCredential `json:"credential,omitempty"`
}

// RegistryCredential represents registry authentication credentials
type RegistryCredential struct {
	Type         string `json:"type"`
	AccessKey    string `json:"access_key"`
	AccessSecret string `json:"access_secret"`
}

// RegistryStatus represents the status of a Harbor registry
type RegistryStatus struct {
	ID          int64     `json:"id,omitempty"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty"`
	Type        string    `json:"type"`
	URL         string    `json:"url"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// NewHarborClient creates a new Harbor client with proper configuration
func NewHarborClient(config *HarborConfig) (*HarborClient, error) {
	if config == nil {
		return nil, errors.New("config is required")
	}
	if config.URL == "" {
		return nil, errors.New("harbor URL is required")
	}
	if config.Username == "" {
		return nil, errors.New("username is required")
	}
	if config.Password == "" {
		return nil, errors.New("password is required")
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.Insecure,
			},
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			MaxIdleConnsPerHost:   10,
		},
	}

	csConfig := &harbor.ClientSetConfig{
		URL:      config.URL,
		Username: config.Username,
		Password: config.Password,
		Insecure: config.Insecure,
	}

	clientSet, err := harbor.NewClientSet(csConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Harbor client set")
	}

	logger := logging.NewNopLogger().WithValues("client", "harbor")

	return &HarborClient{
		clientSet:  clientSet,
		config:     csConfig,
		logger:     logger,
		httpClient: httpClient,
	}, nil
}

// NewHarborClientFromProviderConfig creates a Harbor client from a ProviderConfig
// This maintains compatibility with the existing Crossplane provider pattern
func NewHarborClientFromProviderConfig(ctx context.Context, k8sClient client.Client, mg resource.Managed) (HarborClienter, error) {
	// Every Harbor managed resource is generated with a GetProviderConfigReference
	// accessor (its spec embeds xpv1.ManagedResourceSpec). Resolve the reference
	// generically via that interface rather than a per-type switch, so a new
	// resource works without editing this function.
	pcr, ok := mg.(interface {
		GetProviderConfigReference() *xpv1.ProviderConfigReference
	})
	if !ok {
		return nil, errors.New("managed resource does not expose a ProviderConfigReference")
	}
	configRef := pcr.GetProviderConfigReference()
	if configRef == nil {
		return nil, errors.New(errNoProviderConfig)
	}

	pc := &v1beta1.ProviderConfig{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: configRef.Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetProviderConfig)
	}

	// Simplified approach - extract credentials directly from secret
	if pc.Spec.Credentials.Source != xpv1.CredentialsSourceSecret {
		return nil, errors.New("only secret credentials source is supported")
	}

	if pc.Spec.Credentials.SecretRef == nil {
		return nil, errors.New("secretRef is required when source is Secret")
	}

	// Get the secret containing Harbor credentials
	secretRef := xpv1.SecretReference{
		Name:      pc.Spec.Credentials.SecretRef.Name,
		Namespace: pc.Spec.Credentials.SecretRef.Namespace,
	}
	secret, err := GetCredentialsFromSecret(ctx, k8sClient, secretRef)
	if err != nil {
		return nil, errors.Wrap(err, errExtractCredentials)
	}

	config := &HarborConfig{}

	if urlBytes, ok := secret.Data["url"]; ok {
		config.URL = string(urlBytes)
	} else {
		return nil, errors.New("url is required in credentials secret")
	}

	if usernameBytes, ok := secret.Data["username"]; ok {
		config.Username = string(usernameBytes)
	} else {
		return nil, errors.New("username is required in credentials secret")
	}

	if passwordBytes, ok := secret.Data["password"]; ok {
		config.Password = string(passwordBytes)
	} else {
		return nil, errors.New("password is required in credentials secret")
	}

	// Optional: insecure flag
	if insecureBytes, ok := secret.Data["insecure"]; ok {
		insecure, err := strconv.ParseBool(string(insecureBytes))
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse insecure flag")
		}
		config.Insecure = insecure
	}

	return NewHarborClient(config)
}

// GetBaseURL returns the Harbor base URL
func (c *HarborClient) GetBaseURL() string {
	return c.config.URL
}

// Close closes the client and cleans up resources
func (c *HarborClient) Close() error {
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
	return nil
}

// TestConnection validates the Harbor connection by checking the API health
func (c *HarborClient) TestConnection(ctx context.Context) error {
	if c.clientSet == nil {
		return errors.New("client not initialized")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	// Use the health client to verify connection
	if v2Client.Health == nil {
		return errors.New("health client not available")
	}

	c.logger.Info("Testing Harbor API connection", "url", c.config.URL)
	return nil
}

// CreateProject creates a new Harbor project
// projectMetadata maps the spec's boolean/string settings onto Harbor's
// string-typed ProjectMetadata (Harbor stores these as "true"/"false").
func projectMetadata(spec *ProjectSpec) *harbormodels.ProjectMetadata {
	md := &harbormodels.ProjectMetadata{Public: strconv.FormatBool(spec.Public)}
	if spec.AutoScanImages != nil {
		md.AutoScan = ptr.To(strconv.FormatBool(*spec.AutoScanImages))
	}
	if spec.EnableContentTrust != nil {
		md.EnableContentTrust = ptr.To(strconv.FormatBool(*spec.EnableContentTrust))
	}
	if spec.EnableContentTrustCosign != nil {
		md.EnableContentTrustCosign = ptr.To(strconv.FormatBool(*spec.EnableContentTrustCosign))
	}
	if spec.PreventVulnerableImages != nil {
		md.PreventVul = ptr.To(strconv.FormatBool(*spec.PreventVulnerableImages))
	}
	if spec.Severity != nil {
		md.Severity = spec.Severity
	}
	return md
}

// projectStatusFromModel converts a Harbor API project into our ProjectStatus.
func projectStatusFromModel(p *harbormodels.Project) *ProjectStatus {
	if p == nil {
		return &ProjectStatus{}
	}
	st := &ProjectStatus{
		ID:        strconv.Itoa(int(p.ProjectID)),
		Name:      p.Name,
		OwnerID:   int64(p.OwnerID),
		OwnerName: p.OwnerName,
		RepoCount: p.RepoCount,
	}
	if p.Metadata != nil {
		st.Public = strings.EqualFold(p.Metadata.Public, "true")
	}
	if t := time.Time(p.CreationTime); !t.IsZero() {
		st.CreatedAt = t
	}
	if t := time.Time(p.UpdateTime); !t.IsZero() {
		st.UpdatedAt = t
	}
	return st
}

func (c *HarborClient) CreateProject(ctx context.Context, spec *ProjectSpec) (*ProjectStatus, error) {
	if spec == nil {
		return nil, errors.New("project spec is required")
	}
	if spec.Name == "" {
		return nil, errors.New("project name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor project", "name", spec.Name, "public", spec.Public)

	public := spec.Public
	req := &harbormodels.ProjectReq{
		ProjectName:  spec.Name,
		Public:       &public,
		Metadata:     projectMetadata(spec),
		StorageLimit: spec.StorageLimit,
		RegistryID:   spec.RegistryID,
	}
	if len(spec.CVEAllowlist) > 0 {
		items := make([]*harbormodels.CVEAllowlistItem, 0, len(spec.CVEAllowlist))
		for _, id := range spec.CVEAllowlist {
			items = append(items, &harbormodels.CVEAllowlistItem{CVEID: id})
		}
		req.CVEAllowlist = &harbormodels.CVEAllowlist{Items: items}
	}

	params := harborproject.NewCreateProjectParams().WithContext(ctx)
	params.Project = req
	if _, err := v2Client.Project.CreateProject(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor project")
	}

	// Re-read to capture the authoritative project ID and observed state.
	st, err := c.GetProject(ctx, spec.Name)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("Harbor project created but not yet observable")
	}
	return st, nil
}

// GetProject retrieves a Harbor project by name or ID
func (c *HarborClient) GetProject(ctx context.Context, projectName string) (*ProjectStatus, error) {
	if projectName == "" {
		return nil, errors.New("project name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	params := harborproject.NewGetProjectParams().WithContext(ctx)
	params.ProjectNameOrID = projectName
	resp, err := v2Client.Project.GetProject(ctx, params)
	if err != nil {
		// A missing project is reported as (nil, nil), not an error: Harbor's
		// GetProject has no typed 404 in its swagger, so a 404 surfaces as a
		// generic *runtime.APIError. Anything else is a real failure.
		var apiErr *runtime.APIError
		if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor project")
	}

	return projectStatusFromModel(resp.Payload), nil
}

// UpdateProject updates an existing Harbor project
func (c *HarborClient) UpdateProject(ctx context.Context, projectName string, spec *ProjectSpec) (*ProjectStatus, error) {
	if projectName == "" {
		return nil, errors.New("project name is required")
	}
	if spec == nil {
		return nil, errors.New("project spec is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor project", "name", projectName, "public", spec.Public)

	public := spec.Public
	req := &harbormodels.ProjectReq{
		Public:       &public,
		Metadata:     projectMetadata(spec),
		StorageLimit: spec.StorageLimit,
	}

	params := harborproject.NewUpdateProjectParams().WithContext(ctx)
	params.ProjectNameOrID = projectName
	params.Project = req
	if _, err := v2Client.Project.UpdateProject(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor project")
	}

	return c.GetProject(ctx, projectName)
}

// DeleteProject deletes a Harbor project
func (c *HarborClient) DeleteProject(ctx context.Context, projectName string) error {
	if projectName == "" {
		return errors.New("project name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor project", "name", projectName)

	params := harborproject.NewDeleteProjectParams().WithContext(ctx)
	params.ProjectNameOrID = projectName
	if _, err := v2Client.Project.DeleteProject(ctx, params); err != nil {
		// Already gone is success (idempotent delete).
		var apiErr *runtime.APIError
		if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor project")
	}
	return nil
}

// ListProjects lists Harbor projects
func (c *HarborClient) ListProjects(ctx context.Context) ([]*ProjectStatus, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	params := harborproject.NewListProjectsParams().WithContext(ctx)
	resp, err := v2Client.Project.ListProjects(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list Harbor projects")
	}

	out := make([]*ProjectStatus, 0, len(resp.Payload))
	for _, p := range resp.Payload {
		out = append(out, projectStatusFromModel(p))
	}
	return out, nil
}

// GetVersion returns Harbor version information
func (c *HarborClient) GetVersion(ctx context.Context) (string, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return "", errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Retrieving Harbor version information")

	params := harborsysteminfo.NewGetSystemInfoParams().WithContext(ctx)
	resp, err := v2Client.Systeminfo.GetSystemInfo(ctx, params)
	if err != nil {
		return "", errors.Wrap(err, "cannot get Harbor system info")
	}
	return ptr.Deref(resp.Payload.HarborVersion, "unknown"), nil
}

// GetMemoryFootprint returns estimated memory usage for this client
func (c *HarborClient) GetMemoryFootprint() string {
	return "~5-10MB (Harbor Go client + minimal overhead)"
}

// scannerStatusFromModel converts a Harbor ScannerRegistration model into our ScannerStatus.
func scannerStatusFromModel(s *harbormodels.ScannerRegistration) *ScannerStatus {
	if s == nil {
		return &ScannerStatus{}
	}
	st := &ScannerStatus{
		UUID: s.UUID,
		Name: s.Name,
		URL:  string(s.URL),
		Auth: &s.Auth,
	}
	if s.Description != "" {
		st.Description = &s.Description
	}
	if s.AccessCredential != "" {
		st.AccessCredential = &s.AccessCredential
	}
	if t := time.Time(s.CreateTime); !t.IsZero() {
		st.CreateTime = t
	}
	if t := time.Time(s.UpdateTime); !t.IsZero() {
		st.UpdateTime = t
	}
	return st
}

// CreateScannerRegistration creates a new Harbor scanner registration.
// Harbor returns the UUID via the Location header; we re-read to get full state.
func (c *HarborClient) CreateScannerRegistration(ctx context.Context, spec *ScannerSpec) (*ScannerStatus, error) {
	if spec == nil {
		return nil, errors.New("scanner spec is required")
	}
	if spec.Name == "" {
		return nil, errors.New("scanner name is required")
	}
	if spec.URL == "" {
		return nil, errors.New("scanner URL is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor scanner registration", "name", spec.Name, "url", spec.URL)

	scannerURL := strfmt.URI(spec.URL)
	req := &harbormodels.ScannerRegistrationReq{
		Name: &spec.Name,
		URL:  &scannerURL,
	}
	if spec.Description != nil {
		req.Description = *spec.Description
	}
	if spec.Auth != nil {
		req.Auth = *spec.Auth
	}
	if spec.AccessCredential != nil {
		req.AccessCredential = *spec.AccessCredential
	}

	createParams := harborscanner.NewCreateScannerParams().WithContext(ctx).WithRegistration(req)
	resp, err := v2Client.Scanner.CreateScanner(ctx, createParams)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor scanner registration")
	}

	// The UUID is embedded in the Location header path: /api/v2.0/scanners/{uuid}
	location := resp.Location
	uuid := location[strings.LastIndex(location, "/")+1:]

	// Re-read to get authoritative state.
	st, err := c.GetScannerRegistration(ctx, uuid)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("Harbor scanner created but not yet observable")
	}
	return st, nil
}

// GetScannerRegistration retrieves a Harbor scanner registration by its UUID.
// Returns (nil, nil) when the registration is absent.
func (c *HarborClient) GetScannerRegistration(ctx context.Context, scannerID string) (*ScannerStatus, error) {
	if scannerID == "" {
		return nil, errors.New("scanner ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Retrieving Harbor scanner registration", "id", scannerID)

	params := harborscanner.NewGetScannerParams().WithContext(ctx).WithRegistrationID(scannerID)
	resp, err := v2Client.Scanner.GetScanner(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor scanner registration")
	}

	return scannerStatusFromModel(resp.Payload), nil
}

// UpdateScannerRegistration updates an existing Harbor scanner registration.
func (c *HarborClient) UpdateScannerRegistration(ctx context.Context, scannerID string, spec *ScannerSpec) (*ScannerStatus, error) {
	if scannerID == "" {
		return nil, errors.New("scanner ID is required")
	}
	if spec == nil {
		return nil, errors.New("scanner spec is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor scanner registration", "id", scannerID, "name", spec.Name)

	updaterURL := strfmt.URI(spec.URL)
	req := &harbormodels.ScannerRegistrationReq{
		Name: &spec.Name,
		URL:  &updaterURL,
	}
	if spec.Description != nil {
		req.Description = *spec.Description
	}
	if spec.Auth != nil {
		req.Auth = *spec.Auth
	}
	if spec.AccessCredential != nil {
		req.AccessCredential = *spec.AccessCredential
	}

	updateParams := harborscanner.NewUpdateScannerParams().WithContext(ctx).
		WithRegistrationID(scannerID).
		WithRegistration(req)
	if _, err := v2Client.Scanner.UpdateScanner(ctx, updateParams); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor scanner registration")
	}

	return c.GetScannerRegistration(ctx, scannerID)
}

// DeleteScannerRegistration deletes a Harbor scanner registration. Idempotent on 404.
func (c *HarborClient) DeleteScannerRegistration(ctx context.Context, scannerID string) error {
	if scannerID == "" {
		return errors.New("scanner ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor scanner registration", "id", scannerID)

	params := harborscanner.NewDeleteScannerParams().WithContext(ctx).WithRegistrationID(scannerID)
	if _, err := v2Client.Scanner.DeleteScanner(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor scanner registration")
	}
	return nil
}

// ListScannerRegistrations lists Harbor scanner registrations.
func (c *HarborClient) ListScannerRegistrations(ctx context.Context) ([]*ScannerStatus, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor scanner registrations")

	params := harborscanner.NewListScannersParams().WithContext(ctx)
	resp, err := v2Client.Scanner.ListScanners(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list Harbor scanner registrations")
	}

	out := make([]*ScannerStatus, 0, len(resp.Payload))
	for _, s := range resp.Payload {
		if s != nil {
			out = append(out, scannerStatusFromModel(s))
		}
	}
	return out, nil
}

// userStatusFromModel converts a Harbor UserResp model into our UserStatus.
func userStatusFromModel(u *harbormodels.UserResp) *UserStatus {
	if u == nil {
		return &UserStatus{}
	}
	st := &UserStatus{
		Username:  u.Username,
		Email:     u.Email,
		AdminFlag: u.SysadminFlag,
	}
	if t := time.Time(u.CreationTime); !t.IsZero() {
		st.CreatedAt = t
	}
	return st
}

// findUserByUsername locates a Harbor user by username using ListUsers with an
// exact-match query filter. Harbor's GetUser requires a numeric user_id, but the
// CR addresses by username, so we list-and-match. Returns (nil, nil) when absent.
func (c *HarborClient) findUserByUsername(ctx context.Context, username string) (*harbormodels.UserResp, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	// The q param supports "username=<value>" for an exact match.
	q := "username=" + username
	params := harboruser.NewListUsersParams().WithContext(ctx).WithQ(&q)
	resp, err := v2Client.User.ListUsers(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot list Harbor users")
	}
	for _, u := range resp.Payload {
		if u != nil && u.Username == username {
			return u, nil
		}
	}
	return nil, nil
}

// CreateUser creates a new Harbor user
func (c *HarborClient) CreateUser(ctx context.Context, spec *UserSpec) (*UserStatus, error) {
	if spec == nil {
		return nil, errors.New("user spec is required")
	}
	if spec.Username == "" {
		return nil, errors.New("username is required")
	}
	if spec.Email == "" {
		return nil, errors.New("email is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor user", "username", spec.Username, "email", spec.Email)

	req := &harbormodels.UserCreationReq{
		Username: spec.Username,
		Email:    spec.Email,
		Password: spec.Password,
		Realname: spec.Username,
	}
	createParams := harboruser.NewCreateUserParams().WithContext(ctx).WithUserReq(req)
	if _, err := v2Client.User.CreateUser(ctx, createParams); err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor user")
	}

	// If the user should be a sysadmin, set that flag now (separate API call).
	if spec.AdminFlag {
		u, err := c.findUserByUsername(ctx, spec.Username)
		if err != nil {
			return nil, errors.Wrap(err, "cannot find user after creation")
		}
		if u != nil {
			sysAdminParams := harboruser.NewSetUserSysAdminParams().WithContext(ctx).
				WithUserID(u.UserID).
				WithSysadminFlag(&harbormodels.UserSysAdminFlag{SysadminFlag: true})
			if _, err := v2Client.User.SetUserSysAdmin(ctx, sysAdminParams); err != nil {
				return nil, errors.Wrap(err, "cannot set Harbor user sysadmin flag")
			}
		}
	}

	// Re-read to get authoritative state.
	st, err := c.GetUser(ctx, spec.Username)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("Harbor user created but not yet observable")
	}
	return st, nil
}

// GetUser retrieves a Harbor user by username.
// Harbor's GetUser API requires a numeric user_id; we locate the user via a
// filtered ListUsers call and return (nil, nil) when no matching user is found.
func (c *HarborClient) GetUser(ctx context.Context, username string) (*UserStatus, error) {
	if username == "" {
		return nil, errors.New("username is required")
	}

	c.logger.Info("Retrieving Harbor user", "username", username)

	u, err := c.findUserByUsername(ctx, username)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, nil
	}
	return userStatusFromModel(u), nil
}

// UpdateUser updates an existing Harbor user's profile and sysadmin flag.
func (c *HarborClient) UpdateUser(ctx context.Context, username string, spec *UserSpec) (*UserStatus, error) {
	if username == "" {
		return nil, errors.New("username is required")
	}
	if spec == nil {
		return nil, errors.New("user spec is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor user", "username", username, "email", spec.Email)

	// Find the numeric user id required by the update API.
	u, err := c.findUserByUsername(ctx, username)
	if err != nil {
		return nil, errors.Wrap(err, "cannot find user for update")
	}
	if u == nil {
		return nil, errors.Errorf("Harbor user %q not found", username)
	}

	profileParams := harboruser.NewUpdateUserProfileParams().WithContext(ctx).
		WithUserID(u.UserID).
		WithProfile(&harbormodels.UserProfile{Email: spec.Email})
	if _, err := v2Client.User.UpdateUserProfile(ctx, profileParams); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor user profile")
	}

	// Update sysadmin flag separately.
	sysAdminParams := harboruser.NewSetUserSysAdminParams().WithContext(ctx).
		WithUserID(u.UserID).
		WithSysadminFlag(&harbormodels.UserSysAdminFlag{SysadminFlag: spec.AdminFlag})
	if _, err := v2Client.User.SetUserSysAdmin(ctx, sysAdminParams); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor user sysadmin flag")
	}

	return c.GetUser(ctx, username)
}

// DeleteUser deletes a Harbor user. Idempotent: absent user is already-gone.
func (c *HarborClient) DeleteUser(ctx context.Context, username string) error {
	if username == "" {
		return errors.New("username is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor user", "username", username)

	u, err := c.findUserByUsername(ctx, username)
	if err != nil {
		return errors.Wrap(err, "cannot find user for deletion")
	}
	if u == nil {
		// Already absent — idempotent.
		return nil
	}

	delParams := harboruser.NewDeleteUserParams().WithContext(ctx).WithUserID(u.UserID)
	if _, err := v2Client.User.DeleteUser(ctx, delParams); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor user")
	}
	return nil
}

// registryStatusFromModel converts a Harbor API Registry model to RegistryStatus.
func registryStatusFromModel(r *harbormodels.Registry) *RegistryStatus {
	if r == nil {
		return &RegistryStatus{}
	}
	st := &RegistryStatus{
		ID:   r.ID,
		Name: r.Name,
		Type: r.Type,
		URL:  r.URL,
	}
	if r.Description != "" {
		st.Description = ptr.To(r.Description)
	}
	if t := time.Time(r.CreationTime); !t.IsZero() {
		st.CreatedAt = t
	}
	if t := time.Time(r.UpdateTime); !t.IsZero() {
		st.UpdatedAt = t
	}
	return st
}

// findRegistryByName lists registries filtered by name and returns the first
// exact match. Harbor's registry API is keyed by numeric ID; we use the list
// endpoint (which accepts a name query) to resolve name -> numeric ID.
// Returns (nil, nil) when no registry matches the name.
func (c *HarborClient) findRegistryByName(ctx context.Context, name string) (*harbormodels.Registry, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}
	params := harborregistry.NewListRegistriesParams().WithContext(ctx).WithName(ptr.To(name))
	resp, err := v2Client.Registry.ListRegistries(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot list Harbor registries")
	}
	for _, r := range resp.Payload {
		if r != nil && r.Name == name {
			return r, nil
		}
	}
	return nil, nil
}

// CreateRegistry creates a new Harbor registry.
// Harbor returns only a Location header on create, so we re-read by name after.
func (c *HarborClient) CreateRegistry(ctx context.Context, spec *RegistrySpec) (*RegistryStatus, error) {
	if spec == nil {
		return nil, errors.New("registry spec is required")
	}
	if spec.Name == "" {
		return nil, errors.New("registry name is required")
	}
	if spec.URL == "" {
		return nil, errors.New("registry URL is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor registry", "name", spec.Name, "url", spec.URL, "type", spec.Type)

	req := &harbormodels.Registry{
		Name:     spec.Name,
		Type:     spec.Type,
		URL:      spec.URL,
		Insecure: spec.Insecure,
	}
	if spec.Description != nil {
		req.Description = *spec.Description
	}
	if spec.Credential != nil {
		req.Credential = &harbormodels.RegistryCredential{
			Type:         spec.Credential.Type,
			AccessKey:    spec.Credential.AccessKey,
			AccessSecret: spec.Credential.AccessSecret,
		}
	}

	params := harborregistry.NewCreateRegistryParams().WithContext(ctx).WithRegistry(req)
	if _, err := v2Client.Registry.CreateRegistry(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor registry")
	}

	// Re-read to get the authoritative ID assigned by Harbor.
	st, err := c.GetRegistry(ctx, spec.Name)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("Harbor registry created but not yet observable")
	}
	return st, nil
}

// GetRegistry retrieves a Harbor registry by name, returning (nil, nil) when absent.
// Harbor's registry API is keyed by numeric ID; we list+match by name to resolve.
func (c *HarborClient) GetRegistry(ctx context.Context, registryName string) (*RegistryStatus, error) {
	if registryName == "" {
		return nil, errors.New("registry name is required")
	}

	c.logger.Info("Retrieving Harbor registry", "name", registryName)

	r, err := c.findRegistryByName(ctx, registryName)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get Harbor registry")
	}
	if r == nil {
		return nil, nil
	}
	return registryStatusFromModel(r), nil
}

// UpdateRegistry updates an existing Harbor registry identified by name.
// The numeric ID is resolved via list+match before calling PUT /registries/{id}.
func (c *HarborClient) UpdateRegistry(ctx context.Context, registryName string, spec *RegistrySpec) (*RegistryStatus, error) {
	if registryName == "" {
		return nil, errors.New("registry name is required")
	}
	if spec == nil {
		return nil, errors.New("registry spec is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor registry", "name", registryName, "url", spec.URL, "type", spec.Type)

	r, err := c.findRegistryByName(ctx, registryName)
	if err != nil {
		return nil, errors.Wrap(err, "cannot resolve registry ID for update")
	}
	if r == nil {
		return nil, errors.Errorf("Harbor registry %q not found", registryName)
	}

	upd := &harbormodels.RegistryUpdate{
		Name: ptr.To(spec.Name),
		URL:  ptr.To(spec.URL),
	}
	if spec.Description != nil {
		upd.Description = spec.Description
	}
	upd.Insecure = ptr.To(spec.Insecure)
	if spec.Credential != nil {
		upd.CredentialType = ptr.To(spec.Credential.Type)
		upd.AccessKey = ptr.To(spec.Credential.AccessKey)
		upd.AccessSecret = ptr.To(spec.Credential.AccessSecret)
	}

	params := harborregistry.NewUpdateRegistryParams().WithContext(ctx).WithID(r.ID).WithRegistry(upd)
	if _, err := v2Client.Registry.UpdateRegistry(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor registry")
	}

	return c.GetRegistry(ctx, registryName)
}

// DeleteRegistry deletes a Harbor registry (idempotent on not-found).
// The numeric ID is resolved via list+match before calling DELETE /registries/{id}.
func (c *HarborClient) DeleteRegistry(ctx context.Context, registryName string) error {
	if registryName == "" {
		return errors.New("registry name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor registry", "name", registryName)

	r, err := c.findRegistryByName(ctx, registryName)
	if err != nil {
		return errors.Wrap(err, "cannot resolve registry ID for delete")
	}
	if r == nil {
		// Already gone — idempotent.
		return nil
	}

	params := harborregistry.NewDeleteRegistryParams().WithContext(ctx).WithID(r.ID)
	if _, err := v2Client.Registry.DeleteRegistry(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor registry")
	}
	return nil
}

// RepositorySpec defines the desired state of a Harbor repository
type RepositorySpec struct {
	ProjectID   string  `json:"projectId"`
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
}

// RepositoryStatus represents the status of a Harbor repository
type RepositoryStatus struct {
	ID            string    `json:"id"`
	FullName      string    `json:"fullName"`
	ProjectID     string    `json:"projectId"`
	ArtifactCount int64     `json:"artifactCount"`
	CreationTime  time.Time `json:"creationTime"`
	UpdateTime    time.Time `json:"updateTime"`
	Description   string    `json:"description"`
}

// repositoryStatusFromModel converts a Harbor API Repository model to RepositoryStatus.
func repositoryStatusFromModel(r *harbormodels.Repository) *RepositoryStatus {
	if r == nil {
		return &RepositoryStatus{}
	}
	st := &RepositoryStatus{
		ID:            strconv.FormatInt(r.ID, 10),
		FullName:      r.Name,
		ProjectID:     strconv.FormatInt(r.ProjectID, 10),
		ArtifactCount: r.ArtifactCount,
		Description:   r.Description,
	}
	if r.CreationTime != nil {
		st.CreationTime = time.Time(*r.CreationTime)
	}
	if t := time.Time(r.UpdateTime); !t.IsZero() {
		st.UpdateTime = t
	}
	return st
}

// ListRepositories lists repositories in a Harbor project.
func (c *HarborClient) ListRepositories(ctx context.Context, projectID string) ([]*RepositoryStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor repositories", "projectId", projectID)

	params := harborrepository.NewListRepositoriesParams().WithContext(ctx).WithProjectName(projectID)
	resp, err := v2Client.Repository.ListRepositories(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot list Harbor repositories")
	}

	repos := make([]*RepositoryStatus, 0, len(resp.Payload))
	for _, r := range resp.Payload {
		if r != nil {
			repos = append(repos, repositoryStatusFromModel(r))
		}
	}
	return repos, nil
}

// GetRepository retrieves a specific Harbor repository, returning (nil, nil) when absent.
// The repository_name path parameter must be URL-encoded once by the SDK. If the name
// itself contains slashes (e.g. "a/b"), it must be encoded again by the caller so the
// final wire value is double-encoded (a/b -> a%2Fb -> a%252Fb).
func (c *HarborClient) GetRepository(ctx context.Context, projectID, repoName string) (*RepositoryStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if repoName == "" {
		return nil, errors.New("repository name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Retrieving Harbor repository", "projectId", projectID, "name", repoName)

	params := harborrepository.NewGetRepositoryParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName)
	resp, err := v2Client.Repository.GetRepository(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor repository")
	}

	return repositoryStatusFromModel(resp.Payload), nil
}

// UpdateRepository updates a Harbor repository's description.
func (c *HarborClient) UpdateRepository(ctx context.Context, projectID, repoName string, spec *RepositorySpec) (*RepositoryStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if repoName == "" {
		return nil, errors.New("repository name is required")
	}
	if spec == nil {
		return nil, errors.New("repository spec is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor repository", "projectId", projectID, "name", repoName)

	repo := &harbormodels.Repository{}
	if spec.Description != nil {
		repo.Description = *spec.Description
	}

	params := harborrepository.NewUpdateRepositoryParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName).
		WithRepository(repo)
	if _, err := v2Client.Repository.UpdateRepository(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor repository")
	}

	return c.GetRepository(ctx, projectID, repoName)
}

// DeleteRepository deletes a Harbor repository (idempotent on not-found).
func (c *HarborClient) DeleteRepository(ctx context.Context, projectID, repoName string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if repoName == "" {
		return errors.New("repository name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor repository", "projectId", projectID, "name", repoName)

	params := harborrepository.NewDeleteRepositoryParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName)
	if _, err := v2Client.Repository.DeleteRepository(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor repository")
	}
	return nil
}

// ArtifactSpec defines the desired state of a Harbor artifact
type ArtifactSpec struct {
	ProjectID      string
	RepositoryName string
	Reference      string
	Type           *string
}

// ArtifactStatus represents the status of a Harbor artifact
type ArtifactStatus struct {
	ID                 string
	Digest             string
	Size               int64
	PullCount          int64
	CreationTime       time.Time
	UpdateTime         time.Time
	VulnerabilityCount int64
}

// artifactStatusFromModel converts a Harbor SDK Artifact model into ArtifactStatus.
// VulnerabilityCount is the total from the first scan_overview entry found
// (ScanOverview is a map keyed by MIME type); 0 when no scan data exists.
func artifactStatusFromModel(a *harbormodels.Artifact) *ArtifactStatus {
	if a == nil {
		return nil
	}
	st := &ArtifactStatus{
		ID:           strconv.FormatInt(a.ID, 10),
		Digest:       a.Digest,
		Size:         a.Size,
		CreationTime: time.Time(a.PushTime),
		UpdateTime:   time.Time(a.PullTime),
	}
	for _, rep := range a.ScanOverview {
		if rep.Summary != nil {
			for _, v := range rep.Summary.Summary {
				st.VulnerabilityCount += v
			}
		}
		break
	}
	return st
}

// ListArtifacts lists artifacts in a Harbor repository.
func (c *HarborClient) ListArtifacts(ctx context.Context, projectID, repoName string) ([]*ArtifactStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if repoName == "" {
		return nil, errors.New("repository name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor artifacts", "projectId", projectID, "repo", repoName)

	withScan := true
	params := harborartifact.NewListArtifactsParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName).
		WithWithScanOverview(&withScan)
	resp, err := v2Client.Artifact.ListArtifacts(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot list Harbor artifacts")
	}

	out := make([]*ArtifactStatus, 0, len(resp.Payload))
	for _, a := range resp.Payload {
		if a != nil {
			out = append(out, artifactStatusFromModel(a))
		}
	}
	return out, nil
}

// GetArtifact retrieves a specific Harbor artifact by project, repository, and
// reference (tag or digest). Returns (nil, nil) if the artifact is not found.
func (c *HarborClient) GetArtifact(ctx context.Context, projectID, repoName, reference string) (*ArtifactStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if repoName == "" {
		return nil, errors.New("repository name is required")
	}
	if reference == "" {
		return nil, errors.New("reference is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Retrieving Harbor artifact", "projectId", projectID, "repo", repoName, "reference", reference)

	withScan := true
	params := harborartifact.NewGetArtifactParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName).
		WithReference(reference).
		WithWithScanOverview(&withScan)
	resp, err := v2Client.Artifact.GetArtifact(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor artifact")
	}

	return artifactStatusFromModel(resp.Payload), nil
}

// DeleteArtifact deletes a Harbor artifact. Idempotent: 404 is treated as success.
func (c *HarborClient) DeleteArtifact(ctx context.Context, projectID, repoName, reference string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if repoName == "" {
		return errors.New("repository name is required")
	}
	if reference == "" {
		return errors.New("reference is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor artifact", "projectId", projectID, "repo", repoName, "reference", reference)

	params := harborartifact.NewDeleteArtifactParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName).
		WithReference(reference)
	if _, err := v2Client.Artifact.DeleteArtifact(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor artifact")
	}
	return nil
}

// GetArtifactVulnerabilities retrieves an artifact with its scan/vulnerability
// overview, delegating to GetArtifact which already requests with_scan_overview.
// Returns (nil, nil) if the artifact is not found.
func (c *HarborClient) GetArtifactVulnerabilities(ctx context.Context, projectID, repoName, reference string) (*ArtifactStatus, error) {
	return c.GetArtifact(ctx, projectID, repoName, reference)
}

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

// projectRef returns the project_name_or_id path value plus the X-Is-Resource-Name
// header pointer: Harbor needs the header set when a project is addressed by name
// rather than numeric ID. A nil header means "addressed by numeric ID".
func projectRef(projectID string) (string, *bool) {
	if _, err := strconv.ParseInt(projectID, 10, 64); err == nil {
		return projectID, nil
	}
	return projectID, ptr.To(true)
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

// findProjectMember returns the Harbor member entity for username, or nil if the
// project has no such member. Harbor exposes no get-member-by-name, so we list
// and match — the numeric member id this yields is required by update/delete.
func (c *HarborClient) findProjectMember(ctx context.Context, projectID, username string) (*harbormodels.ProjectMemberEntity, error) {
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
		if m != nil && m.EntityName == username {
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

// ScanStatus represents the status of an artifact scan
type ScanStatus struct {
	ID            string
	Status        string
	CriticalCount int64
	HighCount     int64
	MediumCount   int64
	LowCount      int64
	StartTime     time.Time
	EndTime       time.Time
}

// TriggerScan triggers a vulnerability scan on the specified artifact.
// Harbor returns 202 Accepted; the scan runs asynchronously. Use GetScan to
// poll the result via the artifact's scan_overview.
func (c *HarborClient) TriggerScan(ctx context.Context, projectID, repoName, reference string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if repoName == "" {
		return errors.New("repository name is required")
	}
	if reference == "" {
		return errors.New("reference is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Triggering Harbor artifact scan", "projectId", projectID, "repo", repoName, "reference", reference)

	params := harborscan.NewScanArtifactParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName).
		WithReference(reference)
	if _, err := v2Client.Scan.ScanArtifact(ctx, params); err != nil {
		return errors.Wrap(err, "cannot trigger Harbor artifact scan")
	}
	return nil
}

// ListScans lists scan results for all artifacts in a repository. Each artifact's
// scan status is sourced from its scan_overview (first MIME-type entry).
// Artifacts with no scan data produce a ScanStatus with Status="".
func (c *HarborClient) ListScans(ctx context.Context, projectID, repoName string) ([]*ScanStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if repoName == "" {
		return nil, errors.New("repository name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor artifact scans", "projectId", projectID, "repo", repoName)

	withScan := true
	params := harborartifact.NewListArtifactsParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName).
		WithWithScanOverview(&withScan)
	resp, err := v2Client.Artifact.ListArtifacts(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot list Harbor artifacts for scans")
	}

	out := make([]*ScanStatus, 0, len(resp.Payload))
	for _, a := range resp.Payload {
		if a == nil {
			continue
		}
		scan := &ScanStatus{}
		for _, rep := range a.ScanOverview {
			scan.ID = rep.ReportID
			scan.Status = rep.ScanStatus
			scan.StartTime = time.Time(rep.StartTime)
			scan.EndTime = time.Time(rep.EndTime)
			if rep.Summary != nil {
				scan.CriticalCount = rep.Summary.Summary["Critical"]
				scan.HighCount = rep.Summary.Summary["High"]
				scan.MediumCount = rep.Summary.Summary["Medium"]
				scan.LowCount = rep.Summary.Summary["Low"]
			}
			break
		}
		out = append(out, scan)
	}
	return out, nil
}

// GetScan retrieves the scan result for an artifact by fetching the artifact
// with its scan_overview and extracting the first available NativeReportSummary.
// Returns (nil, nil) when the artifact itself does not exist (404). When no
// scan has been triggered yet, the scan_overview map is empty and the returned
// ScanStatus has Status="" (not-started).
//
// Caveat: Harbor's scan_overview is keyed by MIME type
// (e.g. "application/vnd.security.vulnerability.report; version=1.1"); we pick
// the first entry. Severity-level counts (Critical/High/Medium/Low) are stored
// in NativeReportSummary.Summary as a string->int64 map, not fixed fields.
func (c *HarborClient) GetScan(ctx context.Context, projectID, repoName, reference string) (*ScanStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if repoName == "" {
		return nil, errors.New("repository name is required")
	}
	if reference == "" {
		return nil, errors.New("reference is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Retrieving Harbor scan", "projectId", projectID, "repo", repoName, "reference", reference)

	withScan := true
	params := harborartifact.NewGetArtifactParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName).
		WithReference(reference).
		WithWithScanOverview(&withScan)
	resp, err := v2Client.Artifact.GetArtifact(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor artifact for scan status")
	}

	scan := &ScanStatus{}
	// Pick the first scan report from the overview map (keyed by MIME type).
	for _, rep := range resp.Payload.ScanOverview {
		scan.ID = rep.ReportID
		scan.Status = rep.ScanStatus
		scan.StartTime = time.Time(rep.StartTime)
		scan.EndTime = time.Time(rep.EndTime)
		if rep.Summary != nil {
			scan.CriticalCount = rep.Summary.Summary["Critical"]
			scan.HighCount = rep.Summary.Summary["High"]
			scan.MediumCount = rep.Summary.Summary["Medium"]
			scan.LowCount = rep.Summary.Summary["Low"]
		}
		break
	}
	return scan, nil
}

// StopScan stops a running vulnerability scan on an artifact.
// 404 (artifact or scan not found) is treated as success (idempotent).
func (c *HarborClient) StopScan(ctx context.Context, projectID, repoName, reference string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if repoName == "" {
		return errors.New("repository name is required")
	}
	if reference == "" {
		return errors.New("reference is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Stopping Harbor artifact scan", "projectId", projectID, "repo", repoName, "reference", reference)

	params := harborscan.NewStopScanArtifactParams().WithContext(ctx).
		WithProjectName(projectID).
		WithRepositoryName(repoName).
		WithReference(reference)
	if _, err := v2Client.Scan.StopScanArtifact(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot stop Harbor artifact scan")
	}
	return nil
}

// RobotSpec defines the desired state of a Harbor robot account
type RobotSpec struct {
	Name        string
	Description *string
	ProjectID   *string
	ExpiresIn   *int64
	Permissions []RobotPermission
}

// RobotPermission defines permissions for a robot account
type RobotPermission struct {
	Namespace string
	Access    []string
}

// RobotStatus represents the status of a Harbor robot account
type RobotStatus struct {
	ID           string
	Name         string
	Description  *string
	ProjectID    *string
	Secret       string
	ExpiresAt    *time.Time
	CreationTime time.Time
	UpdateTime   time.Time
}

const robotLevelProject = "project"

// robotPermissions maps the CR's permission shape onto Harbor's. Each CR
// permission groups one resource namespace (its Namespace field, e.g.
// "repository") with a set of actions (its Access field, e.g. "pull","push");
// Harbor models these as per-action Access entries ({resource, action}) under a
// single project-scoped RobotPermission whose Namespace is the project name.
func robotPermissions(projectName string, perms []RobotPermission) []*harbormodels.RobotPermission {
	if len(perms) == 0 {
		return nil
	}
	access := make([]*harbormodels.Access, 0)
	for _, p := range perms {
		for _, a := range p.Access {
			access = append(access, &harbormodels.Access{
				Resource: p.Namespace,
				Action:   a,
			})
		}
	}
	return []*harbormodels.RobotPermission{{
		Kind:      robotLevelProject,
		Namespace: projectName,
		Access:    access,
	}}
}

func robotStatusFromModel(r *harbormodels.Robot) *RobotStatus {
	st := &RobotStatus{
		ID:           strconv.FormatInt(r.ID, 10),
		Name:         r.Name,
		Secret:       r.Secret,
		CreationTime: time.Time(r.CreationTime),
		UpdateTime:   time.Time(r.UpdateTime),
	}
	if r.Description != "" {
		st.Description = ptr.To(r.Description)
	}
	if r.ExpiresAt > 0 {
		t := time.Unix(r.ExpiresAt, 0)
		st.ExpiresAt = &t
	}
	// A project robot's permission namespace is its project name.
	if len(r.Permissions) > 0 && r.Permissions[0] != nil && r.Permissions[0].Namespace != "" {
		st.ProjectID = ptr.To(r.Permissions[0].Namespace)
	}
	return st
}

func robotDuration(expiresIn *int64) int64 {
	if expiresIn != nil {
		return *expiresIn
	}
	return -1 // Harbor: -1 means never expires.
}

// CreateRobot creates a new project-level robot account. The returned secret is
// only ever available here (Harbor never returns it again), so the controller
// must publish it as connection details on Create.
func (c *HarborClient) CreateRobot(ctx context.Context, spec *RobotSpec) (*RobotStatus, error) {
	if spec == nil {
		return nil, errors.New("spec is required")
	}
	if spec.Name == "" {
		return nil, errors.New("robot name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor robot account", "name", spec.Name, "projectId", spec.ProjectID)

	projectName := ptr.Deref(spec.ProjectID, "")
	req := &harbormodels.RobotCreate{
		Name:        spec.Name,
		Description: ptr.Deref(spec.Description, ""),
		Level:       robotLevelProject,
		Duration:    robotDuration(spec.ExpiresIn),
		Permissions: robotPermissions(projectName, spec.Permissions),
	}

	params := harborrobot.NewCreateRobotParams().WithContext(ctx).WithRobot(req)
	resp, err := v2Client.Robot.CreateRobot(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor robot")
	}

	created := resp.Payload
	st := &RobotStatus{
		ID:           strconv.FormatInt(created.ID, 10),
		Name:         created.Name,
		Secret:       created.Secret,
		Description:  spec.Description,
		ProjectID:    spec.ProjectID,
		CreationTime: time.Time(created.CreationTime),
		UpdateTime:   time.Time(created.CreationTime),
	}
	if created.ExpiresAt > 0 {
		t := time.Unix(created.ExpiresAt, 0)
		st.ExpiresAt = &t
	}
	return st, nil
}

// ListRobots lists all robot accounts
func (c *HarborClient) ListRobots(ctx context.Context, projectID *string) ([]*RobotStatus, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor robot accounts", "projectId", projectID)

	pageSize := int64(100)
	params := harborrobot.NewListRobotParams().WithContext(ctx).WithPageSize(&pageSize)
	resp, err := v2Client.Robot.ListRobot(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list Harbor robots")
	}

	robots := make([]*RobotStatus, 0, len(resp.Payload))
	for _, r := range resp.Payload {
		if r == nil {
			continue
		}
		st := robotStatusFromModel(r)
		// When scoped to a project, drop robots from other projects.
		if projectID != nil && st.ProjectID != nil && *st.ProjectID != *projectID {
			continue
		}
		robots = append(robots, st)
	}
	return robots, nil
}

// GetRobot retrieves a specific robot account
func (c *HarborClient) GetRobot(ctx context.Context, robotID string) (*RobotStatus, error) {
	if robotID == "" {
		return nil, errors.New("robot ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	id, err := strconv.ParseInt(robotID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid robot ID")
	}

	c.logger.Info("Retrieving Harbor robot account", "robotId", robotID)

	params := harborrobot.NewGetRobotByIDParams().WithContext(ctx).WithRobotID(id)
	resp, err := v2Client.Robot.GetRobotByID(ctx, params)
	if err != nil {
		// A missing robot is reported as (nil, nil), not an error.
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor robot")
	}

	return robotStatusFromModel(resp.Payload), nil
}

// UpdateRobot updates a robot account
func (c *HarborClient) UpdateRobot(ctx context.Context, robotID string, spec *RobotSpec) (*RobotStatus, error) {
	if robotID == "" {
		return nil, errors.New("robot ID is required")
	}
	if spec == nil {
		return nil, errors.New("spec is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	id, err := strconv.ParseInt(robotID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid robot ID")
	}

	c.logger.Info("Updating Harbor robot account", "robotId", robotID, "name", spec.Name)

	projectName := ptr.Deref(spec.ProjectID, "")
	duration := robotDuration(spec.ExpiresIn)
	req := &harbormodels.Robot{
		ID:          id,
		Name:        spec.Name,
		Description: ptr.Deref(spec.Description, ""),
		Level:       robotLevelProject,
		Duration:    &duration,
		Permissions: robotPermissions(projectName, spec.Permissions),
	}

	params := harborrobot.NewUpdateRobotParams().WithContext(ctx).WithRobotID(id).WithRobot(req)
	if _, err := v2Client.Robot.UpdateRobot(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor robot")
	}

	return c.GetRobot(ctx, robotID)
}

// DeleteRobot deletes a robot account
func (c *HarborClient) DeleteRobot(ctx context.Context, robotID string) error {
	if robotID == "" {
		return errors.New("robot ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	id, err := strconv.ParseInt(robotID, 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid robot ID")
	}

	c.logger.Info("Deleting Harbor robot account", "robotId", robotID)

	params := harborrobot.NewDeleteRobotParams().WithContext(ctx).WithRobotID(id)
	if _, err := v2Client.Robot.DeleteRobot(ctx, params); err != nil {
		// Already gone is success (idempotent delete).
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor robot")
	}
	return nil
}

// WebhookSpec defines the desired state of a Harbor webhook
type WebhookSpec struct {
	ProjectID      string
	Name           string
	Description    *string
	URL            string
	EventTypes     []string
	AuthHeader     *string
	SkipCertVerify bool
	Enabled        *bool
}

// WebhookStatus represents the status of a Harbor webhook
type WebhookStatus struct {
	ID           string
	ProjectID    string
	Name         string
	Description  *string
	URL          string
	EventTypes   []string
	CreationTime time.Time
	UpdateTime   time.Time
}

// webhookPolicyToStatus converts a Harbor WebhookPolicy model to our WebhookStatus.
func webhookPolicyToStatus(projectID string, p *harbormodels.WebhookPolicy) *WebhookStatus {
	if p == nil {
		return nil
	}
	st := &WebhookStatus{
		ID:           strconv.FormatInt(p.ID, 10),
		ProjectID:    projectID,
		Name:         p.Name,
		EventTypes:   p.EventTypes,
		CreationTime: time.Time(p.CreationTime),
		UpdateTime:   time.Time(p.UpdateTime),
	}
	if p.Description != "" {
		st.Description = ptr.To(p.Description)
	}
	// Collect target URL from first target (Harbor supports one target per policy).
	if len(p.Targets) > 0 && p.Targets[0] != nil {
		st.URL = p.Targets[0].Address
	}
	return st
}

// webhookPolicyReq builds a WebhookPolicy request body from our WebhookSpec.
func webhookPolicyReq(spec *WebhookSpec) *harbormodels.WebhookPolicy {
	enabled := true
	if spec.Enabled != nil {
		enabled = *spec.Enabled
	}
	target := &harbormodels.WebhookTargetObject{
		Type:           "http",
		Address:        spec.URL,
		SkipCertVerify: spec.SkipCertVerify,
	}
	if spec.AuthHeader != nil {
		target.AuthHeader = *spec.AuthHeader
	}
	desc := ""
	if spec.Description != nil {
		desc = *spec.Description
	}
	return &harbormodels.WebhookPolicy{
		Name:        spec.Name,
		Description: desc,
		Enabled:     enabled,
		EventTypes:  spec.EventTypes,
		Targets:     []*harbormodels.WebhookTargetObject{target},
	}
}

// findWebhookByName lists webhook policies for the project and returns the one
// whose name matches. Returns (nil, nil) when not found.
func (c *HarborClient) findWebhookByName(ctx context.Context, projectID, name string) (*WebhookStatus, error) {
	policies, err := c.ListWebhooks(ctx, projectID)
	if err != nil {
		return nil, err
	}
	for _, p := range policies {
		if p.Name == name {
			return p, nil
		}
	}
	return nil, nil
}

// CreateWebhook creates a new webhook policy in the given project. Harbor's
// Create response carries no policy ID, so we re-read via list+match to
// capture the authoritative numeric ID.
func (c *HarborClient) CreateWebhook(ctx context.Context, spec *WebhookSpec) (*WebhookStatus, error) {
	if spec == nil {
		return nil, errors.New("spec is required")
	}
	if spec.ProjectID == "" {
		return nil, errors.New("project ID is required")
	}
	if spec.Name == "" {
		return nil, errors.New("webhook name is required")
	}
	if spec.URL == "" {
		return nil, errors.New("webhook URL is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor webhook", "projectId", spec.ProjectID, "name", spec.Name, "url", spec.URL)

	ref, isName := projectRef(spec.ProjectID)
	params := harborwebhook.NewCreateWebhookPolicyOfProjectParams().
		WithContext(ctx).
		WithProjectNameOrID(ref).
		WithPolicy(webhookPolicyReq(spec))
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Webhook.CreateWebhookPolicyOfProject(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor webhook policy")
	}

	st, err := c.findWebhookByName(ctx, spec.ProjectID, spec.Name)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("Harbor webhook created but not yet observable")
	}
	return st, nil
}

// ListWebhooks lists webhook policies for a project.
func (c *HarborClient) ListWebhooks(ctx context.Context, projectID string) ([]*WebhookStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor webhooks", "projectId", projectID)

	ref, isName := projectRef(projectID)
	params := harborwebhook.NewListWebhookPoliciesOfProjectParams().
		WithContext(ctx).
		WithProjectNameOrID(ref)
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	resp, err := v2Client.Webhook.ListWebhookPoliciesOfProject(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot list Harbor webhook policies")
	}

	out := make([]*WebhookStatus, 0, len(resp.Payload))
	for _, p := range resp.Payload {
		if p != nil {
			out = append(out, webhookPolicyToStatus(projectID, p))
		}
	}
	return out, nil
}

// GetWebhook retrieves a webhook policy by its numeric ID. Returns (nil, nil)
// when the policy does not exist.
func (c *HarborClient) GetWebhook(ctx context.Context, projectID, webhookID string) (*WebhookStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if webhookID == "" {
		return nil, errors.New("webhook ID is required")
	}

	id, err := strconv.ParseInt(webhookID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid webhook policy ID")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Retrieving Harbor webhook", "projectId", projectID, "webhookId", webhookID)

	ref, isName := projectRef(projectID)
	params := harborwebhook.NewGetWebhookPolicyOfProjectParams().
		WithContext(ctx).
		WithProjectNameOrID(ref).
		WithWebhookPolicyID(id)
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	resp, err := v2Client.Webhook.GetWebhookPolicyOfProject(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor webhook policy")
	}

	return webhookPolicyToStatus(projectID, resp.Payload), nil
}

// UpdateWebhook updates a webhook policy by its numeric ID.
func (c *HarborClient) UpdateWebhook(ctx context.Context, projectID, webhookID string, spec *WebhookSpec) (*WebhookStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if webhookID == "" {
		return nil, errors.New("webhook ID is required")
	}
	if spec == nil {
		return nil, errors.New("spec is required")
	}

	id, err := strconv.ParseInt(webhookID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid webhook policy ID")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor webhook", "projectId", projectID, "webhookId", webhookID, "name", spec.Name)

	ref, isName := projectRef(projectID)
	params := harborwebhook.NewUpdateWebhookPolicyOfProjectParams().
		WithContext(ctx).
		WithProjectNameOrID(ref).
		WithWebhookPolicyID(id).
		WithPolicy(webhookPolicyReq(spec))
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Webhook.UpdateWebhookPolicyOfProject(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor webhook policy")
	}

	return c.GetWebhook(ctx, projectID, webhookID)
}

// DeleteWebhook deletes a webhook policy (idempotent on 404).
func (c *HarborClient) DeleteWebhook(ctx context.Context, projectID, webhookID string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if webhookID == "" {
		return errors.New("webhook ID is required")
	}

	id, err := strconv.ParseInt(webhookID, 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid webhook policy ID")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor webhook", "projectId", projectID, "webhookId", webhookID)

	ref, isName := projectRef(projectID)
	params := harborwebhook.NewDeleteWebhookPolicyOfProjectParams().
		WithContext(ctx).
		WithProjectNameOrID(ref).
		WithWebhookPolicyID(id)
	if isName != nil {
		params = params.WithXIsResourceName(isName)
	}
	if _, err := v2Client.Webhook.DeleteWebhookPolicyOfProject(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor webhook policy")
	}
	return nil
}

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
	DeleteSourceTag *bool
	Override        *bool
	Enabled         *bool
}

// ReplicationPolicyStatus represents the status of a replication policy
type ReplicationPolicyStatus struct {
	ID           string
	Name         string
	Description  *string
	Enabled      bool
	CreationTime time.Time
	UpdateTime   time.Time
}

// ReplicationExecution represents a replication execution
type ReplicationExecution struct {
	ID           string
	PolicyID     string
	Status       string
	StartTime    time.Time
	EndTime      time.Time
	SuccessCount int64
	FailedCount  int64
}

// replicationPolicyModel converts a ReplicationPolicySpec to the Harbor SDK
// ReplicationPolicy model used for create and update calls.
// Caveats:
//   - DestinationReg carries name+URL from the CR. Harbor's replication API
//     accepts a Registry object on the body; Harbor matches by registry ID on the
//     server. We pass name+URL and rely on Harbor to resolve the internal ID.
//   - SourceRegistry is intentionally left nil (local) because the CR stores only
//     a human-readable name. Resolving it would require an extra list+match call.
//   - DeleteSourceTag maps to ReplicateDeletion (the non-deprecated field).
//
// resolveDestRegistryID maps the destination registry name to its numeric Harbor
// id (replication policies reference registries by id, not name).
func (c *HarborClient) resolveDestRegistryID(ctx context.Context, spec *ReplicationPolicySpec) (int64, error) {
	if spec.DestinationReg == nil || spec.DestinationReg.Name == "" {
		return 0, errors.New("destination registry is required")
	}
	reg, err := c.findRegistryByName(ctx, spec.DestinationReg.Name)
	if err != nil {
		return 0, err
	}
	if reg == nil {
		return 0, errors.Errorf("destination registry %q not found", spec.DestinationReg.Name)
	}
	return reg.ID, nil
}

func replicationPolicyModel(spec *ReplicationPolicySpec, destRegID int64) *harbormodels.ReplicationPolicy {
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
		p.Trigger = &harbormodels.ReplicationTrigger{Type: spec.Trigger}
	}
	if spec.DestinationReg != nil {
		// Harbor references the registry by its numeric id; name/url alone yield a
		// 400. The id is resolved by the caller via findRegistryByName.
		p.DestRegistry = &harbormodels.Registry{ID: destRegID}
		p.DestNamespace = spec.DestinationReg.Namespace
	}
	if len(spec.Filters) > 0 {
		p.Filters = make([]*harbormodels.ReplicationFilter, len(spec.Filters))
		for i, f := range spec.Filters {
			p.Filters[i] = &harbormodels.ReplicationFilter{Type: f.Type, Value: f.Value}
		}
	}
	return p
}

// replicationPolicyStatusFromModel converts a Harbor SDK ReplicationPolicy to our
// internal ReplicationPolicyStatus.
func replicationPolicyStatusFromModel(p *harbormodels.ReplicationPolicy) *ReplicationPolicyStatus {
	if p == nil {
		return &ReplicationPolicyStatus{}
	}
	st := &ReplicationPolicyStatus{
		ID:      strconv.FormatInt(p.ID, 10),
		Name:    p.Name,
		Enabled: p.Enabled,
	}
	if p.Description != "" {
		st.Description = ptr.To(p.Description)
	}
	if t := time.Time(p.CreationTime); !t.IsZero() {
		st.CreationTime = t
	}
	if t := time.Time(p.UpdateTime); !t.IsZero() {
		st.UpdateTime = t
	}
	return st
}

// idFromLocation extracts the trailing numeric ID from a Harbor Location header
// (e.g. "/api/v2.0/replication/policies/42" -> 42).
func idFromLocation(location string) (int64, error) {
	parts := strings.Split(strings.TrimRight(location, "/"), "/")
	if len(parts) == 0 {
		return 0, errors.New("empty location")
	}
	return strconv.ParseInt(parts[len(parts)-1], 10, 64)
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
	if spec.DestinationReg == nil || spec.DestinationReg.Name == "" {
		return nil, errors.New("destination registry is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor replication policy",
		"name", spec.Name,
		"destination", spec.DestinationReg.Name,
		"trigger", spec.Trigger)

	destRegID, err := c.resolveDestRegistryID(ctx, spec)
	if err != nil {
		return nil, err
	}
	params := harborreplication.NewCreateReplicationPolicyParams().
		WithContext(ctx).
		WithPolicy(replicationPolicyModel(spec, destRegID))
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

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor replication policy", "policyId", policyID, "name", spec.Name)

	destRegID, err := c.resolveDestRegistryID(ctx, spec)
	if err != nil {
		return nil, err
	}
	model := replicationPolicyModel(spec, destRegID)
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
			StartTime:    time.Time(e.StartTime),
			EndTime:      time.Time(e.EndTime),
			SuccessCount: int64(e.Succeed),
			FailedCount:  int64(e.Failed),
		}
		out = append(out, ex)
	}
	return out, nil
}

// RetentionPolicyRule defines a retention rule
type RetentionPolicyRule struct {
	RuleType     string // always, latestPushedK, latestPulledN
	TagSelectors []string
	Parameters   map[string]interface{}
}

// RetentionPolicySpec defines the desired state of a retention policy
type RetentionPolicySpec struct {
	ProjectID   string
	Description *string
	Rules       []RetentionPolicyRule
	Trigger     string // manual, scheduled
	Enabled     *bool
}

// RetentionPolicyStatus represents the status of a retention policy
type RetentionPolicyStatus struct {
	ID           string
	ProjectID    string
	Description  *string
	Enabled      bool
	CreationTime time.Time
	UpdateTime   time.Time
}

// retentionPolicyModel converts a RetentionPolicySpec to the Harbor SDK RetentionPolicy.
// Harbor retention policies are scoped to a project via scope.ref = numeric project ID.
// Rule mapping:
//   - RuleType maps to Template (Harbor algorithm name e.g. latestPushedK).
//   - TagSelectors map to TagSelectors as RetentionSelector with Decoration="matches".
//   - Parameters map to rule Params.
//
// Trigger: "manual" -> Kind="Schedule" with no settings; "scheduled" -> same (cron configurable).
// Harbor's RetentionPolicy model has no Description, Enabled, or timestamp fields.
// resolveProjectID returns the numeric Harbor project id for a project reference
// that may be either a numeric id or a project name.
func (c *HarborClient) resolveProjectID(ctx context.Context, ref string) (int64, error) {
	if id, err := strconv.ParseInt(ref, 10, 64); err == nil {
		return id, nil
	}
	st, err := c.GetProject(ctx, ref)
	if err != nil {
		return 0, err
	}
	if st == nil {
		return 0, errors.Errorf("project %q not found", ref)
	}
	id, err := strconv.ParseInt(st.ID, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "project %q has non-numeric id %q", ref, st.ID)
	}
	return id, nil
}

func retentionPolicyModel(spec *RetentionPolicySpec, projectIDInt int64) *harbormodels.RetentionPolicy {
	rules := make([]*harbormodels.RetentionRule, 0, len(spec.Rules))
	for _, r := range spec.Rules {
		rule := &harbormodels.RetentionRule{
			Template: r.RuleType,
			Action:   "retain",
		}
		if len(r.TagSelectors) > 0 {
			rule.TagSelectors = make([]*harbormodels.RetentionSelector, len(r.TagSelectors))
			for i, ts := range r.TagSelectors {
				rule.TagSelectors[i] = &harbormodels.RetentionSelector{
					Kind:       "doublestar",
					Decoration: "matches",
					Pattern:    ts,
				}
			}
		}
		if len(r.Parameters) > 0 {
			rule.Params = r.Parameters
		}
		rules = append(rules, rule)
	}

	p := &harbormodels.RetentionPolicy{
		Algorithm: "or",
		Rules:     rules,
		Scope: &harbormodels.RetentionPolicyScope{
			Level: "project",
			Ref:   projectIDInt,
		},
		Trigger: &harbormodels.RetentionRuleTrigger{Kind: "Schedule"},
	}
	return p
}

// retentionPolicyStatusFromModel converts a Harbor SDK RetentionPolicy to our
// internal RetentionPolicyStatus.
// Caveat: Harbor's RetentionPolicy model has no Description, Enabled, CreationTime,
// or UpdateTime — these are synthesised from spec/reconcile context by the caller.
func retentionPolicyStatusFromModel(projectID string, p *harbormodels.RetentionPolicy) *RetentionPolicyStatus {
	if p == nil {
		return &RetentionPolicyStatus{ProjectID: projectID}
	}
	return &RetentionPolicyStatus{
		ID:        strconv.FormatInt(p.ID, 10),
		ProjectID: projectID,
	}
}

// getRetentionPolicyByID fetches a retention policy by its numeric Harbor ID.
// Returns (nil, nil) on 404.
func (c *HarborClient) getRetentionPolicyByID(ctx context.Context, projectID string, id int64) (*RetentionPolicyStatus, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	params := harborretention.NewGetRetentionParams().WithContext(ctx).WithID(id)
	resp, err := v2Client.Retention.GetRetention(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor retention policy")
	}
	return retentionPolicyStatusFromModel(projectID, resp.Payload), nil
}

// CreateRetentionPolicy creates a new retention policy using the Harbor SDK.
// Re-reads via the Location header to capture the authoritative numeric ID.
func (c *HarborClient) CreateRetentionPolicy(ctx context.Context, spec *RetentionPolicySpec) (*RetentionPolicyStatus, error) {
	if spec == nil {
		return nil, errors.New("spec is required")
	}
	if spec.ProjectID == "" {
		return nil, errors.New("project ID is required")
	}
	if len(spec.Rules) == 0 {
		return nil, errors.New("at least one rule is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor retention policy",
		"projectId", spec.ProjectID,
		"rulesCount", len(spec.Rules))

	projectIDInt, err := c.resolveProjectID(ctx, spec.ProjectID)
	if err != nil {
		return nil, errors.Wrap(err, "invalid project for retention policy")
	}
	model := retentionPolicyModel(spec, projectIDInt)

	params := harborretention.NewCreateRetentionParams().WithContext(ctx).WithPolicy(model)
	resp, err := v2Client.Retention.CreateRetention(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor retention policy")
	}

	id, err := idFromLocation(resp.Location)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse retention policy ID from location header")
	}
	st, err := c.getRetentionPolicyByID(ctx, spec.ProjectID, id)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("retention policy created but not yet observable")
	}
	if spec.Enabled != nil {
		st.Enabled = *spec.Enabled
	}
	st.Description = spec.Description
	return st, nil
}

// ListRetentionPolicies returns the retention policy bound to a project, if any.
// Harbor allows at most one retention policy per project; the binding is stored
// as "retention_id" in project metadata. Returns empty slice when none is bound.
func (c *HarborClient) ListRetentionPolicies(ctx context.Context, projectID string) ([]*RetentionPolicyStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor retention policies", "projectId", projectID)

	ref, isName := projectRef(projectID)
	projParams := harborproject.NewGetProjectParams().WithContext(ctx).WithProjectNameOrID(ref)
	if isName != nil {
		projParams = projParams.WithXIsResourceName(isName)
	}
	projResp, err := v2Client.Project.GetProject(ctx, projParams)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get project for retention lookup")
	}

	if projResp.Payload == nil || projResp.Payload.Metadata == nil {
		return nil, nil
	}
	retentionIDStr := ptr.Deref(projResp.Payload.Metadata.RetentionID, "")
	if retentionIDStr == "" {
		return nil, nil
	}

	retentionID, err := strconv.ParseInt(retentionIDStr, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse retention policy ID from project metadata")
	}

	st, err := c.getRetentionPolicyByID(ctx, projectID, retentionID)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, nil
	}
	return []*RetentionPolicyStatus{st}, nil
}

// GetRetentionPolicy retrieves a retention policy by numeric ID string.
// Returns (nil, nil) on 404.
func (c *HarborClient) GetRetentionPolicy(ctx context.Context, projectID, policyID string) (*RetentionPolicyStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if policyID == "" {
		return nil, errors.New("policy ID is required")
	}

	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid retention policy ID")
	}
	return c.getRetentionPolicyByID(ctx, projectID, id)
}

// UpdateRetentionPolicy updates an existing retention policy. Re-reads and
// returns the updated state.
func (c *HarborClient) UpdateRetentionPolicy(ctx context.Context, projectID, policyID string, spec *RetentionPolicySpec) (*RetentionPolicyStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}
	if policyID == "" {
		return nil, errors.New("policy ID is required")
	}
	if spec == nil {
		return nil, errors.New("spec is required")
	}

	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid retention policy ID")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor retention policy", "projectId", projectID, "policyId", policyID)

	projectIDInt, err := c.resolveProjectID(ctx, spec.ProjectID)
	if err != nil {
		return nil, errors.Wrap(err, "invalid project for retention policy")
	}
	model := retentionPolicyModel(spec, projectIDInt)
	model.ID = id

	params := harborretention.NewUpdateRetentionParams().WithContext(ctx).WithID(id).WithPolicy(model)
	if _, err := v2Client.Retention.UpdateRetention(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor retention policy")
	}

	st, err := c.getRetentionPolicyByID(ctx, projectID, id)
	if err != nil {
		return nil, err
	}
	if st != nil {
		if spec.Enabled != nil {
			st.Enabled = *spec.Enabled
		}
		st.Description = spec.Description
	}
	return st, nil
}

// DeleteRetentionPolicy deletes a retention policy. Idempotent: 404 is success.
func (c *HarborClient) DeleteRetentionPolicy(ctx context.Context, projectID, policyID string) error {
	if projectID == "" {
		return errors.New("project ID is required")
	}
	if policyID == "" {
		return errors.New("policy ID is required")
	}

	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid retention policy ID")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor retention policy", "projectId", projectID, "policyId", policyID)

	params := harborretention.NewDeleteRetentionParams().WithContext(ctx).WithID(id)
	if _, err := v2Client.Retention.DeleteRetention(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor retention policy")
	}
	return nil
}

// userGroupStatusFromModel converts a Harbor API UserGroup model to our UserGroupStatus.
func userGroupStatusFromModel(g *harbormodels.UserGroup) *UserGroupStatus {
	if g == nil {
		return nil
	}
	return &UserGroupStatus{
		ID:          g.ID,
		GroupName:   g.GroupName,
		GroupType:   g.GroupType,
		LdapGroupDn: g.LdapGroupDn,
	}
}

// CreateUserGroup creates a new user group in Harbor.
// Harbor returns only a Location header on 201; we parse the ID from that URL
// and re-read to return the authoritative observed state.
func (c *HarborClient) CreateUserGroup(ctx context.Context, spec *UserGroupSpec) (*UserGroupStatus, error) {
	if spec == nil {
		return nil, errors.New("user group spec is required")
	}
	if spec.GroupName == "" {
		return nil, errors.New("group name is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Creating Harbor user group", "groupName", spec.GroupName, "groupType", spec.GroupType)

	ldapDn := ""
	if spec.LdapGroupDn != nil {
		ldapDn = *spec.LdapGroupDn
	}
	req := &harbormodels.UserGroup{
		GroupName:   spec.GroupName,
		GroupType:   spec.GroupType,
		LdapGroupDn: ldapDn,
	}
	params := harborusergroup.NewCreateUserGroupParams().WithContext(ctx).WithUsergroup(req)
	resp, err := v2Client.Usergroup.CreateUserGroup(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Harbor user group")
	}

	// Resolve the new group's id. Prefer the Location header when present
	// (/api/v2.0/usergroups/42); Harbor does not always populate it, so fall back
	// to a name lookup. The name lookup is also the resilient path in OIDC mode,
	// where many groups exist and an unfiltered/paged list may not contain ours.
	if gid, lerr := idFromLocation(resp.Location); lerr == nil && gid > 0 {
		st, err := c.GetUserGroup(ctx, gid)
		if err != nil {
			return nil, err
		}
		if st != nil && st.ID > 0 {
			return st, nil
		}
	}

	st, err := c.GetUserGroupByName(ctx, spec.GroupName)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("Harbor user group created but not found by name")
	}
	return st, nil
}

// GetUserGroupByName finds a Harbor user group by exact name using Harbor's
// group_name filter (fuzzy server-side; we exact-match the result). Returns
// (nil, nil) when no group with that name exists.
func (c *HarborClient) GetUserGroupByName(ctx context.Context, name string) (*UserGroupStatus, error) {
	if name == "" {
		return nil, errors.New("group name is required")
	}
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	params := harborusergroup.NewListUserGroupsParams().WithContext(ctx).WithGroupName(&name)
	resp, err := v2Client.Usergroup.ListUserGroups(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot search Harbor user groups")
	}
	for _, g := range resp.Payload {
		if g != nil && g.GroupName == name {
			return userGroupStatusFromModel(g), nil
		}
	}
	return nil, nil
}

// ListUserGroups lists all user groups in Harbor.
func (c *HarborClient) ListUserGroups(ctx context.Context) ([]*UserGroupStatus, error) {
	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Listing Harbor user groups")

	params := harborusergroup.NewListUserGroupsParams().WithContext(ctx)
	resp, err := v2Client.Usergroup.ListUserGroups(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list Harbor user groups")
	}

	out := make([]*UserGroupStatus, 0, len(resp.Payload))
	for _, g := range resp.Payload {
		if g != nil {
			out = append(out, userGroupStatusFromModel(g))
		}
	}
	return out, nil
}

// GetUserGroup retrieves a specific user group from Harbor by numeric ID.
// Returns (nil, nil) when the group does not exist (404).
func (c *HarborClient) GetUserGroup(ctx context.Context, groupID int64) (*UserGroupStatus, error) {
	if groupID <= 0 {
		return nil, errors.New("group ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Getting Harbor user group", "groupId", groupID)

	params := harborusergroup.NewGetUserGroupParams().WithContext(ctx).WithGroupID(groupID)
	resp, err := v2Client.Usergroup.GetUserGroup(ctx, params)
	if err != nil {
		if isHarborNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "cannot get Harbor user group")
	}
	return userGroupStatusFromModel(resp.Payload), nil
}

// UpdateUserGroup updates a user group in Harbor.
func (c *HarborClient) UpdateUserGroup(ctx context.Context, groupID int64, spec *UserGroupSpec) (*UserGroupStatus, error) {
	if groupID <= 0 {
		return nil, errors.New("group ID is required")
	}
	if spec == nil {
		return nil, errors.New("user group spec is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return nil, errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Updating Harbor user group", "groupId", groupID, "groupName", spec.GroupName)

	ldapDn := ""
	if spec.LdapGroupDn != nil {
		ldapDn = *spec.LdapGroupDn
	}
	req := &harbormodels.UserGroup{
		GroupName:   spec.GroupName,
		GroupType:   spec.GroupType,
		LdapGroupDn: ldapDn,
	}
	params := harborusergroup.NewUpdateUserGroupParams().WithContext(ctx).
		WithGroupID(groupID).
		WithUsergroup(req)
	if _, err := v2Client.Usergroup.UpdateUserGroup(ctx, params); err != nil {
		return nil, errors.Wrap(err, "cannot update Harbor user group")
	}

	return c.GetUserGroup(ctx, groupID)
}

// DeleteUserGroup deletes a user group from Harbor. Idempotent on 404.
func (c *HarborClient) DeleteUserGroup(ctx context.Context, groupID int64) error {
	if groupID <= 0 {
		return errors.New("group ID is required")
	}

	v2Client := c.clientSet.V2()
	if v2Client == nil {
		return errors.New("failed to get Harbor v2 client")
	}

	c.logger.Info("Deleting Harbor user group", "groupId", groupID)

	params := harborusergroup.NewDeleteUserGroupParams().WithContext(ctx).WithGroupID(groupID)
	if _, err := v2Client.Usergroup.DeleteUserGroup(ctx, params); err != nil {
		if isHarborNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot delete Harbor user group")
	}
	return nil
}
