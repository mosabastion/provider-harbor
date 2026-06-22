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
	"net/http"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/utils/ptr"
)

// isHarborCode reports whether err carries the given HTTP status code, covering
// both the goharbor typed responses (which expose IsCode) and a fallback substring.
func isHarborCode(err error, code int) bool {
	if err == nil {
		return false
	}
	var coder interface{ IsCode(int) bool }
	if errors.As(err, &coder) {
		return coder.IsCode(code)
	}
	return strings.Contains(err.Error(), strconv.Itoa(code))
}

// isHarborNotFound maps a Harbor 404 to the (nil, nil) not-found contract.
func isHarborNotFound(err error) bool {
	return isHarborCode(err, http.StatusNotFound)
}

// idFromLocation extracts the trailing numeric ID from a Harbor Location header
// (e.g. "/api/v2.0/projects/1/members/42" -> 42).
func idFromLocation(location string) (int64, error) {
	parts := strings.Split(strings.TrimRight(location, "/"), "/")
	if len(parts) == 0 || parts[len(parts)-1] == "" {
		return 0, errors.New("empty location")
	}
	return strconv.ParseInt(parts[len(parts)-1], 10, 64)
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
