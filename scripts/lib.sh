#!/usr/bin/env bash
# Shared helpers for the scripts/ task runners. Source, don't execute.
set -euo pipefail

PROVIDER="${PROVIDER:-provider-harbor}"
MODULE="${MODULE:-github.com/rossigee/provider-harbor}"
REGISTRY="${REGISTRY:-ghcr.io/mosabastion}"
IMAGE="${IMAGE:-${REGISTRY}/${PROVIDER}}"
VERSION="${VERSION:-$(git describe --tags --dirty --always 2>/dev/null || echo v0.0.0-dev)}"
PLATFORMS="${PLATFORMS:-amd64 arm64}"
CROSSPLANE_CLI_VERSION="${CROSSPLANE_CLI_VERSION:-v2.3.2}"
KIND_CLUSTER="${KIND_CLUSTER:-provider-harbor-dev}"
CRD_GROUP="${CRD_GROUP:-harbor.crossplane.io}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '\033[33m! \033[0m%s\n' "$*" >&2; }
die()  { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

require_cmd() {
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || die "required command not found: $c"
  done
}

ensure_crossplane_cli() {
  if command -v crossplane >/dev/null 2>&1; then echo "crossplane"; return; fi
  local dst="${ROOT}/bin/crossplane"
  mkdir -p "${ROOT}/bin"
  log "fetching crossplane CLI ${CROSSPLANE_CLI_VERSION}" >&2
  curl -fsSL "https://releases.crossplane.io/stable/${CROSSPLANE_CLI_VERSION}/bin/linux_amd64/crank" -o "$dst"
  chmod +x "$dst"
  echo "$dst"
}
