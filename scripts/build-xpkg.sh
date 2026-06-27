#!/usr/bin/env bash
# Build a Crossplane package (.xpkg) for ONE architecture.
source "$(dirname "$0")/lib.sh"
require_cmd go docker
ARCH="${1:?usage: build-xpkg.sh <amd64|arm64>}"
CRANK="$(ensure_crossplane_cli)"
cd "$ROOT"

log "[$ARCH] build provider binary" >&2
mkdir -p "bin/linux_${ARCH}"
CGO_ENABLED=0 GOOS=linux GOARCH="${ARCH}" go build -trimpath \
  -ldflags "-X ${MODULE}/internal/version.Version=${VERSION}" \
  -o "bin/linux_${ARCH}/provider" ./cmd/provider

RUNTIME="${PROVIDER}-runtime:${VERSION}-${ARCH}"
log "[$ARCH] build runtime image locally (${RUNTIME})" >&2
docker build --platform "linux/${ARCH}" \
  --build-arg TARGETOS=linux --build-arg TARGETARCH="${ARCH}" \
  -t "${RUNTIME}" -f "cluster/images/${PROVIDER}/Dockerfile" . >&2

XPKG="${PROVIDER}-${ARCH}.xpkg"
log "[$ARCH] crossplane xpkg build -> ${XPKG}" >&2
"$CRANK" xpkg build \
  --package-root=package \
  --embed-runtime-image="${RUNTIME}" \
  --package-file="${XPKG}" >&2

echo "${XPKG}"
