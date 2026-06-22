FROM gcr.io/distroless/static:nonroot

# OpenContainers Image Spec Labels
LABEL org.opencontainers.image.title="provider-harbor"
LABEL org.opencontainers.image.description="Native Crossplane provider for Harbor container registry management"
LABEL org.opencontainers.image.vendor="rossigee"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.source="https://github.com/rossigee/provider-harbor"
LABEL org.opencontainers.image.url="https://github.com/rossigee/provider-harbor"
LABEL org.opencontainers.image.documentation="https://github.com/rossigee/provider-harbor/blob/master/README.md"

ARG TARGETOS
ARG TARGETARCH

# Copy the native provider binary (already 0755 from `go build`; no --chmod so the
# legacy docker builder works without BuildKit/buildx).
COPY bin/${TARGETOS}_${TARGETARCH}/provider /usr/local/bin/provider

# Copy the Crossplane package
COPY package/ /package/

# Use nonroot user from distroless image
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/provider"]
