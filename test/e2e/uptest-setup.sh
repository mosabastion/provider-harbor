#!/usr/bin/env bash
set -aeuo pipefail
: "${KUBECTL:=kubectl}"
NS="${UPTEST_NAMESPACE:-uptest}"
HARBOR_URL="${HARBOR_URL:-http://harbor.harbor.svc}"
HARBOR_PASSWORD="${HARBOR_PASSWORD:-Harbor12345}"

echo "uptest-setup: namespace + creds + ProviderConfig in $NS"
${KUBECTL} create namespace "$NS" --dry-run=client -o yaml | ${KUBECTL} apply -f -
${KUBECTL} -n "$NS" create secret generic harbor-creds \
  --from-literal=url="$HARBOR_URL" --from-literal=username=admin --from-literal=password="$HARBOR_PASSWORD" \
  --dry-run=client -o yaml | ${KUBECTL} apply -f -
cat <<YAML | ${KUBECTL} apply -f -
apiVersion: harbor.m.crossplane.io/v1beta1
kind: ProviderConfig
metadata:
  name: harbor-e2e
  namespace: ${NS}
spec:
  credentials:
    source: Secret
    secretRef:
      namespace: ${NS}
      name: harbor-creds
      key: password
YAML
${KUBECTL} wait provider.pkg --all --for condition=Healthy --timeout 5m
echo "uptest-setup: done"
