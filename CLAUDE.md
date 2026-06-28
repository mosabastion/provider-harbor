# provider-harbor

Crossplane provider for Harbor (`github.com/rossigee/provider-harbor`).

Managed resources live under `harbor.m.crossplane.io/v1beta1`. ProviderConfig lives under `harbor.crossplane.io/v1beta1`.

Resources: Project, Member, Registry, Replication, Retention, Robot, Scanner, User, UserGroup, Webhook.

## Commands

```bash
bash scripts/generate.sh   # regenerate deepcopy + CRDs (run after editing types)
go build ./...
go test -race ./...
bash scripts/validate.sh   # lint + check generated files are committed
```

## Key rules

- Every non-root API type (`*Spec`, `*Status`, `*Parameters`, `*Observation`) needs `// +kubebuilder:object:generate=true` — without it the CI build breaks after regeneration.
- `Get*` returns `(nil, nil)` on 404. `Delete*` returns nil on 404.
- `Observe` calls `cr.SetConditions(xpv1.Available())` when up-to-date.
- `Create` stamps `meta.SetExternalName(cr, id)` from the backend response.
- `Delete` returns `(managed.ExternalDelete{}, err)` — crossplane-runtime v2 signature.

## Harbor API quirks

- Most Harbor resources are keyed by numeric ID internally; client code resolves name → ID via list+match before PUT/DELETE calls.
- Registry, Webhook: no single-GET by name — list all and match.
- Webhook Create response carries no policy ID — re-read via list+match after creation to get the ID for `SetExternalName`.
- UserGroup: lookup by name uses `GetUserGroupByName` (list+match); lookup by ID uses `GetUserGroup`.
- Robot names are prefixed by Harbor (`robot$<name>`) — strip the prefix when comparing to the spec name.
- Project-level robots address permissions by project name (not numeric project ID) in the permission namespace field.
- Promote users to sysadmin via a separate `PUT /users/{id}/sysadmin` call — it is not part of the create/update body.

## E2e

Requires: `kind`, `kubectl`, `helm`, `chainsaw`.

```bash
bash scripts/e2e.sh        # full run: kind cluster + real Harbor (goharbor Helm chart) + uptest
KEEP=1 bash scripts/e2e.sh # keep cluster for inspection
```

E2e drives a real Harbor instance (goharbor Helm chart, in-cluster, no TLS/persistence, default admin password `Harbor12345`). E2e manifests are in `examples/e2e/`.

## Skills / agents

See `.claude/commands/` and `.claude/agents/` for slash commands and agent definitions.
