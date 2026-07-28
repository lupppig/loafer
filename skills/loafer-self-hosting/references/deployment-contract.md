# Deployment contract

## Logical services

```text
edge / TLS
  → web, Better Auth boundary, and control-plane API
       → PostgreSQL metadata
       → NATS JetStream durable transport
       → object storage
scheduler → metadata and queue
workers   → metadata, JetStream, object storage, approved sources/targets
browser workers → the same contracts with isolated browser runtime and restricted egress
```

Run the CLI either locally against the engine or remotely against the API through an explicit
profile. The browser always uses the API.

Embedded mode may keep Better Auth and Loafer metadata in one SQLite database with separate table
ownership. Distributed mode uses PostgreSQL and stateless auth/control-plane replicas. Enterprise
deployments may federate Better Auth with Keycloak, Authentik, Entra ID, Okta, or another OIDC
provider.

## Profiles

| Profile | Shape | Intended use |
|---|---|---|
| Local developer | local CLI and disposable dependencies | development and tests |
| Startup Compose | one host, separate service containers, managed or bundled state | small teams |
| Production | external HA state services, multiple API/worker replicas | serious workloads |
| Enterprise | SSO, external secrets, audit export, isolated worker pools, policy controls | regulated environments |

## Required state

- PostgreSQL: tenants, configs, versions, schedules, runs, events, checkpoints, audit.
- PostgreSQL outbox and JetStream: authoritative publication intent plus durable job delivery.
  Leases/fencing and checkpoints remain in metadata; transport can be rebuilt where designed.
- Object storage: artifacts, redacted log archives, quarantine data, staged file outputs.
- Secret manager: credentials and encryption keys; never embedded in metadata records.

Document ownership, backup, retention, encryption, and recovery objectives for each.
