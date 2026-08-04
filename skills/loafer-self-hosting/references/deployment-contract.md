# Deployment contract

## Logical services

```text
edge / TLS
  → web and Better Auth boundary
  → one or more stateless `loaferd` replicas exposing HTTPS `/api/v1`
       → PostgreSQL metadata
       → NATS JetStream durable transport
       → object storage
scheduler → metadata and queue
workers   → metadata, JetStream, object storage, approved sources/targets
browser workers → the same contracts with isolated browser runtime and restricted egress
```

Run the CLI remotely against `loaferd` over HTTPS by default. Explicit `--local` compatibility mode
may compose the application client in-process; it must never be selected as a fallback after a
remote failure. The browser uses a same-origin BFF that forwards a short-lived signed token to the
same `loaferd` API. Do not expose a Unix socket or a second client-specific RPC protocol.

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
