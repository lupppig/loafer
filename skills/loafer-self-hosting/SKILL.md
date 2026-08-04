---
name: loafer-self-hosting
description: Package, deploy, secure, document, release, or review Loafer as an open-source self-hosted platform. Use for Docker images, Compose, Kubernetes or Helm, NATS JetStream, browser worker pools, configuration, secrets, TLS, migrations, backups, health checks, observability, startup and enterprise deployment profiles, upgrades, supply-chain security, licensing, release automation, and operator documentation.
---

# Loafer Self-Hosting

Ship one architecture from a laptop-sized Compose profile to a horizontally scalable production
installation. Preserve process boundaries even when all services run on one host.

## Start

1. Use `$loafer-auth` for Better Auth deployment, secrets, sessions, OIDC, and bootstrap policy.
2. Read [references/deployment-contract.md](references/deployment-contract.md).
3. Inventory images, processes, ports, state, secrets, volumes, migrations, and external
   dependencies.
4. Separate what ships today from preview or roadmap functionality.
5. Define install, upgrade, rollback, backup, restore, and uninstall behavior.

## Package the platform

- Build pinned, minimal, non-root images for web/API, scheduler, lightweight worker, and isolated
  browser-worker roles.
- Reuse one versioned engine package across CLI, API, and workers.
- Publish multi-architecture images with OCI labels, SBOMs, provenance, and vulnerability scans.
- Run `loafer metadata migrate` as an explicit one-shot job before starting or rolling out any
  API, scheduler, or worker replicas. Application startup only verifies schema compatibility and
  must never run DDL.
- Keep immutable containers; store metadata, artifacts, logs, and temporary outputs externally.
- Provide startup Compose and production Kubernetes/Helm profiles without different semantics.

## Make setup easy

- Provide one documented bootstrap path, generated secure defaults, and a first-admin workflow.
- Validate configuration before starting and explain missing values precisely.
- Include readiness, liveness, startup, and dependency health endpoints.
- Supply resource estimates and sane limits for startup, standard, and high-throughput profiles.
- Keep optional services optional; do not require an LLM for deterministic pipelines.
- Keep NATS optional for local CLI execution and explicit in distributed profiles. Pin its server
  version, storage limits, replicas, credentials, monitoring, backup, and restore procedure.

## Secure by default

- Require TLS at the edge, server-side secret references, encryption at rest, and key rotation.
- Use least-privilege service identities, network policies, non-root containers, read-only
  filesystems, and restricted capabilities.
- Keep worker network access and credentials scoped by pool/environment.
- Disable public sign-up and anonymous mutations by default.
- Document SSO, external secret manager, audit retention, and air-gapped considerations for
  enterprise deployments.

## Operate upgrades

- Support backward-compatible rolling upgrades within a documented window.
- Back up metadata and artifact locations before migrations.
- Test restore, downgrade/rollback constraints, queue drain, and worker version skew.
- Export OpenTelemetry metrics/traces and structured logs with documented dashboards and alerts.
- Publish release notes, upgrade notes, checksums, signatures, known limitations, and supported
  version matrices.

## Keep the project open-source ready

- Maintain concise README, contribution guide, code of conduct, security policy, governance,
  license, issue/PR templates, reproducible development setup, and release automation.
- Keep enterprise integration points modular; do not cripple the open-source core.
- Never claim production, enterprise, HA, or 100M-row readiness without reproducible evidence.
- Document telemetry behavior and default it to off unless explicitly accepted.

## Validate

Test clean install, restart, upgrade, rollback constraints, backup/restore, expired certificates,
secret rotation, database/queue/object-store outage, worker drain, horizontal scale, tenant
isolation, and image scanning. Run smoke tests against built artifacts, not the development
environment.
