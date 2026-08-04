---
name: loafer-auth
description: Design, implement, harden, debug, or review Loafer authentication and tenant access using Better Auth. Use for browser sessions, signup and invitations, organizations, memberships, roles, enterprise OIDC SSO, CLI device login, personal access tokens, service accounts, JWT/JWKS verification, auth persistence, security controls, and authentication tests.
---

# Loafer Authentication

Use Better Auth as Loafer's identity and session boundary. Keep pipeline authorization and data
isolation in the Loafer control plane.

## Start

1. Read [references/better-auth-contract.md](references/better-auth-contract.md).
2. Inspect the current Better Auth version, enabled plugins, schema, route mount, cookie settings,
   proxy/TLS configuration, and control-plane `AuthContext`.
3. Identify the actor: browser user, CLI user, service account, worker, or bootstrap administrator.
4. Trace authentication → organization membership → workspace permission → audit event.
5. Preserve local CLI mode, which does not require platform authentication.

## Protect ownership boundaries

- Let Better Auth own users, credentials, accounts, sessions, verification records, organizations,
  organization memberships, invitations, OAuth clients, and authentication tokens.
- Let Loafer own workspaces, environments, pipeline permissions, connections, secret-use policy,
  schedules, runs, worker pools, artifacts, and audit policy.
- Link Loafer workspaces to a Better Auth organization ID. Do not model workspaces as Better Auth
  teams.
- Derive organization identity from the verified session/token. Never trust a browser-supplied
  organization or workspace ID without membership and permission checks.
- Map authentication into one immutable request-scoped `AuthContext`; keep Better Auth objects out
  of engine and worker state.

## Use the right flow

- Browser: use same-origin, secure, HTTP-only, SameSite cookies and server-side session validation.
- CLI: use the Better Auth device-authorization flow and store refresh credentials in the OS
  keyring, never a project file.
- CI and automation: use hashed, scoped, expiring personal access tokens or OAuth client
  credentials.
- Workers: use workload credentials and short-lived job authorization, not user sessions.
- Enterprise: federate through OIDC SSO; do not make Keycloak, Authentik, or a commercial provider
  mandatory for the startup profile.

If the Python API runs separately from the TypeScript auth gateway, forward only a short-lived,
audience-bound signed token and validate it against pinned issuer/JWKS configuration. Never trust
unsigned identity headers from a public proxy.

All browser, CLI, and automation identities ultimately authorize the same HTTPS `/api/v1`
`loaferd` commands. Never let an auth adapter become an alternate control plane or forward an
unsigned workspace/role header.

## Configure Better Auth

- Pin Better Auth and plugin versions in the lockfile.
- Mount auth under one same-origin path such as `/api/auth`.
- Use the organization plugin for membership, invitations, and authentication-level roles.
- Use device authorization plus bearer/JWT support for CLI login.
- Use API-key or OAuth client-credential support for automation.
- Enable email verification, password reset, session revocation, rate limits, and secure trusted
  origins before enabling local accounts.
- Disable public registration by default; bootstrap one administrator with a single-use,
  short-lived setup token.
- Support SQLite for embedded same-host mode and PostgreSQL for distributed mode.
- Keep Better Auth migrations isolated from Loafer application migrations and test both together.

## Enforce authorization in Loafer

- Translate roles into explicit permissions such as `pipeline.write`, `run.execute`,
  `connection.manage`, `secret.use`, and `audit.read`.
- Authorize in application use cases and repositories, not only React components or route guards.
- Add PostgreSQL row-level security as defense in depth for tenant-owned application tables.
- Include organization, workspace, actor, request, action, resource, and outcome in audit events.
- Revoke or re-evaluate sessions after membership, role, password, MFA, or organization changes.

## Validate

Test signup-disabled bootstrap, invitation acceptance, email verification, login, logout, session
rotation, session revocation, password reset, OIDC linking, device login, token expiry, token
rotation, service accounts, organization switching, permission denial, CSRF, origin validation,
rate limiting, guessed tenant IDs, cross-tenant cache keys, and audit events.

Pin the Better Auth database matrix in CI: SQLite embedded mode and PostgreSQL distributed mode.
Run dependency and security-advisory checks on every auth upgrade.
