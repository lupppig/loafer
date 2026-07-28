# Better Auth contract

## Selected library

Use Better Auth as the built-in authentication framework:

- project and license: <https://github.com/better-auth/better-auth>
- organization plugin: <https://better-auth.com/docs/plugins/organization>
- device authorization: <https://better-auth.com/docs/plugins/device-authorization>
- JWT/JWKS: <https://better-auth.com/docs/plugins/jwt>
- SQLite adapter: <https://better-auth.com/docs/adapters/sqlite>
- PostgreSQL adapter: <https://better-auth.com/docs/adapters/postgresql>
- enterprise OIDC SSO: <https://better-auth.com/docs/plugins/sso>

Review release notes and security advisories before upgrading; do not copy examples without
checking them against the pinned version.

## Logical boundary

```text
browser ── secure session cookie ──┐
CLI ───── device/access token ─────┼─ Better Auth boundary
automation ─ scoped credential ────┘          │
                                      verified AuthContext
                                              │
                                      Loafer authorization
                                              │
                                      tenant-scoped use case
```

The engine receives execution identity and policy only when needed for auditing. It never imports
Better Auth, parses cookies, or evaluates organization membership.

## Required plugins

Adopt the minimum set:

| Capability | Better Auth support |
|---|---|
| Users and sessions | Core |
| Organizations, memberships, invitations | Organization plugin |
| CLI login | Device authorization and bearer/JWT support |
| CI/service accounts | API key or OAuth client credentials |
| MFA | Two-factor plugin |
| Enterprise federation | SSO/OIDC plugin |

Do not enable a plugin until its schema, endpoints, rate limits, revocation behavior, and license
have been reviewed.

## Data ownership

Better Auth owns authentication tables and migrations. Prefix them or place them in a dedicated
PostgreSQL schema. Loafer stores the Better Auth user and organization identifiers as external
identity references; it does not directly update Better Auth tables.

Loafer remains authoritative for:

```text
workspace membership extensions
resource permissions and environment policy
pipeline/run/connection/secret authorization
worker-pool policy
audit retention and application events
```

## Deployment

For embedded mode, run the auth boundary and SQLite file on the same host. For distributed mode,
use PostgreSQL and multiple stateless auth/control-plane replicas behind one canonical HTTPS
origin.

Set explicit canonical URL, trusted origins, secure-cookie behavior, proxy trust, encryption/session
secrets, token issuer and audience, email delivery, and bootstrap policy. Refuse startup with
development secrets in production mode.
