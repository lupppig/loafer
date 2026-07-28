# Security policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub's private vulnerability reporting for this repository:

<https://github.com/lupppig/loafer/security/advisories/new>

Include:

- the affected version or commit;
- the deployment context and operating system;
- reproduction steps or a minimal proof of concept;
- the expected and observed impact;
- any suggested mitigation;
- whether the report may involve exposed credentials or customer data.

Do not include active credentials, production data, or unrestricted database samples.

## Response process

Maintainers will acknowledge a valid private report, reproduce and assess it, coordinate a fix and
release, and publish an advisory when users can safely upgrade. Exact response times are not
guaranteed while the project is maintained by a small open-source team.

## Supported versions

Security fixes target the latest released minor version. Older versions may be asked to upgrade
before a fix is provided. Supported versions and exceptions will be identified in published
advisories.

## Security scope

Reports are especially valuable for:

- credential or connection-string disclosure;
- tenant or workspace authorization bypass;
- SQL identifier injection;
- sandbox escape or unrestricted transform execution;
- unsafe file path handling;
- secret leakage in events, logs, CLI output, or the web UI;
- worker lease/fencing failures that permit unauthorized execution;
- vulnerable release artifacts or container images.

Resource exhaustion caused by workloads beyond documented limits is not automatically a security
issue, but bypasses of configured resource limits are.
