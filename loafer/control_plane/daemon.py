"""`loaferd` process entrypoint; worker execution remains a separate process."""

from __future__ import annotations

import argparse
import os

import uvicorn

from loafer.control_plane.app import ControlPlaneSettings, create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Loafer HTTPS control plane")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=9443, type=int)
    parser.add_argument("--tls-cert", default=os.environ.get("LOAFER_TLS_CERT"))
    parser.add_argument("--tls-key", default=os.environ.get("LOAFER_TLS_KEY"))
    parser.add_argument(
        "--behind-tls-proxy",
        action="store_true",
        help="Trust X-Forwarded-Proto and X-Forwarded-For from the TLS reverse proxy.",
    )
    args = parser.parse_args()
    direct_tls = bool(args.tls_cert and args.tls_key)
    if not direct_tls and not args.behind_tls_proxy:
        parser.error("provide --tls-cert/--tls-key or use --behind-tls-proxy")
    if bool(args.tls_cert) != bool(args.tls_key):
        parser.error("--tls-cert and --tls-key must be supplied together")

    settings = ControlPlaneSettings.from_environment()
    if args.behind_tls_proxy:
        settings = ControlPlaneSettings(
            issuer=settings.issuer,
            audience=settings.audience,
            jwks_url=settings.jwks_url,
            allowed_origins=settings.allowed_origins,
            jwks_timeout_seconds=settings.jwks_timeout_seconds,
            enforce_https=True,
            trust_proxy_headers=True,
            rate_limit_requests=settings.rate_limit_requests,
            rate_limit_window_seconds=settings.rate_limit_window_seconds,
            sse_poll_seconds=settings.sse_poll_seconds,
            sse_heartbeat_seconds=settings.sse_heartbeat_seconds,
        )
    uvicorn.run(
        create_app(settings=settings),
        host=args.host,
        port=args.port,
        proxy_headers=False,
        server_header=False,
        ssl_certfile=args.tls_cert,
        ssl_keyfile=args.tls_key,
    )


if __name__ == "__main__":
    main()
