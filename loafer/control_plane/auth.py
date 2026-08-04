"""Better Auth JWT/JWKS verification boundary."""

from __future__ import annotations

import json
import math
import urllib.request
from collections.abc import Mapping
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit

import jwt

from loafer.control_plane.domain import AuthContext


class AuthenticationError(ValueError):
    """Raised when a bearer credential cannot be trusted."""


class TokenVerifier(Protocol):
    def verify(self, token: str) -> AuthContext:
        """Validate a bearer token and return an immutable identity."""


def _https_origin(url: str) -> tuple[str, str, int]:
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid URL authority") from exc
    if parsed.scheme.lower() != "https" or parsed.hostname is None:
        raise ValueError("URL must use HTTPS and include an authority")
    return ("https", parsed.hostname.lower(), port or 443)


class _SameOriginRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self, jwks_url: str) -> None:
        self._origin = _https_origin(jwks_url)

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        try:
            redirect_origin = _https_origin(newurl)
        except ValueError as exc:
            raise URLError("JWKS redirect must preserve its HTTPS authority") from exc
        if redirect_origin != self._origin:
            raise URLError("JWKS redirect must preserve its HTTPS authority")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class _SameOriginJWKClient(jwt.PyJWKClient):
    """Fetch JWKS documents without allowing cross-origin redirects."""

    def fetch_data(self) -> Any:
        try:
            request = urllib.request.Request(url=self.uri, headers=self.headers)
            opener = urllib.request.build_opener(
                _SameOriginRedirectHandler(self.uri),
                urllib.request.HTTPSHandler(context=self.ssl_context),
            )
            with opener.open(request, timeout=self.timeout) as response:
                jwk_set = json.load(response)
        except (URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError):
                exc.close()
            raise jwt.PyJWKClientConnectionError(
                f'Fail to fetch data from the url, err: "{exc}"'
            ) from exc

        if self.jwk_set_cache is not None:
            self.jwk_set_cache.put(jwk_set)
        return jwk_set


class BetterAuthJWTVerifier:
    """Validate short-lived audience-bound JWTs from Better Auth's JWKS."""

    def __init__(
        self,
        *,
        jwks_url: str,
        issuer: str,
        audience: str,
        jwks_timeout_seconds: float = 5.0,
    ) -> None:
        try:
            _https_origin(jwks_url)
        except ValueError as exc:
            raise ValueError("Better Auth JWKS URL must use HTTPS") from exc
        if not issuer.startswith("https://") or not audience.startswith("https://"):
            raise ValueError("Better Auth issuer and audience must use HTTPS")
        if not math.isfinite(jwks_timeout_seconds) or jwks_timeout_seconds <= 0:
            raise ValueError("JWKS timeout must be positive and finite")
        self._jwks = _SameOriginJWKClient(
            jwks_url,
            cache_keys=True,
            lifespan=3600,
            timeout=jwks_timeout_seconds,
        )
        self._issuer = issuer
        self._audience = audience

    def verify(self, token: str) -> AuthContext:
        try:
            signing_key = self._jwks.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=["EdDSA", "ES256", "RS256", "PS256"],
                issuer=self._issuer,
                audience=self._audience,
                options={"require": ["sub", "iss", "aud", "exp", "iat"]},
            )
        except jwt.PyJWTError as exc:
            raise AuthenticationError("invalid or expired bearer token") from exc
        return _context_from_claims(claims)


class StaticTokenVerifier:
    """Small deterministic verifier for in-process contract tests."""

    def __init__(self, tokens: Mapping[str, AuthContext]) -> None:
        self._tokens = dict(tokens)

    def verify(self, token: str) -> AuthContext:
        try:
            return self._tokens[token]
        except KeyError as exc:
            raise AuthenticationError("invalid or expired bearer token") from exc


def _context_from_claims(claims: Mapping[str, Any]) -> AuthContext:
    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject:
        raise AuthenticationError("bearer token is missing a subject")
    token_id = claims.get("jti")
    expires_at = claims.get("exp")
    role_claim = claims.get("role")
    if isinstance(role_claim, str):
        global_roles = frozenset(
            role.strip().lower() for role in role_claim.split(",") if role.strip()
        )
    elif isinstance(role_claim, list):
        global_roles = frozenset(
            role.lower() for role in role_claim if isinstance(role, str) and role
        )
    else:
        global_roles = frozenset()
    return AuthContext(
        subject_id=subject,
        token_id=token_id if isinstance(token_id, str) else None,
        expires_at=int(expires_at) if isinstance(expires_at, (int, float)) else None,
        global_roles=global_roles,
    )
