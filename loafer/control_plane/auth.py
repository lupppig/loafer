"""Better Auth JWT/JWKS verification boundary."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, Protocol

import jwt

from loafer.control_plane.domain import AuthContext


class AuthenticationError(ValueError):
    """Raised when a bearer credential cannot be trusted."""


class TokenVerifier(Protocol):
    def verify(self, token: str) -> AuthContext:
        """Validate a bearer token and return an immutable identity."""


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
        if not jwks_url.startswith("https://"):
            raise ValueError("Better Auth JWKS URL must use HTTPS")
        if not issuer.startswith("https://") or not audience.startswith("https://"):
            raise ValueError("Better Auth issuer and audience must use HTTPS")
        if not math.isfinite(jwks_timeout_seconds) or jwks_timeout_seconds <= 0:
            raise ValueError("JWKS timeout must be positive and finite")
        self._jwks = jwt.PyJWKClient(
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
