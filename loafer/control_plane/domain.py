"""Framework-independent control-plane identities and authorization rules."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class WorkspaceRole(StrEnum):
    OWNER = "owner"
    ADMIN = "admin"
    OPERATOR = "operator"
    VIEWER = "viewer"


class Permission(StrEnum):
    READ = "read"
    OPERATE = "operate"
    ADMIN = "admin"


_ROLE_PERMISSIONS: dict[WorkspaceRole, frozenset[Permission]] = {
    WorkspaceRole.OWNER: frozenset(Permission),
    WorkspaceRole.ADMIN: frozenset(Permission),
    WorkspaceRole.OPERATOR: frozenset({Permission.READ, Permission.OPERATE}),
    WorkspaceRole.VIEWER: frozenset({Permission.READ}),
}


@dataclass(frozen=True, slots=True)
class AuthContext:
    """Verified identity only; resource scope is always loaded from persistence."""

    subject_id: str
    token_id: str | None = None
    expires_at: int | None = None
    global_roles: frozenset[str] = frozenset()

    @property
    def is_platform_admin(self) -> bool:
        return "admin" in self.global_roles


def role_allows(role: WorkspaceRole, permission: Permission) -> bool:
    return permission in _ROLE_PERMISSIONS[role]
