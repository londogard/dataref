from __future__ import annotations


class FluxelError(ValueError):
    """Base exception for all Fluxel domain errors."""


class RefConflictError(FluxelError):
    """Raised when a reference update conflicts with another client or concurrent operation."""

    def __init__(
        self,
        *,
        branch: str,
        operation: str,
        expected_commit_id: str | None,
        current_commit_id: str | None,
    ) -> None:
        expected = expected_commit_id or "<empty>"
        current = current_commit_id or "<empty>"
        super().__init__(
            f"Branch update conflict for '{branch}' during {operation}: expected {expected}, found {current}"
        )
        self.branch = branch
        self.operation = operation
        self.expected_commit_id = expected_commit_id
        self.current_commit_id = current_commit_id


class OptimisticLockError(FluxelError):
    """Raised when an optimistic lock check fails during CAS operations."""


class NonFastForwardError(FluxelError):
    """Raised when a merge, push, or pull operation is not a fast-forward update."""

    def __init__(self, *, branch: str, current_commit: str, target_commit: str) -> None:
        super().__init__(
            f"Cannot fast-forward '{branch}' from {current_commit} to {target_commit}: "
            "current head is not an ancestor of target"
        )
        self.branch = branch
        self.current_commit = current_commit
        self.target_commit = target_commit
