from __future__ import annotations


class DatarefError(ValueError):
    """Base exception for all Dataref domain errors."""


class RefConflictError(DatarefError):
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


class OptimisticLockError(DatarefError):
    """Raised when an optimistic lock check fails during CAS operations."""


class NonFastForwardError(DatarefError):
    """Raised when a merge, push, or pull operation is not a fast-forward update."""

    def __init__(self, *, branch: str, current_commit: str, target_commit: str) -> None:
        super().__init__(
            f"Cannot fast-forward '{branch}' from {current_commit} to {target_commit}: "
            "current head is not an ancestor of target"
        )
        self.branch = branch
        self.current_commit = current_commit
        self.target_commit = target_commit


class ObjectMissingError(DatarefError):
    """Raised by storage adapters when a requested object does not exist."""


class PreconditionFailedError(OptimisticLockError):
    """Raised when an S3 write precondition (IfNoneMatch) fails.

    Subclasses ``OptimisticLockError`` so existing CAS handlers keep working.
    """


class StorageUnavailableError(DatarefError):
    """Raised by storage adapters for unrecoverable S3 transport failures."""


class MergeConflictError(DatarefError):
    """Raised when a 3-way merge hits paths modified on both sides."""

    def __init__(self, *, paths: list[str]) -> None:
        self.paths = paths
        joined = ", ".join(paths[:5])
        suffix = "" if len(paths) <= 5 else f", … ({len(paths)} total)"
        super().__init__(f"Merge conflict at: {joined}{suffix}")


class NotARepositoryError(DatarefError):
    """Raised when an operation is executed outside a Dataref repository."""

    def __init__(self, root: object = ".") -> None:
        self.root = str(root)
        super().__init__(
            f"not a dataref repository (or any of the parent directories): .dataref"
        )
