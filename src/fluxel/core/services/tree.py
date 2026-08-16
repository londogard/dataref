"""Tree writing: bottom-up tree construction, overlays, commits, and exports.

``TreeWriter`` replaces v1's ``SnapshotWriter`` (``services/snapshot.py``):
commits now produce a Merkle tree DAG instead of a full JSONL manifest.

- ``build_worktree_tree`` — full commit: walks the worktree, reuses unchanged
  parent leaves (no re-hash for same-size/same-content files) and unchanged
  subtrees by content-addressing, shards directories past
  ``MAX_TREE_ENTRIES`` into name-range subtrees, and prunes staged removals.
- ``build_from_entries`` — rebuild a tree from a sorted leaf-entry stream
  (used by ``verify``/``rm``/``mv``/``import`` and by the staged overlay).
- ``overlay_staged`` — ``commit --staged``: merge materialized additions and
  removals into the parent tree.
- ``write_commit_object`` — commit objects with ``{tree, parents, ...}`` and
  CAS ref advancement.
- ``export_derived_manifest`` — flatten a tree to a JSONL manifest with block
  offsets, cached per-client keyed by root-tree hash (never in the store).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator

from blake3 import blake3

from ..client_state import LocalClientState
from ..domain import CommitObject
from ..hashing import blake3_digest_file
from ..manifest import FileEntry, ManifestEntry, ManifestWriter, walk_files
from ..objects.query import TreeWalker
from ..objects.tree import (
    KIND_BLOB,
    KIND_BP,
    KIND_META,
    KIND_MP,
    KIND_SHARD,
    KIND_TREE,
    MAX_TREE_ENTRIES,
    TreeEntry,
    leaf_to_tree_entry,
)
from ..objects import ObjectStore
from ..repository_support import metadata_identity
from .refs import RefManager

#: Derived-manifest block size for the optional client-side point-lookup cache.
DERIVED_BLOCK_ENTRY_COUNT = 4096

_Child = tuple[str, str]  # (name, serialized tree line)
_Frame = tuple[str, list[_Child], dict[str, TreeEntry] | None]


def _lookup_block_index(paths: list[str], logical_path: str) -> int | None:
    """Binary search for the block that may contain *logical_path*."""
    low, high = 0, len(paths)
    while low < high:
        mid = (low + high) // 2
        if paths[mid] <= logical_path:
            low = mid + 1
        else:
            high = mid
    if low == 0:
        return None
    return low - 1


def _parent_dir(path: str) -> str:
    parts = path.split("/")
    return "/".join(parts[:-1])


def _path_is_removed(path: str, removed: set[str]) -> bool:
    if path in removed:
        return True
    for prefix in removed:
        if path.startswith(f"{prefix}/"):
            return True
    return False


def _leaf_line(
    kind: str,
    name: str,
    hash_value: str,
    size: int,
    mtime_ns: int,
    source_uri: str | None,
    footer: str | None,
) -> str:
    if kind == KIND_BLOB:
        payload: list[object] = [kind, name, hash_value, size, mtime_ns]
    elif kind == KIND_META:
        payload = [kind, name, hash_value, size, mtime_ns, source_uri]
    elif kind == KIND_BP:
        payload = [kind, name, hash_value, size, mtime_ns, footer]
    else:  # KIND_MP
        payload = [kind, name, hash_value, size, mtime_ns, source_uri, footer]
    return json.dumps(payload, separators=(",", ":"))


def _subtree_line(kind: str, name: str, hash_value: str) -> str:
    return json.dumps([kind, name, hash_value], separators=(",", ":"))


def _entry_to_leaf_line(entry: ManifestEntry) -> str:
    name = entry.path.rsplit("/", 1)[-1]
    if entry.identity_mode == "blake3":
        kind = KIND_BP if entry.footer is not None else KIND_BLOB
        source_uri: str | None = None
    else:
        kind = KIND_MP if entry.footer is not None else KIND_META
        source_uri = entry.source_uri
    return _leaf_line(
        kind=kind,
        name=name,
        hash_value=entry.hash,
        size=entry.size,
        mtime_ns=entry.mtime_ns,
        source_uri=source_uri,
        footer=entry.footer,
    )


class TreeWriter:
    def __init__(
        self,
        *,
        store: ObjectStore,
        refs: RefManager,
        client_state: LocalClientState | None = None,
    ) -> None:
        self.store = store
        self.refs = refs
        self.client_state = client_state
        self._walker = TreeWalker(read_tree=store.read_tree_bytes)

    # ── Low-level tree object writing ────────────────────────────────────

    def _write_tree_bytes(self, payload: bytes) -> str:
        tree_hash = blake3(payload).hexdigest()
        if self.store.object_exists("tree", tree_hash):
            return tree_hash
        with NamedTemporaryFile(mode="wb", suffix=".tree", delete=False) as temp:
            temp_path = Path(temp.name)
            temp.write(payload)
        try:
            self.store.write_tree_file(tree_hash, temp_path, if_missing=True)
        finally:
            temp_path.unlink(missing_ok=True)
        return tree_hash

    def _build_children(self, children: list[_Child]) -> str:
        """Build (and shard) a tree object from children; returns its hash.

        Children are sorted by name here — directory frames close lazily
        during a walk, so callers may hand us out-of-order lists (e.g. root
        files appended while an earlier sibling directory is still open).
        """
        if len(children) > 1:
            children = sorted(children, key=lambda item: item[0])
        if len(children) <= MAX_TREE_ENTRIES:
            payload = ("\n".join(line for _, line in children) + "\n").encode("utf-8")
            return self._write_tree_bytes(payload)

        shard_children: list[_Child] = []
        for start in range(0, len(children), MAX_TREE_ENTRIES):
            chunk = children[start : start + MAX_TREE_ENTRIES]
            shard_hash = self._build_children(chunk)
            shard_name = chunk[0][0]
            shard_children.append(
                (shard_name, _subtree_line(KIND_SHARD, shard_name, shard_hash))
            )
        return self._build_children(shard_children)

    # ── Full-commit builder (worktree walk + parent reuse) ───────────────

    def build_worktree_tree(
        self,
        *,
        worktree_root: Path,
        parent_tree: str | None,
        identity_mode: str,
        removed_paths: set[str],
        staged_additions: list[ManifestEntry] | None = None,
        capture_footers: bool = False,
    ) -> str:
        """Build the root tree from a worktree walk.

        Files unchanged since the parent commit are reused without re-hashing
        (blake3 mode re-hashes on same size, exactly like v1).  Parent-only
        leaves and subtrees survive the commit — matching v1's merge
        semantics, where deletions require explicit staging.  Staged additions
        join the effective parent (they survive full commits even when the
        source file is gone).  Staged removals are pruned in a final pass.
        """
        removed = set(removed_paths)
        root_hash = self._build_worktree_inner(
            worktree_root=worktree_root,
            parent_tree=parent_tree,
            identity_mode=identity_mode,
            removed_paths=removed,
            staged_additions=staged_additions,
            capture_footers=capture_footers,
        )
        if removed:
            pruned = self._prune_tree(root_hash, removed)
            if pruned is None:
                root_hash = self._write_tree_bytes(b"")
            else:
                root_hash = pruned
        if staged_additions:
            leftovers = self._unconsumed_additions(staged_additions)
            if leftovers:
                root_hash = self.overlay_staged(
                    parent_tree=root_hash,
                    additions=leftovers,
                    removed_prefixes=set(),
                )
        return root_hash

    def _unconsumed_additions(
        self, additions: list[ManifestEntry]
    ) -> list[ManifestEntry]:
        """Staged additions whose directory never opened during the walk.

        Only directories with worktree files get frames, so additions under
        otherwise-empty directories are never merged into a parent frame and
        must be overlaid onto the built tree afterwards.
        """
        leftover_dirs = getattr(self, "_leftover_additions_dirs", None)
        if not leftover_dirs:
            return []
        return sorted(
            (entry for entry in additions if _parent_dir(entry.path) in leftover_dirs),
            key=lambda entry: entry.path,
        )

    def _build_worktree_inner(
        self,
        *,
        worktree_root: Path,
        parent_tree: str | None,
        identity_mode: str,
        removed_paths: set[str],
        staged_additions: list[ManifestEntry] | None,
        capture_footers: bool,
    ) -> str:
        root_children: list[_Child] = []
        stack: list[_Frame] = []

        additions_by_dir: dict[str, dict[str, TreeEntry]] = {}
        if staged_additions:
            for entry in staged_additions:
                parts = entry.path.split("/")
                dir_path = "/".join(parts[:-1])
                additions_by_dir.setdefault(dir_path, {})[parts[-1]] = (
                    leaf_to_tree_entry(entry)
                )
        self._leftover_additions_dirs = set()

        def parent_children(dir_path: str) -> dict[str, TreeEntry] | None:
            merged: dict[str, TreeEntry] = {}
            if parent_tree:
                entries = self._walker.resolve_subtree(parent_tree, dir_path)
                if entries:
                    for entry in entries:
                        merged[entry.name] = entry
            additions = additions_by_dir.pop(dir_path, None)
            if additions:
                merged.update(additions)
            return merged or None

        root_parent = parent_children("")

        def open_frames(dir_path: str) -> None:
            while stack and not (
                dir_path == stack[-1][0] or dir_path.startswith(f"{stack[-1][0]}/")
            ):
                _close_frame(stack, root_children, self)
            parts = [p for p in dir_path.split("/") if p]
            for index in range(len(stack), len(parts)):
                frame_dir = "/".join(parts[: index + 1])
                stack.append((frame_dir, [], parent_children(frame_dir)))

        for file_entry in walk_files(worktree_root):
            relative_path = file_entry.relative_path
            if _path_is_removed(relative_path, removed_paths):
                continue
            parts = relative_path.split("/")
            dir_path = "/".join(parts[:-1])
            name = parts[-1]
            if dir_path:
                open_frames(dir_path)
                frame = stack[-1]
                frame[1].append(
                    self._materialize_worktree_file(
                        file_entry=file_entry,
                        name=name,
                        full_path=relative_path,
                        parent_by_name=frame[2],
                        identity_mode=identity_mode,
                        capture_footers=capture_footers,
                    )
                )
            else:
                root_children.append(
                    self._materialize_worktree_file(
                        file_entry=file_entry,
                        name=name,
                        full_path=relative_path,
                        parent_by_name=root_parent,
                        identity_mode=identity_mode,
                        capture_footers=capture_footers,
                    )
                )

        while stack:
            _close_frame(stack, root_children, self)

        self._leftover_additions_dirs = set(additions_by_dir)

        merged_root = _merge_parent_children(root_children, root_parent)
        if merged_root:
            return self._build_children(merged_root)
        return self._write_tree_bytes(b"")

    def _materialize_worktree_file(
        self,
        *,
        file_entry: FileEntry,
        name: str,
        full_path: str,
        parent_by_name: dict[str, TreeEntry] | None,
        identity_mode: str,
        capture_footers: bool,
    ) -> _Child:
        parent_entry = parent_by_name.get(name) if parent_by_name else None
        if parent_entry is not None and not parent_entry.is_subtree:
            if identity_mode == "blake3":
                if (
                    parent_entry.kind in {KIND_BLOB, KIND_BP}
                    and parent_entry.size == file_entry.size
                    and blake3_digest_file(file_entry.path) == parent_entry.hash
                ):
                    return self._reuse_or_backfill_footer(
                        parent_entry=parent_entry,
                        name=name,
                        source_path=file_entry.path,
                        source_uri=None,
                        capture_footers=capture_footers,
                    )
            elif (
                parent_entry.kind in {KIND_META, KIND_MP}
                and metadata_identity(full_path, file_entry.size) == parent_entry.hash
            ):
                return self._reuse_or_backfill_footer(
                    parent_entry=parent_entry,
                    name=name,
                    source_path=file_entry.path,
                    source_uri=file_entry.path.as_uri(),
                    capture_footers=capture_footers,
                )

        if identity_mode == "blake3":
            identity_value = blake3_digest_file(file_entry.path)
            self.store.write_blob_file(identity_value, file_entry.path, if_missing=True)
            footer = self._capture_footer(file_entry.path, capture_footers)
            line = _leaf_line(
                KIND_BP if footer else KIND_BLOB,
                name, identity_value, file_entry.size, file_entry.mtime_ns, None, footer,
            )
        else:
            identity_value = metadata_identity(full_path, file_entry.size)
            footer = self._capture_footer(file_entry.path, capture_footers)
            line = _leaf_line(
                KIND_MP if footer else KIND_META,
                name, identity_value, file_entry.size, file_entry.mtime_ns,
                file_entry.path.as_uri(), footer,
            )
        return name, line

    def _reuse_or_backfill_footer(
        self,
        *,
        parent_entry: TreeEntry,
        name: str,
        source_path: Path,
        source_uri: str | None,
        capture_footers: bool,
    ) -> _Child:
        """Reuse a parent leaf, capturing a footer when newly enabled."""
        if parent_entry.footer is not None or not capture_footers:
            return name, parent_entry.serialize()
        footer = self._capture_footer(source_path, True)
        if footer is None:
            return name, parent_entry.serialize()
        kind = KIND_BP if parent_entry.kind == KIND_BLOB else KIND_MP
        return name, _leaf_line(
            kind, name, parent_entry.hash, parent_entry.size,
            parent_entry.mtime_ns, source_uri, footer,
        )

    def _capture_footer(self, source_path: Path, capture_footers: bool) -> str | None:
        if not capture_footers or source_path.suffix.lower() != ".parquet":
            return None
        from ..objects.footer import capture_footer_stats

        with source_path.open("rb") as handle:
            return capture_footer_stats(self.store, handle)

    def _prune_tree(
        self, root_tree: str, removed: set[str]
    ) -> str | None:
        """Rebuild the tree DAG, dropping entries under removal prefixes.

        Returns ``None`` when the whole tree is removed.  Subtrees untouched
        by any removal are reused by hash.
        """

        def prune(dir_path: str, tree_hash: str) -> str | None:
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                raise ValueError(f"Unknown tree object: {tree_hash}")
            kept: list[_Child] = []
            changed = False
            for entry in entries:
                full = f"{dir_path}/{entry.name}" if dir_path else entry.name
                if _path_is_removed(full, removed):
                    changed = True
                    continue
                if entry.is_subtree:
                    child_dir = dir_path if entry.kind == KIND_SHARD else full
                    needs_rebuild = any(
                        _path_is_removed(full, {prefix}) is False
                        and (prefix == full or prefix.startswith(f"{full}/"))
                        for prefix in removed
                    )
                    if not needs_rebuild:
                        kept.append((entry.name, _subtree_line(entry.kind, entry.name, entry.hash)))
                        continue
                    new_hash = prune(child_dir, entry.hash)
                    if new_hash is None:
                        changed = True
                        continue
                    if new_hash != entry.hash:
                        changed = True
                    kept.append((entry.name, _subtree_line(entry.kind, entry.name, new_hash)))
                else:
                    kept.append((entry.name, entry.serialize()))
            if not kept:
                return None
            if not changed:
                return tree_hash
            return self._build_children(kept)

        return prune("", root_tree)

    # ── Rebuild from a sorted leaf stream (verify/rm/mv/import/overlay) ──

    def build_from_entries(self, entries: Iterator[ManifestEntry]) -> str:
        """Build a root tree from a *sorted* stream of ``ManifestEntry``."""
        root_children: list[_Child] = []
        stack: list[_Frame] = []
        previous_path: str | None = None

        for entry in entries:
            path = entry.path
            if previous_path is not None and path <= previous_path:
                raise ValueError(
                    f"Tree entries must be sorted by path; {path!r} after "
                    f"{previous_path!r}"
                )
            previous_path = path
            parts = path.split("/")
            dir_path = "/".join(parts[:-1])
            name = parts[-1]
            if not dir_path:
                root_children.append((name, _entry_to_leaf_line(entry)))
                continue
            while stack and not (
                dir_path == stack[-1][0] or dir_path.startswith(f"{stack[-1][0]}/")
            ):
                _close_frame(stack, root_children, self)
            parts_list = [p for p in dir_path.split("/") if p]
            for index in range(len(stack), len(parts_list)):
                frame_dir = "/".join(parts_list[: index + 1])
                stack.append((frame_dir, [], None))
            stack[-1][1].append((name, _entry_to_leaf_line(entry)))

        while stack:
            _close_frame(stack, root_children, self)

        if root_children:
            return self._build_children(root_children)
        return self._write_tree_bytes(b"")

    # ── Staged overlay (commit --staged) ─────────────────────────────────

    def three_way_merge(
        self,
        base_tree: str | None,
        ours_tree: str,
        theirs_tree: str,
    ) -> tuple[str, list[str]]:
        """Metadata-only 3-way merge (docs/architecture.md §9).

        Streams the base/ours/theirs trees as sorted leaf entries and applies
        standard merge rules: take ours/theirs where only one side changed,
        auto-keep identical additions, and report a conflict for every path
        modified on both sides.  Returns ``(merged_tree_hash, conflict_paths)``.
        """
        from ..domain import MergeConflictError  # noqa: F401  (re-exported below)

        base_iter = (
            iter(())
            if base_tree is None
            else self._walker.iter_all_entries(base_tree)
        )
        ours_iter = self._walker.iter_all_entries(ours_tree)
        theirs_iter = self._walker.iter_all_entries(theirs_tree)

        conflicts: list[str] = []
        merged: list[ManifestEntry] = []
        seen_paths: set[str] = set()

        def same(left: ManifestEntry | None, right: ManifestEntry | None) -> bool:
            if left is None or right is None:
                return left is right
            return left.hash == right.hash and left.size == right.size

        base = next(base_iter, None)
        ours = next(ours_iter, None)
        theirs = next(theirs_iter, None)

        def advance() -> None:
            nonlocal base, ours, theirs
            path = min(
                p.path for p in (base, ours, theirs) if p is not None
            )
            if base is not None and base.path == path:
                base = next(base_iter, None)
            if ours is not None and ours.path == path:
                ours = next(ours_iter, None)
            if theirs is not None and theirs.path == path:
                theirs = next(theirs_iter, None)

        while base is not None or ours is not None or theirs is not None:
            path = min(p.path for p in (base, ours, theirs) if p is not None)
            b = base if base is not None and base.path == path else None
            o = ours if ours is not None and ours.path == path else None
            t = theirs if theirs is not None and theirs.path == path else None

            if o is not None and t is not None:
                if same(o, t):
                    merged.append(o)
                elif b is not None and same(o, b):
                    merged.append(t)
                elif b is not None and same(t, b):
                    merged.append(o)
                else:
                    conflicts.append(path)
                    merged.append(o)
            elif o is not None:
                if b is None or not same(o, b):
                    if b is not None:
                        conflicts.append(path)  # we changed; they removed
                    merged.append(o)
            elif t is not None:
                if b is None or not same(t, b):
                    if b is not None:
                        conflicts.append(path)  # they changed; we removed
                    merged.append(t)
            # base-only: both sides removed — drop

            if path in seen_paths:
                raise ValueError(f"Merge produced duplicate path: {path}")
            seen_paths.add(path)
            advance()

        merged_tree = self.build_from_entries(iter(merged))
        return merged_tree, conflicts

    def merge_trees(
        self,
        base_tree: str | None,
        ours_tree: str,
        theirs_tree: str,
    ) -> str:
        """Three-way merge that raises ``MergeConflictError`` on conflicts."""
        from ..domain import MergeConflictError

        merged_tree, conflicts = self.three_way_merge(
            base_tree, ours_tree, theirs_tree
        )
        if conflicts:
            raise MergeConflictError(paths=sorted(conflicts))
        return merged_tree

    # ── Commits ──────────────────────────────────────────────────────────

    def overlay_staged(
        self,
        *,
        parent_tree: str,
        additions: list[ManifestEntry],
        removed_prefixes: set[str],
    ) -> str:
        """Merge materialized staged additions/removals into the parent tree.

        Implemented as a leaf-stream merge: the parent tree is flattened, the
        overlay is applied, and the tree is rebuilt.  O(N) compute, but
        unchanged subtrees reuse their hashes (content-addressing), so the
        metadata write stays O(changes).
        """
        removed = set(removed_prefixes)
        additions_by_path = {entry.path: entry for entry in additions}

        def merged() -> Iterator[ManifestEntry]:
            for entry in self._walker.iter_all_entries(parent_tree):
                if _path_is_removed(entry.path, removed):
                    continue
                overlay = additions_by_path.pop(entry.path, None)
                if overlay is not None:
                    yield overlay
                    continue
                yield entry
            for path in sorted(additions_by_path):
                yield additions_by_path[path]

        return self.build_from_entries(merged())

    # ── Commits ──────────────────────────────────────────────────────────

    def write_commit_object(
        self,
        *,
        branch: str,
        message: str,
        parent_commit: str | None = None,
        parents: list[str] | None = None,
        tree_hash: str,
        expected_version_token: str | None,
        operation: str,
    ) -> str:
        """Create a commit object, persist it, and advance the branch ref via CAS.

        ``parents`` overrides ``parent_commit`` (used for merge commits with
        multiple parents); the CAS expectation stays the branch head, i.e.
        the first parent.
        """
        if parents is None:
            parents = [parent_commit] if parent_commit else []
        generation = 0
        if parents:
            generation = (
                max(self.refs.read_commit(parent_id).generation for parent_id in parents)
                + 1
            )
        created_at = datetime.now(timezone.utc).isoformat()
        commit_body: dict[str, object] = {
            "message": message,
            "tree": tree_hash,
            "parents": parents,
            "created_at": created_at,
            "branch": branch,
            "generation": generation,
        }
        canonical = json.dumps(
            commit_body, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        commit_id = blake3(canonical).hexdigest()
        commit_object = CommitObject(
            id=commit_id,
            message=message,
            tree=tree_hash,
            parents=tuple(parents),
            created_at=created_at,
            branch=branch,
            generation=generation,
        )
        self.refs.cache_commit(commit_object)
        self.store.write_commit_bytes(
            commit_id,
            (
                json.dumps(
                    {
                        "id": commit_id,
                        "message": message,
                        "tree": tree_hash,
                        "parents": parents,
                        "created_at": created_at,
                        "branch": branch,
                        "generation": generation,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8"),
        )
        self.refs.update_branch_ref(
            branch=branch,
            commit_id=commit_id,
            expected_version_token=expected_version_token,
            expected_commit_id=parents[0] if parents else None,
            operation=operation,
        )
        return commit_id

    # ── Derived manifest (optional per-client materialization) ───────────

    def iter_tree_hashes(self, root_tree: str) -> Iterator[str]:
        """Yield every tree object hash reachable from *root_tree*."""
        seen: set[str] = set()
        stack = [root_tree]
        while stack:
            tree_hash = stack.pop()
            if tree_hash in seen:
                continue
            seen.add(tree_hash)
            yield tree_hash
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                continue
            for entry in reversed(entries):
                if entry.is_subtree:
                    stack.append(entry.hash)

    def iter_leaf_refs(self, root_tree: str) -> Iterator[tuple[str | None, str | None]]:
        """Yield ``(blob_hash, footer_hash)`` for every leaf in the tree DAG.

        Metadata-only leaves yield ``(None, None)`` — they reference no
        canonical object.  Used by GC to compute the reachable set.
        """
        from ..objects.tree import KIND_BLOB, KIND_BP

        seen_trees: set[str] = set()
        stack = [root_tree]
        while stack:
            tree_hash = stack.pop()
            if tree_hash in seen_trees:
                continue
            seen_trees.add(tree_hash)
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                continue
            for entry in entries:
                if entry.is_subtree:
                    stack.append(entry.hash)
                elif entry.kind in (KIND_BLOB, KIND_BP):
                    yield entry.hash, entry.footer
                elif entry.footer is not None:
                    yield None, entry.footer

    def export_derived_manifest(self, tree_hash: str) -> Path:
        """Flatten *tree_hash* into a JSONL manifest with block offsets.

        The artifact (and its block index sidecar) is cached in local client
        state keyed by the root-tree hash (content-addressed ⇒ cache-safe) and
        is never written to the shared store.
        """
        if self.client_state is None:
            raise RuntimeError("TreeWriter has no client_state for derived exports")
        cache_path = self.client_state.derived_manifest_path(tree_hash)
        if cache_path.exists():
            return cache_path
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(mode="wb", suffix=".jsonl", delete=False) as temp:
            temp_path = Path(temp.name)
        writer = ManifestWriter(temp_path, block_entry_count=DERIVED_BLOCK_ENTRY_COUNT)
        writer.write_entries(
            (entry.path, entry.serialize())
            for entry in self._walker.iter_all_entries(tree_hash)
        )
        index = writer.build_index()
        if index is not None:
            self.client_state.write_derived_index(tree_hash, index)
        temp_path.replace(cache_path)
        return cache_path

    def lookup_derived_entry(
        self, tree_hash: str, logical_path: str
    ) -> ManifestEntry | None:
        """Point lookup through the cached derived manifest (optional path).

        Binary-searches the block index and range-reads one slice of the
        JSONL manifest.  Falls back to a tree-walk lookup when the derived
        manifest has not been materialized.
        """
        if self.client_state is None:
            return None
        cache_path = self.client_state.derived_manifest_path(tree_hash)
        if not cache_path.exists():
            return None
        from ..objects.derived import load_derived_index

        index = self.client_state.read_derived_index(tree_hash)
        if index is None or index.is_empty:
            return None
        paths = [block.first_path for block in index.blocks]
        block_index = _lookup_block_index(paths, logical_path)
        if block_index is None:
            return None
        block = index.blocks[block_index]
        end = (
            index.blocks[block_index + 1].offset
            if block_index + 1 < len(index.blocks)
            else index.manifest_size
        )
        with cache_path.open("rb") as handle:
            handle.seek(block.offset)
            slice_bytes = handle.read(end - block.offset)
        for raw_line in slice_bytes.decode("utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            entry = ManifestEntry.deserialize(line)
            if entry.path == logical_path:
                return entry
            if entry.path > logical_path:
                break
        return None


def _close_frame(
    stack: list[_Frame], root_children: list[_Child], writer: TreeWriter
) -> None:
    """Pop one open directory frame, build its subtree, attach to parent."""
    dir_path, children, parent_by_name = stack.pop()
    merged = _merge_parent_children(children, parent_by_name)
    if not merged:
        return
    subtree_hash = writer._build_children(merged)  # noqa: SLF001
    name = dir_path.rsplit("/", 1)[-1]
    line = _subtree_line(KIND_TREE, name, subtree_hash)
    if stack:
        stack[-1][1].append((name, line))
    else:
        root_children.append((name, line))


def _merge_parent_children(
    children: list[_Child], parent_by_name: dict[str, TreeEntry] | None
) -> list[_Child]:
    """Fold parent-only entries into the worktree children (sorted by name).

    Files present in both worktree and parent were already resolved by
    ``_materialize_worktree_file``; parent-only leaves and subtrees survive.
    """
    if not parent_by_name:
        return children
    child_names = {name for name, _ in children}
    extra: list[_Child] = []
    for name, entry in parent_by_name.items():
        if name in child_names:
            continue
        if entry.is_subtree:
            extra.append((name, _subtree_line(entry.kind, name, entry.hash)))
        else:
            extra.append((name, entry.serialize()))
    merged = children + extra
    merged.sort(key=lambda item: item[0])
    return merged
