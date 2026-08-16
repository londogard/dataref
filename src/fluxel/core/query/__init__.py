"""Analytical query interop: DuckDB catalogs and footer-stats pruning.

Absorbs the former ``core/index.py``: a commit's tree is flattened into a
DuckDB table (path, hash, size, mtime_ns, footer, commit_id, branch) with an
optional parquet export.  The catalog is derivable from tree + footer objects
without re-reading object bytes, and ``plan_pruned_scan`` prunes parquet row
groups from the footer stats alone (docs/architecture.md §4).
"""

from __future__ import annotations

from .catalog import (
    AnalyticalIndexPaths,
    build_analytical_index,
    drop_analytical_index,
    query_analytical_index,
)
from .pruning import (
    Predicate,
    PrunedFileScan,
    PruneResult,
    parse_where_clause,
    plan_pruned_scan,
    prune_row_groups,
)

__all__ = [
    "AnalyticalIndexPaths",
    "Predicate",
    "PrunedFileScan",
    "PruneResult",
    "build_analytical_index",
    "drop_analytical_index",
    "parse_where_clause",
    "plan_pruned_scan",
    "prune_row_groups",
    "query_analytical_index",
]
