from ._version import __version__

# Registers the fsspec protocol "strand://..."
from . import fsspec_strand as _fsspec_strand  # noqa: F401

__all__ = ["__version__"]
