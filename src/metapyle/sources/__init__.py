"""Source adapters for metapyle.

This module provides the base interface for data sources and a registry
for managing source adapters.
"""

from metapyle.sources.base import BaseSource, SourceRegistry, register_source

__all__ = ["BaseSource", "SourceRegistry", "register_source"]
