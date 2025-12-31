"""Source adapters for metapyle.

This module provides the base interface for data sources and a registry
for managing source adapters.
"""

# Import source modules to trigger auto-registration
from metapyle.sources import (
    bloomberg,  # noqa: F401
    gsquant,  # noqa: F401
    localfile,  # noqa: F401
    macrobond,  # noqa: F401
)
from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
    SourceRegistry,
    make_column_name,
    register_source,
)
from metapyle.sources.bloomberg import BloombergSource
from metapyle.sources.gsquant import GSQuantSource
from metapyle.sources.localfile import LocalFileSource
from metapyle.sources.macrobond import MacrobondSource

__all__ = [
    "BaseSource",
    "BloombergSource",
    "FetchRequest",
    "GSQuantSource",
    "LocalFileSource",
    "MacrobondSource",
    "SourceRegistry",
    "make_column_name",
    "register_source",
]
