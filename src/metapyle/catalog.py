"""Catalog system for mapping human-readable names to data sources."""

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from metapyle.sources.base import SourceRegistry

import yaml

from metapyle.exceptions import (
    CatalogValidationError,
    DuplicateNameError,
    SymbolNotFoundError,
)

__all__ = ["Catalog", "CatalogEntry"]

logger = logging.getLogger(__name__)

_ALL_COLUMNS = ["my_name", "source", "symbol", "field", "path", "description", "unit"]

_SOURCE_COLUMNS: dict[str, list[str]] = {
    "bloomberg": ["my_name", "source", "symbol", "field", "description", "unit"],
    "localfile": ["my_name", "source", "symbol", "path", "description", "unit"],
    "macrobond": ["my_name", "source", "symbol", "description", "unit"],
}


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    """
    A single catalog entry mapping a name to a data source.

    Parameters
    ----------
    my_name : str
        Unique human-readable identifier for this data series.
    source : str
        Name of the registered source adapter (e.g., "bloomberg").
    symbol : str
        Source-specific identifier (e.g., "SPX Index").
    field : str | None, optional
        Source-specific field name (e.g., "PX_LAST" for Bloomberg).
    path : str | None, optional
        File path for localfile source (e.g., "/data/macro.csv").
    description : str | None, optional
        Human-readable description of the data series.
    unit : str | None, optional
        Unit of measurement (e.g., "USD billions", "points").
    """

    my_name: str
    source: str
    symbol: str
    field: str | None = None
    path: str | None = None
    description: str | None = None
    unit: str | None = None


class Catalog:
    """
    Collection of catalog entries with name-based lookup.

    Parameters
    ----------
    entries : dict[str, CatalogEntry]
        Dictionary mapping my_name to CatalogEntry.
    """

    def __init__(self, entries: dict[str, CatalogEntry]) -> None:
        self._entries = entries

    @classmethod
    def from_yaml(cls, paths: str | Path | list[str | Path]) -> Self:
        """
        Load catalog entries from one or more YAML files.

        Parameters
        ----------
        paths : str | Path | list[str | Path]
            Path or list of paths to YAML catalog files.

        Returns
        -------
        Catalog
            Catalog instance with loaded entries.

        Raises
        ------
        CatalogValidationError
            If file not found, YAML malformed, or entries invalid.
        DuplicateNameError
            If the same my_name appears in multiple entries.
        """
        if isinstance(paths, (str, Path)):
            paths = [paths]

        entries: dict[str, CatalogEntry] = {}

        for path in paths:
            file_path = Path(path)

            if not file_path.exists():
                raise CatalogValidationError(f"Catalog file not found: {path}")

            logger.info("loading_catalog: path=%s", path)

            try:
                with open(file_path) as f:
                    raw_entries = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise CatalogValidationError(f"Malformed YAML in {path}: {e}") from e

            if not isinstance(raw_entries, list):
                raise CatalogValidationError(f"Catalog file {path} must contain a list of entries")

            for raw in raw_entries:
                entry = cls._parse_entry(raw, path)

                if entry.my_name in entries:
                    raise DuplicateNameError(f"Duplicate catalog name: {entry.my_name}")

                entries[entry.my_name] = entry

        logger.info("catalog_loaded: entries=%d", len(entries))
        return cls(entries)

    @classmethod
    def from_csv(cls, paths: str | Path | list[str | Path]) -> Self:
        """
        Load catalog entries from one or more CSV files.

        Parameters
        ----------
        paths : str | Path | list[str | Path]
            Path or list of paths to CSV catalog files.

        Returns
        -------
        Catalog
            Catalog instance with loaded entries.

        Raises
        ------
        CatalogValidationError
            If file not found, CSV malformed, or entries invalid.
            Reports all validation errors at once.
        DuplicateNameError
            If the same my_name appears in multiple entries.
        """
        if isinstance(paths, (str, Path)):
            paths = [paths]

        entries: dict[str, CatalogEntry] = {}
        errors: list[str] = []

        for path in paths:
            file_path = Path(path)

            if not file_path.exists():
                raise CatalogValidationError(f"Catalog file not found: {path}")

            logger.info("loading_catalog_csv: path=%s", path)

            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for row_num, row in enumerate(reader, start=2):  # Row 1 is header
                    # Trim whitespace from all values
                    row = {k: v.strip() if v else v for k, v in row.items()}

                    # Check required fields
                    for field in ["my_name", "source", "symbol"]:
                        if not row.get(field):
                            errors.append(f"Row {row_num}: Missing required field '{field}'")

                    # Skip row if any errors already found for this row
                    if any(f"Row {row_num}:" in e for e in errors):
                        continue

                    my_name = row.get("my_name", "")

                    # Check for duplicates
                    if my_name in entries:
                        errors.append(f"Row {row_num}: Duplicate my_name '{my_name}'")
                        continue

                    # Create entry (empty strings become None)
                    entry = CatalogEntry(
                        my_name=my_name,
                        source=row.get("source", ""),
                        symbol=row.get("symbol", ""),
                        field=row.get("field") or None,
                        path=row.get("path") or None,
                        description=row.get("description") or None,
                        unit=row.get("unit") or None,
                    )
                    entries[my_name] = entry

        if errors:
            error_list = "\n  ".join(errors)
            raise CatalogValidationError(f"{len(errors)} error(s) in CSV:\n  {error_list}")

        logger.info("catalog_loaded_csv: entries=%d", len(entries))
        return cls(entries)

    @staticmethod
    def _parse_entry(raw: dict[str, Any], source_file: str | Path) -> CatalogEntry:
        """Parse a raw dictionary into a CatalogEntry."""
        required_fields = ["my_name", "source", "symbol"]

        for field in required_fields:
            if field not in raw:
                raise CatalogValidationError(f"Missing required field '{field}' in {source_file}")

        return CatalogEntry(
            my_name=raw["my_name"],
            source=raw["source"],
            symbol=raw["symbol"],
            field=raw.get("field"),
            path=raw.get("path"),
            description=raw.get("description"),
            unit=raw.get("unit"),
        )

    def get(self, name: str) -> CatalogEntry:
        """Get a catalog entry by name."""
        if name not in self._entries:
            raise SymbolNotFoundError(
                f"Symbol not found in catalog: {name}. "
                f"Available: {', '.join(sorted(self._entries.keys())[:5])}"
                + ("..." if len(self._entries) > 5 else "")
            )
        return self._entries[name]

    def list_names(self) -> list[str]:
        """List all catalog entry names."""
        return list(self._entries.keys())

    def __len__(self) -> int:
        """Return the number of entries in the catalog."""
        return len(self._entries)

    def __contains__(self, name: str) -> bool:
        """Check if a name exists in the catalog."""
        return name in self._entries

    def validate_sources(self, registry: "SourceRegistry") -> None:
        """
        Validate that all catalog sources are registered.

        Parameters
        ----------
        registry : SourceRegistry
            Source registry to validate against.

        Raises
        ------
        UnknownSourceError
            If any catalog entry references an unregistered source.
        """
        from metapyle.exceptions import UnknownSourceError

        registered = set(registry.list_sources())
        catalog_sources = {entry.source for entry in self._entries.values()}

        unknown = catalog_sources - registered
        if unknown:
            raise UnknownSourceError(
                f"Unknown source(s) in catalog: {', '.join(sorted(unknown))}. "
                f"Registered sources: {', '.join(sorted(registered))}"
            )

    @staticmethod
    def csv_template(source: str | None = None, path: str | Path | None = None) -> str:
        """
        Generate CSV template with headers.

        Parameters
        ----------
        source : str | None, optional
            If provided, generates source-specific template with relevant
            columns only. Valid: "bloomberg", "localfile", "macrobond".
            If None, includes all columns.
        path : str | Path | None, optional
            If provided, writes template to file.

        Returns
        -------
        str
            Template string (header row + optional example row).
        """
        if source is None:
            columns = _ALL_COLUMNS
            template = ",".join(columns) + "\n"
        else:
            if source not in _SOURCE_COLUMNS:
                valid = ", ".join(sorted(_SOURCE_COLUMNS.keys()))
                raise ValueError(f"Unknown source: {source}. Valid sources: {valid}")

            columns = _SOURCE_COLUMNS[source]
            header = ",".join(columns)
            # Example row: only source column filled, rest empty
            example_values = [source if col == "source" else "" for col in columns]
            example = ",".join(example_values)
            template = f"{header}\n{example}\n"

        if path is not None:
            Path(path).write_text(template)

        return template

    def to_csv(self, path: str | Path) -> None:
        """
        Export catalog entries to CSV file.

        Parameters
        ----------
        path : str | Path
            Path to output CSV file.
        """
        file_path = Path(path)

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_ALL_COLUMNS)
            writer.writeheader()

            for entry in self._entries.values():
                writer.writerow(
                    {
                        "my_name": entry.my_name,
                        "source": entry.source,
                        "symbol": entry.symbol,
                        "field": entry.field or "",
                        "path": entry.path or "",
                        "description": entry.description or "",
                        "unit": entry.unit or "",
                    }
                )

        logger.info("catalog_exported_csv: path=%s, entries=%d", path, len(self._entries))

    def to_yaml(self, path: str | Path) -> None:
        """
        Export catalog entries to YAML file.

        Parameters
        ----------
        path : str | Path
            Path to output YAML file.
        """
        file_path = Path(path)

        entries_list = []
        for entry in self._entries.values():
            entry_dict: dict[str, str] = {
                "my_name": entry.my_name,
                "source": entry.source,
                "symbol": entry.symbol,
            }
            # Only include non-None optional fields
            if entry.field is not None:
                entry_dict["field"] = entry.field
            if entry.path is not None:
                entry_dict["path"] = entry.path
            if entry.description is not None:
                entry_dict["description"] = entry.description
            if entry.unit is not None:
                entry_dict["unit"] = entry.unit

            entries_list.append(entry_dict)

        with open(file_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(entries_list, f, default_flow_style=False, sort_keys=False)

        logger.info("catalog_exported_yaml: path=%s, entries=%d", path, len(self._entries))
