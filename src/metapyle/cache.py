"""
Cache module for storing fetched time-series data.

This module provides SQLite-based caching for metapyle data fetches.
Data is stored per-symbol (not per-batch), enabling granular cache
management and efficient partial cache hits.

SQLite Schema
-------------
The cache uses two tables:

**cache_entries**
    Metadata about cached data entries.

    - id: INTEGER PRIMARY KEY
    - source: TEXT NOT NULL (e.g., "bloomberg", "macrobond")
    - symbol: TEXT NOT NULL (e.g., "SPX Index", "usgdp")
    - field: TEXT (e.g., "PX_LAST" for Bloomberg)
    - path: TEXT (e.g., file path for localfile source)
    - start_date: TEXT NOT NULL (ISO format YYYY-MM-DD)
    - end_date: TEXT NOT NULL (ISO format YYYY-MM-DD)
    - created_at: TEXT NOT NULL (ISO timestamp)
    - UNIQUE(source, symbol, field, path, start_date, end_date)

**cache_data**
    Actual time-series data as serialized parquet bytes.

    - entry_id: INTEGER PRIMARY KEY REFERENCES cache_entries(id)
    - data: BLOB NOT NULL (parquet-serialized DataFrame)

Cache Key Components
--------------------
Each cache entry is uniquely identified by:
``(source, symbol, field, path, start_date, end_date)``

Where ``field`` and ``path`` may be NULL/None.

Batch Fetch Behavior
--------------------
When fetching multiple symbols from the same source:

1. Check cache for each symbol individually
2. Group uncached symbols by source
3. Batch fetch per source (single API call)
4. Split result and cache each symbol separately

Example
-------
>>> from metapyle import Client
>>> with Client(catalog="my_catalog.yaml") as client:
...     # First fetch - calls API and caches
...     df = client.fetch(["SPX", "VIX"], "2024-01-01", "2024-03-31")
...     # Second fetch - served from cache
...     df = client.fetch(["SPX", "VIX"], "2024-01-01", "2024-03-31")
"""

import io
import logging
import os
import sqlite3
from pathlib import Path

import pandas as pd

__all__ = ["Cache"]

logger = logging.getLogger(__name__)

DEFAULT_CACHE_PATH = "./cache/data_cache.db"


class Cache:
    """
    Cache for time-series data with SQLite storage.

    Uses two tables:
    - cache_entries: metadata about cached data (source, symbol, field, date range)
    - cache_data: the actual DataFrame data in Parquet format

    Parameters
    ----------
    path : str | None, optional
        Path to SQLite database file. If None, uses METAPYLE_CACHE_PATH
        environment variable or defaults to "./cache/data_cache.db".
    enabled : bool, optional
        Whether caching is enabled. If False, put() is a no-op and get()
        always returns None. Default is True.
    """

    def __init__(
        self,
        path: str | None = None,
        *,
        enabled: bool = True,
    ) -> None:
        self._enabled = enabled
        self._conn: sqlite3.Connection | None = None

        if not enabled:
            logger.debug("cache_disabled")
            return

        if path is None:
            path = os.environ.get("METAPYLE_CACHE_PATH", DEFAULT_CACHE_PATH)

        self._path = path
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Create database and tables if they don't exist."""
        # Ensure parent directory exists
        db_path = Path(self._path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(self._path)

        # Check if migration is needed (old schema without path column)
        cursor = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='cache_entries'"
        )
        row = cursor.fetchone()
        if row is not None and "path TEXT" not in row[0]:
            # Old schema detected, drop tables and recreate
            logger.info("cache_migration: dropping old schema without path column")
            self._conn.execute("DROP TABLE IF EXISTS cache_data")
            self._conn.execute("DROP TABLE IF EXISTS cache_entries")
            self._conn.commit()

        # Create cache_entries table for metadata
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                symbol TEXT NOT NULL,
                field TEXT,
                path TEXT,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, symbol, field, path, start_date, end_date)
            )
        """)

        # Create cache_data table for storing DataFrame as blob
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_data (
                entry_id INTEGER PRIMARY KEY,
                data BLOB NOT NULL,
                FOREIGN KEY (entry_id) REFERENCES cache_entries(id)
                    ON DELETE CASCADE
            )
        """)

        # Create index for faster lookups
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cache_lookup
            ON cache_entries(source, symbol, field, path)
        """)

        self._conn.commit()
        logger.info("cache_initialized: path=%s", self._path)

    def put(
        self,
        source: str,
        symbol: str,
        field: str | None,
        path: str | None,
        start_date: str,
        end_date: str,
        data: pd.DataFrame,
    ) -> None:
        """
        Store DataFrame in cache.

        Parameters
        ----------
        source : str
            Data source name.
        symbol : str
            Source-specific symbol.
        field : str | None
            Field name (can be None for sources without fields).
        path : str | None
            Source path (e.g., file path for localfile source).
        start_date : str
            Start date in ISO format (YYYY-MM-DD).
        end_date : str
            End date in ISO format (YYYY-MM-DD).
        data : pd.DataFrame
            DataFrame to cache.
        """
        if not self._enabled:
            return

        if self._conn is None:
            return

        try:
            # Serialize DataFrame to Parquet bytes
            data_bytes = data.to_parquet()

            # Delete existing entry if present (for overwrite)
            self._delete_entry(source, symbol, field, path, start_date, end_date)

            # Insert new entry
            cursor = self._conn.execute(
                """
                INSERT INTO cache_entries (source, symbol, field, path, start_date, end_date)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source, symbol, field, path, start_date, end_date),
            )
            entry_id = cursor.lastrowid

            # Insert data
            self._conn.execute(
                """
                INSERT INTO cache_data (entry_id, data)
                VALUES (?, ?)
                """,
                (entry_id, data_bytes),
            )

            self._conn.commit()
            logger.debug(
                "cache_put: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
                source,
                symbol,
                field,
                path,
                start_date,
                end_date,
            )
        except Exception:
            logger.warning(
                "cache_put_failed: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
                source,
                symbol,
                field,
                path,
                start_date,
                end_date,
                exc_info=True,
            )

    def get(
        self,
        source: str,
        symbol: str,
        field: str | None,
        path: str | None,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame | None:
        """
        Retrieve DataFrame from cache.

        Returns data if requested range is a subset of a cached range.

        Parameters
        ----------
        source : str
            Data source name.
        symbol : str
            Source-specific symbol.
        field : str | None
            Field name (can be None for sources without fields).
        path : str | None
            Source path (e.g., file path for localfile source).
        start_date : str
            Start date in ISO format (YYYY-MM-DD).
        end_date : str
            End date in ISO format (YYYY-MM-DD).

        Returns
        -------
        pd.DataFrame | None
            Cached DataFrame if found, None otherwise.
        """
        if not self._enabled:
            return None

        if self._conn is None:
            return None

        try:
            # Find a cached entry that covers the requested range
            # field and path can be None, so we need special handling
            if field is None and path is None:
                cursor = self._conn.execute(
                    """
                    SELECT ce.id, ce.start_date, ce.end_date, cd.data
                    FROM cache_entries ce
                    JOIN cache_data cd ON cd.entry_id = ce.id
                    WHERE ce.source = ?
                      AND ce.symbol = ?
                      AND ce.field IS NULL
                      AND ce.path IS NULL
                      AND ce.start_date <= ?
                      AND ce.end_date >= ?
                    """,
                    (source, symbol, start_date, end_date),
                )
            elif field is None and path is not None:
                cursor = self._conn.execute(
                    """
                    SELECT ce.id, ce.start_date, ce.end_date, cd.data
                    FROM cache_entries ce
                    JOIN cache_data cd ON cd.entry_id = ce.id
                    WHERE ce.source = ?
                      AND ce.symbol = ?
                      AND ce.field IS NULL
                      AND ce.path = ?
                      AND ce.start_date <= ?
                      AND ce.end_date >= ?
                    """,
                    (source, symbol, path, start_date, end_date),
                )
            elif field is not None and path is None:
                cursor = self._conn.execute(
                    """
                    SELECT ce.id, ce.start_date, ce.end_date, cd.data
                    FROM cache_entries ce
                    JOIN cache_data cd ON cd.entry_id = ce.id
                    WHERE ce.source = ?
                      AND ce.symbol = ?
                      AND ce.field = ?
                      AND ce.path IS NULL
                      AND ce.start_date <= ?
                      AND ce.end_date >= ?
                    """,
                    (source, symbol, field, start_date, end_date),
                )
            else:  # field is not None and path is not None
                cursor = self._conn.execute(
                    """
                    SELECT ce.id, ce.start_date, ce.end_date, cd.data
                    FROM cache_entries ce
                    JOIN cache_data cd ON cd.entry_id = ce.id
                    WHERE ce.source = ?
                      AND ce.symbol = ?
                      AND ce.field = ?
                      AND ce.path = ?
                      AND ce.start_date <= ?
                      AND ce.end_date >= ?
                    """,
                    (source, symbol, field, path, start_date, end_date),
                )

            row = cursor.fetchone()
            if row is None:
                logger.debug(
                    "cache_miss: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
                    source,
                    symbol,
                    field,
                    path,
                    start_date,
                    end_date,
                )
                return None

            _, cached_start, cached_end, data_bytes = row

            # Deserialize DataFrame
            df = pd.read_parquet(io.BytesIO(data_bytes))

            # If requested range is subset, filter the data
            if start_date != cached_start or end_date != cached_end:
                # Filter to requested range
                start_dt = pd.Timestamp(start_date)
                end_dt = pd.Timestamp(end_date)
                df = df[(df.index >= start_dt) & (df.index <= end_dt)]

            logger.debug(
                "cache_hit: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
                source,
                symbol,
                field,
                path,
                start_date,
                end_date,
            )
            return df
        except Exception:
            logger.warning(
                "cache_get_failed: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
                source,
                symbol,
                field,
                path,
                start_date,
                end_date,
                exc_info=True,
            )
            return None

    def clear(
        self,
        source: str | None = None,
        symbol: str | None = None,
    ) -> None:
        """
        Clear cache entries.

        If source and symbol are provided, only clears entries matching both.
        Otherwise, clears all entries.

        Parameters
        ----------
        source : str | None, optional
            Data source name to clear.
        symbol : str | None, optional
            Symbol to clear. Must be provided with source.
        """
        if not self._enabled:
            return

        if self._conn is None:
            return

        if source is not None and symbol is not None:
            # Get entry IDs to delete
            cursor = self._conn.execute(
                """
                SELECT id FROM cache_entries
                WHERE source = ? AND symbol = ?
                """,
                (source, symbol),
            )
            entry_ids = [row[0] for row in cursor.fetchall()]

            if entry_ids:
                # Delete data first (foreign key constraint)
                placeholders = ",".join("?" * len(entry_ids))
                self._conn.execute(
                    f"DELETE FROM cache_data WHERE entry_id IN ({placeholders})",
                    entry_ids,
                )
                self._conn.execute(
                    f"DELETE FROM cache_entries WHERE id IN ({placeholders})",
                    entry_ids,
                )

            logger.info("cache_cleared: source=%s, symbol=%s", source, symbol)
        else:
            # Clear all
            self._conn.execute("DELETE FROM cache_data")
            self._conn.execute("DELETE FROM cache_entries")
            logger.info("cache_cleared: all entries")

        self._conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.debug("cache_closed")

    def list_cached_symbols(self) -> list[dict[str, str | None]]:
        """
        List all cached entries.

        Returns
        -------
        list[dict[str, str | None]]
            List of dicts with keys: source, symbol, field, path, start_date, end_date.
        """
        if not self._enabled or self._conn is None:
            return []

        cursor = self._conn.execute(
            """
            SELECT source, symbol, field, path, start_date, end_date
            FROM cache_entries
            ORDER BY source, symbol
            """
        )
        return [
            {
                "source": row[0],
                "symbol": row[1],
                "field": row[2],
                "path": row[3],
                "start_date": row[4],
                "end_date": row[5],
            }
            for row in cursor.fetchall()
        ]

    def clear_symbol(
        self,
        source: str,
        symbol: str,
        field: str | None,
        path: str | None,
    ) -> int:
        """
        Clear all cache entries for a specific symbol.

        Parameters
        ----------
        source : str
            Data source name.
        symbol : str
            Symbol identifier.
        field : str | None
            Field name (can be None).
        path : str | None
            File path (can be None).

        Returns
        -------
        int
            Number of entries cleared.
        """
        if not self._enabled or self._conn is None:
            return 0

        # Delete data first (foreign key)
        self._conn.execute(
            """
            DELETE FROM cache_data
            WHERE entry_id IN (
                SELECT id FROM cache_entries
                WHERE source = ? AND symbol = ?
                AND (field IS ? OR (field IS NULL AND ? IS NULL))
                AND (path IS ? OR (path IS NULL AND ? IS NULL))
            )
            """,
            (source, symbol, field, field, path, path),
        )

        cursor = self._conn.execute(
            """
            DELETE FROM cache_entries
            WHERE source = ? AND symbol = ?
            AND (field IS ? OR (field IS NULL AND ? IS NULL))
            AND (path IS ? OR (path IS NULL AND ? IS NULL))
            """,
            (source, symbol, field, field, path, path),
        )

        count = cursor.rowcount
        self._conn.commit()

        logger.info(
            "cache_cleared: source=%s, symbol=%s, field=%s, count=%d",
            source,
            symbol,
            field,
            count,
        )
        return count

    def _delete_entry(
        self,
        source: str,
        symbol: str,
        field: str | None,
        path: str | None,
        start_date: str,
        end_date: str,
    ) -> None:
        """Delete a specific cache entry if it exists."""
        if self._conn is None:
            return

        # Get entry ID - handle all 4 combinations of field/path NULL
        if field is None and path is None:
            cursor = self._conn.execute(
                """
                SELECT id FROM cache_entries
                WHERE source = ? AND symbol = ? AND field IS NULL AND path IS NULL
                  AND start_date = ? AND end_date = ?
                """,
                (source, symbol, start_date, end_date),
            )
        elif field is None and path is not None:
            cursor = self._conn.execute(
                """
                SELECT id FROM cache_entries
                WHERE source = ? AND symbol = ? AND field IS NULL AND path = ?
                  AND start_date = ? AND end_date = ?
                """,
                (source, symbol, path, start_date, end_date),
            )
        elif field is not None and path is None:
            cursor = self._conn.execute(
                """
                SELECT id FROM cache_entries
                WHERE source = ? AND symbol = ? AND field = ? AND path IS NULL
                  AND start_date = ? AND end_date = ?
                """,
                (source, symbol, field, start_date, end_date),
            )
        else:  # field is not None and path is not None
            cursor = self._conn.execute(
                """
                SELECT id FROM cache_entries
                WHERE source = ? AND symbol = ? AND field = ? AND path = ?
                  AND start_date = ? AND end_date = ?
                """,
                (source, symbol, field, path, start_date, end_date),
            )

        row = cursor.fetchone()
        if row is not None:
            entry_id = row[0]
            self._conn.execute("DELETE FROM cache_data WHERE entry_id = ?", (entry_id,))
            self._conn.execute("DELETE FROM cache_entries WHERE id = ?", (entry_id,))
