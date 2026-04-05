"""
The Glass - ETL Database Loader

Bulk database writer for the ETL pipeline.  Provides ``bulk_upsert``
(using ``execute_values`` with ``ON CONFLICT``) and ``bulk_copy``
(using PostgreSQL ``COPY FROM``).  Both honour the column-quoting
required by digit-starting column names (e.g. ``fg2m``).
"""

import logging
from io import StringIO
from typing import Any, Dict, List, Optional

from psycopg2.extras import execute_values

from src.db import db_connection, quote_col

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500


# ============================================================================
# BULK DATABASE WRITER
# ============================================================================

class BulkDatabaseWriter:
    """Optimised bulk writer wrapping a single connection."""

    def __init__(self, conn: Any, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
        self.conn = conn
        self.batch_size = batch_size

    # ------------------------------------------------------------------ upsert
    def bulk_upsert(
        self,
        table: str,
        columns: List[str],
        data: List[tuple],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> int:
        """INSERT … ON CONFLICT DO UPDATE SET for a batch of rows.

        Args:
            table:            Schema-qualified table name (e.g. ``nba.player_season_stats``).
            columns:          Ordered column names matching ``data`` tuples.
            data:             List of tuples — one per row.
            conflict_columns: Unique-constraint columns for conflict detection.
            update_columns:   Columns to overwrite on conflict.
                              *None* → all non-conflict columns.

        Returns:
            Number of rows written.
        """
        if not data:
            return 0

        if update_columns is None:
            conflict_set = set(conflict_columns)
            update_columns = [c for c in columns if c not in conflict_set]

        cols_sql = ', '.join(quote_col(c) for c in columns)
        conflict_sql = ', '.join(quote_col(c) for c in conflict_columns)
        update_sql = ', '.join(
            f'{quote_col(c)} = EXCLUDED.{quote_col(c)}' for c in update_columns
        )

        query = (
            f'INSERT INTO {table} ({cols_sql}) VALUES %s '
            f'ON CONFLICT ({conflict_sql}) '
            f'DO UPDATE SET {update_sql}, updated_at = NOW()'
        )

        cursor = self.conn.cursor()
        written = 0

        for offset in range(0, len(data), self.batch_size):
            batch = data[offset : offset + self.batch_size]
            try:
                execute_values(cursor, query, batch, page_size=self.batch_size)
                written += len(batch)
            except Exception:
                logger.error('Batch failed at offset %d in %s', offset, table)
                self.conn.rollback()
                raise

        self.conn.commit()
        return written

    # ------------------------------------------------------------------ copy
    def bulk_copy(
        self,
        table: str,
        columns: List[str],
        data: List[tuple],
    ) -> int:
        """Ultra-fast initial load via PostgreSQL ``COPY FROM``.

        Does **not** handle conflicts — use for initial loads only.

        Returns:
            Number of rows copied.
        """
        if not data:
            return 0

        buf = StringIO()
        for row in data:
            line = '\t'.join(
                str(v) if v is not None else '\\N' for v in row
            )
            buf.write(line + '\n')
        buf.seek(0)

        cursor = self.conn.cursor()
        try:
            cursor.copy_from(buf, table, columns=columns, null='\\N')
            self.conn.commit()
            return len(data)
        except Exception:
            logger.error('COPY into %s failed', table)
            self.conn.rollback()
            raise


# ============================================================================
# HIGH-LEVEL WRITE HELPERS
# ============================================================================

def upsert_entity_rows(
    conn: Any,
    table: str,
    rows: Dict[int, Dict[str, Any]],
    conflict_columns: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Convert ``{entity_id: {col: val}}`` dicts to tuples and upsert.

    Collects the union of all column names across every entity, fills
    missing values with ``None``, and delegates to ``BulkDatabaseWriter``.

    Args:
        conn:             psycopg2 connection.
        table:            Schema-qualified table name.
        rows:             Extraction result from ``extract_columns_from_result``.
        conflict_columns: PK / unique columns (e.g. ``['nba_api_id', 'season']``).
        batch_size:       Rows per batch.

    Returns:
        Number of rows written.
    """
    if not rows:
        return 0

    # Collect the union of all columns present in the data
    all_cols: set = set()
    for entity_vals in rows.values():
        all_cols.update(entity_vals.keys())

    columns = sorted(all_cols)
    data = [
        tuple(entity_vals.get(c) for c in columns)
        for entity_vals in rows.values()
    ]

    writer = BulkDatabaseWriter(conn, batch_size=batch_size)
    return writer.bulk_upsert(table, columns, data, conflict_columns)
