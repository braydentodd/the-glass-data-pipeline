"""
The Glass - ETL Progress Tracking

Database operations for tracking ETL run state and per-group progress.
Uses the etl_runs and etl_progress tables defined in config.ETL_TABLES.

Supports auto-resume: if a run was interrupted mid-flight, the runner
can detect the orphaned 'running' record and resume from the last
pending group.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# RUN LIFECYCLE
# ============================================================================

def create_run(
    conn: Any,
    db_schema: str,
    run_type: str,
    season: str,
    season_type: int,
    entity_type: str,
    total_groups: int,
) -> int:
    """Insert a new etl_runs record and return the run id."""
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {db_schema}.etl_runs "
            f"(run_type, status, season, season_type, entity_type, total_groups) "
            f"VALUES (%s, 'running', %s, %s, %s, %s) RETURNING id",
            (run_type, season, str(season_type), entity_type, total_groups),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    logger.info('Created ETL run %d (%s)', run_id, run_type)
    return run_id


def complete_run(conn: Any, db_schema: str, run_id: int, total_rows: int) -> None:
    """Mark a run as completed."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.etl_runs "
            f"SET status = 'completed', completed_at = NOW(), total_rows = %s "
            f"WHERE id = %s",
            (total_rows, run_id),
        )
    conn.commit()


def fail_run(conn: Any, db_schema: str, run_id: int, error_message: str) -> None:
    """Mark a run as failed."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.etl_runs "
            f"SET status = 'failed', completed_at = NOW(), error_message = %s "
            f"WHERE id = %s",
            (error_message, run_id),
        )
    conn.commit()


# ============================================================================
# GROUP PROGRESS
# ============================================================================

def register_groups(
    conn: Any,
    db_schema: str,
    run_id: int,
    groups: List[Dict[str, Any]],
    entity_type: str,
) -> List[int]:
    """Insert etl_progress rows for each call group. Returns progress ids."""
    progress_ids: List[int] = []
    with conn.cursor() as cur:
        for group in groups:
            endpoint = group['endpoint']
            tier = group['tier']
            col_names = sorted(group.get('columns', {}).keys())
            col_name_str = ','.join(col_names) if col_names else None

            cur.execute(
                f"INSERT INTO {db_schema}.etl_progress "
                f"(run_id, entity_type, endpoint, tier, column_name, status) "
                f"VALUES (%s, %s, %s, %s, %s, 'pending') RETURNING id",
                (run_id, entity_type, endpoint, tier, col_name_str),
            )
            progress_ids.append(cur.fetchone()[0])
    conn.commit()
    return progress_ids


def mark_group_started(conn: Any, db_schema: str, progress_id: int) -> None:
    """Mark a progress entry as running."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.etl_progress "
            f"SET status = 'running', started_at = NOW() "
            f"WHERE id = %s",
            (progress_id,),
        )
    conn.commit()


def mark_group_completed(
    conn: Any, db_schema: str, progress_id: int, rows_written: int,
) -> None:
    """Mark a progress entry as completed."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.etl_progress "
            f"SET status = 'completed', completed_at = NOW(), rows_written = %s "
            f"WHERE id = %s",
            (rows_written, progress_id),
        )
    conn.commit()


def mark_group_failed(
    conn: Any, db_schema: str, progress_id: int, error_message: str,
) -> None:
    """Mark a progress entry as failed and increment retry count."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.etl_progress "
            f"SET status = 'failed', completed_at = NOW(), "
            f"error_message = %s, retry_count = retry_count + 1 "
            f"WHERE id = %s",
            (error_message, progress_id),
        )
    conn.commit()


# ============================================================================
# AUTO-RESUME
# ============================================================================

def find_resumable_run(
    conn: Any,
    db_schema: str,
    season: str,
    season_type: int,
    entity_type: str,
) -> Optional[int]:
    """Find an interrupted run matching the given parameters.

    Returns the run_id if a 'running' record exists, else None.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {db_schema}.etl_runs "
            f"WHERE status = 'running' AND season = %s AND season_type = %s "
            f"AND entity_type = %s ORDER BY started_at DESC LIMIT 1",
            (season, str(season_type), entity_type),
        )
        row = cur.fetchone()
    return row[0] if row else None


def get_pending_progress_ids(
    conn: Any,
    db_schema: str,
    run_id: int,
) -> List[Tuple[int, str, str]]:
    """Get (progress_id, endpoint, column_name) for incomplete groups.

    Returns groups with status 'pending' or 'running' (interrupted).
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, endpoint, column_name FROM {db_schema}.etl_progress "
            f"WHERE run_id = %s AND status IN ('pending', 'running') "
            f"ORDER BY id",
            (run_id,),
        )
        return cur.fetchall()


def update_run_completed_groups(
    conn: Any, db_schema: str, run_id: int,
) -> None:
    """Sync the completed_groups counter on the run record."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.etl_runs SET completed_groups = ("
            f"  SELECT COUNT(*) FROM {db_schema}.etl_progress "
            f"  WHERE run_id = %s AND status = 'completed'"
            f") WHERE id = %s",
            (run_id, run_id),
        )
    conn.commit()
