import logging
from typing import List

from src.core.db import db_connection, get_db_connection, quote_col
from src.core.config import STAT_DOMAINS
from src.etl.definitions import DB_COLUMNS, get_source_id_column

logger = logging.getLogger(__name__)

def cleanup_stat_domains(db_schema: str, entity: str, season: str, season_type: str) -> int:
    """
    Run a sweeping ELT clean up on non-primary stat domains for a specific entity type, season, and season type.
    If the domain's minutes column is 0 or NULL, all stats for that domain are set to NULL.
    If the domain's minutes > 0, any NULL stats for that domain are set to 0.
    """
    affected_rows = 0
    table_name = f"{db_schema}.{entity}_season_stats"

    # Group columns by domain
    domain_cols = {}
    for col_name, col_meta in DB_COLUMNS.items():
        if entity not in col_meta.get('entity_types', []):
            continue
        domain_name = col_meta.get('domain')
        if domain_name and domain_name in STAT_DOMAINS and not STAT_DOMAINS[domain_name].get('primary', True):
            # Exclude the exact minutes_col from being coerced to 0 if it's NULL?
            # Wait, the minutes col itself should maybe be set to 0 if NULL?
            # Actually, the user says "If the domain's minutes column is 0 or null, then it (the stats) should be null."
            if col_name != STAT_DOMAINS[domain_name]['minutes_col']:
                domain_cols.setdefault(domain_name, []).append(col_name)

    logger.info(f"Running ELT domain cleanup for {entity} in {season} ({season_type})..")

    with db_connection() as conn:
        with conn.cursor() as cur:
            for domain_name, cols in domain_cols.items():
                if not cols:
                    continue
                
                minutes_col = quote_col(STAT_DOMAINS[domain_name]['minutes_col'])
                
                # Condition 1: If minutes > 0, NULL stats -> 0
                set_to_zero_clause = ", ".join(f"{quote_col(c)} = COALESCE({quote_col(c)}, 0)" for c in cols)
                cur.execute(f"""
                    UPDATE {table_name}
                    SET {set_to_zero_clause}
                    WHERE season = %s AND season_type = %s
                      AND {minutes_col} > 0
                """, (season, season_type))
                affected_rows += cur.rowcount
                
                # Condition 2: If minutes is 0 or NULL, stats -> NULL
                set_to_null_clause = ", ".join(f"{quote_col(c)} = NULL" for c in cols)
                cur.execute(f"""
                    UPDATE {table_name}
                    SET {set_to_null_clause}
                    WHERE season = %s AND season_type = %s
                      AND ({minutes_col} IS NULL OR {minutes_col} = 0)
                """, (season, season_type))
                affected_rows += cur.rowcount

    return affected_rows


def prune_stale(entities: List[str], oldest_season: str, db_schema: str) -> int:
    """Delete stats rows older than the retention window, then remove orphaned entities.

    Orphaned entities are entity rows (players/teams) that have no remaining
    stats rows after the prune -- e.g. a player who only appeared in seasons
    that are now outside the retention window.

    Returns total rows deleted.
    """
    logger.info('Phase: prune_stale (before %s)', oldest_season)
    conn = get_db_connection()
    pruned = 0
    try:
        with conn.cursor() as cur:
            for table_name, meta in TABLES.items():
                if meta['scope'] != 'stats':
                    continue
                qualified = f"{db_schema}.{table_name}"
                cur.execute(
                    f"DELETE FROM {qualified} WHERE season < %s",
                    (oldest_season,),
                )
                count = cur.rowcount
                if count:
                    logger.info('Pruned %d rows from %s', count, qualified)
                    pruned += count

            for table_name, meta in TABLES.items():
                if meta['scope'] != 'entity':
                    continue
                entity_type = meta['entity']
                if entity_type not in entities:
                    continue
                stats_table = None
                for st_name, st_meta in TABLES.items():
                    if st_meta['scope'] == 'stats' and st_meta['entity'] == entity_type:
                        stats_table = f"{db_schema}.{st_name}"
                        break
                if not stats_table:
                    continue
                entity_qualified = f"{db_schema}.{table_name}"
                cur.execute(
                    f"DELETE FROM {entity_qualified} e "
                    f"WHERE NOT EXISTS ("
                    f"  SELECT 1 FROM {stats_table} s WHERE s.entity_id = e.id"
                    f")",
                )
                count = cur.rowcount
                if count:
                    logger.info(
                        'Pruned %d orphaned entities from %s', count, entity_qualified,
                    )
                    pruned += count

        conn.commit()
        return pruned
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
