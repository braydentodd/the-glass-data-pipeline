"""
The Glass — Shared Data Access Layer (DAL)

Unified SQL fetching logic for Google Sheets pipelines across all leagues.
Functions are dynamically configured via the LeagueSyncContext to avoid hardcoding
league-specific tables, schemas, or entity fields.
"""
import logging
from typing import Dict, List, Optional, Tuple
from psycopg2.extras import RealDictCursor

from src.core.db import get_db_connection
from src.publish.definitions.config import SEASON_TYPE_GROUPS, COMPUTED_ENTITY_FIELDS

logger = logging.getLogger(__name__)


def _season_types_for_section(section: str) -> tuple:
    """Return the season_type codes to filter by, based on the stats section."""
    if section == 'historical_stats':
        return SEASON_TYPE_GROUPS['regular_season']
    return SEASON_TYPE_GROUPS['postseason']


def get_teams_from_db(db_schema: str) -> Dict[int, Tuple[str, str]]:
    """Fetch all teams from the database.

    Returns {id: (abbr, name)} for the given schema.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, abbr, name FROM {db_schema}.teams "
                f"ORDER BY abbr"
            )
            return {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    finally:
        conn.close()


def _quote_col(col: str) -> str:
    """Quote a column name for SQL to prevent reserved keyword collisions."""
    if col.lower() in ('limit', 'offset', 'all', 'user', 'year', 'season', 'age', 'rank'):
        return f'"{col}"'
    return col


def _build_entity_fields(entity_fields, table_alias='p'):
    """Build SELECT fragments for entity fields, including computed fields.

    Returns (select_fields, group_fields) where select_fields may contain
    computed expressions (like age from birthdate) and group_fields contains
    only raw column refs needed for GROUP BY.
    """
    select_f = []
    group_f = []
    group_raw = []  # raw columns needed for computed fields' GROUP BY

    for f in sorted(entity_fields):
        select_f.append(f'{table_alias}.{_quote_col(f)}')
        group_f.append(f'{table_alias}.{_quote_col(f)}')

    for field_name, sql_expr in COMPUTED_ENTITY_FIELDS.items():
        select_f.append(f'{sql_expr} AS {_quote_col(field_name)}')
        # birthdate is the raw column needed for age computation
        if 'birthdate' in sql_expr:
            group_raw.append(f'{table_alias}.birthdate')

    return select_f, group_f + group_raw


def _build_season_filter(historical_config: Optional[dict], current_season_year: int,
                         season_col: str, season_format_fn) -> Tuple[str, tuple]:
    """
    Build SQL season filter clause and params tuple.
    Historical/postseason sections never include the current season.
    Takes a format function (season_format_fn) to handle formatting disparities.
    """
    if not historical_config:
        seasons = tuple(season_format_fn(current_season_year - i) for i in range(1, 4))
        return f"AND s.{season_col} IN %s", (seasons,)

    mode = historical_config.get('mode', 'seasons')
    value = historical_config.get('value', 3)

    if isinstance(value, int):
        seasons = tuple(season_format_fn(current_season_year - i) for i in range(1, 1 + value))
        return f"AND s.{season_col} IN %s", (seasons,)

    elif isinstance(value, list):
        return f"AND s.{season_col} IN %s", (tuple(value),)
    else:
        return "", ()


def fetch_players_for_team(conn, team_abbr: str, section: str,
                           historical_config: Optional[dict],
                           ctx, # The LeagueSyncContext holding dynamic properties
                           current_season: str, current_season_year: int, season_type_val,
                           season_col_name: str = 'season') -> List[dict]:
    """Fetch player data for a team with all stats needed for formula evaluation."""
    
    players_tbl = ctx.player_entity_table
    teams_tbl = ctx.team_entity_table
    stats_tbl = ctx.player_stats_table

    p_select, p_group = _build_entity_fields(ctx.player_entity_fields, 'p')
    team_abbr_col = _quote_col(ctx.team_abbr_col)
    t_select = [f"t.{team_abbr_col} AS team_abbr"]
    t_group = [f"t.{team_abbr_col}"]

    if section == 'current_stats':
        s_fields = [f's.{_quote_col(f)}' for f in sorted(ctx.stat_fields)]
        all_fields = p_select + t_select + s_fields

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_tbl} p
        INNER JOIN {teams_tbl} t ON p.team_id = t.id
        LEFT JOIN {stats_tbl} s
            ON s.entity_id = p.id
            AND s.{season_col_name} = %s AND s.season_type = %s
        WHERE t.{ctx.team_abbr_col} = %s
        ORDER BY COALESCE(s.{ctx.primary_minutes_col}, 0) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type_val, team_abbr))
            return [dict(r) for r in cur.fetchall()]

    else:
        season_types = _season_types_for_section(section)
        season_filter, params = _build_season_filter(
            historical_config, current_season_year, season_col_name, ctx.season_format_fn
        )

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(ctx.stat_fields)]
        s_fields.append(f'COUNT(DISTINCT s.{season_col_name}) AS {season_col_name}')
        
        all_fields = p_select + t_select + s_fields
        group_fields = p_group + t_group

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_tbl} p
        INNER JOIN {teams_tbl} t ON p.team_id = t.id
        LEFT JOIN {stats_tbl} s
            ON s.entity_id = p.id
            {season_filter}
            AND s.season_type IN %s
        WHERE t.{ctx.team_abbr_col} = %s
        GROUP BY {', '.join(group_fields)}
        ORDER BY SUM(COALESCE(s.{ctx.primary_minutes_col}, 0)) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (*params, season_types, team_abbr))
            return [dict(r) for r in cur.fetchall()]


def fetch_all_players(conn, section: str, historical_config: Optional[dict],
                      ctx, current_season: str, current_season_year: int, season_type_val,
                      season_col_name: str = 'season') -> List[dict]:
    """Fetch all players across the entire league for percentile calculations."""
    players_tbl = ctx.player_entity_table
    teams_tbl = ctx.team_entity_table
    stats_tbl = ctx.player_stats_table

    stat_f = [f"s.{_quote_col(f)}" for f in sorted(ctx.stat_fields)]
    ent_select, ent_group = _build_entity_fields(ctx.player_entity_fields, 'p')
    all_f = stat_f + ent_select + [f"t.{_quote_col(ctx.team_abbr_col)} AS team_abbr"]

    if section == 'current_stats':
        query = f"""
            SELECT {', '.join(all_f)}
            FROM {stats_tbl} s
            INNER JOIN {players_tbl} p ON s.entity_id = p.id
            INNER JOIN {teams_tbl} t ON p.team_id = t.id
            WHERE s.{season_col_name} = %s AND s.season_type = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type_val))
            return [dict(r) for r in cur.fetchall()]
    else:
        season_types = _season_types_for_section(section)
        season_filter, params = _build_season_filter(
            historical_config, current_season_year, season_col_name, ctx.season_format_fn
        )

        s_sums = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(ctx.stat_fields)]
        s_sums.append(f'COUNT(DISTINCT s.{season_col_name}) AS {season_col_name}')
        all_aggregates = s_sums + ent_select + [f"t.{_quote_col(ctx.team_abbr_col)} AS team_abbr"]
        
        group_f = ent_group + [f"t.{_quote_col(ctx.team_abbr_col)}"]

        query = f"""
            SELECT {', '.join(all_aggregates)}
            FROM {stats_tbl} s
            INNER JOIN {players_tbl} p ON s.entity_id = p.id
            INNER JOIN {teams_tbl} t ON p.team_id = t.id
            WHERE s.season_type IN %s {season_filter}
            GROUP BY {', '.join(group_f)}
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (season_types, *params))
            return [dict(r) for r in cur.fetchall()]


def fetch_team_stats(conn, team_abbr: str, section: str, historical_config: Optional[dict],
                     ctx, current_season: str, current_season_year: int, season_type_val,
                     season_col_name: str = 'season') -> dict:
    """Fetch aggregated team data and opponent data."""
    teams_tbl = ctx.team_entity_table
    stats_tbl = ctx.team_stats_table

    t_select, t_group = _build_entity_fields([f for f in sorted(ctx.team_entity_fields) if f != 'updated_at'], 't')
    s_fields = [f's.{_quote_col(f)}' for f in sorted(ctx.team_stat_fields)]
    all_fields = t_select + s_fields + [f"t.{_quote_col(ctx.team_abbr_col)} AS team_abbr"]

    if section == 'current_stats':
        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {teams_tbl} t
        LEFT JOIN {stats_tbl} s
            ON s.entity_id = t.id
            AND s.{season_col_name} = %s AND s.season_type = %s
        WHERE t.{ctx.team_abbr_col} = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type_val, team_abbr))
            rows = [dict(r) for r in cur.fetchall()]
    else:
        season_types = _season_types_for_section(section)
        season_filter, params = _build_season_filter(
            historical_config, current_season_year, season_col_name, ctx.season_format_fn
        )

        s_sums = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(ctx.team_stat_fields)]
        s_sums.append(f'COUNT(DISTINCT s.{season_col_name}) AS {season_col_name}')
        all_aggregates = t_select + s_sums + [f"t.{_quote_col(ctx.team_abbr_col)} AS team_abbr"]
        
        query = f"""
        SELECT {', '.join(all_aggregates)}
        FROM {teams_tbl} t
        LEFT JOIN {stats_tbl} s
            ON s.entity_id = t.id
            {season_filter}
            AND s.season_type IN %s
        WHERE t.{ctx.team_abbr_col} = %s
        GROUP BY {', '.join(t_group)}
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (*params, season_types, team_abbr))
            rows = [dict(r) for r in cur.fetchall()]

    # Separate team vs opp rows
    teams, opps = [], []
    for row in rows:
        if row.get('is_opponent') == 1 or row.get('is_opponent') is True:
            opps.append(row)
        else:
            teams.append(row)

    # Note: Using dict access over object properties since RealDictCursor returns dict-like
    return {
        'team': teams[0] if teams else {},
        'opponent': opps[0] if opps else {}
    }


def fetch_all_teams(conn, section: str, historical_config: Optional[dict],
                    ctx, current_season: str, current_season_year: int, season_type_val,
                    season_col_name: str = 'season') -> dict:
    """Fetch all teams' aggregated stats (used for team pacing and percentiles)."""
    teams_tbl = ctx.team_entity_table
    stats_tbl = ctx.team_stats_table

    t_select, t_group = _build_entity_fields([f for f in sorted(ctx.team_entity_fields) if f != 'updated_at'], 't')
    s_fields = [f's.{_quote_col(f)}' for f in sorted(ctx.team_stat_fields)]
    all_fields = t_select + s_fields + [f"t.{_quote_col(ctx.team_abbr_col)} AS team_abbr"]

    if section == 'current_stats':
        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {teams_tbl} t
        INNER JOIN {stats_tbl} s ON s.entity_id = t.id
        WHERE s.{season_col_name} = %s AND s.season_type = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type_val))
            rows = [dict(r) for r in cur.fetchall()]
    else:
        season_types = _season_types_for_section(section)
        season_filter, params = _build_season_filter(
            historical_config, current_season_year, season_col_name, ctx.season_format_fn
        )

        s_sums = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(ctx.team_stat_fields)]
        s_sums.append(f'COUNT(DISTINCT s.{season_col_name}) AS {season_col_name}')
        all_aggregates = t_select + s_sums + [f"t.{_quote_col(ctx.team_abbr_col)} AS team_abbr"]

        query = f"""
        SELECT {', '.join(all_aggregates)}
        FROM {stats_tbl} s
        INNER JOIN {teams_tbl} t ON s.entity_id = t.id
        WHERE s.season_type IN %s {season_filter}
        GROUP BY {', '.join(t_group)}
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (season_types, *params))
            rows = [dict(r) for r in cur.fetchall()]

    teams, opps = [], []
    for row in rows:
        if row.get('is_opponent') == 1 or row.get('is_opponent') is True:
            opps.append(row)
        else:
            teams.append(row)

    return {
        'teams': teams,
        'opponents': opps
    }
