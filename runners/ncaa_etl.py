"""
The Glass NCAA ETL - Orchestration Module

Entry points for the NCAA data pipeline:
  - Daily ETL: updates current season (2 API calls)
  - Roster sync: updates height/weight/jersey from roster endpoint
  - Backfill: loads historical seasons (2 API calls per season)
  - Schema: creates/updates database tables

Usage:
    python -m runners.ncaa_etl                           # Daily ETL (current season)
    python -m runners.ncaa_etl --backfill                 # Backfill all historical seasons
    python -m runners.ncaa_etl --backfill --season 2025   # Backfill single season
    python -m runners.ncaa_etl --roster                   # Sync roster data (height/weight/jersey)
    python -m runners.ncaa_etl --schema                   # Print schema DDL
    python -m runners.ncaa_etl --create-tables            # Create tables in DB
    python -m runners.ncaa_etl --status                   # Show what's loaded
"""
import os
import sys
import argparse
import logging
from typing import Optional
from psycopg2.extras import execute_values

from config.ncaa_etl import (
    NCAA_CONFIG, CBBD_ENDPOINTS, DB_COLUMNS, SEASON_TYPE_CONFIG,
    season_to_display, season_int_to_display, display_to_season_int,
    get_table_name,
)
from lib.ncaa_etl import (
    CBBDClient,
    db_connection, get_db_connection,
    extract_entity_data, extract_stats_data,
    upsert_entities, upsert_stats,
    generate_schema_ddl,
    get_season_player_count,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# SCHEMA MANAGEMENT
# ============================================================================

def create_tables():
    """Create NCAA tables in the database."""
    ddl = generate_schema_ddl()
    with db_connection() as conn:
        with conn.cursor() as cur:
            for statement in ddl.split(';'):
                statement = statement.strip()
                if statement:
                    cur.execute(statement + ';')
        conn.commit()
    logger.info("NCAA database tables created successfully")


def print_schema():
    """Print the generated DDL without executing."""
    print(generate_schema_ddl())


# ============================================================================
# ETL: TEAMS (entity data from /teams endpoint)
# ============================================================================

def sync_teams(client: CBBDClient, conn) -> int:
    """
    Sync all NCAA D1 teams into ncaa_teams.
    Called monthly or during initial setup.
    """
    logger.info("Syncing NCAA teams...")
    teams_data = client.fetch_teams_list()
    if not teams_data:
        logger.error("Failed to fetch teams list")
        return 0

    entity_data = {}
    for team in teams_data:
        team_id = team.get('id')
        if team_id is None:
            continue
        entity_data[team_id] = {
            'institution': team.get('school') or team.get('displayName'),
            'mascot': team.get('mascot'),
            'abbr': team.get('abbreviation'),
            'conference': team.get('conference'),
        }

    return upsert_entities(conn, 'team', entity_data)


# ============================================================================
# ETL: PLAYER & TEAM SEASON STATS
# ============================================================================

def sync_player_stats(client: CBBDClient, conn, season_int: int,
                      season_type_code: int = 1,
                      season_type_param: str = None,
                      d1_team_ids: set = None) -> int:
    """
    Sync player season stats for a given season.
    1 API call → ~9800 player records (filtered to D1 only).
    """
    display = season_int_to_display(season_int)
    st_label = 'postseason' if season_type_code == 2 else 'regular'
    logger.info(f"Syncing player stats for {display} ({st_label}, type={season_type_code})...")

    extra_params = {}
    if season_type_param:
        extra_params['seasonType'] = season_type_param

    records = client.fetch_endpoint('player_season_stats', season_int,
                                    extra_params=extra_params)
    if not records:
        logger.warning(f"No player stats for {display} ({st_label})")
        return 0

    # Extract entity data (update ncaa_players with name, team)
    player_entities = extract_entity_data(records, 'player', 'player_season_stats')

    for record in records:
        pid = record.get('athleteId')
        tid = record.get('teamId')
        if pid and tid and pid in player_entities:
            player_entities[pid]['team_id'] = tid

    # D1 filter: only upsert entities for D1 players
    if d1_team_ids:
        player_entities = {
            pid: data for pid, data in player_entities.items()
            if data.get('team_id') and int(data['team_id']) in d1_team_ids
        }

    upsert_entities(conn, 'player', player_entities)

    # Extract stats data (includes season display and season_type)
    stats_data = extract_stats_data(records, 'player', 'player_season_stats',
                                    season_int, season_type_code,
                                    d1_team_ids=d1_team_ids)

    return upsert_stats(conn, 'player', stats_data)


def sync_team_stats(client: CBBDClient, conn, season_int: int,
                    season_type_code: int = 1,
                    season_type_param: str = None) -> tuple:
    """
    Sync team season stats for a given season (D1 only).
    1 API call → ~700 team records, filtered to ~364 D1.
    Also updates team entity conference from the API data.

    Returns: (rows_upserted, d1_team_ids_set)
    """
    display = season_int_to_display(season_int)
    st_label = 'postseason' if season_type_code == 2 else 'regular'
    logger.info(f"Syncing team stats for {display} ({st_label}, type={season_type_code})...")

    extra_params = {}
    if season_type_param:
        extra_params['seasonType'] = season_type_param

    records = client.fetch_endpoint('team_season_stats', season_int,
                                    extra_params=extra_params)
    if not records:
        logger.warning(f"No team stats for {display} ({st_label})")
        return 0, set()

    # D1 filter: only teams with a conference
    d1_records = [r for r in records if r.get('conference')]
    d1_team_ids = {int(r['teamId']) for r in d1_records if r.get('teamId')}
    skipped = len(records) - len(d1_records)
    if skipped:
        logger.info(f"  Filtered out {skipped} non-D1 teams (no conference)")

    # Update team entity conference from team stats API data
    team_entities = {}
    for rec in d1_records:
        team_id = rec.get('teamId')
        conference = rec.get('conference')
        if team_id and conference:
            team_entities[team_id] = {'conference': conference}
    if team_entities:
        upsert_entities(conn, 'team', team_entities)

    stats_data = extract_stats_data(d1_records, 'team', 'team_season_stats',
                                    season_int, season_type_code)

    count = upsert_stats(conn, 'team', stats_data)
    return count, d1_team_ids


# ============================================================================
# ETL: ROSTER (height, weight, jersey from /teams/roster endpoint)
# ============================================================================

def sync_roster(client: CBBDClient, conn, season_int: int) -> int:
    """
    Sync roster data (height, weight, jersey, years_experience) for players in a given season.

    Fetches ALL rosters in a single API call (the roster endpoint returns
    every team when called with just a season parameter), then UPDATEs
    the ncaa.players entity table for players we already have.

    Cost: 1 API call per season.
    """
    display = season_int_to_display(season_int)
    logger.info(f"Syncing roster data for {display}...")

    entity_table = get_table_name('player', 'entity')

    # Fetch all rosters for this season in one call (no team_id filter)
    roster_data = client.fetch_endpoint('roster', season_int)
    if not roster_data:
        logger.warning(f"No roster data for {display}")
        return 0

    # roster_data is a list of team objects:
    #   [{teamId, team, players: [{id, height, weight, jersey, startSeason, ...}]}, ...]
    # Collect all rows first, then batch update via temp table
    # Use dict keyed by player_id to deduplicate (player can be on multiple rosters if transferred)
    player_map = {}
    for team_entry in roster_data:
        players = team_entry.get('players', [])
        for player in players:
            player_id = player.get('id')
            if player_id is None:
                continue

            height = player.get('height')
            weight = player.get('weight')
            jersey = player.get('jersey')
            start_season = player.get('startSeason')

            height_val = int(height) if height else None
            weight_val = int(weight) if weight else None
            try:
                jersey_val = int(jersey) if jersey is not None else None
            except (ValueError, TypeError):
                jersey_val = None
            years_exp = None
            if start_season is not None:
                ye = season_int - int(start_season)
                if ye >= 0:
                    years_exp = ye

            player_map[player_id] = (player_id, height_val, weight_val, jersey_val, years_exp)

    rows = list(player_map.values())

    if not rows:
        logger.warning(f"No roster rows to update for {display}")
        return 0

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS _tmp_roster")
        cur.execute("""
            CREATE TEMP TABLE _tmp_roster (
                player_id   INTEGER PRIMARY KEY,
                height_inches SMALLINT,
                weight_lbs    SMALLINT,
                jersey_number SMALLINT,
                years_experience SMALLINT
            )
        """)
        execute_values(
            cur,
            "INSERT INTO _tmp_roster (player_id, height_inches, weight_lbs, jersey_number, years_experience) VALUES %s",
            rows,
            page_size=1000
        )
        cur.execute(f"""
            UPDATE {entity_table} p
            SET height_inches    = COALESCE(t.height_inches, p.height_inches),
                weight_lbs       = COALESCE(t.weight_lbs, p.weight_lbs),
                jersey_number    = COALESCE(t.jersey_number, p.jersey_number),
                years_experience = COALESCE(t.years_experience, p.years_experience)
            FROM _tmp_roster t
            WHERE p.player_id = t.player_id
        """)
        count = cur.rowcount
        cur.execute("DROP TABLE IF EXISTS _tmp_roster")

    conn.commit()
    logger.info(f"Updated roster data for {count} players ({client.call_count} API calls)")
    return count


# ============================================================================
# ETL: COMPUTED COLUMNS (d_rebound_pct, player possessions)
# ============================================================================

def backfill_computed_columns(conn) -> dict:
    """
    Compute and fill derived columns that the API doesn't provide directly.

    1. d_rebound_pct_x1000  — for both player and team stats
       Team:   d_rebounds / (d_rebounds + opp_o_rebounds) * 1000
       Player: approximated using team opp_o_rebounds prorated by minutes

    2. opp_d_rebound_pct_x1000 — for team stats only
       opp_d_rebounds / (opp_d_rebounds + o_rebounds) * 1000

    3. possessions (player only)
       team_possessions * (player_minutes / team_minutes)

    Returns: {metric: rows_updated, ...}
    """
    pss = get_table_name('player', 'stats')
    tss = get_table_name('team', 'stats')
    results = {}

    with conn.cursor() as cur:
        # ── 1a. Team d_rebound_pct_x1000 ──
        cur.execute(f"""
            UPDATE {tss}
            SET d_rebound_pct_x1000 = ROUND(
                d_rebounds::numeric / NULLIF(d_rebounds + opp_o_rebounds, 0) * 1000
            )::integer
            WHERE d_rebound_pct_x1000 IS NULL
              AND d_rebounds IS NOT NULL
              AND opp_o_rebounds IS NOT NULL
        """)
        results['team_d_reb_pct'] = cur.rowcount
        logger.info(f"  Team d_rebound_pct_x1000: {cur.rowcount} rows")

        # ── 1b. Team opp_d_rebound_pct_x1000 ──
        cur.execute(f"""
            UPDATE {tss}
            SET opp_d_rebound_pct_x1000 = ROUND(
                opp_d_rebounds::numeric / NULLIF(opp_d_rebounds + o_rebounds, 0) * 1000
            )::integer
            WHERE opp_d_rebound_pct_x1000 IS NULL
              AND opp_d_rebounds IS NOT NULL
              AND o_rebounds IS NOT NULL
        """)
        results['team_opp_d_reb_pct'] = cur.rowcount
        logger.info(f"  Team opp_d_rebound_pct_x1000: {cur.rowcount} rows")

        # ── 1c. Player d_rebound_pct_x1000 ──
        # d_reb_pct ≈ player_d_reb / (player_d_reb + team_opp_o_reb * player_min / team_min)
        cur.execute(f"""
            UPDATE {pss} p
            SET d_rebound_pct_x1000 = ROUND(
                p.d_rebounds::numeric
                / NULLIF(
                    p.d_rebounds
                    + (t.opp_o_rebounds::numeric * p.minutes_x10 / NULLIF(t.minutes_x10, 0)),
                  0)
                * 1000
            )::integer
            FROM {tss} t
            WHERE p.d_rebound_pct_x1000 IS NULL
              AND p.team_id = t.team_id
              AND p.season = t.season
              AND p.season_type = t.season_type
              AND p.d_rebounds IS NOT NULL
              AND t.opp_o_rebounds IS NOT NULL
              AND p.minutes_x10 > 0
              AND t.minutes_x10 > 0
        """)
        results['player_d_reb_pct'] = cur.rowcount
        logger.info(f"  Player d_rebound_pct_x1000: {cur.rowcount} rows")

        # ── 2. Player possessions ──
        # possessions ≈ team_possessions * (player_minutes / team_minutes)
        cur.execute(f"""
            UPDATE {pss} p
            SET possessions = ROUND(
                t.possessions::numeric * p.minutes_x10 / NULLIF(t.minutes_x10, 0)
            )::smallint
            FROM {tss} t
            WHERE p.possessions IS NULL
              AND p.team_id = t.team_id
              AND p.season = t.season
              AND p.season_type = t.season_type
              AND t.possessions IS NOT NULL
              AND p.minutes_x10 > 0
              AND t.minutes_x10 > 0
        """)
        results['player_possessions'] = cur.rowcount
        logger.info(f"  Player possessions: {cur.rowcount} rows")

    conn.commit()
    return results


def backfill_player_team_id(client: CBBDClient, conn) -> int:
    """
    Add team_id to ncaa.player_season_stats (if missing) and populate it
    using the roster API for all backfill seasons.

    The roster API returns {teamId, players: [{id: player_id, ...}]} per team,
    giving us the correct team_id for each player per season.

    Cost: 1 API call per season = 8 calls total.
    """
    pss = get_table_name('player', 'stats')
    start_int = display_to_season_int(NCAA_CONFIG['backfill_start_season'])
    end_int = display_to_season_int(NCAA_CONFIG['backfill_end_season'])

    # Add team_id column if it doesn't exist
    with conn.cursor() as cur:
        cur.execute(f"""
            ALTER TABLE {pss}
            ADD COLUMN IF NOT EXISTS team_id INTEGER
        """)
    conn.commit()
    logger.info(f"Ensured team_id column exists on {pss}")

    total = 0
    for season_int in range(start_int, end_int + 1):
        season_display = season_int_to_display(season_int)
        roster_data = client.fetch_endpoint('roster', season_int)
        if not roster_data:
            logger.warning(f"  No roster data for {season_display}, skipping")
            continue

        # Build player_id → team_id mapping from this season's roster
        player_team_map = {}
        for team_entry in roster_data:
            team_id = team_entry.get('teamId')
            if not team_id:
                continue
            for player in team_entry.get('players', []):
                pid = player.get('id')
                if pid:
                    player_team_map[int(pid)] = int(team_id)

        if not player_team_map:
            logger.warning(f"  No player→team mappings for {season_display}")
            continue

        # Bulk UPDATE using a VALUES list
        season_updated = 0
        with conn.cursor() as cur:
            for player_id, team_id in player_team_map.items():
                cur.execute(f"""
                    UPDATE {pss}
                    SET team_id = %s
                    WHERE player_id = %s AND season = %s AND team_id IS NULL
                """, (team_id, player_id, season_display))
                season_updated += cur.rowcount

        conn.commit()
        total += season_updated
        logger.info(f"  {season_display}: populated team_id for {season_updated} player-rows")

    logger.info(f"backfill_player_team_id complete: {total} rows updated")
    return total


def backfill_team_conference(client: CBBDClient, conn) -> int:
    """
    Update ncaa.teams conference using roster API data.

    The roster endpoint returns conference per team. We iterate through
    the most recent season's roster response to fill conference on teams
    that are missing it.

    Cost: 1 API call (fetches all rosters for most recent season).
    """
    teams_table = get_table_name('team', 'entity')

    # Fetch roster for most recent season (has conference per team)
    current_season = NCAA_CONFIG['current_season_int']
    roster_data = client.fetch_endpoint('roster', current_season)
    if not roster_data:
        logger.warning("No roster data for conference backfill")
        return 0

    count = 0
    with conn.cursor() as cur:
        for team_entry in roster_data:
            team_id = team_entry.get('teamId')
            conference = team_entry.get('conference')
            if team_id and conference:
                cur.execute(f"""
                    UPDATE {teams_table}
                    SET conference = %s
                    WHERE team_id = %s
                      AND (conference IS NULL OR conference = '')
                """, (conference, team_id))
                if cur.rowcount > 0:
                    count += 1

    conn.commit()
    logger.info(f"Updated conference for {count} teams")
    return count


# ============================================================================
# ORCHESTRATION: DAILY ETL
# ============================================================================

def run_daily_etl():
    """
    Daily ETL for current NCAA season.

    Calls:
        1. Player season stats (1 API call)
        2. Team season stats (1 API call)
        Total: 2 API calls/day
    """
    season_int = NCAA_CONFIG['current_season_int']
    display = NCAA_CONFIG['current_season']
    logger.info(f"{'=' * 60}")
    logger.info(f"NCAA DAILY ETL — {display}")
    logger.info(f"{'=' * 60}")

    client = CBBDClient()
    conn = get_db_connection()

    try:
        # Ensure teams exist
        teams_table = get_table_name('team', 'entity')
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {teams_table}")
            team_count = cur.fetchone()[0]
        if team_count == 0:
            logger.info("No teams in DB — syncing teams first...")
            sync_teams(client, conn)

        # 1. Team stats first (to get D1 team IDs for player filter)
        team_count, d1_team_ids = sync_team_stats(client, conn, season_int)

        # 2. Player stats (D1 only)
        player_count = sync_player_stats(client, conn, season_int,
                                         d1_team_ids=d1_team_ids)

        logger.info(f"{'=' * 60}")
        logger.info(f"NCAA DAILY ETL COMPLETE")
        logger.info(f"  Players: {player_count}")
        logger.info(f"  Teams: {team_count}")
        logger.info(f"  API calls used: {client.call_count}")
        logger.info(f"{'=' * 60}")

    except Exception as e:
        logger.error(f"Daily ETL failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()


# ============================================================================
# ORCHESTRATION: ROSTER SYNC
# ============================================================================

def run_roster_sync(target_season: Optional[int] = None):
    """
    Sync roster data (height/weight/jersey) for one or all backfill seasons.

    If target_season is specified, syncs only that season (1 API call).
    Otherwise syncs all backfill seasons (1 API call per season = 8 calls).
    """
    client = CBBDClient()
    conn = get_db_connection()

    try:
        if target_season:
            season_ints = [target_season]
        else:
            start_int = display_to_season_int(NCAA_CONFIG['backfill_start_season'])
            end_int = display_to_season_int(NCAA_CONFIG['backfill_end_season'])
            season_ints = list(range(start_int, end_int + 1))

        logger.info(f"{'=' * 60}")
        logger.info(f"NCAA ROSTER SYNC — {len(season_ints)} seasons "
                     f"(~{len(season_ints)} API calls)")
        logger.info(f"{'=' * 60}")

        total_updated = 0
        for season_int in season_ints:
            count = sync_roster(client, conn, season_int)
            total_updated += count

        logger.info(f"{'=' * 60}")
        logger.info(f"Roster sync complete: {total_updated} players updated")
        logger.info(f"API calls used: {client.call_count}")
        logger.info(f"{'=' * 60}")
    except Exception as e:
        logger.error(f"Roster sync failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()


# ============================================================================
# ORCHESTRATION: BACKFILL
# ============================================================================

def run_backfill(target_season: Optional[int] = None):
    """
    Backfill historical seasons (D1 only, regular season + postseason).

    If target_season is specified, backfills only that season (as int, e.g., 2025).
    Otherwise backfills all seasons from backfill_start_season to backfill_end_season.

    Per-season cost: 4 calls (team regular + player regular + team post + player post)
    Full backfill (8 seasons): 32 calls
    """
    if target_season:
        start_int = target_season
        end_int = target_season
    else:
        start_int = display_to_season_int(NCAA_CONFIG['backfill_start_season'])
        end_int = display_to_season_int(NCAA_CONFIG['backfill_end_season'])

    season_ints = list(range(start_int, end_int + 1))
    total_calls = len(season_ints) * 4  # team + player × (regular + postseason)
    logger.info(f"{'=' * 60}")
    logger.info(f"NCAA BACKFILL — {len(season_ints)} seasons "
                f"({season_int_to_display(start_int)} to {season_int_to_display(end_int)})")
    logger.info(f"Estimated API calls: {total_calls}")
    logger.info(f"{'=' * 60}")

    client = CBBDClient()
    conn = get_db_connection()

    try:
        # Ensure teams exist
        teams_table = get_table_name('team', 'entity')
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {teams_table}")
            team_count = cur.fetchone()[0]
        if team_count == 0:
            logger.info("Syncing teams first...")
            sync_teams(client, conn)

        for season_int in season_ints:
            display = season_int_to_display(season_int)
            logger.info(f"\n{'—' * 40}")
            logger.info(f"Backfilling {display} (season={season_int})")
            logger.info(f"{'—' * 40}")

            existing_count = get_season_player_count(conn, display)
            if existing_count > 0:
                logger.info(f"  Season {display} already has {existing_count} player records — updating...")

            # Regular season: team stats first (get D1 IDs), then player stats
            t_count, d1_team_ids = sync_team_stats(client, conn, season_int,
                                                    season_type_code=1)
            logger.info(f"  Regular — Teams: {t_count}")

            p_count = sync_player_stats(client, conn, season_int,
                                        season_type_code=1,
                                        d1_team_ids=d1_team_ids)
            logger.info(f"  Regular — Players: {p_count}")

            # Postseason: same D1 filter
            pt_count, _ = sync_team_stats(client, conn, season_int,
                                          season_type_code=2,
                                          season_type_param='postseason')
            logger.info(f"  Postseason — Teams: {pt_count}")

            pp_count = sync_player_stats(client, conn, season_int,
                                         season_type_code=2,
                                         season_type_param='postseason',
                                         d1_team_ids=d1_team_ids)
            logger.info(f"  Postseason — Players: {pp_count}")

            logger.info(f"  API calls so far: {client.call_count}")

    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()
        logger.info(f"\nTotal API calls used: {client.call_count}")


# ============================================================================
# STATUS REPORTING
# ============================================================================

def show_status():
    """Show what data is currently loaded."""
    teams_table = get_table_name('team', 'entity')
    players_table = get_table_name('player', 'entity')
    pss_table = get_table_name('player', 'stats')
    tss_table = get_table_name('team', 'stats')

    conn = get_db_connection()
    try:
        print(f"\n{'=' * 60}")
        print("NCAA DATA STATUS")
        print(f"{'=' * 60}")

        with conn.cursor() as cur:
            for table in [teams_table, players_table, pss_table, tss_table]:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    print(f"  {table}: {count:,} rows")
                except Exception:
                    print(f"  {table}: TABLE NOT FOUND")
                    conn.rollback()

        print(f"\nLoaded seasons:")
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT season, COUNT(DISTINCT player_id) as players,
                           MIN(games) as min_gp, MAX(games) as max_gp
                    FROM {pss_table}
                    WHERE season_type = 1
                    GROUP BY season ORDER BY season
                """)
                for row in cur.fetchall():
                    print(f"  {row[0]}: {row[1]:,} players (GP range: {row[2]}-{row[3]})")
        except Exception:
            print("  (no season data)")
            conn.rollback()

        # Roster coverage
        print(f"\nRoster coverage (height/weight):")
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT s.season,
                           COUNT(*) as total,
                           COUNT(p.height_inches) as has_height,
                           COUNT(p.weight_lbs) as has_weight
                    FROM {pss_table} s
                    JOIN {players_table} p ON s.player_id = p.player_id
                    WHERE s.season_type = 1
                    GROUP BY s.season ORDER BY s.season
                """)
                for row in cur.fetchall():
                    pct = (row[2] / row[1] * 100) if row[1] > 0 else 0
                    print(f"  {row[0]}: {row[2]:,}/{row[1]:,} have height ({pct:.0f}%)")
        except Exception:
            print("  (no roster data)")
            conn.rollback()

        print()
    finally:
        conn.close()


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='The Glass NCAA ETL')
    parser.add_argument('--backfill', action='store_true', help='Backfill historical seasons')
    parser.add_argument('--season', type=int, help='Target season (e.g., 2025)')
    parser.add_argument('--roster', action='store_true', help='Sync roster data (height/weight/jersey)')
    parser.add_argument('--compute', action='store_true', help='Compute derived columns (d_reb_pct, possessions)')
    parser.add_argument('--schema', action='store_true', help='Print schema DDL')
    parser.add_argument('--create-tables', action='store_true', help='Create tables in database')
    parser.add_argument('--status', action='store_true', help='Show data status')
    parser.add_argument('--sync-teams', action='store_true', help='Sync teams list')

    args = parser.parse_args()

    if args.schema:
        print_schema()
    elif args.create_tables:
        create_tables()
    elif args.status:
        show_status()
    elif args.sync_teams:
        client = CBBDClient()
        with db_connection() as conn:
            sync_teams(client, conn)
    elif args.backfill:
        run_backfill(target_season=args.season)
    elif args.roster:
        run_roster_sync(target_season=args.season)
    elif args.compute:
        logger.info(f"{'=' * 60}")
        logger.info("COMPUTING DERIVED COLUMNS")
        logger.info(f"{'=' * 60}")
        client = CBBDClient()
        conn = get_db_connection()
        try:
            logger.info("Step 1/2: Backfilling player team_id from roster API...")
            backfill_player_team_id(client, conn)
            logger.info("Step 2/2: Computing derived columns (d_reb_pct, possessions)...")
            results = backfill_computed_columns(conn)
            logger.info(f"{'=' * 60}")
            logger.info("Computed columns complete:")
            for k, v in results.items():
                logger.info(f"  {k}: {v:,} rows")
            logger.info(f"{'=' * 60}")
        finally:
            conn.close()
    else:
        run_daily_etl()


if __name__ == '__main__':
    main()
