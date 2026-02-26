"""
THE GLASS - Google Sheets Sync
Thin orchestrator: Google Sheets API calls + sync loop.
All data, formulas, and calculations live in lib/sheets.py.

Entry point for sync_sheets.sh:
    python src/sheets.py [--team BOS] [--mode per_game|per_36|per_100|totals]

Architecture:
    config/sheets.py  -- pure data (SHEETS_COLUMNS, SECTIONS, SECTION_CONFIG, SHEET_FORMATTING)
    lib/sheets.py     -- all functions (DB queries, formula eval, row building, formatting)
    src/sheets.py     -- this file: Google Sheets API + sync orchestration
"""

import time
import logging
import argparse
import os

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import gspread

from config.etl import NBA_CONFIG
from config.sheets import GOOGLE_SHEETS_CONFIG, STAT_CONSTANTS, SHEET_FORMATTING
from lib.etl import get_teams_from_db
from lib.sheets import (
    get_db_connection,
    fetch_players_for_team,
    fetch_all_players,
    fetch_team_stats,
    fetch_all_teams,
    build_sheet_columns,
    build_headers,
    build_entity_row,
    build_merged_entity_row,
    build_formatting_requests,
    calculate_all_percentiles,
    format_years_range,
    format_section_header,
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# GOOGLE SHEETS CLIENT
# ============================================================================

def get_sheets_client():
    """Initialise Google Sheets API client from service-account credentials."""
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CONFIG['credentials_file'],
        scopes=GOOGLE_SHEETS_CONFIG['scopes'],
    )
    return gspread.authorize(creds)


def _get_or_create_worksheet(spreadsheet, title: str, rows: int = 200, cols: int = 200):
    """Get existing worksheet or create a new one, clearing if it exists."""
    try:
        ws = spreadsheet.worksheet(title)
        ws.clear()
        return ws
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


# ============================================================================
# FORMATTING
# ============================================================================

def _apply_sheet_formatting(worksheet, columns_list, header_merges: list,
                            n_data_rows: int, team_name: str,
                            percentile_cells: list, n_player_rows: int):
    """
    Apply ALL Google Sheets formatting via a single batch API request.
    Delegates entirely to lib/sheets.build_formatting_requests (config-driven).
    """
    requests = build_formatting_requests(
        ws_id=worksheet.id,
        columns_list=columns_list,
        header_merges=header_merges,
        n_data_rows=n_data_rows,
        team_name=team_name,
        percentile_cells=percentile_cells,
        n_player_rows=n_player_rows,
    )
    if requests:
        worksheet.spreadsheet.batch_update({'requests': requests})


# ============================================================================
# TEAM SHEET SYNC
# ============================================================================

def sync_team_sheet(client, spreadsheet, team_abbr: str,
                    team_name: str = '',
                    mode: str = 'per_100',
                    show_percentiles: bool = False,
                    historical_config: dict = None,
                    postseason_config: dict = None):
    """
    Sync a single team's worksheet with merged row layout.

    Sheet layout (all config-driven via SHEET_FORMATTING, 4 header rows):
        Row 1            : Section headers (team name merged into entities section)
        Row 2            : Subsection headers (hidden by default)
        Row 3            : Column names
        Row 4            : Filter row (auto-filter — excludes team/opp rows)
        Rows 5..N        : Player data — ONE row per player with current +
                           historical + postseason stats side-by-side
        Last 2 rows      : Team row + Opponents row (outside filter range)

    All formatting (colors, fonts, borders, percentile shading, column
    visibility, widths, banding) applied via batch_update from config.
    """
    logger.info(f'  Syncing {team_abbr}...')
    fmt = SHEET_FORMATTING
    current_year = NBA_CONFIG['current_season_year']
    display_name = team_name or team_abbr
    worksheet = _get_or_create_worksheet(spreadsheet, team_abbr)

    conn = get_db_connection()
    try:
        # ---- Fetch raw data ----
        current_players = fetch_players_for_team(conn, team_abbr, 'current_stats')
        hist_players = fetch_players_for_team(conn, team_abbr, 'historical_stats', historical_config)
        post_players = fetch_players_for_team(conn, team_abbr, 'postseason_stats', postseason_config)
        team_data = fetch_team_stats(conn, team_abbr, 'current_stats')

        # ---- Percentile populations (league-wide) ----
        all_players_curr = fetch_all_players(conn, 'current_stats')
        all_players_hist = fetch_all_players(conn, 'historical_stats', historical_config)
        all_players_post = fetch_all_players(conn, 'postseason_stats', postseason_config)
        all_teams = fetch_all_teams(conn, 'current_stats')

        pct_curr_p = calculate_all_percentiles(all_players_curr, 'player', mode)
        pct_hist_p = calculate_all_percentiles(all_players_hist, 'player', mode)
        pct_post_p = calculate_all_percentiles(all_players_post, 'player', mode)
        pct_team = calculate_all_percentiles(all_teams['teams'], 'team', mode)
        pct_opp = calculate_all_percentiles(all_teams['opponents'], 'opponents', mode)

        # ---- Column structure ----
        columns = build_sheet_columns(entity='player', stat_mode='both',
                                      show_percentiles=show_percentiles,
                                      sheet_type='team')

        # ---- Timeframe display strings for section headers ----
        headers = build_headers(columns, mode=mode,
                                team_name=display_name,
                                current_year=current_year,
                                historical_config=historical_config,
                                postseason_config=postseason_config)
        n_cols = len(columns)

        # ---- Index players by player_id for merging ----
        curr_by_id = {p.get('player_id'): p for p in current_players}
        hist_by_id = {p.get('player_id'): p for p in hist_players}
        post_by_id = {p.get('player_id'): p for p in post_players}

        # Union all player IDs, maintain current_players order first
        all_player_ids = []
        seen = set()
        for p in current_players:
            pid = p.get('player_id')
            if pid and pid not in seen:
                all_player_ids.append(pid)
                seen.add(pid)
        for p in hist_players + post_players:
            pid = p.get('player_id')
            if pid and pid not in seen:
                all_player_ids.append(pid)
                seen.add(pid)

        # ---- Build merged player rows ----
        data_rows = []
        all_percentile_cells = []

        for pid in all_player_ids:
            row, pct_cells = build_merged_entity_row(
                player_id=pid,
                columns_list=columns,
                current_data=curr_by_id.get(pid),
                historical_data=hist_by_id.get(pid),
                postseason_data=post_by_id.get(pid),
                pct_curr=pct_curr_p,
                pct_hist=pct_hist_p,
                pct_post=pct_post_p,
                entity_type='player',
                mode=mode,
            )
            # Set row index for percentile cells (data_start + row position)
            for cell in pct_cells:
                cell['row'] = fmt['data_start_row'] + len(data_rows)
            all_percentile_cells.extend(pct_cells)
            data_rows.append(row)

        n_player_rows = len(data_rows)

        # ---- Team + Opponents rows (current stats only) ----
        team_row = build_entity_row(
            team_data.get('team', {}), columns, pct_team,
            entity_type='team', mode=mode,
            years_str='', row_section='current_stats',
        )
        opp_row = build_entity_row(
            team_data.get('opponent', {}), columns, pct_opp,
            entity_type='opponents', mode=mode,
            years_str='', row_section='current_stats',
        )

        # Percentile cells for team/opp rows
        for entity_data, pcts, etype in [
            (team_data.get('team', {}), pct_team, 'team'),
            (team_data.get('opponent', {}), pct_opp, 'opponents'),
        ]:
            from lib.sheets import calculate_entity_stats, get_percentile_rank, SECTION_CONFIG, SHEETS_COLUMNS, _get_minute_weight
            calculated = calculate_entity_stats(entity_data, etype, mode)
            row_offset = fmt['data_start_row'] + len(data_rows) + (0 if etype == 'team' else 1)
            for col_idx, entry in enumerate(columns):
                col_key, col_def = entry[0], entry[1]
                col_ctx = entry[3] if len(entry) > 3 else None
                if not col_def.get('has_percentile', False):
                    continue
                col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
                if not col_ctx_cfg.get('is_stats_section') or col_ctx != 'current_stats':
                    continue
                value = calculated.get(col_key)
                if value is not None and col_key in pcts:
                    reverse = col_def.get('reverse_percentile', False)
                    weight = _get_minute_weight(col_key, entity_data)
                    rank = get_percentile_rank(value, pcts[col_key], reverse, weight)
                    all_percentile_cells.append({
                        'row': row_offset,
                        'col': col_idx,
                        'percentile': rank,
                        'reverse': reverse,
                    })

        data_rows.append(team_row)
        data_rows.append(opp_row)

        # ---- Assemble all rows (4 header rows + data) ----
        filter_row = [''] * n_cols
        all_rows = [headers['row1'], headers['row2'], headers['row3'],
                     filter_row] + data_rows

        # Pad rows to full width
        all_rows = [r + [''] * (n_cols - len(r)) for r in all_rows]

        # ---- Resize worksheet to exact dimensions before writing ----
        total_rows = len(all_rows)
        worksheet.resize(rows=total_rows, cols=n_cols)

        # ---- Write values ----
        worksheet.update(range_name='A1', values=all_rows, value_input_option='USER_ENTERED')

        # ---- Apply formatting ----
        _apply_sheet_formatting(
            worksheet, columns,
            header_merges=headers['merges'],
            n_data_rows=len(data_rows),
            team_name=display_name,
            percentile_cells=all_percentile_cells,
            n_player_rows=n_player_rows,
        )

        logger.info(
            f'  {team_abbr} done: {len(all_player_ids)} players (merged), '
            f'{len(all_percentile_cells)} percentile cells'
        )

    finally:
        conn.close()


# ============================================================================
# FULL LEAGUE SYNC
# ============================================================================

def sync_all_teams(mode: str = 'per_100', show_percentiles: bool = False,
                   historical_config: dict = None, postseason_config: dict = None,
                   priority_team: str = None):
    """Sync every team worksheet. priority_team is synced first if given."""
    logger.info('Starting full league sync...')
    delay = SHEET_FORMATTING.get('sync_delay_seconds', 3)

    client = get_sheets_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEETS_CONFIG['spreadsheet_id'])

    teams = get_teams_from_db()           # {team_id: (abbr, name)}
    # Build abbr → full name lookup
    team_names = {abbr: name for _, (abbr, name) in teams.items()}
    abbrs = sorted(team_names.keys())

    if priority_team:
        pt = priority_team.upper()
        if pt in abbrs:
            abbrs = [pt] + [a for a in abbrs if a != pt]

    for i, abbr in enumerate(abbrs):
        try:
            sync_team_sheet(
                client, spreadsheet, abbr,
                team_name=team_names.get(abbr, abbr),
                mode=mode,
                show_percentiles=show_percentiles,
                historical_config=historical_config,
                postseason_config=postseason_config,
            )
        except Exception as exc:
            logger.error(f'  {abbr} failed: {exc}', exc_info=True)

        # Rate limit — avoid Google Sheets 429
        if i < len(abbrs) - 1:
            logger.info(f'  Rate limit pause ({delay}s)...')
            time.sleep(delay)

    logger.info('Full sync complete.')


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Sync NBA data to Google Sheets')
    parser.add_argument('--team', metavar='ABBR',
                        help='Sync this team first (e.g. BOS)')
    parser.add_argument('--mode',
                        choices=['per_game', 'per_36', 'per_100', 'totals'],
                        default=None,
                        help='Stats display mode (default: per_100)')
    parser.add_argument('--percentiles', action='store_true',
                        help='Show percentile rank columns')
    parser.add_argument('--hist-years', type=int, default=None,
                        help='Past seasons for historical stats (default: 3)')
    parser.add_argument('--post-years', type=int, default=None,
                        help='Past seasons for postseason stats (default: 3)')
    args = parser.parse_args()

    # Environment variables (set by API subprocess) override CLI defaults.
    # Priority: CLI arg > env var > hardcoded default
    mode = args.mode or os.environ.get('STATS_MODE', 'per_100')
    show_percentiles = args.percentiles or os.environ.get('TOGGLE_PERCENTILES') == 'true'
    priority_team = args.team or os.environ.get('PRIORITY_TEAM_ABBR')

    # Historical timeframe — env vars from API or CLI defaults
    hist_mode = os.environ.get('HISTORICAL_MODE', 'years')
    include_current = os.environ.get('INCLUDE_CURRENT_YEAR', 'false') == 'true'

    if hist_mode == 'career':
        historical_config = {'mode': 'career'}
    elif hist_mode == 'seasons':
        season_str = os.environ.get('HISTORICAL_SEASONS', '')
        seasons = [s.strip() for s in season_str.split(',') if s.strip()]
        historical_config = {'mode': 'seasons', 'value': seasons, 'include_current': include_current}
    else:
        hist_years = args.hist_years or int(os.environ.get('HISTORICAL_YEARS', '3'))
        historical_config = {'mode': 'years', 'value': hist_years, 'include_current': include_current}

    # Postseason timeframe — same as historical unless SYNC_SECTION limits scope
    sync_section = os.environ.get('SYNC_SECTION')
    season_type_env = os.environ.get('SEASON_TYPE')

    if hist_mode == 'career':
        postseason_config = {'mode': 'career'}
    elif hist_mode == 'seasons':
        season_str = os.environ.get('HISTORICAL_SEASONS', '')
        seasons = [s.strip() for s in season_str.split(',') if s.strip()]
        postseason_config = {'mode': 'seasons', 'value': seasons, 'include_current': include_current}
    else:
        post_years = args.post_years or int(os.environ.get('HISTORICAL_YEARS', '3'))
        postseason_config = {'mode': 'years', 'value': post_years, 'include_current': include_current}

    sync_all_teams(
        mode=mode,
        show_percentiles=show_percentiles,
        historical_config=historical_config,
        postseason_config=postseason_config,
        priority_team=priority_team,
    )


if __name__ == '__main__':
    main()
