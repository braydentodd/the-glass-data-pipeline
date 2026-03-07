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
    build_merged_entity_row,
    build_formatting_requests,
    calculate_all_percentiles,
    format_years_range,
    format_section_header,
    build_summary_rows,
    _eval_dynamic_formula,
    get_percentile_rank,
    SUMMARY_THRESHOLDS,
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
                            percentile_cells: list, n_player_rows: int,
                            sheet_type: str = 'team',
                            show_advanced: bool = False,
                            show_percentiles: bool = False):
    """
    Apply ALL Google Sheets formatting via batch API requests.
    Delegates entirely to lib/sheets.build_formatting_requests (config-driven).

    Removes any pre-existing banded ranges on the worksheet first so that
    addBanding does not collide with stale banding from a previous sync
    (ws.clear() removes cell values but NOT banding properties).

    For large sheets (500+ players), requests are chunked to stay under
    the Google Sheets API ~10 MB request size limit.
    """
    # --- Remove existing banded ranges (survive ws.clear()) --------------
    meta = worksheet.spreadsheet.fetch_sheet_metadata(
        params={'fields': 'sheets(properties.sheetId,bandedRanges)'}
    )
    delete_requests = []
    for sheet in meta.get('sheets', []):
        if sheet.get('properties', {}).get('sheetId') == worksheet.id:
            for br in sheet.get('bandedRanges', []):
                delete_requests.append({
                    'deleteBanding': {'bandedRangeId': br['bandedRangeId']}
                })
            break

    requests = build_formatting_requests(
        ws_id=worksheet.id,
        columns_list=columns_list,
        header_merges=header_merges,
        n_data_rows=n_data_rows,
        team_name=team_name,
        percentile_cells=percentile_cells,
        n_player_rows=n_player_rows,
        sheet_type=sheet_type,
        show_advanced=show_advanced,
        show_percentiles=show_percentiles,
    )
    # Prepend deleteBanding so old banding is removed before new is added
    all_requests = delete_requests + requests
    if not all_requests:
        return

    # Google Sheets API has a ~10 MB request body limit.
    # For large sheets (500+ players), percentile shading alone can
    # generate 50K+ requests.  Chunk to stay well under the limit.
    CHUNK_SIZE = 5000
    for i in range(0, len(all_requests), CHUNK_SIZE):
        chunk = all_requests[i:i + CHUNK_SIZE]
        if chunk:
            worksheet.spreadsheet.batch_update({'requests': chunk})


# ============================================================================
# TEAM SHEET SYNC
# ============================================================================

def sync_team_sheet(client, spreadsheet, team_abbr: str,
                    team_name: str = '',
                    mode: str = 'per_100',
                    show_percentiles: bool = False,
                    show_advanced: bool = False,
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
        team_data_curr = fetch_team_stats(conn, team_abbr, 'current_stats')
        team_data_hist = fetch_team_stats(conn, team_abbr, 'historical_stats', historical_config)
        team_data_post = fetch_team_stats(conn, team_abbr, 'postseason_stats', postseason_config)

        # ---- Compute team row player averages for info columns ----
        # Minute-weighted averages: players with more minutes influence the average more.
        # Players with null values or 0 minutes are excluded.
        avg_fields = ['years_experience', 'age', 'height_inches', 'weight_lbs', 'wingspan_inches']
        if current_players:
            for field in avg_fields:
                weighted_sum = 0.0
                weight_sum = 0.0
                for p in current_players:
                    val = p.get(field)
                    if val is None:
                        continue
                    minutes = (p.get('minutes_x10', 0) or 0) / 10.0
                    if minutes <= 0:
                        continue
                    weighted_sum += val * minutes
                    weight_sum += minutes
                if weight_sum > 0:
                    avg_val = round(weighted_sum / weight_sum, 1)
                    for td in [team_data_curr, team_data_hist, team_data_post]:
                        if td.get('team'):
                            td['team'][field] = avg_val

        # ---- Percentile populations (league-wide) ----
        all_players_curr = fetch_all_players(conn, 'current_stats')
        all_players_hist = fetch_all_players(conn, 'historical_stats', historical_config)
        all_players_post = fetch_all_players(conn, 'postseason_stats', postseason_config)
        all_teams_curr = fetch_all_teams(conn, 'current_stats')
        all_teams_hist = fetch_all_teams(conn, 'historical_stats', historical_config)
        all_teams_post = fetch_all_teams(conn, 'postseason_stats', postseason_config)

        pct_curr_p = calculate_all_percentiles(all_players_curr, 'player', mode)
        pct_hist_p = calculate_all_percentiles(all_players_hist, 'player', mode)
        pct_post_p = calculate_all_percentiles(all_players_post, 'player', mode)
        pct_team_curr = calculate_all_percentiles(all_teams_curr['teams'], 'team', mode)
        pct_opp_curr = calculate_all_percentiles(all_teams_curr['opponents'], 'opponents', mode)
        pct_team_hist = calculate_all_percentiles(all_teams_hist['teams'], 'team', mode)
        pct_opp_hist = calculate_all_percentiles(all_teams_hist['opponents'], 'opponents', mode)
        pct_team_post = calculate_all_percentiles(all_teams_post['teams'], 'team', mode)
        pct_opp_post = calculate_all_percentiles(all_teams_post['opponents'], 'opponents', mode)

        # ---- Enrich all teams with minute-weighted player info averages ----
        # Needed so team-level percentile populations are correct for info
        # columns (experience, age, height, weight, wingspan).
        # Without this, team populations lack these fields → all = 0 → 100%ile.
        from collections import defaultdict
        player_groups_by_team = defaultdict(list)
        for p in all_players_curr:
            ta = p.get('team_abbr')
            if ta:
                player_groups_by_team[ta].append(p)

        for team_d in all_teams_curr['teams']:
            ta = team_d.get('team_abbr')
            if not ta:
                continue
            tp = player_groups_by_team.get(ta, [])
            for field in avg_fields:
                wsum, wweight = 0.0, 0.0
                for p in tp:
                    val = p.get(field)
                    if val is None:
                        continue
                    mins = (p.get('minutes_x10', 0) or 0) / 10.0
                    if mins <= 0:
                        continue
                    wsum += val * mins
                    wweight += mins
                if wweight > 0:
                    team_d[field] = round(wsum / wweight, 1)

        # Recalculate team percentiles with enriched info data
        pct_team_curr = calculate_all_percentiles(all_teams_curr['teams'], 'team', mode)

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

        # ---- Team + Opponents rows (merged across all sections) ----
        team_row, team_pct_cells = build_merged_entity_row(
            player_id=None,
            columns_list=columns,
            current_data=team_data_curr.get('team') or None,
            historical_data=team_data_hist.get('team') or None,
            postseason_data=team_data_post.get('team') or None,
            pct_curr=pct_team_curr,
            pct_hist=pct_team_hist,
            pct_post=pct_team_post,
            entity_type='team',
            mode=mode,
        )
        opp_row, opp_pct_cells = build_merged_entity_row(
            player_id=None,
            columns_list=columns,
            current_data=team_data_curr.get('opponent') or None,
            historical_data=team_data_hist.get('opponent') or None,
            postseason_data=team_data_post.get('opponent') or None,
            pct_curr=pct_opp_curr,
            pct_hist=pct_opp_hist,
            pct_post=pct_opp_post,
            entity_type='opponents',
            mode=mode,
        )

        # Set row indices for team/opp percentile cells
        team_row_idx = fmt['data_start_row'] + len(data_rows)
        opp_row_idx = team_row_idx + 1
        for cell in team_pct_cells:
            cell['row'] = team_row_idx
        for cell in opp_pct_cells:
            cell['row'] = opp_row_idx
        all_percentile_cells.extend(team_pct_cells)
        all_percentile_cells.extend(opp_pct_cells)

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
            sheet_type='team',
            show_advanced=show_advanced,
            show_percentiles=show_percentiles,
        )

        logger.info(
            f'  {team_abbr} done: {len(all_player_ids)} players (merged), '
            f'{len(all_percentile_cells)} percentile cells'
        )

    finally:
        conn.close()


# ============================================================================
# PLAYERS SHEET SYNC (ALL PLAYERS, NO TEAM/OPP AGGREGATE ROWS)
# ============================================================================

def sync_players_sheet(client, spreadsheet, mode: str = 'per_100',
                       show_percentiles: bool = False,
                       show_advanced: bool = False,
                       historical_config: dict = None,
                       postseason_config: dict = None):
    """
    Sync the league-wide Players sheet.

    Same layout as team sheets but:
    - Contains ALL players from every team
    - No team/opponent aggregate rows at bottom
    - Team column is visible (shows each player's team)
    - Placed as the first sheet in the workbook
    """
    logger.info('  Syncing Players sheet...')
    fmt = SHEET_FORMATTING
    current_year = NBA_CONFIG['current_season_year']
    worksheet = _get_or_create_worksheet(spreadsheet, 'Players')

    conn = get_db_connection()
    try:
        # ---- Fetch all players league-wide ----
        all_players_curr = fetch_all_players(conn, 'current_stats')
        all_players_hist = fetch_all_players(conn, 'historical_stats', historical_config)
        all_players_post = fetch_all_players(conn, 'postseason_stats', postseason_config)

        # ---- Percentile populations ----
        pct_curr = calculate_all_percentiles(all_players_curr, 'player', mode)
        pct_hist = calculate_all_percentiles(all_players_hist, 'player', mode)
        pct_post = calculate_all_percentiles(all_players_post, 'player', mode)

        # ---- Column structure (players sheet type keeps team column visible) ----
        columns = build_sheet_columns(entity='player', stat_mode='both',
                                      show_percentiles=show_percentiles,
                                      sheet_type='players')

        # ---- Headers ----
        headers = build_headers(columns, mode=mode,
                                team_name='Players',
                                current_year=current_year,
                                historical_config=historical_config,
                                postseason_config=postseason_config)
        n_cols = len(columns)

        # ---- Index players by player_id ----
        curr_by_id = {p.get('player_id'): p for p in all_players_curr}
        hist_by_id = {p.get('player_id'): p for p in all_players_hist}
        post_by_id = {p.get('player_id'): p for p in all_players_post}

        # ---- Unique player IDs sorted by current-season minutes ----
        all_player_ids = []
        seen = set()
        for p in sorted(all_players_curr,
                        key=lambda x: x.get('minutes_x10', 0) or 0,
                        reverse=True):
            pid = p.get('player_id')
            if pid and pid not in seen:
                all_player_ids.append(pid)
                seen.add(pid)
        for p in all_players_hist + all_players_post:
            pid = p.get('player_id')
            if pid and pid not in seen:
                all_player_ids.append(pid)
                seen.add(pid)

        # ---- Build player rows ----
        data_rows = []
        all_percentile_cells = []

        for pid in all_player_ids:
            row, pct_cells = build_merged_entity_row(
                player_id=pid,
                columns_list=columns,
                current_data=curr_by_id.get(pid),
                historical_data=hist_by_id.get(pid),
                postseason_data=post_by_id.get(pid),
                pct_curr=pct_curr,
                pct_hist=pct_hist,
                pct_post=pct_post,
                entity_type='player',
                mode=mode,
            )
            for cell in pct_cells:
                cell['row'] = fmt['data_start_row'] + len(data_rows)
            all_percentile_cells.extend(pct_cells)
            data_rows.append(row)

        n_player_rows = len(data_rows)

        # ---- Summary rows (Best, 75th, Average, 25th, Worst) ----
        merged_pops = {}
        for k, v in pct_curr.items():
            merged_pops[f'current_stats:{k}'] = v
        for k, v in pct_hist.items():
            merged_pops[f'historical_stats:{k}'] = v
        for k, v in pct_post.items():
            merged_pops[f'postseason_stats:{k}'] = v
        summary_rows, summary_pct = build_summary_rows(columns, merged_pops, mode)
        summary_start = fmt['data_start_row'] + n_player_rows
        for cell in summary_pct:
            cell['row'] = summary_start + cell.pop('row_offset')
        all_percentile_cells.extend(summary_pct)
        data_rows.extend(summary_rows)

        # ---- Assemble rows ----
        filter_row = [''] * n_cols
        all_rows = [headers['row1'], headers['row2'], headers['row3'],
                     filter_row] + data_rows
        all_rows = [r + [''] * (n_cols - len(r)) for r in all_rows]

        total_rows = len(all_rows)
        worksheet.resize(rows=total_rows, cols=n_cols)
        worksheet.update(range_name='A1', values=all_rows,
                         value_input_option='USER_ENTERED')

        # ---- Apply formatting (players sheet type: team column stays visible) ----
        _apply_sheet_formatting(
            worksheet, columns,
            header_merges=headers['merges'],
            n_data_rows=n_player_rows,
            team_name='Players',
            percentile_cells=all_percentile_cells,
            n_player_rows=n_player_rows,
            sheet_type='players',
            show_advanced=show_advanced,
            show_percentiles=show_percentiles,
        )

        # ---- Move Players sheet to first position ----
        try:
            worksheet.spreadsheet.batch_update({'requests': [{
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': worksheet.id,
                        'index': 0,
                    },
                    'fields': 'index',
                }
            }]})
        except Exception as e:
            logger.warning(f'  Could not move Players sheet to first position: {e}')

        logger.info(
            f'  Players sheet done: {n_player_rows} players, '
            f'{len(all_percentile_cells)} percentile cells'
        )
    finally:
        conn.close()


# ============================================================================
# TEAMS SHEET SYNC (ALL TEAMS, WITH OPPONENT SUBSECTIONS)
# ============================================================================

def sync_teams_sheet(client, spreadsheet, mode: str = 'per_100',
                     show_percentiles: bool = False,
                     show_advanced: bool = False,
                     historical_config: dict = None,
                     postseason_config: dict = None):
    """
    Sync the league-wide Teams sheet.

    Same layout as the Players sheet but:
    - Contains one row per team (30 rows)
    - Names column shows team name instead of 'TEAM'
    - Player-specific info columns (jersey, position, draft) are excluded
    - Opponent stats appear as subsections after each stats category
    - Placed as the second sheet in the workbook (after Players)
    """
    logger.info('  Syncing Teams sheet...')
    fmt = SHEET_FORMATTING
    current_year = NBA_CONFIG['current_season_year']
    worksheet = _get_or_create_worksheet(spreadsheet, 'Teams')

    conn = get_db_connection()
    try:
        # ---- Fetch all teams for 3 sections ----
        all_teams_curr = fetch_all_teams(conn, 'current_stats')
        all_teams_hist = fetch_all_teams(conn, 'historical_stats', historical_config)
        all_teams_post = fetch_all_teams(conn, 'postseason_stats', postseason_config)

        # ---- Enrich team data with minute-weighted player info averages ----
        all_players_curr = fetch_all_players(conn, 'current_stats')
        from collections import defaultdict
        player_groups = defaultdict(list)
        for p in all_players_curr:
            ta = p.get('team_abbr')
            if ta:
                player_groups[ta].append(p)

        avg_fields = ['years_experience', 'age', 'height_inches', 'weight_lbs', 'wingspan_inches']
        for team_d in all_teams_curr['teams']:
            ta = team_d.get('team_abbr')
            if not ta:
                continue
            tp = player_groups.get(ta, [])
            for field in avg_fields:
                wsum, wweight = 0.0, 0.0
                for p in tp:
                    val = p.get(field)
                    if val is None:
                        continue
                    mins = (p.get('minutes_x10', 0) or 0) / 10.0
                    if mins <= 0:
                        continue
                    wsum += val * mins
                    wweight += mins
                if wweight > 0:
                    team_d[field] = round(wsum / wweight, 1)

        # ---- Recombine team + opponent fields into full rows ----
        def _combine_full(teams_dict):
            full = []
            for team_d, opp_d in zip(teams_dict['teams'], teams_dict['opponents']):
                combined = dict(team_d)
                for k, v in opp_d.items():
                    if k.startswith('opp_'):
                        combined[k] = v
                full.append(combined)
            return full

        full_curr = _combine_full(all_teams_curr)
        full_hist = _combine_full(all_teams_hist)
        full_post = _combine_full(all_teams_post)

        curr_by_abbr = {d.get('team_abbr'): d for d in full_curr}
        hist_by_abbr = {d.get('team_abbr'): d for d in full_hist}
        post_by_abbr = {d.get('team_abbr'): d for d in full_post}

        # ---- Team percentile populations (regular columns only) ----
        pct_team_curr = calculate_all_percentiles(all_teams_curr['teams'], 'team', mode)
        pct_team_hist = calculate_all_percentiles(all_teams_hist['teams'], 'team', mode)
        pct_team_post = calculate_all_percentiles(all_teams_post['teams'], 'team', mode)

        # ---- Opponent percentile populations (for percentile coloring) ----
        opp_percentiles = {}
        # We need columns list first; build it early (also used below)
        columns = build_sheet_columns(entity='team', stat_mode='both',
                                      show_percentiles=show_percentiles,
                                      sheet_type='teams')
        for entry in columns:
            col_key, col_def, _, ctx = entry
            if not col_def.get('is_opponent_col'):
                continue
            formula = col_def.get('team_formula')
            if not formula:
                continue
            if ctx == 'current_stats':
                data_list = full_curr
            elif ctx == 'historical_stats':
                data_list = full_hist
            elif ctx == 'postseason_stats':
                data_list = full_post
            else:
                continue
            values = []
            for d in data_list:
                val = _eval_dynamic_formula(formula, d, col_def, mode)
                if val is not None:
                    values.append(val)
            if values:
                if col_key not in opp_percentiles:
                    opp_percentiles[col_key] = {}
                opp_percentiles[col_key][ctx] = sorted(values)

        # ---- Team names lookup ----
        teams_db = get_teams_from_db()
        team_names = {abbr: name for _, (abbr, name) in teams_db.items()}
        abbrs = sorted(team_names.keys())

        # ---- Headers ----
        headers = build_headers(columns, mode=mode,
                                team_name='Teams',
                                current_year=current_year,
                                historical_config=historical_config,
                                postseason_config=postseason_config)
        n_cols = len(columns)

        # ---- Build team rows ----
        data_rows = []
        all_percentile_cells = []

        for abbr in abbrs:
            curr_data = curr_by_abbr.get(abbr)
            hist_data = hist_by_abbr.get(abbr)
            post_data = post_by_abbr.get(abbr)

            # Set TEAM key so names column shows team name instead of literal 'TEAM'
            for d in [curr_data, hist_data, post_data]:
                if d:
                    d['TEAM'] = team_names.get(abbr, abbr)

            row, pct_cells = build_merged_entity_row(
                player_id=None,
                columns_list=columns,
                current_data=curr_data,
                historical_data=hist_data,
                postseason_data=post_data,
                pct_curr=pct_team_curr,
                pct_hist=pct_team_hist,
                pct_post=pct_team_post,
                entity_type='team',
                mode=mode,
                opp_percentiles=opp_percentiles,
            )
            for cell in pct_cells:
                cell['row'] = fmt['data_start_row'] + len(data_rows)
            all_percentile_cells.extend(pct_cells)
            data_rows.append(row)

        n_team_rows = len(data_rows)

        # ---- Summary rows (Best, 75th, Average, 25th, Worst) ----
        merged_pops = {}
        for k, v in pct_team_curr.items():
            merged_pops[f'current_stats:{k}'] = v
        for k, v in pct_team_hist.items():
            merged_pops[f'historical_stats:{k}'] = v
        for k, v in pct_team_post.items():
            merged_pops[f'postseason_stats:{k}'] = v
        summary_rows, summary_pct = build_summary_rows(
            columns, merged_pops, mode, opp_percentiles=opp_percentiles
        )
        summary_start = fmt['data_start_row'] + n_team_rows
        for cell in summary_pct:
            cell['row'] = summary_start + cell.pop('row_offset')
        all_percentile_cells.extend(summary_pct)
        data_rows.extend(summary_rows)

        # ---- Assemble rows ----
        filter_row = [''] * n_cols
        all_rows = [headers['row1'], headers['row2'], headers['row3'],
                     filter_row] + data_rows
        all_rows = [r + [''] * (n_cols - len(r)) for r in all_rows]

        total_rows = len(all_rows)
        worksheet.resize(rows=total_rows, cols=n_cols)
        worksheet.update(range_name='A1', values=all_rows,
                         value_input_option='USER_ENTERED')

        # ---- Apply formatting ----
        _apply_sheet_formatting(
            worksheet, columns,
            header_merges=headers['merges'],
            n_data_rows=n_team_rows,
            team_name='Teams',
            percentile_cells=all_percentile_cells,
            n_player_rows=n_team_rows,
            sheet_type='teams',
            show_advanced=show_advanced,
            show_percentiles=show_percentiles,
        )

        # ---- Move Teams sheet to second position (after Players) ----
        try:
            worksheet.spreadsheet.batch_update({'requests': [{
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': worksheet.id,
                        'index': 1,
                    },
                    'fields': 'index',
                }
            }]})
        except Exception as e:
            logger.warning(f'  Could not move Teams sheet to position: {e}')

        logger.info(
            f'  Teams sheet done: {n_team_rows} teams, '
            f'{len(all_percentile_cells)} percentile cells'
        )
    finally:
        conn.close()


# ============================================================================
# FULL LEAGUE SYNC
# ============================================================================

def sync_all_teams(mode: str = 'per_100', show_percentiles: bool = False,
                   show_advanced: bool = False,
                   historical_config: dict = None, postseason_config: dict = None,
                   priority_team: str = None):
    """Sync all sheets. priority_team is synced first, then other teams, then Players/Teams."""
    logger.info('Starting full league sync...')
    delay = SHEET_FORMATTING.get('sync_delay_seconds', 3)

    client = get_sheets_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEETS_CONFIG['spreadsheet_id'])

    sync_kwargs = dict(mode=mode, show_percentiles=show_percentiles,
                       show_advanced=show_advanced,
                       historical_config=historical_config,
                       postseason_config=postseason_config)

    # ---- Sync individual team sheets (priority first) ----
    teams = get_teams_from_db()           # {team_id: (abbr, name)}
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
                **sync_kwargs,
            )
        except Exception as exc:
            logger.error(f'  {abbr} failed: {exc}', exc_info=True)

        logger.info(f'  Rate limit pause ({delay}s)...')
        time.sleep(delay)

    # ---- Sync aggregate sheets last (Players then Teams) ----
    try:
        sync_players_sheet(client, spreadsheet, **sync_kwargs)
    except Exception as exc:
        logger.error(f'  Players sheet failed: {exc}', exc_info=True)

    logger.info(f'  Rate limit pause ({delay}s)...')
    time.sleep(delay)

    try:
        sync_teams_sheet(client, spreadsheet, **sync_kwargs)
    except Exception as exc:
        logger.error(f'  Teams sheet failed: {exc}', exc_info=True)

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
    show_percentiles = args.percentiles or os.environ.get('SHOW_PERCENTILES') == 'true'
    show_advanced = os.environ.get('SHOW_ADVANCED') == 'true'
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
        show_advanced=show_advanced,
        historical_config=historical_config,
        postseason_config=postseason_config,
        priority_team=priority_team,
    )


if __name__ == '__main__':
    main()
