import logging
from typing import Callable, Optional
import os
import time

from src.db import get_db_connection
from src.sheets.lib.db import fetch_all_players, fetch_all_teams, fetch_players_for_team, fetch_team_stats
from sheets.lib.layout import build_headers, build_sheet_columns
from sheets.lib.calculations import calculate_all_percentiles, _eval_dynamic_formula
from sheets.lib.formatting import build_formatting_requests, build_merged_entity_row, build_summary_rows
from sheets.lib.google import get_sheets_client, get_or_create_worksheet, apply_sheet_formatting

logger = logging.getLogger(__name__)

# ============================================================================
# TIMEFRAME CONFIG BUILDER (used by runners)
# ============================================================================

def build_timeframe_configs(hist_years_arg=None, post_years_arg=None,
                            default_mode='years'):
    """Build historical and postseason config dicts from env vars and CLI args.

    Centralises the duplicated env-var → config-dict parsing that was in both
    runners/nba_sheets.py and runners/ncaa_sheets.py.

    Args:
        hist_years_arg: --hist-years CLI value (or None for env/default)
        post_years_arg: --post-years CLI value (or None for env/default)
        default_mode: Default when HISTORICAL_MODE env not set
                      ('years' for NBA, 'career' for NCAA)

    Returns:
        Tuple of (historical_config, postseason_config)
    """
    hist_mode = os.environ.get('HISTORICAL_MODE', default_mode)
    include_current = os.environ.get('INCLUDE_CURRENT_YEAR', 'false') == 'true'

    def _build(years_arg):
        if hist_mode == 'career':
            return {'mode': 'career', 'include_current': include_current}
        if hist_mode == 'seasons':
            season_str = os.environ.get('HISTORICAL_SEASONS', '')
            seasons = [s.strip() for s in season_str.split(',') if s.strip()]
            return {'mode': 'seasons', 'value': seasons, 'include_current': include_current}
        years = years_arg or int(os.environ.get('HISTORICAL_YEARS', '3'))
        return {'mode': 'years', 'value': years, 'include_current': include_current}

    return _build(hist_years_arg), _build(post_years_arg)

# ============================================================================

def _combine_team_opp(teams_dict):
    """Merge team and opponent dicts into combined rows for the Teams sheet."""
    full = []
    for team_d, opp_d in zip(teams_dict['teams'], teams_dict['opponents']):
        combined = dict(team_d)
        for k, v in opp_d.items():
            if k.startswith('opp_'):
                combined[k] = v
        full.append(combined)
    return full


def _write_and_format(worksheet, columns, headers, data_rows,
                      percentile_cells, n_entity_rows,
                      team_name, sheet_type, show_advanced,
                      show_percentiles, data_only, build_fn):
    """Resize worksheet, write values, and apply formatting.

    Args:
        build_fn: League-specific build_formatting_requests callable
                  (build_formatting_requests).
    """
    n_cols = len(columns)
    filter_row = [''] * n_cols
    all_rows = [headers['row1'], headers['row2'], headers['row3'],
                filter_row] + data_rows

    # Pad rows to full width
    all_rows = [r + [''] * (n_cols - len(r)) for r in all_rows]

    total_rows = len(all_rows)
    worksheet.resize(rows=total_rows, cols=n_cols)
    worksheet.update(range_name='A1', values=all_rows,
                     value_input_option='USER_ENTERED')

    apply_sheet_formatting(
        worksheet, columns,
        header_merges=headers['merges'],
        n_data_rows=len(data_rows),
        team_name=team_name,
        percentile_cells=percentile_cells,
        n_player_rows=n_entity_rows,
        sheet_type=sheet_type,
        show_advanced=show_advanced,
        show_percentiles=show_percentiles,
        data_only=data_only,
        build_fn=build_fn,
    )


def _move_sheet_to_position(worksheet, index):
    """Move a worksheet to a specific tab position in the workbook."""
    try:
        worksheet.spreadsheet.batch_update({'requests': [{
            'updateSheetProperties': {
                'properties': {
                    'sheetId': worksheet.id,
                    'index': index,
                },
                'fields': 'index',
            }
        }]})
    except Exception as e:
        logger.warning(f'  Could not move sheet to position {index}: {e}')


def _get_team_names(ctx):
    """Get {abbr: display_name} dict from the league's ETL lib."""
    teams_db = ctx.etl_lib.get_teams_from_db()
    return {abbr: name for _, (abbr, name) in teams_db.items()}


def _filter_to_desired(ctx, abbrs, team_names):
    """Apply optional desired-teams filter, returning filtered abbrs list.

    Returns (filtered_abbrs, desired_abbrs_set_or_None).
    """
    if not ctx.load_desired_teams:
        return sorted(abbrs), None

    desired = ctx.load_desired_teams()
    if not desired:
        return sorted(abbrs), None

    inst_to_abbr = {name: abbr for abbr, name in team_names.items()}
    matched = set()
    unmatched = set()
    for name in desired:
        if name in inst_to_abbr:
            matched.add(inst_to_abbr[name])
        else:
            unmatched.add(name)
    if unmatched:
        logger.warning(
            f'  {len(unmatched)} desired teams not found in DB: {sorted(unmatched)}'
        )
    return sorted(matched), matched


# ============================================================================
# TEAM SHEET SYNC
# ============================================================================

def sync_team_sheet(ctx, client, spreadsheet, team_abbr,
                    team_name='', mode='per_100',
                    show_advanced=False, show_percentiles=False,
                    historical_config=None, postseason_config=None,
                    data_only=False, precomputed=None):
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
    fmt = ctx.sheet_formatting
    current_year = ctx.league_config[ctx.season_year_key]
    display_name = team_name or team_abbr
    worksheet = get_or_create_worksheet(spreadsheet, team_abbr, clear=not data_only)

    conn = get_db_connection()
    try:
        # ---- Fetch raw data ----
        current_players = fetch_players_for_team(conn, team_abbr, 'current_stats')
        hist_players = fetch_players_for_team(
            conn, team_abbr, 'historical_stats', historical_config)
        post_players = fetch_players_for_team(
            conn, team_abbr, 'postseason_stats', postseason_config)
        team_data_curr = fetch_team_stats(conn, team_abbr, 'current_stats')
        team_data_hist = fetch_team_stats(
            conn, team_abbr, 'historical_stats', historical_config)
        team_data_post = fetch_team_stats(
            conn, team_abbr, 'postseason_stats', postseason_config)

# ---- Percentile populations (league-wide) ----
        if precomputed:
            pct_curr = precomputed['pct_curr']
            pct_hist = precomputed['pct_hist']
            pct_post = precomputed['pct_post']
            pct_team_curr = precomputed['pct_team_curr']
            pct_opp_curr = precomputed['pct_opp_curr']
            pct_team_hist = precomputed['pct_team_hist']
            pct_opp_hist = precomputed['pct_opp_hist']
            pct_team_post = precomputed['pct_team_post']
            pct_opp_post = precomputed['pct_opp_post']
        else:
            all_players_curr = fetch_all_players(conn, 'current_stats')
            all_players_hist = fetch_all_players(
                conn, 'historical_stats', historical_config)
            all_players_post = fetch_all_players(
                conn, 'postseason_stats', postseason_config)
            all_teams_curr = fetch_all_teams(conn, 'current_stats')
            all_teams_hist = fetch_all_teams(
                conn, 'historical_stats', historical_config)
            all_teams_post = fetch_all_teams(
                conn, 'postseason_stats', postseason_config)

            pct_curr = calculate_all_percentiles(all_players_curr, 'player', mode)
            pct_hist = calculate_all_percentiles(all_players_hist, 'player', mode)
            pct_post = calculate_all_percentiles(all_players_post, 'player', mode)
            pct_team_curr = calculate_all_percentiles(
                all_teams_curr['teams'], 'team', mode)
            pct_opp_curr = calculate_all_percentiles(
                all_teams_curr['opponents'], 'opponents', mode)
            pct_team_hist = calculate_all_percentiles(
                all_teams_hist['teams'], 'team', mode)
            pct_opp_hist = calculate_all_percentiles(
                all_teams_hist['opponents'], 'opponents', mode)
            pct_team_post = calculate_all_percentiles(
                all_teams_post['teams'], 'team', mode)
            pct_opp_post = calculate_all_percentiles(
                all_teams_post['opponents'], 'opponents', mode)

            # Enrich all teams with minute-weighted player info averages
            player_groups = defaultdict(list)
            for p in all_players_curr:
                ta = p.get(ctx.team_abbr_field)
                if ta:
                    player_groups[ta].append(p)
            pct_team_curr = calculate_all_percentiles(
                all_teams_curr['teams'], 'team', mode)

        # ---- Column structure ----
        columns = build_sheet_columns(
            entity='player', stat_mode='both',
            sheet_type='team')

        # ---- Headers ----
        headers = build_headers(
            columns, mode=mode, team_name=display_name,
            current_year=current_year,
            historical_config=historical_config,
            postseason_config=postseason_config)

        # ---- Index players by player_id for merging ----
        curr_by_id = {p.get('player_id'): p for p in current_players}
        hist_by_id = {p.get('player_id'): p for p in hist_players}
        post_by_id = {p.get('player_id'): p for p in post_players}

        # ---- Collect player IDs ----
        all_player_ids = []
        seen = set()
        for p in current_players:
            pid = p.get('player_id')
            if pid and pid not in seen:
                all_player_ids.append(pid)
                seen.add(pid)
        if ctx.include_hist_post_players:
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

        # ---- Team + Opponents rows ----
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

        # ---- Write and format ----
        _write_and_format(
            worksheet, columns, headers, data_rows,
            all_percentile_cells, n_player_rows,
            display_name, 'team', show_advanced,
            show_percentiles, data_only,
            build_fn=build_formatting_requests,
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

def sync_players_sheet(ctx, client, spreadsheet, mode='per_100',
                       show_advanced=False, show_percentiles=False,
                       historical_config=None, postseason_config=None,
                       data_only=False):
    """
    Sync the league-wide Players sheet.

    Same layout as team sheets but:
    - Contains ALL players from every team
    - No team/opponent aggregate rows at bottom
    - Team column is visible (shows each player's team)
    - Placed as the first sheet in the workbook
    """
    logger.info('  Syncing Players sheet...')
    fmt = ctx.sheet_formatting
    current_year = ctx.league_config[ctx.season_year_key]
    worksheet = get_or_create_worksheet(spreadsheet, 'Players', clear=not data_only)

    conn = get_db_connection()
    try:
        # ---- Fetch all players league-wide ----
        all_players_curr = fetch_all_players(conn, 'current_stats')
        all_players_hist = fetch_all_players(
            conn, 'historical_stats', historical_config)
        all_players_post = fetch_all_players(
            conn, 'postseason_stats', postseason_config)

        # ---- Percentile populations (single-mode) ----
        pct_curr = calculate_all_percentiles(all_players_curr, 'player', mode)
        pct_hist = calculate_all_percentiles(all_players_hist, 'player', mode)
        pct_post = calculate_all_percentiles(all_players_post, 'player', mode)

        # ---- Column structure (players sheet type keeps team column visible) ----
        columns = build_sheet_columns(
            entity='player', stat_mode='both',
            sheet_type='players')

        # ---- Headers ----
        headers = build_headers(
            columns, mode=mode, team_name='Players',
            current_year=current_year,
            historical_config=historical_config,
            postseason_config=postseason_config)

        # ---- Index players by player_id ----
        curr_by_id = {p.get('player_id'): p for p in all_players_curr}
        hist_by_id = {p.get('player_id'): p for p in all_players_hist}
        post_by_id = {p.get('player_id'): p for p in all_players_post}

        # ---- Filter to desired teams (percentile pops stay league-wide) ----
        if ctx.load_desired_teams:
            desired = ctx.load_desired_teams()
            if desired:
                team_names = _get_team_names(ctx)
                inst_to_abbr = {name: abbr for abbr, name in team_names.items()}
                desired_abbrs = {inst_to_abbr[n] for n in desired if n in inst_to_abbr}
                players_curr_iter = [
                    p for p in all_players_curr
                    if p.get(ctx.team_abbr_field) in desired_abbrs
                ]
                players_hist_iter = [
                    p for p in all_players_hist
                    if p.get(ctx.team_abbr_field) in desired_abbrs
                ]
                players_post_iter = [
                    p for p in all_players_post
                    if p.get(ctx.team_abbr_field) in desired_abbrs
                ]
            else:
                players_curr_iter = all_players_curr
                players_hist_iter = all_players_hist
                players_post_iter = all_players_post
        else:
            players_curr_iter = all_players_curr
            players_hist_iter = all_players_hist
            players_post_iter = all_players_post

        # ---- Unique player IDs sorted by current-season minutes ----
        all_player_ids = []
        seen = set()
        for p in sorted(players_curr_iter,
                        key=lambda x: x.get('minutes_x10', 0) or 0,
                        reverse=True):
            pid = p.get('player_id')
            if pid and pid not in seen:
                all_player_ids.append(pid)
                seen.add(pid)
        for p in players_hist_iter + players_post_iter:
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
            merged_pops[k] = v
        for k, v in pct_hist.items():
            merged_pops[f'historical_stats:{k}'] = v
            merged_pops[k] = v
        for k, v in pct_post.items():
            merged_pops[f'postseason_stats:{k}'] = v
            merged_pops[k] = v
        summary_rows, summary_pct = build_summary_rows(columns, merged_pops, mode)
        summary_start = fmt['data_start_row'] + n_player_rows
        for cell in summary_pct:
            cell['row'] = summary_start + cell.pop('row_offset')
        all_percentile_cells.extend(summary_pct)
        data_rows.extend(summary_rows)

        # ---- Write and format ----
        _write_and_format(
            worksheet, columns, headers, data_rows,
            all_percentile_cells, n_player_rows,
            'Players', 'players', show_advanced,
            show_percentiles, data_only,
            build_fn=build_formatting_requests,
        )

        _move_sheet_to_position(worksheet, 0)

        logger.info(
            f'  Players sheet done: {n_player_rows} players, '
            f'{len(all_percentile_cells)} percentile cells'
        )
    finally:
        conn.close()


# ============================================================================
# TEAMS SHEET SYNC (ALL TEAMS, WITH OPPONENT SUBSECTIONS)
# ============================================================================

def sync_teams_sheet(ctx, client, spreadsheet, mode='per_100',
                     show_advanced=False, show_percentiles=False,
                     historical_config=None, postseason_config=None,
                     data_only=False):
    """
    Sync the league-wide Teams sheet.

    Same layout as the Players sheet but:
    - Contains one row per team
    - Names column shows team name instead of 'TEAM'
    - Player-specific info columns (jersey, position, draft) are excluded
    - Opponent stats appear as subsections after each stats category
    - Placed as the second sheet in the workbook (after Players)
    """
    logger.info('  Syncing Teams sheet...')
    fmt = ctx.sheet_formatting
    current_year = ctx.league_config[ctx.season_year_key]
    worksheet = get_or_create_worksheet(spreadsheet, 'Teams', clear=not data_only)

    conn = get_db_connection()
    try:
        # ---- Fetch all teams for 3 sections ----
        all_teams_curr = fetch_all_teams(conn, 'current_stats')
        all_teams_hist = fetch_all_teams(
            conn, 'historical_stats', historical_config)
        all_teams_post = fetch_all_teams(
            conn, 'postseason_stats', postseason_config)

        # ---- Enrich team data with minute-weighted player info averages ----
        all_players_curr = fetch_all_players(conn, 'current_stats')
        player_groups = defaultdict(list)
        for p in all_players_curr:
            ta = p.get(ctx.team_abbr_field)
            if ta:
                player_groups[ta].append(p)

        # ---- Recombine team + opponent fields into full rows ----
        full_curr = _combine_team_opp(all_teams_curr)
        full_hist = _combine_team_opp(all_teams_hist)
        full_post = _combine_team_opp(all_teams_post)

        curr_by_abbr = {d.get(ctx.team_abbr_field): d for d in full_curr}
        hist_by_abbr = {d.get(ctx.team_abbr_field): d for d in full_hist}
        post_by_abbr = {d.get(ctx.team_abbr_field): d for d in full_post}

        # ---- Team percentile populations ----
        pct_team_curr = calculate_all_percentiles(
            all_teams_curr['teams'], 'team', mode)
        pct_team_hist = calculate_all_percentiles(
            all_teams_hist['teams'], 'team', mode)
        pct_team_post = calculate_all_percentiles(
            all_teams_post['teams'], 'team', mode)

        # ---- Opponent percentile populations ----
        columns = build_sheet_columns(
            entity='team', stat_mode='both',
            sheet_type='teams')

        opp_percentiles = {}
        for entry in columns:
            col_key, col_def, _, section_ctx = entry
            if not col_def.get('is_opponent_col'):
                continue
            formula = col_def.get('team_formula')
            if not formula:
                continue
            if section_ctx == 'current_stats':
                data_list = full_curr
            elif section_ctx == 'historical_stats':
                data_list = full_hist
            elif section_ctx == 'postseason_stats':
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
                opp_percentiles[col_key][section_ctx] = ctx.wrap_opp_pct(values)

        # ---- Team names + optional filter ----
        team_names = _get_team_names(ctx)
        abbrs = list(team_names.keys())
        abbrs, _ = _filter_to_desired(ctx, abbrs, team_names)

        # ---- Headers ----
        headers = build_headers(
            columns, mode=mode, team_name='Teams',
            current_year=current_year,
            historical_config=historical_config,
            postseason_config=postseason_config)

        # ---- Build team rows ----
        data_rows = []
        all_percentile_cells = []

        for abbr in abbrs:
            curr_data = curr_by_abbr.get(abbr)
            hist_data = hist_by_abbr.get(abbr)
            post_data = post_by_abbr.get(abbr)

            # Set TEAM key so names column shows team name
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
            merged_pops[k] = v
        for k, v in pct_team_hist.items():
            merged_pops[f'historical_stats:{k}'] = v
            merged_pops[k] = v
        for k, v in pct_team_post.items():
            merged_pops[f'postseason_stats:{k}'] = v
            merged_pops[k] = v
        summary_rows, summary_pct = build_summary_rows(
            columns, merged_pops, mode, opp_percentiles=opp_percentiles)
        summary_start = fmt['data_start_row'] + n_team_rows
        for cell in summary_pct:
            cell['row'] = summary_start + cell.pop('row_offset')
        all_percentile_cells.extend(summary_pct)
        data_rows.extend(summary_rows)

        # ---- Write and format ----
        _write_and_format(
            worksheet, columns, headers, data_rows,
            all_percentile_cells, n_team_rows,
            'Teams', 'teams', show_advanced,
            show_percentiles, data_only,
            build_fn=build_formatting_requests,
        )

        _move_sheet_to_position(worksheet, 1)

        logger.info(
            f'  Teams sheet done: {n_team_rows} teams, '
            f'{len(all_percentile_cells)} percentile cells'
        )
    finally:
        conn.close()


# ============================================================================
# FULL LEAGUE SYNC
# ============================================================================

def sync_all_teams(league, mode='per_100',
                   show_advanced=False, show_percentiles=False,
                   historical_config=None, postseason_config=None,
                   priority_team=None, data_only=False):
    """Sync all sheets. priority_team is synced first, then teams, then Players/Teams."""
    class Context: pass
    ctx = Context()
    ctx.league = league
    from src.sheets.config import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING
    from src.sheets.config import SHEETS_COLUMNS
    ctx.google_sheets_config = GOOGLE_SHEETS_CONFIG
    ctx.sheet_formatting = SHEET_FORMATTING
    ctx.season_year_key = 'current_season'
    ctx.team_abbr_field = 'abbr'
    ctx.include_hist_post_players = True
    ctx.wrap_opp_pct = lambda vals: sorted(vals)
    ctx.load_desired_teams = None
    
    if league == 'nba':
        import etl.nba.lib as etl_lib
        from etl.nba.config import NBA_CONFIG as league_config
    else:
        import etl.ncaa.lib as etl_lib
        from etl.ncaa.config import NCAA_CONFIG as league_config
        
    ctx.etl_lib = etl_lib
    ctx.league_config = league_config
    
    ctx.player_entity_table = etl_lib.get_table_name('player_entity')
    ctx.team_entity_table = etl_lib.get_table_name('team_entity')
    ctx.player_stats_table = etl_lib.get_table_name('player_stats')
    ctx.team_stats_table = etl_lib.get_table_name('team_stats')
    
    ctx.player_entity_fields = {
        'player_id', 'name', 'team_id', 'height_inches', 'weight_lbs',
        'wingspan_inches', 'years_experience', 'age', 'jersey_number',
        'hand', 'notes', 'birthdate', 'updated_at',
    }
    ctx.team_entity_fields = {
        'team_id', 'abbr', 'team_name', 'notes', 'updated_at',
    }
    
    all_cols = {k for k, v in SHEETS_COLUMNS.items() if league in v.get('leagues', []) and v.get('stat_category') != 'none'}
    ctx.stat_fields = {c for c in (all_cols - ctx.player_entity_fields - ctx.team_entity_fields) if not c[0].isupper()}
    ctx.team_stat_fields = {c for c in (all_cols - ctx.team_entity_fields) if not c[0].isupper()}
    ctx.team_abbr_col = 'abbr'
    ctx.primary_minutes_col = 'minutes_x10' if 'minutes_x10' in ctx.stat_fields else 'minutes'
    
    # Text season column formatted like '2023-24' needs no python logic formatting func
    ctx.year_format_fn = str

    logger.info('Starting %s sync...', 'data-only' if data_only else 'full')
    delay = 0.5 if data_only else ctx.sheet_formatting.get('sync_delay_seconds', 3)

    client = get_sheets_client(ctx.google_sheets_config)
    spreadsheet = client.open_by_key(ctx.google_sheets_config['spreadsheet_id'])

    sync_kwargs = dict(mode=mode,
                       show_advanced=show_advanced,
                       show_percentiles=show_percentiles,
                       historical_config=historical_config,
                       postseason_config=postseason_config,
                       data_only=data_only)

    # ---- Pre-compute league-wide percentile populations ONCE ----
    logger.info('  Pre-computing league-wide percentile populations...')
    conn = get_db_connection()
    try:
        all_players_curr = fetch_all_players(conn, 'current_stats')
        all_players_hist = fetch_all_players(
            conn, 'historical_stats', historical_config)
        all_players_post = fetch_all_players(
            conn, 'postseason_stats', postseason_config)
        all_teams_curr = fetch_all_teams(conn, 'current_stats')
        all_teams_hist = fetch_all_teams(
            conn, 'historical_stats', historical_config)
        all_teams_post = fetch_all_teams(
            conn, 'postseason_stats', postseason_config)

        pct_curr = calculate_all_percentiles(all_players_curr, 'player', mode)
        pct_hist = calculate_all_percentiles(all_players_hist, 'player', mode)
        pct_post = calculate_all_percentiles(all_players_post, 'player', mode)
        pct_team_curr = calculate_all_percentiles(
            all_teams_curr['teams'], 'team', mode)
        pct_opp_curr = calculate_all_percentiles(
            all_teams_curr['opponents'], 'opponents', mode)
        pct_team_hist = calculate_all_percentiles(
            all_teams_hist['teams'], 'team', mode)
        pct_opp_hist = calculate_all_percentiles(
            all_teams_hist['opponents'], 'opponents', mode)
        pct_team_post = calculate_all_percentiles(
            all_teams_post['teams'], 'team', mode)
        pct_opp_post = calculate_all_percentiles(
            all_teams_post['opponents'], 'opponents', mode)

        # Enrich teams with minute-weighted player info averages
        player_groups = defaultdict(list)
        for p in all_players_curr:
            ta = p.get(ctx.team_abbr_field)
            if ta:
                player_groups[ta].append(p)
        pct_team_curr = calculate_all_percentiles(
            all_teams_curr['teams'], 'team', mode)

        precomputed = {
            'pct_curr': pct_curr,
            'pct_hist': pct_hist,
            'pct_post': pct_post,
            'pct_team_curr': pct_team_curr,
            'pct_opp_curr': pct_opp_curr,
            'pct_team_hist': pct_team_hist,
            'pct_opp_hist': pct_opp_hist,
            'pct_team_post': pct_team_post,
            'pct_opp_post': pct_opp_post,
        }
        logger.info('  Percentile populations ready')
    finally:
        conn.close()

    # ---- Build team list + optional filter ----
    team_names = _get_team_names(ctx)
    abbrs, desired_set = _filter_to_desired(ctx, team_names.keys(), team_names)

    if desired_set is not None:
        logger.info(
            f'  Syncing {len(abbrs)} desired teams (of {len(team_names)} total)')

    if priority_team:
        pt = priority_team.upper()
        if pt in abbrs:
            abbrs = [pt] + [a for a in abbrs if a != pt]

    for i, abbr in enumerate(abbrs):
        try:
            sync_team_sheet(
                ctx, client, spreadsheet, abbr,
                team_name=team_names.get(abbr, abbr),
                precomputed=precomputed,
                **sync_kwargs,
            )
        except Exception as exc:
            logger.error(f'  {abbr} failed: {exc}', exc_info=True)

        logger.info(f'  Rate limit pause ({delay}s)...')
        time.sleep(delay)

    # ---- Sync aggregate sheets last (Players then Teams) ----
    try:
        sync_players_sheet(ctx, client, spreadsheet, **sync_kwargs)
    except Exception as exc:
        logger.error(f'  Players sheet failed: {exc}', exc_info=True)

    logger.info(f'  Rate limit pause ({delay}s)...')
    time.sleep(delay)

    try:
        sync_teams_sheet(ctx, client, spreadsheet, **sync_kwargs)
    except Exception as exc:
        logger.error(f'  Teams sheet failed: {exc}', exc_info=True)

    logger.info('Full sync complete.')


# ============================================================================
# LEAGUE PROFILE BUILDER
# ============================================================================

def get_league_profile(league: str) -> dict:
    """Builds the dictionary kwargs used to initialize the Sync Context."""
    import importlib
    from src.db import get_db_table_columns
    from src.sheets.config import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING, STAT_CONSTANTS
    from sheets.engine import resolve_columns_for_league

    if league == 'nba':
        from etl.nba.config import NBA_CONFIG as LEAGUE_CONFIG, DB_CONFIG
        from etl.nba.lib import get_table_name
    elif league == 'ncaa':
        from etl.ncaa.config import NCAA_CONFIG as LEAGUE_CONFIG, DB_CONFIG
        from etl.ncaa.lib import get_table_name
    else:
        raise ValueError(f"Unknown league: {league}")

    etl_module = importlib.import_module(f"etl.{league}.lib")
    
    player_fields = get_db_table_columns(DB_CONFIG, league, 'players', set())
    team_fields = get_db_table_columns(DB_CONFIG, league, 'teams', set())
    
    all_stat_sys = {'player_id', 'updated_at', 'team_id', 'season', 'season_type'}
    stat_fields = get_db_table_columns(DB_CONFIG, league, 'player_season_stats', all_stat_sys)
    team_stat_fields = get_db_table_columns(DB_CONFIG, league, 'team_season_stats', all_stat_sys)

    return {
        'etl_lib': etl_module,
        'db_config': DB_CONFIG,
        'league_config': LEAGUE_CONFIG,
        'google_sheets_config': GOOGLE_SHEETS_CONFIG[league],
        'sheet_formatting': SHEET_FORMATTING,
        'season_year_key': 'current_season_year' if league == 'nba' else 'current_season_int',
        'team_abbr_field': 'abbr',
        'include_hist_post_players': True,
        
        'data_fetcher_kwargs': dict(
            current_season=LEAGUE_CONFIG['current_season'],
            current_year=LEAGUE_CONFIG['current_season_year' if league == 'nba' else 'current_season_int'],
            season_type=LEAGUE_CONFIG.get('season_type', 1),
            season_col='season',
            team_abbr_col='abbr',
            player_entity_fields=player_fields,
            team_entity_fields=team_fields,
            stat_table_fields={f for f in stat_fields - player_fields - team_fields if not f[0].isupper()},
            team_stat_table_fields={f for f in team_stat_fields - team_fields if not f[0].isupper()},
            get_table_name=get_table_name,
            player_join_type='LEFT JOIN',
            player_group_extras=['p.birthdate'] if league == 'nba' else None,
            age_expr='EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birthdate))::int AS "age"' if league == 'nba' else None,
            career_excludes_current=False,
        ),
        
        'engine_kwargs': dict(
            sheets_columns=resolve_columns_for_league(league),
            per_minute_mode=f"per_{int(STAT_CONSTANTS['default_per_minute'])}",
            league_key=league,
        )
    }