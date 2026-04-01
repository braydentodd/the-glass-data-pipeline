import logging
from collections import defaultdict
from typing import Optional

from src.db import get_db_connection
from src.sheets.lib.db import fetch_all_players, fetch_all_teams
from sheets.lib.layout import build_headers, build_sheet_columns
from sheets.lib.calculations import calculate_all_percentiles, _eval_dynamic_formula
from sheets.lib.formatting import build_formatting_requests, build_merged_entity_row, build_summary_rows
from sheets.lib.google import get_or_create_worksheet, write_and_format, move_sheet_to_position

logger = logging.getLogger(__name__)

# ============================================================================
# TEAMS SHEET SYNC (ALL TEAMS, WITH OPPONENT SUBSECTIONS)
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

def sync_teams_sheet(ctx, client, spreadsheet, mode='per_100',
                     show_advanced=False,
                     historical_config=None,
                     partial_update=False):
    """Sync the league-wide Teams sheet."""
    logger.info('  Syncing Teams sheet...')
    fmt = ctx.sheet_formatting
    current_season = ctx.league_config[ctx.season_key]
    worksheet = get_or_create_worksheet(spreadsheet, 'Teams', clear=not partial_update)

    conn = get_db_connection()
    try:
        # ---- Fetch all teams for 3 sections ----
        all_teams_curr = fetch_all_teams(conn, 'current_stats')
        all_teams_hist = fetch_all_teams(
            conn, 'historical_stats', historical_config)
        all_teams_post = fetch_all_teams(
            conn, 'postseason_stats', historical_config)

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

        # ---- Team names ----
        teams_db = ctx.etl_lib.get_teams_from_db()
        team_names_map = {abbr: name for _, (abbr, name) in teams_db.items()}
        abbrs = [abbr for _, (abbr, name) in teams_db.items()]

        # ---- Headers ----
        headers = build_headers(
            columns, mode=mode, team_name='Teams',
            current_season=current_season,
            historical_config=historical_config)

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
                    d['TEAM'] = team_names_map.get(abbr, abbr)

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
        write_and_format(
            worksheet, columns, headers, data_rows,
            all_percentile_cells, n_team_rows,
            'Teams', 'teams', show_advanced,
            True, partial_update,
            build_fn=build_formatting_requests,
        )

        move_sheet_to_position(worksheet, 1)

        logger.info(
            f'  Teams sheet done: {n_team_rows} teams, '
            f'{len(all_percentile_cells)} percentile cells'
        )
    finally:
        conn.close()