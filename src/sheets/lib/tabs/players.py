import logging
from typing import Optional

from src.db import get_db_connection
from src.sheets.lib.db import fetch_all_players
from sheets.lib.layout import build_headers, build_sheet_columns
from sheets.lib.calculations import calculate_all_percentiles
from sheets.lib.formatting import build_formatting_requests, build_merged_entity_row, build_summary_rows
from sheets.lib.google import get_or_create_worksheet, write_and_format, move_sheet_to_position

logger = logging.getLogger(__name__)

# ============================================================================
# PLAYERS SHEET SYNC (ALL PLAYERS, NO TEAM/OPP AGGREGATE ROWS)
# ============================================================================

def sync_players_sheet(ctx, client, spreadsheet, mode='per_100',
                       show_advanced=False,
                       historical_config=None,
                       partial_update=False,
                       sync_section=None):
    """Sync the league-wide Players sheet."""
    logger.info('  Syncing Players sheet...')
    fmt = ctx.sheet_formatting
    current_season = ctx.league_config[ctx.season_key]
    worksheet = get_or_create_worksheet(spreadsheet, 'Players', clear=not partial_update)

    conn = get_db_connection()
    try:
        # ---- Fetch all players league-wide ----
        all_players_curr = fetch_all_players(conn, 'current_stats')
        all_players_hist = fetch_all_players(
            conn, 'historical_stats', historical_config)
        all_players_post = fetch_all_players(
            conn, 'postseason_stats', historical_config)

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
            current_season=current_season,
            historical_config=historical_config)

        # ---- Index players by player_id ----
        curr_by_id = {p.get('player_id'): p for p in all_players_curr}
        hist_by_id = {p.get('player_id'): p for p in all_players_hist}
        post_by_id = {p.get('player_id'): p for p in all_players_post}

        # ---- Filter to desired teams (percentile pops stay league-wide) ----
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
        write_and_format(
            worksheet, columns, headers, data_rows,
            all_percentile_cells, n_player_rows,
            'Players', 'players', show_advanced,
            True, partial_update,
            build_fn=build_formatting_requests,
        )

        move_sheet_to_position(worksheet, 0)

        logger.info(
            f'  Players sheet done: {n_player_rows} players, '
            f'{len(all_percentile_cells)} percentile cells'
        )
    finally:
        conn.close()