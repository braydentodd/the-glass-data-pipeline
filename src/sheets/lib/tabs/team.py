import logging
from collections import defaultdict
from typing import Optional

from src.db import get_db_connection
from src.sheets.lib.db import fetch_all_players, fetch_all_teams, fetch_players_for_team, fetch_team_stats
from sheets.lib.layout import build_headers, build_sheet_columns
from sheets.lib.calculations import calculate_all_percentiles
from sheets.lib.formatting import build_formatting_requests, build_merged_entity_row
from sheets.lib.google import get_or_create_worksheet, write_and_format

logger = logging.getLogger(__name__)

# ============================================================================
# TEAM SHEET SYNC
# ============================================================================

def sync_team_sheet(ctx, client, spreadsheet, team_abbr,
                    team_name='', mode='per_100',
                    show_advanced=False,
                    historical_config=None,
                    partial_update=False, precomputed=None,
                    sync_section=None):
    """Sync a single team's worksheet with merged row layout."""
    logger.info(f'  Syncing {team_abbr}...')
    fmt = ctx.sheet_formatting
    current_season = ctx.league_config[ctx.season_key]
    display_name = team_name or team_abbr
    worksheet = get_or_create_worksheet(spreadsheet, team_abbr, clear=not partial_update)

    conn = get_db_connection()
    try:
        # ---- Fetch raw data ----
        current_players = fetch_players_for_team(conn, team_abbr, 'current_stats')
        hist_players = fetch_players_for_team(
            conn, team_abbr, 'historical_stats', historical_config)
        post_players = fetch_players_for_team(
            conn, team_abbr, 'postseason_stats', historical_config)
        team_data_curr = fetch_team_stats(conn, team_abbr, 'current_stats')
        team_data_hist = fetch_team_stats(
            conn, team_abbr, 'historical_stats', historical_config)
        team_data_post = fetch_team_stats(
            conn, team_abbr, 'postseason_stats', historical_config)

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
                conn, 'postseason_stats', historical_config)
            all_teams_curr = fetch_all_teams(conn, 'current_stats')
            all_teams_hist = fetch_all_teams(
                conn, 'historical_stats', historical_config)
            all_teams_post = fetch_all_teams(
                conn, 'postseason_stats', historical_config)

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
            current_season=current_season,
            historical_config=historical_config)

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
        write_and_format(
            worksheet, columns, headers, data_rows,
            all_percentile_cells, n_player_rows,
            display_name, 'team', show_advanced,
            True, partial_update,
            build_fn=build_formatting_requests,
        )

        logger.info(
            f'  {team_abbr} done: {len(all_player_ids)} players (merged), '
            f'{len(all_percentile_cells)} percentile cells'
        )

    finally:
        conn.close()
