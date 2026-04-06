import logging
from collections import defaultdict

from src.db import get_db_connection
from src.sheets.config import STAT_MODES
from src.sheets.core.db import fetch_all_players, fetch_all_teams, fetch_players_for_team, fetch_team_stats
from src.sheets.core.layout import build_headers, build_sheet_columns, build_merged_entity_row, build_summary_rows
from src.sheets.google.payloads import build_formatting_requests
from src.sheets.core.calculations import calculate_all_percentiles, evaluate_expression

from src.sheets.google.client import get_or_create_worksheet, write_and_format, move_sheet_to_position

logger = logging.getLogger(__name__)


def _compute_pct_by_mode(section_data, entity_type):
    """Compute percentile populations for all stat modes.

    Args:
        section_data: {base_section: data_list} e.g.
            {'current_stats': [...], 'historical_stats': [...], 'postseason_stats': [...]}
        entity_type: 'player', 'team', or 'opponents'

    Returns:
        {mode: {base_section: {col_key: sorted_values}}}
    """
    result = {}
    for mode in STAT_MODES:
        result[mode] = {}
        for section, data_list in section_data.items():
            if data_list:
                result[mode][section] = calculate_all_percentiles(
                    data_list, entity_type, mode)
            else:
                result[mode][section] = {}
    return result

# ============================================================================
# TEAM SHEET SYNC
# ============================================================================

def sync_team_sheet(ctx, client, spreadsheet, team_abbr,
                    team_name='', mode='per_possession',
                    show_advanced=False,
                    historical_config=None,
                    partial_update=False, precomputed=None,
                    sync_section=None):
    """Sync a single team's worksheet with merged row layout.

    All 3 stat modes are written simultaneously. The `mode` parameter controls
    which mode's columns are visible by default.
    """
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

        # ---- Percentile populations (all modes, league-wide) ----
        if precomputed:
            player_pct_by_mode = precomputed['player']
            team_pct_by_mode = precomputed['team']
            opp_pct_by_mode = precomputed['opponents']
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

            player_pct_by_mode = _compute_pct_by_mode({
                'current_stats': all_players_curr,
                'historical_stats': all_players_hist,
                'postseason_stats': all_players_post,
            }, 'player')
            team_pct_by_mode = _compute_pct_by_mode({
                'current_stats': all_teams_curr['teams'],
                'historical_stats': all_teams_hist['teams'],
                'postseason_stats': all_teams_post['teams'],
            }, 'team')
            opp_pct_by_mode = _compute_pct_by_mode({
                'current_stats': all_teams_curr['opponents'],
                'historical_stats': all_teams_hist['opponents'],
                'postseason_stats': all_teams_post['opponents'],
            }, 'opponents')

        # ---- Column structure (tripled stats sections) ----
        columns = build_sheet_columns(
            entity='player', stats_mode='both',
            sheet_type='team', default_mode=mode)

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

        # ---- Build merged player rows (all modes at once) ----
        data_rows = []
        all_percentile_cells = []

        for pid in all_player_ids:
            row, pct_cells = build_merged_entity_row(
                player_id=pid,
                columns_list=columns,
                current_data=curr_by_id.get(pid),
                historical_data=hist_by_id.get(pid),
                postseason_data=post_by_id.get(pid),
                pct_by_mode=player_pct_by_mode,
                entity_type='player',
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
            pct_by_mode=team_pct_by_mode,
            entity_type='team',
        )
        opp_row, opp_pct_cells = build_merged_entity_row(
            player_id=None,
            columns_list=columns,
            current_data=team_data_curr.get('opponent') or None,
            historical_data=team_data_hist.get('opponent') or None,
            postseason_data=team_data_post.get('opponent') or None,
            pct_by_mode=opp_pct_by_mode,
            entity_type='opponents',
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
            partial_update, build_fn=build_formatting_requests,
        )

        logger.info(
            f'  {team_abbr} done: {len(all_player_ids)} players (merged), '
            f'{len(all_percentile_cells)} percentile cells'
        )

    finally:
        conn.close()

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

def sync_teams_sheet(ctx, client, spreadsheet, mode='per_possession',
                     show_advanced=False,
                     historical_config=None,
                     partial_update=False,
                     sync_section=None):
    """Sync the league-wide Teams sheet with all stat modes."""
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

        # ---- Team percentile populations (all modes) ----
        team_pct_by_mode = _compute_pct_by_mode({
            'current_stats': all_teams_curr['teams'],
            'historical_stats': all_teams_hist['teams'],
            'postseason_stats': all_teams_post['teams'],
        }, 'team')

        # ---- Column structure (tripled stats sections) ----
        columns = build_sheet_columns(
            entity='team', stats_mode='both',
            sheet_type='teams', default_mode=mode)

        # ---- Opponent percentile populations (base-section-keyed) ----
        _data_by_base = {
            'current_stats': full_curr,
            'historical_stats': full_hist,
            'postseason_stats': full_post,
        }
        opp_percentiles = {}
        seen_opp = set()
        for entry in columns:
            col_key, col_def, _, section_ctx = entry
            if not col_def.get('is_opponent_col'):
                continue
            base_section = section_ctx.split('__')[0] if '__' in section_ctx else section_ctx
            dedup_key = (col_key, base_section)
            if dedup_key in seen_opp:
                continue
            seen_opp.add(dedup_key)
            formula = col_def.get('values', {}).get('team')
            if not formula:
                continue
            data_list = _data_by_base.get(base_section)
            if not data_list:
                continue
            values = []
            for d in data_list:
                val = evaluate_expression(formula, d, col_def)
                if val is not None:
                    values.append(val)
            if values:
                if col_key not in opp_percentiles:
                    opp_percentiles[col_key] = {}
                opp_percentiles[col_key][base_section] = ctx.wrap_opp_pct(values)

        # ---- Team names ----
        teams_db = ctx.etl_lib.get_teams_from_db()
        team_names_map = {abbr: name for _, (abbr, name) in teams_db.items()}
        abbrs = [abbr for _, (abbr, name) in teams_db.items()]

        # ---- Headers ----
        headers = build_headers(
            columns, mode=mode, team_name='Teams',
            current_season=current_season,
            historical_config=historical_config)

        # ---- Build team rows (all modes at once) ----
        data_rows = []
        all_percentile_cells = []

        for abbr in abbrs:
            curr_data = curr_by_abbr.get(abbr)
            hist_data = hist_by_abbr.get(abbr)
            post_data = post_by_abbr.get(abbr)

            for d in [curr_data, hist_data, post_data]:
                if d:
                    d['TEAM'] = team_names_map.get(abbr, abbr)

            row, pct_cells = build_merged_entity_row(
                player_id=None,
                columns_list=columns,
                current_data=curr_data,
                historical_data=hist_data,
                postseason_data=post_data,
                pct_by_mode=team_pct_by_mode,
                entity_type='team',
                opp_percentiles=opp_percentiles,
            )
            for cell in pct_cells:
                cell['row'] = fmt['data_start_row'] + len(data_rows)
            all_percentile_cells.extend(pct_cells)
            data_rows.append(row)

        n_team_rows = len(data_rows)

        # ---- Summary rows ----
        merged_pops = {}
        for mode_name in STAT_MODES:
            mode_pcts = team_pct_by_mode.get(mode_name, {})
            for section, pcts in mode_pcts.items():
                for k, v in pcts.items():
                    merged_pops[f'{section}__{mode_name}:{k}'] = v
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
            partial_update, build_fn=build_formatting_requests,
        )

        move_sheet_to_position(worksheet, 1)

        logger.info(
            f'  Teams sheet done: {n_team_rows} teams, '
            f'{len(all_percentile_cells)} percentile cells'
        )
    finally:
        conn.close()

# ============================================================================
# PLAYERS SHEET SYNC (ALL PLAYERS, NO TEAM/OPP AGGREGATE ROWS)
# ============================================================================

def sync_players_sheet(ctx, client, spreadsheet, mode='per_possession',
                       show_advanced=False,
                       historical_config=None,
                       partial_update=False,
                       sync_section=None):
    """Sync the league-wide Players sheet with all stat modes."""
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

        # ---- Percentile populations (all modes) ----
        player_pct_by_mode = _compute_pct_by_mode({
            'current_stats': all_players_curr,
            'historical_stats': all_players_hist,
            'postseason_stats': all_players_post,
        }, 'player')

        # ---- Column structure (tripled stats sections) ----
        columns = build_sheet_columns(
            entity='player', stats_mode='both',
            sheet_type='players', default_mode=mode)

        # ---- Headers ----
        headers = build_headers(
            columns, mode=mode, team_name='Players',
            current_season=current_season,
            historical_config=historical_config)

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

        # ---- Build player rows (all modes at once) ----
        data_rows = []
        all_percentile_cells = []

        for pid in all_player_ids:
            row, pct_cells = build_merged_entity_row(
                player_id=pid,
                columns_list=columns,
                current_data=curr_by_id.get(pid),
                historical_data=hist_by_id.get(pid),
                postseason_data=post_by_id.get(pid),
                pct_by_mode=player_pct_by_mode,
                entity_type='player',
            )
            for cell in pct_cells:
                cell['row'] = fmt['data_start_row'] + len(data_rows)
            all_percentile_cells.extend(pct_cells)
            data_rows.append(row)

        n_player_rows = len(data_rows)

        # ---- Summary rows ----
        merged_pops = {}
        for mode_name in STAT_MODES:
            mode_pcts = player_pct_by_mode.get(mode_name, {})
            for section, pcts in mode_pcts.items():
                for k, v in pcts.items():
                    merged_pops[f'{section}__{mode_name}:{k}'] = v
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
            partial_update, build_fn=build_formatting_requests,
        )

        move_sheet_to_position(worksheet, 0)

        logger.info(
            f'  Players sheet done: {n_player_rows} players, '
            f'{len(all_percentile_cells)} percentile cells'
        )
    finally:
        conn.close()
