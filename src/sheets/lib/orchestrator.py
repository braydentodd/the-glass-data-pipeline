import logging
from collections import defaultdict
import time

from src.db import get_db_connection
from src.sheets.lib.db import fetch_all_players, fetch_all_teams
from sheets.lib.calculations import calculate_all_percentiles
from src.sheets.lib.google.client import get_sheets_client

from src.sheets.lib.worksheets.team import sync_team_sheet
from src.sheets.lib.worksheets.players import sync_players_sheet
from src.sheets.lib.worksheets.teams import sync_teams_sheet

logger = logging.getLogger(__name__)

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
    abbrs = list(team_names.keys())

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
    
def _get_team_names(ctx):
    """Get {abbr: display_name} dict from the league's ETL lib."""
    teams_db = ctx.etl_lib.get_teams_from_db()
    return {abbr: name for _, (abbr, name) in teams_db.items()}