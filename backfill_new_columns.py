#!/usr/bin/env python3
"""
Targeted backfill for new Real Def FG% columns and league-wide shot tracking.

Directly calls run_endpoint_backfill for ONLY these 4 endpoints:
  - leaguedashptdefend       (3 defense categories → real_d_*_pct_x1000)
  - leaguedashptteamdefend   (3 defense categories → real_d_*_pct_x1000)
  - leaguedashplayerptshot   (shot tracking: cont/open 2fg/3fg made/attempted)
  - leaguedashteamptshot     (shot tracking: same for teams)
"""
import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from nba.etl.nba_etl_config import ENDPOINTS_CONFIG, SEASON_TYPE_CONFIG
from nba.etl.nba_etl_lib import (
    db_connection,
    get_endpoint_parameter_combinations,
    calculate_current_season,
    ENDPOINT_TRACKER_TABLE,
)
from nba.etl.nba_etl_main import ETLContext, run_endpoint_backfill

# Targeted endpoints and their configs
TARGET_ENDPOINTS = [
    # (endpoint_name, entity, param_combinations_to_process)
    # None for params means use get_endpoint_parameter_combinations()
    ('leaguedashptdefend', 'player', [
        {'defense_category': 'Less Than 10Ft'},
        {'defense_category': '2 Pointers'},
        {'defense_category': '3 Pointers'},
    ]),
    ('leaguedashptteamdefend', 'team', [
        {'defense_category': 'Less Than 10Ft'},
        {'defense_category': '2 Pointers'},
        {'defense_category': '3 Pointers'},
    ]),
    ('leaguedashplayerptshot', 'player', None),
    ('leaguedashteamptshot', 'team', None),
]

def main():
    current_season = calculate_current_season()
    start_season = '2013-14'

    # Build season list: 2013-14 through current
    start_year = int('20' + start_season.split('-')[1])
    end_year = int('20' + current_season.split('-')[1])
    all_seasons = [f"{y-1}-{str(y)[-2:]}" for y in range(start_year, end_year + 1)]

    print(f"{'='*70}")
    print(f"TARGETED BACKFILL: Real Def FG% + League-wide Shot Tracking")
    print(f"  Seasons: {start_season} → {current_season} ({len(all_seasons)} seasons)")
    print(f"{'='*70}")

    ctx = ETLContext()
    total_ok = 0
    total_fail = 0
    total_skip = 0

    # Load completed combinations to skip
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT endpoint, year, season_type, params, entity
            FROM {ENDPOINT_TRACKER_TABLE}
            WHERE status = 'complete'
              AND (missing_data IS NULL OR missing_data = 'null'::jsonb)
        """)
        completed = set()
        for row in cursor.fetchall():
            ep, yr, st, p, ent = row
            try:
                pn = json.dumps(json.loads(p), sort_keys=True) if p and p != '{}' else '{}'
            except Exception:
                pn = p or '{}'
            completed.add((ep, yr, st, pn, ent))
        cursor.close()

    for endpoint_name, entity, param_list in TARGET_ENDPOINTS:
        ep_config = ENDPOINTS_CONFIG[endpoint_name]
        min_season = ep_config.get('min_season', '2013-14')
        min_year = int('20' + min_season.split('-')[1])
        scope = ep_config.get('execution_tier', 'league')

        # Resolve param combinations
        if param_list is None:
            param_list = get_endpoint_parameter_combinations(endpoint_name, entity)

        for params in param_list:
            params_str = json.dumps(params, sort_keys=True) if params else '{}'
            param_label = f" [{list(params.values())[0]}]" if params else ""

            print(f"\n{'='*70}")
            print(f"ENDPOINT: {endpoint_name} ({entity}){param_label}")
            print(f"{'='*70}")

            for season in all_seasons:
                season_year = int('20' + season.split('-')[1])
                if season_year < min_year:
                    continue

                for st_name, st_config in SEASON_TYPE_CONFIG.items():
                    season_type = st_config['season_code']
                    minimum_season = st_config.get('minimum_season')
                    if minimum_season and season_year < int('20' + minimum_season.split('-')[1]):
                        continue

                    # Skip already-complete
                    if (endpoint_name, season, season_type, params_str, entity) in completed:
                        total_skip += 1
                        continue

                    print()
                    try:
                        success = run_endpoint_backfill(
                            ctx, endpoint_name, season, season_type, scope, params,
                            entity=entity,
                            backfill_mode=False,
                        )
                    except Exception as e:
                        print(f"  ERROR: {e}")
                        success = False

                    if success:
                        total_ok += 1
                        completed.add((endpoint_name, season, season_type, params_str, entity))
                    else:
                        total_fail += 1

    print(f"\n{'='*70}")
    print(f"BACKFILL COMPLETE")
    print(f"  Processed: {total_ok} | Skipped: {total_skip} | Failed: {total_fail}")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
