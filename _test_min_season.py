"""Test min_season for teamplayeronoffsummary — how far back does it go?"""
import time
import sys

from nba_api.stats.library import http as _stats_http
from nba_api.library import http as _base_http
_NBA_STATS_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
    "Origin": "https://www.nba.com",
    "Pragma": "no-cache",
    "Referer": "https://www.nba.com/",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}
_stats_http.STATS_HEADERS = _NBA_STATS_HEADERS
_stats_http.NBAStatsHTTP.headers = _NBA_STATS_HEADERS
_stats_http.NBAStatsHTTP._session = None
_base_http.NBAHTTP._session = None

from nba_api.stats.endpoints import teamplayeronoffsummary
teamplayeronoffsummary.TeamPlayerOnOffSummary.expected_data = {}

# LAL = 1610612747
# Test seasons from oldest to newest, stop when we find data
# We already know 2007-08 works. Let's check older.
seasons_to_test = []

# TEST 2: Multi-team aggregation - Kai Jones (1630539) on DAL + LAC 2024-25
# What are his actual GP, MIN, OFF_RATING, DEF_RATING from each team?
import time as _t
print("=== Multi-team aggregation check: Kai Jones (1630539) ===")
for team_id, abbr in [(1610612742, 'DAL'), (1610612746, 'LAC')]:
    try:
        r = teamplayeronoffsummary.TeamPlayerOnOffSummary(
            team_id=team_id, season='2024-25', timeout=30
        )
        d = r.get_dict()
        for rs in d['resultSets']:
            if rs['name'] == 'PlayersOffCourtTeamPlayerOnOffSummary':
                headers = rs['headers']
                for row in rs['rowSet']:
                    rd = dict(zip(headers, row))
                    if rd['VS_PLAYER_ID'] == 1630539:
                        print(f"  {abbr}: GP={rd['GP']}, MIN={rd['MIN']}, "
                              f"OFF_RATING={rd['OFF_RATING']}, DEF_RATING={rd['DEF_RATING']}, "
                              f"PLUS_MINUS={rd['PLUS_MINUS']}, NET_RATING={rd['NET_RATING']}")
    except Exception as e:
        print(f"  {abbr}: ERROR - {e}")
    _t.sleep(2)

print("\nFor aggregation:")
print("  GP: sum (21 + 59 = 80)")
print("  MIN: sum")
print("  OFF_RATING: minute-weighted avg = (DAL_OFF * DAL_MIN + LAC_OFF * LAC_MIN) / (DAL_MIN + LAC_MIN)")
print("  DEF_RATING: same minute-weighted avg")

for season in seasons_to_test:
    try:
        r = teamplayeronoffsummary.TeamPlayerOnOffSummary(
            team_id=1610612747, season=season, timeout=30
        )
        d = r.get_dict()
        for rs in d['resultSets']:
            if rs['name'] == 'PlayersOffCourtTeamPlayerOnOffSummary':
                rows = rs['rowSet']
                headers = rs['headers']
                if rows:
                    first = dict(zip(headers, rows[0]))
                    print(f"{season}: {len(rows)} rows — "
                          f"first player: {first.get('VS_PLAYER_NAME')}, "
                          f"GP={first.get('GP')}, MIN={first.get('MIN')}, "
                          f"OFF_RATING={first.get('OFF_RATING')}, DEF_RATING={first.get('DEF_RATING')}")
                else:
                    print(f"{season}: 0 rows (empty)")
                break
    except Exception as e:
        print(f"{season}: ERROR — {type(e).__name__}: {str(e)[:120]}")
    time.sleep(2)

print("\nDone!")
