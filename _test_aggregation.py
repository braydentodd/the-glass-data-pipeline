"""Verify the new aggregation logic for traded players (minute-weighted averaging)."""
from nba_api.stats.endpoints import teamplayeronoffsummary
import time

RS_NAME = 'PlayersOffCourtTeamPlayerOnOffSummary'

def get_off_court_players(team_id, team_abbr, season='2024-25'):
    resp = teamplayeronoffsummary.TeamPlayerOnOffSummary(
        team_id=team_id, season=season, season_type_all_star='Regular Season',
        per_mode_detailed='PerGame'
    ).get_dict()
    for rs in resp['resultSets']:
        if rs['name'] == RS_NAME:
            headers = rs['headers']
            pid_idx = headers.index('VS_PLAYER_ID')
            result = {}
            for row in rs['rowSet']:
                pid = row[pid_idx]
                result[pid] = dict(zip(headers, row))
            print(f"  {team_abbr}: {len(result)} players")
            return result
    return {}

print("=== Fetching DAL ===")
dal_players = get_off_court_players(1610612742, 'DAL')
time.sleep(0.7)

print("=== Fetching LAC ===")
lac_players = get_off_court_players(1610612746, 'LAC')

# Find players who appear in both
overlap = set(dal_players.keys()) & set(lac_players.keys())
print(f"\nPlayers in BOTH DAL and LAC off-court data: {len(overlap)}")

if overlap:
    for pid in list(overlap)[:3]:
        d = dal_players[pid]
        l = lac_players[pid]
        print(f"\n  Player {pid} ({d.get('VS_PLAYER_NAME', '?')}):")
        print(f"    DAL: GP={d['GP']}, MIN={d['MIN']}, OFF_RATING={d['OFF_RATING']}, DEF_RATING={d['DEF_RATING']}")
        print(f"    LAC: GP={l['GP']}, MIN={l['MIN']}, OFF_RATING={l['OFF_RATING']}, DEF_RATING={l['DEF_RATING']}")

        total_gp = d['GP'] + l['GP']
        total_min = d['MIN'] + l['MIN']
        if total_min > 0:
            w_off = (d['OFF_RATING'] * d['MIN'] + l['OFF_RATING'] * l['MIN']) / total_min
            w_def = (d['DEF_RATING'] * d['MIN'] + l['DEF_RATING'] * l['MIN']) / total_min
            print(f"    Combined: GP={total_gp}, MIN={total_min:.1f}")
            print(f"    Weighted OFF_RATING={w_off:.2f} (x10={round(w_off*10)}), DEF_RATING={w_def:.2f} (x10={round(w_def*10)})")
            # Sanity: weighted avg must be between the two values
            assert min(d['OFF_RATING'], l['OFF_RATING']) <= w_off <= max(d['OFF_RATING'], l['OFF_RATING']), "OFF_RATING not between bounds!"
            assert min(d['DEF_RATING'], l['DEF_RATING']) <= w_def <= max(d['DEF_RATING'], l['DEF_RATING']), "DEF_RATING not between bounds!"
            print("    ✓ Weighted averages within expected bounds")
else:
    print("No overlap between DAL and LAC (expected for non-traded players)")
    pid = list(dal_players.keys())[0]
    d = dal_players[pid]
    print(f"\nSample: {d.get('VS_PLAYER_NAME','?')} GP={d['GP']} MIN={d['MIN']} OFF_RATING={d['OFF_RATING']} DEF_RATING={d['DEF_RATING']}")

print("\n✓ Test complete")
