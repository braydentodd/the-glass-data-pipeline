"""One-off verification script — delete after use."""
from dotenv import load_dotenv
load_dotenv()

from src.core.db import get_db_connection

conn = get_db_connection()
cur = conn.cursor()

for t in ['players', 'teams', 'player_season_stats', 'team_season_stats']:
    cur.execute(f'SELECT COUNT(*) FROM nba.{t}')
    print(f'nba.{t}: {cur.fetchone()[0]} rows')

cur.execute(
    'SELECT season, season_type, COUNT(*) '
    'FROM nba.player_season_stats '
    'GROUP BY season, season_type ORDER BY season, season_type'
)
print('\nPlayer stats by season/type:')
for row in cur.fetchall():
    print(f'  {row[0]} {row[1]}: {row[2]} rows')

cur.execute(
    'SELECT season, season_type, COUNT(*) '
    'FROM nba.team_season_stats '
    'GROUP BY season, season_type ORDER BY season, season_type'
)
print('\nTeam stats by season/type:')
for row in cur.fetchall():
    print(f'  {row[0]} {row[1]}: {row[2]} rows')

cur.execute("SELECT count(*) FROM nba.players WHERE height_ins IS NOT NULL")
print(f'\nPlayers with height: {cur.fetchone()[0]}')
cur.execute("SELECT count(*) FROM nba.players WHERE weight_lbs IS NOT NULL")
print(f'Players with weight: {cur.fetchone()[0]}')
cur.execute("SELECT count(*) FROM nba.players WHERE from_year IS NOT NULL")
print(f'Players with rookie year: {cur.fetchone()[0]}')

conn.close()
