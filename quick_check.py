import os, sys
sys.path.insert(0, '.')
from config.etl import DB_CONFIG
import psycopg2

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

print('='*80)
print('CURRENT DATA - Before re-running ETL with fix')
print('='*80)

cursor.execute("""
    SELECT season_type, games_played, cont_3fgm, open_3fgm, cont_2fgm, open_2fgm, putbacks
    FROM player_season_stats
    WHERE player_id = 1631170 AND year = '2024-25'
    ORDER BY season_type
""")

print('\nPlayer (Jaime): ST | GP | cC3M | oC3M | c2M | o2M | PB')
print('-'*80)

rows = cursor.fetchall()
for row in rows:
    st = {1: 'RS', 2: 'PO', 3: 'PI'}[row[0]]
    print(f'{st} | {row[1]:2} | {row[2]:4} | {row[3]:4} | {row[4]:3} | {row[5]:3} | {row[6] if row[6] else "NULL":>4}')

if len(rows) >= 2:
    rs, po = rows[0][2:], rows[1][2:]
    print(f'\n{"❌ BROKEN" if rs == po else "✅ FIXED"}: RS == PO? {rs == po}')

print('\n' + '='*80)
print('NOW RE-RUN ETL TO TEST THE FIX!')
print('='*80)
