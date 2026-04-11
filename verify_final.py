"""Final DB verification script – delete after use."""
from src.core.db import db_connection

with db_connection() as conn:
    with conn.cursor() as cur:
        print("=== ENTITY TABLES ===")
        cur.execute("SELECT count(*) FROM nba.players")
        print(f"Total players: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM nba.players WHERE name IS NOT NULL")
        print(f"Players with names: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM nba.players WHERE height_ins > 0")
        print(f"Players with height: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM nba.players WHERE weight_lbs > 0")
        print(f"Players with weight: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM nba.players WHERE rookie_season IS NOT NULL")
        print(f"Players with rookie_season: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM nba.teams")
        print(f"Total teams: {cur.fetchone()[0]}")

        print("\n=== STATS TABLES ===")
        cur.execute(
            "SELECT season_type, count(*) FROM nba.player_season_stats "
            "GROUP BY season_type ORDER BY season_type"
        )
        for row in cur.fetchall():
            print(f"Player stats ({row[0]}): {row[1]} rows")
        cur.execute(
            "SELECT season_type, count(*) FROM nba.team_season_stats "
            "GROUP BY season_type ORDER BY season_type"
        )
        for row in cur.fetchall():
            print(f"Team stats ({row[0]}): {row[1]} rows")

        print("\n=== SEASON COVERAGE ===")
        cur.execute(
            "SELECT season, season_type, count(*) FROM nba.player_season_stats "
            "GROUP BY season, season_type ORDER BY season, season_type"
        )
        for row in cur.fetchall():
            print(f"  {row[0]} ({row[1]}): {row[2]} players")

        print("\n=== SAMPLE BIO DATA ===")
        cur.execute(
            "SELECT name, height_ins, weight_lbs, rookie_season "
            "FROM nba.players "
            "WHERE name IS NOT NULL AND height_ins IS NOT NULL "
            "ORDER BY name LIMIT 5"
        )
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}in, {row[2]}lbs, rookie={row[3]}")
