-- Add missing defense tracking columns to player_season_stats
-- These were defined in config but never added to the database

ALTER TABLE player_season_stats
    ADD COLUMN IF NOT EXISTS d_rim_fgm SMALLINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS d_rim_fga SMALLINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS d_2fgm SMALLINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS d_2fga SMALLINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS d_3fgm SMALLINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS d_3fga SMALLINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS real_def_fg_pct_x1000 SMALLINT DEFAULT 0;

-- Verification
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'player_season_stats' 
  AND column_name LIKE 'd_%'
ORDER BY column_name;
