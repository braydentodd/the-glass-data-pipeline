# THE GLASS DATA PIPELINE

Automated data pipeline that extracts NBA game statistics from the NBA API and loads them into PostgreSQL

## What It Does

- **Extracts** player and team statistics from NBA API endpoints
- **Transforms** raw data into analytics-ready format
- **Loads** into PostgreSQL database
- **Runs automatically** every day year-round via GitHub Actions
- **Auto-detects** the current NBA season (changes July 1st)
- **Captures** all game types: Regular Season, Playoffs, PlayIn, Pre Season, and Summer League


### GitHub Actions Setup

1. **Add GitHub Secrets**
   
   Go to your repository → Settings → Secrets and variables → Actions → New repository secret
   - `DB_HOST`: Your PostgreSQL host
   - `DB_NAME`: Your database name
   - `DB_USER`: Your database user
   - `DB_PASSWORD`: Your database password

2. **Enable GitHub Actions**
   
   Go to your repository → Actions tab → Enable workflows

3. **Pipeline will run automatically**
   - Every day at 8 AM UTC (processes previous day's games)
   - Or trigger manually from Actions tab

⚠️ **Known Limitation**: GitHub Actions may experience timeouts connecting to the NBA API. If this occurs, run the pipeline locally or use a self-hosted runner.


## ETL Pipeline Details

### Extraction
- Fetches games for specified date(s)
- Auto-detects current NBA season (July 1 cutoff)
- Retrieves all season types: Regular Season, Playoffs, PlayIn, Pre Season, Summer League
- Retrieves box scores:
  - Traditional (points, rebounds, assists)
  - Advanced (ratings, efficiency)
  - Hustle (charges, deflections)
  - Scoring (shot zones, assisted %)
- Fetches shot chart data for every player
- Fetches defensive matchup data

### Transformation
- Merges data from multiple endpoints
- Calculates derived metrics (FG2A, FG2%)
- Aggregates shot location stats (rim, mid-range, open 3s)
- Aggregates matchup stats (contested 3s, defensive eFG%)
- Handles missing data gracefully
- Converts to database-ready format

### Loading
- Upserts into PostgreSQL (updates if exists)
- Handles conflicts automatically
- Updates timestamps

## Running the Pipeline

### Daily Automated Runs
The pipeline runs daily at 8 AM UTC and processes **yesterday's games** (since games finish late at night).

### Manual Runs

**Via GitHub Actions:**
- Go to Actions → The Glass ETL Pipeline → Run workflow
- Leave dates empty for yesterday's games
- Or enter specific dates for backfill:
  - Start date: `2024-10-22`
  - End date: `2024-10-24`
- Click "Run workflow"

**Locally (recommended for backfills):**
```bash
# Load environment variables and run for yesterday
export $(cat .env | xargs)
python etl_pipeline.py

# Or run for specific date range
START_DATE=2024-10-22 END_DATE=2024-10-24 python etl_pipeline.py
```

## Database Schema

The pipeline loads into these tables:
- `games` - Game metadata and scores
- `player_game_stats` - Per-game player statistics
- `players` - Player biographical data
- `teams` - Team information

## Monitoring

- **Logs**: Check the GitHub Actions run logs or `etl_pipeline.log` locally
- **Artifacts**: GitHub Actions saves log files for 30 days

## Configuration

### Schedule
The pipeline runs **daily at 8 AM UTC year-round** to capture:
- Regular season (October - April)
- Playoffs (April - June)
- Summer League (July)
- Pre-season (September - October)

To change the schedule, edit `.github/workflows/etl_pipeline.yml`:
```yaml
schedule:
  # Every day at 8 AM UTC
  - cron: '0 8 * * *'
```