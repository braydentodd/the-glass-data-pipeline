# THE GLASS DATA PIPELINE

Automated data pipeline that extracts NBA game statistics from the NBA API and loads them into PostgreSQL

## 📊 What It Does

- **Extracts** player and team statistics from NBA API endpoints
- **Transforms** raw data into analytics-ready format
- **Loads** into PostgreSQL database
- **Runs automatically** every day year-round via GitHub Actions
- **Auto-detects** the current NBA season (changes July 1st)
- **Captures** all game types: Regular Season, Playoffs, PlayIn, Pre Season, and Summer League


### GitHub Actions Setup

1. **Add GitHub Secrets**
   
   Go to your repository → Settings → Secrets and variables → Actions → New repository secret


2. **Enable GitHub Actions**
   
   Go to your repository → Actions tab → Enable workflows

3. **Pipeline will run automatically**
   - Every day at 4 AM UTC (after most games finish)
   - Or trigger manually from Actions tab


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

## Running Backfills

To load historical data:

1. **Via GitHub Actions (recommended)**
   - Go to Actions → The Glass ETL Pipeline → Run workflow
   - Enter start date: `2024-10-22`
   - Enter end date: `2024-10-24`
   - Click "Run workflow"

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

### Season Settings
The NBA season is **automatically detected** based on the current date:
- **Before July 1**: Uses previous year's season (e.g., `2024-25` in May 2025)
- **July 1 or later**: Uses next season (e.g., `2025-26` in July 2025)

No manual updates needed! The code automatically handles the season transition.