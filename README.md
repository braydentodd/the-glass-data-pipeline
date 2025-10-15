# THE GLASS DATA PIPELINE

Automated data pipeline that extracts NBA game statistics from the NBA API and loads them into PostgreSQL

## 📊 What It Does

- **Extracts** player and team statistics from NBA API endpoints
- **Transforms** raw data into analytics-ready format
- **Loads** into PostgreSQL database
- **Runs automatically** every day via GitHub Actions


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

## 🗄️ Database Schema

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
The pipeline runs **October 1 - June 30** at **6 AM UTC** daily.

To change the schedule, edit `.github/workflows/etl_pipeline.yml`:
```yaml
schedule:
  # October 1 - December 31 at 6 AM UTC
  - cron: '0 6 1-31 10-12 *'
  # January 1 - June 30 at 6 AM UTC
  - cron: '0 6 1-30 1-6 *'
```

### Season Settings
Edit `etl_pipeline.py` to customize:

```python
class Config:
    # Season
    CURRENT_SEASON = "2024-25"
    SEASON_TYPE = "Regular Season"
```