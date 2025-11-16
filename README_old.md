# The Glass - NBA Data Pipeline

An automated NBA basketball analytics pipeline that syncs player statistics from the NBA API to an OCI PostgreSQL database and Google Sheets with percentile-based color coding.

## Features

- **Automated Data Collection**: Nightly ETL jobs fetch player rosters, stats, and team data from the NBA API
- **Database Storage**: OCI PostgreSQL database stores historical player and team statistics
- **Google Sheets Integration**: Syncs all 30 NBA teams to Google Sheets with color-coded percentile rankings
- **Interactive Stats API**: Flask API for switching between stat modes (totals, per-game, per-100, per-36, custom)
- **Percentile Color Coding**: Visual representation of player performance (Red 0-33%, Yellow 33-66%, Green 66-100%)
- **GitHub Actions**: Automated workflows for nightly updates and monthly roster syncs

## Project Structure

```
the-glass-data-pipeline/
├── src/                               # Core synchronization modules
│   ├── config.py                      # Centralized configuration
│   ├── sync_all_teams.py              # Sync all 30 teams to Google Sheets
│   ├── stat_calculator.py             # Stat calculation utilities (multiple modes)
│   └── api.py                         # Flask API for interactive stats
│
├── scripts/                           # ETL and automation scripts
│   ├── nightly_etl_job.py             # ETL orchestrator
│   ├── nightly_player_roster_update.py # Player roster updates
│   ├── nightly_stats_update.py        # Player stats updates
│   ├── nightly_team_stats_update.py   # Team stats updates
│   ├── monthly_player_update.py       # Monthly comprehensive update
│   ├── backfill_historical_stats.py   # Historical data backfill utility
│   └── test_api.py                    # API test suite
│
├── .github/
│   └── workflows/                     # GitHub Actions workflows
│       ├── nightly_season_stats.yml   # Runs daily at 5 AM EST
│       ├── monthly_player_update.yml  # Runs 1st of each month
│       └── sync_google_sheets.yml     # Manual/scheduled Sheets sync
│
├── google_apps_script.gs              # Google Sheets Apps Script for UI
├── API_SETUP_GUIDE.md                 # Comprehensive API deployment guide
├── README.md
├── requirements.txt
└── .gitignore
```

## Setup

### Prerequisites

- Python 3.11+
- OCI PostgreSQL database
- Google Cloud Service Account with Sheets API access
- GitHub repository (for automated workflows)

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd the-glass-data-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   
   Create a `.env` file in the project root:
   ```bash
   # Database Configuration
   DB_HOST=your_database_host
   DB_NAME=the_glass_db
   DB_USER=the_glass_user
   DB_PASSWORD=your_password
   
   # Google Sheets (optional, defaults work for most cases)
   SPREADSHEET_NAME=The Glass
   ```

5. **Add Google credentials**
   
   Place your `google-credentials.json` service account key in the project root.

### Configuration

All configuration is centralized in `src/config.py`:

- **Database**: Connection parameters (supports environment variables)
- **Google Sheets**: Spreadsheet name, credentials file, scopes
- **NBA API**: Current season year (auto-calculated), season type, rate limiting
- **Stats**: 17 tracked statistics with column mappings
- **Formatting**: Colors, fonts, column widths, frozen panes
- **Percentiles**: Thresholds, weight factors, color gradients

## Usage

### Interactive Stats API

The Glass now includes a Flask API for switching between different stat modes in Google Sheets.

See **[API_SETUP_GUIDE.md](./API_SETUP_GUIDE.md)** for complete setup and deployment instructions.

**Quick Start:**

```bash
# Install dependencies
pip install -r requirements.txt

# Start API locally
DB_PASSWORD='your_password' python -m src.api

# Test all endpoints
python scripts/test_api.py
```

**Supported Modes:**
- **Totals**: Season cumulative stats
- **Per Game**: Stats per game played
- **Per 100 Possessions**: Current default
- **Per 36 Minutes**: Standard game length
- **Per X Minutes**: Custom minutes
- **Per X Possessions**: Custom possessions

### Sync All Teams to Google Sheets

```bash
DB_PASSWORD='your_password' python src/sync_all_teams.py
```

This creates/updates sheets for all 30 NBA teams with:
- Player roster information (name, jersey #, experience, age, height, wingspan, weight)
- 17 statistics (default: per-100 possession)
- Percentile-based color coding for quick visual analysis

### Run Nightly ETL

```bash
DB_PASSWORD='your_password' python scripts/nightly_etl_job.py
```

This orchestrates:
1. Player roster updates (new players, trades)
2. Player statistics updates (latest games)
3. Team statistics aggregation

### Backfill Historical Data

```bash
DB_PASSWORD='your_password' python scripts/backfill_historical_stats.py
```

## Tracked Statistics (Per 100 Possessions)

1. Games Played
2. Minutes
3. Points
4. True Shooting % (TS%)
5. 2-Point Attempts (2PA)
6. 2-Point % (2P%)
7. 3-Point Attempts (3PA)
8. 3-Point % (3P%)
9. Free Throw Attempts (FTA)
10. Free Throw % (FT%)
11. Assists (Ast)
12. Turnovers (Tov) - *reversed scale*
13. Offensive Rebound % (OR%)
14. Defensive Rebound % (DR%)
15. Steals (Stl)
16. Blocks (Blk)
17. Fouls (Fls) - *reversed scale*

## GitHub Actions

### Nightly Stats Update
- **Schedule**: Daily at 9:00 AM EST
- **Workflow**: `.github/workflows/nightly-stats-update.yml`
- **Tasks**: Updates rosters, player stats, and team stats

### Monthly Player Update
- **Schedule**: 1st day of each month at 3:00 AM EST
- **Workflow**: `.github/workflows/monthly-player-update.yml`
- **Tasks**: Comprehensive player database refresh

### Sheets Sync
- **Trigger**: Manual (workflow_dispatch)
- **Workflow**: `.github/workflows/sync-sheets.yml`
- **Tasks**: Syncs all 30 teams to Google Sheets

## Database Schema

### Tables

- **teams**: NBA team information
- **players**: Player roster data
- **player_season_stats**: Per-season statistics per player
- **team_season_stats**: Aggregated team statistics

## Color Coding System

Statistics are color-coded based on percentile rankings across all players:

- 🔴 **Red (0-33%)**: Below average performance
- 🟡 **Yellow (33-66%)**: Average performance
- 🟢 **Green (66-100%)**: Above average performance

*Note: Turnovers and Fouls use reversed scale (lower is better)*

## Development

### Adding New Statistics

1. Add stat to `STAT_COLUMNS` in `src/config.py`
2. Update database query in sync scripts
3. Add column to `HEADERS['row_2']` in `src/config.py`
4. Update `SHEET_FORMAT['total_columns']` if needed

### Modifying Colors/Formatting

All visual styling is configured in `src/config.py`:
- `COLORS`: RGB values for red, yellow, green, black, white, gray
- `COLOR_THRESHOLDS`: Percentile breakpoints (default: 33, 66, 100)
- `SHEET_FORMAT`: Fonts, column widths, frozen rows/columns

## License

Private project for The Glass basketball analytics.

## Support

For issues or questions, contact the repository owner.
