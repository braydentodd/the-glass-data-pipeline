# The Glass - NBA Data Pipeline

Automated ETL pipeline that syncs NBA statistics from the NBA API to PostgreSQL and Google Sheets.

## Quick Start

### Run ETL
\`\`\`bash
# Test mode (single player/team)
python3 -m src.etl --test

# Full ETL
python3 -m src.etl
\`\`\`

### Sync to Google Sheets
\`\`\`bash
python3 -m src.sheets_sync
\`\`\`

## Environment Variables

Create a `.env` file:
\`\`\`bash
DB_HOST=your_host
DB_NAME=the_glass_db
DB_USER=the_glass_user
DB_PASSWORD=your_password
\`\`\`

## Configuration

All settings in `config/etl.py`:
- Database schema and column definitions
- API endpoints and transformations
- Test subjects (player/team for validation)

## Project Structure

\`\`\`
├── src/
│   ├── etl.py           # Core ETL logic
│   ├── sheets_sync.py   # Google Sheets sync
│   └── api.py           # Flask API
├── config/
│   └── etl.py           # All configuration
└── apps-script/
    └── Code.js          # Google Sheets UI
\`\`\`
