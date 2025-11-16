# The Glass - Cleanup & Reorganization Plan

## Current System Analysis

### What You Have
1. **Frontend**: Google Sheets with Apps Script (Code.js)
2. **API**: Flask API (api.py) running on Oracle Cloud (150.136.255.23:5001)
3. **Database**: PostgreSQL with player/team/stats data
4. **Automation**: GitHub Actions workflows running on self-hosted runner
5. **Scripts**: Multiple Python scripts for data updates

### Current Workflow
1. **Nightly (5 AM EST)**: GitHub Action runs `nightly_etl_job.py`
   - Updates player rosters
   - Updates player season stats
   - Updates team season stats
2. **Nightly (5:30 AM EST)**: GitHub Action runs `sync_all_teams.py`
   - Syncs database → Google Sheets
3. **Monthly (1st of month)**: GitHub Action runs `monthly_player_update.py`
   - Updates player details (height, weight, etc.)
4. **On-Demand**: Users interact with Google Sheets
   - Stats View menu (totals/per game/per minute)
   - Historical stats configuration
   - Percentile display toggle
   - Manual data entry (wingspan, notes)

---

## Immediate Bug Fixes

### ✅ Bug #1: AQ Column Merge
**Issue**: Historical stats header doesn't extend to column AQ (Fouls)
**Fix**: Changed `endColumnIndex: 42` → `43` (exclusive index)
**File**: `src/sync_all_teams.py` line 915
**Status**: FIXED

### ⏳ Bug #2: Include Current Year Not Working
**Issue**: Unchecking "Include current year" in Career mode still includes 2025-26
**Investigation Needed**: Logic appears correct, need to add debug logging
**Status**: INVESTIGATING

---

## Proposed Cleanup & Reorganization

### Phase 1: Consolidate Scripts (HIGH PRIORITY)

#### Current State - TOO MANY SCRIPTS:
```
scripts/
├── nightly_etl_job.py            # Orchestrator (KEEP)
├── nightly_player_roster_update.py  # Called by ETL
├── nightly_stats_update.py       # Called by ETL
├── nightly_team_stats_update.py  # Called by ETL
├── monthly_player_update.py      # Separate monthly job
├── backfill_historical_stats.py  # One-time utility
└── read_sheet_properties.py      # Debug utility
```

#### Proposed New Structure:
```
src/
├── etl/
│   ├── __init__.py
│   ├── nightly.py               # Main nightly ETL (consolidate 3 scripts)
│   └── monthly.py               # Monthly player details update
├── api/
│   ├── __init__.py
│   └── app.py                   # Flask API (rename from api.py)
├── database/
│   ├── __init__.py
│   ├── connection.py            # DB connection utilities
│   └── models.py                # Database models/queries
├── sheets/
│   ├── __init__.py
│   ├── sync.py                  # Google Sheets sync (from sync_all_teams.py)
│   └── formatter.py             # Sheet formatting logic
├── utils/
│   ├── __init__.py
│   ├── stats.py                 # Stat calculations (from stat_calculator.py)
│   └── logging.py               # Centralized logging
└── config.py                    # Central configuration (KEEP)
```

### Phase 2: Simplify GitHub Actions

#### Current State:
```
.github/workflows/
├── nightly_season_stats.yml     # Runs nightly_etl_job.py
├── sync_google_sheets.yml       # Runs sync_all_teams.py
└── monthly_player_update.yml    # Runs monthly_player_update.py
```

#### Proposed:
```
.github/workflows/
├── nightly_update.yml           # One workflow, two jobs:
│                                 #   1. ETL (update DB)
│                                 #   2. Sync (DB → Sheets)
└── monthly_update.yml           # Player details (once/month)
```

### Phase 3: Clean Up Root Directory

#### Files to Remove/Consolidate:
- ❌ `deploy_api.sh` → Use systemd service only
- ❌ `deploy_sync_to_server.sh` → Not needed (direct git pull)
- ❌ `deploy_updated_api.sh` → Consolidate deployment
- ❌ `sync_sheets.sh` → Not needed (GitHub Actions handles this)
- ❌ `api_deploy.tar.gz` → Old artifact
- ✅ `Code.js` → KEEP (Google Apps Script)
- ✅ `HistoricalStatsDialog.html` → KEEP (Apps Script HTML)
- ✅ `.env` → KEEP (local dev config)
- ✅ `requirements.txt` → KEEP (Python dependencies)
- ✅ `flask-api.service` → KEEP (systemd service)
- ✅ `README.md` → KEEP & UPDATE

#### Proposed Root Structure:
```
the-glass-data-pipeline/
├── .github/workflows/           # GitHub Actions
├── apps-script/                 # NEW: Apps Script files
│   ├── Code.js
│   └── HistoricalStatsDialog.html
├── src/                         # Reorganized Python code
│   ├── api/
│   ├── etl/
│   ├── database/
│   ├── sheets/
│   ├── utils/
│   └── config.py
├── scripts/                     # NEW: One-time utilities only
│   ├── backfill_historical_stats.py
│   └── debug_sheet_properties.py
├── .env                         # Local environment config
├── .gitignore
├── requirements.txt
├── flask-api.service            # Systemd service
├── README.md
└── DEPLOYMENT.md                # NEW: Deployment guide
```

### Phase 4: Improve Configuration Management

#### Current Issues:
- Environment variables scattered across files
- Hardcoded values (API_BASE_URL in Code.js)
- No clear separation of dev/prod config

#### Proposed Solution:
Create `.env.example`:
```bash
# Database
DB_HOST=localhost
DB_NAME=the_glass
DB_USER=postgres
DB_PASSWORD=your_password_here

# NBA API
API_RATE_LIMIT_DELAY=3.0
SEASON_TYPE=1

# Google Sheets
GOOGLE_SHEET_ID=1kqVNHu8cs4lFAEAflI4Ow77oEZEusX7_VpQ6xt8CgB4
GOOGLE_CREDENTIALS_PATH=./google-credentials.json

# Flask API
FLASK_HOST=0.0.0.0
FLASK_PORT=5001
FLASK_DEBUG=False
```

Update `Code.js` to use ScriptProperties for API_BASE_URL instead of hardcoding.

---

## Implementation Steps

### Step 1: Fix Immediate Bugs ⏳
- [x] Fix AQ column merge
- [ ] Debug include_current issue (add logging)
- [ ] Test and verify fixes

### Step 2: Create New Directory Structure
```bash
mkdir -p src/{api,etl,database,sheets,utils}
mkdir -p apps-script
mkdir -p scripts_old  # Backup old scripts
```

### Step 3: Reorganize Code
1. Move and consolidate ETL scripts → `src/etl/nightly.py`
2. Rename `api.py` → `src/api/app.py`
3. Refactor `sync_all_teams.py` → `src/sheets/sync.py`
4. Extract DB utilities → `src/database/connection.py`
5. Move stats calc → `src/utils/stats.py`
6. Move Apps Script → `apps-script/`

### Step 4: Update GitHub Actions
1. Combine nightly workflows
2. Update script paths
3. Test workflows

### Step 5: Update Documentation
1. Update README.md with new structure
2. Create DEPLOYMENT.md
3. Create .env.example

### Step 6: Clean Up Root
1. Move old scripts to `scripts_old/`
2. Remove unnecessary deploy scripts
3. Update .gitignore

---

## Questions for You

1. **Deployment**: How do you currently deploy API changes to the Oracle Cloud server?
   - SSH and git pull?
   - Manual file upload?
   - Should we create a simple deployment script?

2. **Scripts**: Do you ever need to run these manually, or only via GitHub Actions?
   - If manual: Keep as standalone scripts
   - If automated only: Can consolidate into modules

3. **Database**: Do you have database migrations tracked anywhere?
   - Should we create a migrations/ folder?

4. **Testing**: Any existing tests we should preserve?

5. **Backup**: Before we start moving files, should we create a backup branch?

---

## Next Steps

**Immediate Actions (Today)**:
1. ✅ Fix AQ column merge bug
2. ⏳ Add debug logging for include_current
3. Deploy fixes and test

**Short Term (This Week)**:
1. Create backup branch
2. Implement new directory structure
3. Consolidate ETL scripts
4. Update GitHub Actions

**Medium Term (Next Week)**:
1. Refactor API and sync code
2. Create deployment documentation
3. Clean up root directory
4. Update README

---

## Benefits of This Cleanup

1. **Maintainability**: Clear separation of concerns, easier to find code
2. **Reliability**: Fewer moving parts, less chance of errors
3. **Scalability**: Easier to add new features or data sources
4. **Documentation**: Clear structure makes onboarding easier
5. **Debugging**: Centralized logging, easier to trace issues

---

## Risk Assessment

**Low Risk**:
- Moving files (Git tracks renames)
- Creating new directories
- Documentation updates

**Medium Risk**:
- Consolidating scripts (requires testing)
- Updating GitHub Actions (test with workflow_dispatch first)

**High Risk**:
- Refactoring API code (currently running in production)
- Database schema changes (none planned)

**Mitigation**:
- Create backup branch before starting
- Test each change incrementally
- Use workflow_dispatch to test Actions manually
- Keep old files until everything works

---

Let me know which parts you want to tackle first!
