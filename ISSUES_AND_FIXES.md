# THE GLASS - Data Quality Issues & Fixes

## Issue 1: PlayIn NULLs ❓ POLICY DECISION NEEDED

**Status:** ✅ UNDERSTOOD - Not a bug, API limitation

**What:** 10 columns NULL in PlayIn for players with minutes:
- charges_drawn, deflections, contests (from `leaguehustlestatsplayer`)
- d_close_2fgm, d_close_2fga, d_2fgm, d_2fga, d_3fgm, d_3fga, real_d_fg_pct_x1000 (from `leaguedashptdefend`)

**Root Cause:** NBA API doesn't return hustle/defense tracking for ALL players in PlayIn
- API returns 83 players for hustle, 87 for defense
- Jaime Jaquez (1631170) has minutes_x10=13 but NOT in API results
- Limited tracking in PlayIn due to small sample size

**Fix Options:**
1. **Keep NULL** ✓ Accurate - API doesn't provide data
2. **Default to 0** - Assumption that no events occurred

**Recommendation:** Keep NULL - it's accurate and prevents false zeros

---

## Issue 2: Repeated Values Across Season Types ✅ FIXED

**Status:** ✅ FIXED - Removed hardcoded season types from config

**What:** 15 player + 1 team columns had identical values across RS/PO/PI:
- Shooting splits: cont_3fgm/fga, open_3fgm/fga, cont_2fgm/fga, open_2fgm/fga
- Rebounds: cont_d_rebs, cont_o_rebs  
- Putbacks: 12 (same across all season types)

**Example (Jaime Jaquez BEFORE fix):**
```
      | cont_3fgm | open_3fgm | cont_2fgm | putbacks
RS    |     0     |    37     |   145     |    12
PO    |     0     |    37     |   145     |    12  ← IDENTICAL!
PI    |     0     |    37     |   145     |    12  ← IDENTICAL!
```

**Root Cause:** Config had hardcoded `'season_type_all_star': 'Regular Season'` in endpoint_params
- Config at lines 764, 798, 832, 866, 905, 937, 969, 1001, 1033, 1064, 1095, 1126, 1246, 1273, 1325
- ETL was fetching Regular Season data for ALL season types

**Fix Applied:**
1. **Removed** all hardcoded `'season_type_all_star': 'Regular Season'` from config (sed command)
2. **Added** runtime override in ETL:
   - Team-based transformations: [src/etl.py#L2903-2911](src/etl.py#L2903-2911)
   - Player-based transformations: [src/etl.py#L3015-3023](src/etl.py#L3015-3023)

**ETL Logic:**
```python
# Load config params first (may have placeholder values)
endpoint_params = {'season': season, 'timeout': API_CONFIG['timeout_default']}
if 'endpoint_params' in first_transform:
    endpoint_params.update(first_transform['endpoint_params'])

# CRITICAL: Override season type with RUNTIME value
endpoint_params[season_type_param] = season_type_name  # 'Regular Season', 'Playoffs', or 'PlayIn'
```

**Config is correct:** `'season_type_param': 'season_type_all_star'` tells ETL WHICH parameter name to use, not the value!

**Expected After Fix:** Values should differ across season types proportional to games played

---

## Issue 3: Data Duplication (Contested = Open) ❌ NOT FIXED

**Status:** ❌ BUG IDENTIFIED - Aggregation filtering broken

**What:** Contested and open shots have identical values when they should differ:

**Team Stats (BROKEN):**
```
open_close_2fgm  = 1495
cont_close_2fgm  = 1495  ← IDENTICAL! Should be different

open_3fgm = 150
cont_3fgm = 150  ← IDENTICAL! Should be different
```

**Player Stats (WORKING for some reason):**
```
open_close_2fgm  = 28
cont_close_2fgm  = 141  ← DIFFERENT ✓
```

**Root Cause:** Per-team aggregation in `_execute_per_team_endpoint` doesn't filter by `shot_distance`

**Config at [config/etl.py#L847-850](config/etl.py#L847-850):**
```python
'team_source': {
    'endpoint': 'teamdashptshots',
    'execution_tier': 'team',
    'result_set': 'ClosestDefenderShooting',
    'defender_distance_category': 'open',  ← Checked
    'shot_distance': '<10ft',               ← NOT CHECKED!
    'field': 'FG2M',
    'transform': 'safe_int'
}
```

**Bug Location:** [src/etl.py#L971-991](src/etl.py#L971-991)
```python
# Check for defender_distance_category (contested vs open)
if 'defender_distance_category' in first_col_source:
    category = first_col_source['defender_distance_category']
    # ... filters by defender distance ...
    
# ⚠️  MISSING: No check for 'shot_distance' field!
# This causes ALL shot distances to be aggregated together
```

**Fix Needed:** Add shot_distance filtering logic in `_execute_per_team_endpoint`:
```python
# After defender_distance_category check, add:
if 'shot_distance' in first_col_source:
    shot_dist_value = first_col_source['shot_distance']
    # Check SHOT_DIST column in API result
    if 'SHOT_DIST' in headers:
        actual_dist = row[headers.index('SHOT_DIST')]
        # Map '<10ft' to actual API values
        if shot_dist_value == '<10ft':
            # Need to check what actual values API returns
            # Probably: 'Less Than 8 ft.' or 'Less Than 10 ft.'
            pass
```

---

## Testing Plan

### Quick Smoke Test (No Full ETL Run):
```bash
# Check current state
python3 quick_check.py

# Should show:
# ❌ BROKEN: RS == PO? True  ← Before fix
# ✅ FIXED: RS != PO? True   ← After ETL runs with fix
```

### Full Test After ETL:
```bash
# Run ETL in test mode
ETL_TEST_MODE=1 python3 -m src.etl --test

# Check results
python3 test_issue_analysis.py
```

---

## Summary

| Issue | Status | Action |
|-------|--------|--------|
| Issue 1: PlayIn NULLs | ✅ Understood | Policy decision: Keep NULL or default to 0? |
| Issue 2: Repeated Values | ✅ Fixed | Removed hardcoded season types, added runtime override |
| Issue 3: Duplication | ❌ Not Fixed | Need to add shot_distance filtering in aggregation |

**NEXT STEPS:**
1. ✅ Issue 2 fix is complete and ready to test
2. ❓ Issue 1 needs policy decision on NULL vs 0
3. ❌ Issue 3 needs code fix for shot_distance filtering
