# NBA API Shot Distance Research

## Summary of Findings

After researching the NBA API endpoints, here are the **actual** shot distance options available:

---

## 🎯 Shot Distance Categories Available

### 1. **Player/Team Shooting Data (with Closest Defender)**
**Endpoints:** `playerdashptshots`, `teamdashptshots`

**Result Sets:**
- `ClosestDefenderShooting` - ALL shots with defender distance breakdowns
  - Buckets: "0-2 Feet - Very Tight", "2-4 Feet - Tight", "4-6 Feet - Open", "6+ Feet - Wide Open"
  - NO shot distance filtering (returns all distances)
  
- `ClosestDefender10ftPlusShooting` - **ONLY shots from 10+ feet** with defender distance
  - Same defender distance buckets as above
  - **CRITICAL**: This is pre-filtered to shots 10ft+ from rim
  - This gives us a way to split by rim distance!

**Shot Distance Buckets Available:** ❌ NONE in the data
- The API does NOT return shot distance ranges like "Less Than 8ft", "8-16ft" etc.
- Only defender distance ranges are provided

---

### 2. **Player Defended Shot Data (Defense Tracking)**
**Endpoint:** `leaguedashptdefend`

**defense_category Parameter Options:**
- ✅ `'Overall'` - All shots defended
- ✅ `'3 Pointers'` - Only 3-point shots defended
- ✅ `'2 Pointers'` - Only 2-point shots defended
- ✅ `'Less Than 6Ft'` - **Shots within 6ft of rim** that player defended
- ✅ `'Less Than 10Ft'` - **Shots within 10ft of rim** that player defended
- ❌ `'6-10 ft'` - NOT SUPPORTED (errors)
- ❌ `'10-15 ft'` - NOT SUPPORTED
- ❌ `'15+ ft'` - NOT SUPPORTED

**CRITICAL**: Defense tracking has **Less Than 10Ft** as a native category!

---

### 3. **Player/Team Shooting Data (No Defender Info)**
**Endpoint:** `leaguedashptstats` with `pt_measure_type`

**Relevant Measure Types:**
- `'CatchShoot'` - Catch & shoot attempts
- `'PullUpShot'` - Pull-up attempts  
- `'Drives'` - Drive attempts
- ❌ No shot distance measure type available

---

## 🎯 RECOMMENDATION: Use 10ft as Universal Threshold

Based on the API capabilities, here's what's actually possible:

### Option 1: **10ft Threshold (RECOMMENDED)** ⭐
- **Why:** Native support in `ClosestDefender10ftPlusShooting` result set
- **Player/Team Shooting:**
  - Close (<10ft): `ClosestDefenderShooting` minus `ClosestDefender10ftPlusShooting`
  - Far (10ft+): `ClosestDefender10ftPlusShooting` directly
- **Defense Tracking:**
  - Close (<10ft): `defense_category='Less Than 10Ft'` ✅ PERFECT MATCH!
  - Far (10ft+): `defense_category='Overall'` minus `'Less Than 10Ft'`

### Option 2: **6ft Threshold**
- **Player/Team Shooting:** ❌ NO native support (would need manual calculation)
- **Defense Tracking:** ✅ `defense_category='Less Than 6Ft'`
- **Problem:** Can't cleanly split player/team shooting at 6ft

### Option 3: **8ft Threshold** 
- ❌ NOT SUPPORTED anywhere in the API

---

## 📊 Consistency Analysis for 4 Categories

### Using 10ft Threshold ✅ CONSISTENT

| Category | Close Shots (<10ft) | Far Shots (10ft+) | Defender Distance |
|----------|---------------------|-------------------|-------------------|
| **Player Shooting** | ClosestDefenderShooting - ClosestDefender10ftPlus | ClosestDefender10ftPlus | 4ft split ✅ |
| **Team Shooting** | ClosestDefenderShooting - ClosestDefender10ftPlus | ClosestDefender10ftPlus | 4ft split ✅ |
| **Player Defense** | defense_category='Less Than 10Ft' | Overall - Less Than 10Ft | N/A (inherently contested) |
| **Team Defense** | defense_category='Less Than 10Ft' | Overall - Less Than 10Ft | N/A (inherently contested) |

**Defender Distance Split (4ft):**
- Contested: "0-2 Feet - Very Tight" + "2-4 Feet - Tight"
- Open: "4-6 Feet - Open" + "6+ Feet - Wide Open"

---

## 🔧 Implementation Changes Needed

### Current Config (WRONG):
```python
SHOT_DISTANCE_API_MAP = {
    '<10ft': ['Less Than 8 ft.'],  # ❌ This bucket doesn't exist!
    '>=10ft': ['8-16 ft.', '16-24 ft.', '24+ ft.']  # ❌ These don't exist either!
}
```

### Correct Implementation:
We can't filter by shot distance buckets - the API doesn't provide them. Instead:

**For Player/Team Shooting:**
1. Query `ClosestDefenderShooting` result set → ALL shots
2. Query `ClosestDefender10ftPlusShooting` result set → 10ft+ shots only  
3. Calculate close shots: ALL - (10ft+)

**For Defense Tracking:**
1. Query with `defense_category='Less Than 10Ft'` → close shots
2. Query with `defense_category='Overall'` → all shots
3. Calculate far shots: Overall - Less Than 10Ft

---

## ✅ IMPLEMENTATION COMPLETE

**Config-Driven Result Set Subtraction Approach:**

### Config Changes ([config/etl.py](config/etl.py)):
1. **Removed**: `SHOT_DISTANCE_API_MAP` (fake buckets that don't exist)
2. **Kept**: `DEFENDER_DISTANCE_API_MAP` (4ft threshold for contested/open)
3. **Added**: `result_set_subtract` field in column configs

**Example Column Config:**
```python
'cont_close_2fgm': {
    'team_source': {
        'endpoint': 'teamdashptshots',
        'execution_tier': 'team',
        'result_set': 'ClosestDefenderShooting',  # ALL shots
        'result_set_subtract': 'ClosestDefender10ftPlusShooting',  # Subtract 10ft+ to get <10ft
        'defender_distance_category': 'contested',  # Filter by 4ft defender distance
        'field': 'FG2M'
    }
}
```

### ETL Changes ([src/etl.py](src/etl.py)):
1. **Removed**: `SHOT_DISTANCE_API_MAP` from imports
2. **Removed**: Broken `shot_distance` filter logic (tried to filter non-existent row buckets)
3. **Added**: Result set subtraction logic in `_execute_per_team_endpoint`:
   - Fetch BOTH result sets if `result_set_subtract` specified
   - Filter each by `defender_distance_category` (contested/open)
   - Calculate final: `entity_stats[col] = ALL - 10ft+`

### How It Works:
1. **For close shots** (cont_close_2fgm, open_close_2fgm):
   - Fetch `ClosestDefenderShooting` → filter by defender distance → get ALL shots
   - Fetch `ClosestDefender10ftPlusShooting` → filter by defender distance → get 10ft+ shots
   - Calculate: close shots = ALL - 10ft+

2. **For total shots** (cont_2fgm, open_2fgm):
   - Fetch `ClosestDefenderShooting` → filter by defender distance → get ALL shots
   - No subtraction needed

### Testing:
Run `python3 test_result_set_subtraction.py` to validate:
- ✅ Close shots < Total shots (proves subtraction worked)
- ✅ Contested ≠ Open for same distance (proves defender filter worked)

---

## ✅ Verdict

**Use 10ft as the universal threshold:**
- Native API support for player/team shooting (ClosestDefender10ftPlus)
- Native API support for defense tracking (Less Than 10Ft)
- Clean 4ft defender distance split for contested/open
- **Perfect consistency across all 4 shot data categories**

The current implementation trying to filter by "Less Than 8 ft." buckets is fundamentally wrong - those buckets don't exist in these endpoints!
