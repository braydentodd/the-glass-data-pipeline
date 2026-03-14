/**
 * apps-script/Code.js
 *
 * Pure UI layer for The Glass NBA stats spreadsheet.
 * ALL data calculations are performed by the Python backend (src/sheets.py).
 *
 * Responsibilities:
 *   - Load config from /api/config (single source of truth)
 *   - Menu creation and user interactions
 *   - Trigger Python-side syncs via the API (including stat mode switching)
 *   - Write-back wingspan / notes edits to the DB via PATCH
 *   - Column visibility toggles (sections, advanced stats, percentiles)
 *
 * Explicitly NOT done here:
 *   - Stat calculations of any kind
 *   - Percentile math
 *   - Hardcoded team lists or column indices
 */

// ============================================================
// CONFIG
// ============================================================

/** Bootstrap URL — single place to change if server moves. */
var BOOTSTRAP_URL = 'http://150.136.255.23:5000/api/config';

var CONFIG = null;

/**
 * Load config from the Python API. Cached for the lifetime of the script run.
 * Falls back to safe defaults so the spreadsheet never hard-breaks.
 */
function loadConfig() {
  if (CONFIG) return CONFIG;

  try {
    var response = UrlFetchApp.fetch(BOOTSTRAP_URL, { muteHttpExceptions: true });
    if (response.getResponseCode() === 200) {
      CONFIG = JSON.parse(response.getContentText());
      return CONFIG;
    }
    Logger.log('loadConfig: API returned ' + response.getResponseCode());
  } catch (e) {
    Logger.log('loadConfig: failed to reach API - ' + e);
  }

  // Minimal fallback — derive base URL from BOOTSTRAP_URL
  var fallbackBase = BOOTSTRAP_URL.replace(/\/api\/config$/, '');
  CONFIG = {
    api_base_url: fallbackBase,
    nba_teams: {},
    column_ranges: { team_sheet: {}, nba_sheet: {} },
    column_indices: {},
    layout: { header_row_count: 4 },
    colors: {
      red:    { r: 238, g: 75,  b: 43  },
      yellow: { r: 252, g: 245, b: 95  },
      green:  { r: 76,  g: 187, b: 23  }
    }
  };
  return CONFIG;
}

function getApiBaseUrl() {
  return loadConfig().api_base_url || BOOTSTRAP_URL.replace(/\/api\/config$/, '');
}

function getColors() {
  return loadConfig().colors || {
    red:    { r: 238, g: 75,  b: 43  },
    yellow: { r: 252, g: 245, b: 95  },
    green:  { r: 76,  g: 187, b: 23  }
  };
}

// ============================================================
// MENU
// ============================================================

function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('Display Settings')
    .addSubMenu(ui.createMenu('Stat Mode')
      .addItem('Per 100 Possessions', 'switchToPer100')
      .addItem('Per Game',            'switchToPerGame')
      .addItem('Per 36 Minutes',      'switchToPer36')
      .addItem('Totals',              'switchToTotals'))
    .addSubMenu(ui.createMenu('Advanced Stats')
      .addItem('Show', 'showAdvancedStats')
      .addItem('Hide', 'hideAdvancedStats'))
    .addSubMenu(ui.createMenu('Percentiles')
      .addItem('Show', 'showPercentiles')
      .addItem('Hide', 'hidePercentiles'))
    .addSeparator()
    .addSubMenu(ui.createMenu('Player Info')
      .addItem('Show', 'showPlayerInfo')
      .addItem('Hide', 'hidePlayerInfo'))
    .addSubMenu(ui.createMenu('Analysis')
      .addItem('Show', 'showAnalysis')
      .addItem('Hide', 'hideAnalysis'))
    .addSubMenu(ui.createMenu('Current Stats')
      .addItem('Show', 'showCurrentStats')
      .addItem('Hide', 'hideCurrentStats'))
    .addSubMenu(ui.createMenu('Historical Stats')
      .addItem('Show', 'showHistoricalStats')
      .addItem('Hide', 'hideHistoricalStats'))
    .addSubMenu(ui.createMenu('Postseason Stats')
      .addItem('Show', 'showPostseasonStats')
      .addItem('Hide', 'hidePostseasonStats'))
    .addSeparator()
    .addItem('Historical Timeframe', 'showHistoricalStatsDialog')
    .addToUi();
}

// ============================================================
// EDIT TRIGGER - config-driven write-back to DB for all
//                editable fields (wingspan, hand, notes, …)
// ============================================================

function onEditInstallable(e) {
  var sheet     = e.range.getSheet();
  var sheetName = sheet.getName().toUpperCase();
  var config    = loadConfig();
  var nbaTeams  = config.nba_teams || {};
  var sheetType = _getSheetType(sheetName);

  if (!sheetType) return;

  var layout    = config.layout || {};
  var editedRow = e.range.getRow();
  if (editedRow <= (layout.header_row_count || 4)) return;

  var editedCol = e.range.getColumn();
  var ss        = SpreadsheetApp.getActiveSpreadsheet();

  // ---- Teams sheet: every row is a team, save notes to teams table ----
  if (sheetType === 'teams') {
    var teamsEditable = config.teams_editable_columns || [];
    var matched = null;
    for (var i = 0; i < teamsEditable.length; i++) {
      if (teamsEditable[i].col_index === editedCol) { matched = teamsEditable[i]; break; }
    }
    if (!matched) return;

    // Get entity name (team name or abbreviation) from Names column
    var entityName = sheet.getRange(editedRow, 1).getValue();
    if (!entityName) return;

    // Resolve team abbreviation (could be full name or abbreviation)
    var nameToAbbr = config.team_name_to_abbr || {};
    var teamAbbr = nbaTeams.hasOwnProperty(String(entityName).toUpperCase())
      ? String(entityName).toUpperCase()
      : nameToAbbr[entityName];
    if (!teamAbbr) {
      ss.toast('Could not identify team: ' + entityName, 'Error', 3);
      return;
    }
    var teamId = nbaTeams[teamAbbr];
    if (!teamId) {
      ss.toast('Could not find team: ' + teamAbbr, 'Error', 3);
      return;
    }
    try {
      updateTeamField(teamId, matched.db_field, e.range.getValue());
      ss.toast(matched.display_name + ' saved for ' + teamAbbr, 'Saved \u2713', 3);
    } catch (err) {
      ss.toast('Error saving: ' + err.message, 'Error', 5);
    }
    return;
  }

  // ---- Team/Players sheets: player or team row editing ----
  var editableColumns = config.editable_columns || [];
  var isPlayersSheet  = (sheetType === 'players');
  var matched         = null;

  for (var i = 0; i < editableColumns.length; i++) {
    var ec     = editableColumns[i];
    var colIdx = isPlayersSheet ? ec.nba_col_index : ec.team_col_index;
    if (colIdx === editedCol) { matched = ec; break; }
  }
  if (!matched) return;

  var entityName = sheet.getRange(editedRow, 1).getValue();
  if (!entityName) return;

  var colIndices = config.column_indices || {};
  var teamAbbr   = isPlayersSheet
    ? sheet.getRange(editedRow, colIndices.team || 2).getValue()
    : sheetName;

  var value = e.range.getValue();

  // ---- TEAM row: save to teams table ----
  if (entityName === 'TEAM') {
    if (matched.col_key !== 'notes') {
      ss.toast('Only notes can be edited for teams', 'Info', 3);
      return;
    }
    var teamId = nbaTeams[teamAbbr];
    if (!teamId) {
      ss.toast('Could not find team: ' + teamAbbr, 'Error', 3);
      return;
    }
    try {
      updateTeamField(teamId, 'notes', value);
      ss.toast('Notes saved for ' + teamAbbr, 'Saved \u2713', 3);
    } catch (err) {
      ss.toast('Error saving team notes: ' + err.message, 'Error', 5);
    }
    return;
  }

  // ---- OPPONENTS row: not editable ----
  if (entityName === 'OPPONENTS') {
    ss.toast('Opponent rows cannot be edited', 'Info', 3);
    return;
  }

  // ---- Player row: save via player API ----
  if (matched.col_key === 'wingspan') {
    value = parseWingspan(value);
    if (value === null) {
      ss.toast("Invalid wingspan. Use feet'inches (e.g. 6'8) or total inches.", 'Error', 5);
      return;
    }
  }

  var playerId = getPlayerIdByName(entityName, teamAbbr);
  if (!playerId) return;

  try {
    updatePlayerField(playerId, matched.db_field, value);
    ss.toast(matched.display_name + ' saved for ' + entityName, 'Saved \u2713', 3);
  } catch (err) {
    ss.toast('Error saving ' + matched.display_name + ': ' + err.message, 'Error', 5);
  }
}

// ============================================================
// MODE SWITCHING
// ============================================================

function switchToTotals()  { _switchStatMode('totals'); }
function switchToPerGame() { _switchStatMode('per_game'); }
function switchToPer36()   { _switchStatMode('per_36'); }
function switchToPer100()  { _switchStatMode('per_100'); }

/**
 * Switch stat mode by triggering a re-sync from the Python backend.
 * The backend rebuilds all sheets with the new mode's calculations.
 * Also updates section header text for immediate visual feedback.
 */
function _switchStatMode(newMode) {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var config = loadConfig();
  var props  = PropertiesService.getDocumentProperties();

  var currentMode = props.getProperty('STATS_MODE') || config.default_stat_mode || 'per_100';
  if (newMode === currentMode) {
    ss.toast('Already in ' + newMode + ' mode', 'Mode', 2);
    return;
  }

  // Update section headers immediately for visual feedback
  var sheets = ss.getSheets();
  for (var i = 0; i < sheets.length; i++) {
    var name = sheets[i].getName().toUpperCase();
    if (_getSheetType(name)) {
      _updateSectionHeaders(sheets[i], newMode);
    }
  }
  SpreadsheetApp.flush();

  // Trigger backend re-sync with the new mode
  triggerSync(newMode, { priorityTeam: _getActiveTeamAbbr() });
}

/**
 * Update section header text (row 1) to reflect the current stat mode.
 * Replaces mode labels in stats section headers.
 */
function _updateSectionHeaders(sheet, newMode) {
  var labels = {
    'per_100': 'per 100 Poss',
    'per_game': 'per Game',
    'per_36': 'per 36 Mins',
    'totals': 'Totals',
  };
  var allLabels = Object.values(labels);
  var newLabel = labels[newMode] || '';

  try {
    var lastCol = sheet.getMaxColumns();
    if (lastCol < 1) return;
    var values = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    // Write only the cells that change — setValues on the full range would
    // clear cell formatting (including borders) on every cell it touches.
    for (var i = 0; i < values.length; i++) {
      var text = String(values[i]);
      if (!text) continue;
      for (var j = 0; j < allLabels.length; j++) {
        if (text.indexOf(allLabels[j]) !== -1) {
          sheet.getRange(1, i + 1).setValue(text.replace(allLabels[j], newLabel));
          break;
        }
      }
    }
  } catch (e) {
    Logger.log('_updateSectionHeaders error: ' + e);
  }
}

// ============================================================
// ADVANCED STATS & PERCENTILES TOGGLE
// ============================================================

/**
 * Show/Hide advanced stat columns across all sheets.
 * Uses column_metadata for precise control.
 * Advanced toggle determines whether percentiles or values are shown (never both).
 */
function showAdvancedStats() { _setAdvancedStats(true); }
function hideAdvancedStats() { _setAdvancedStats(false); }

/** Keep legacy toggle for backward compatibility */
function toggleAdvancedStats() {
  var props = PropertiesService.getDocumentProperties();
  var current = props.getProperty('SHOW_ADVANCED') === 'true';
  _setAdvancedStats(!current);
}

// ============================================================
// DRY HELPERS — shared by all toggle functions
// ============================================================

/**
 * Apply a function to every managed sheet, active sheet first for responsiveness.
 *
 * @param {function(Sheet, string)} fn  - Receives (sheet, sheetType)
 * @param {string}  [toastMsg]         - Optional toast when done
 */
function _applyToAllSheets(fn, toastMsg) {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var active = ss.getActiveSheet();
  var sheets = ss.getSheets();

  var activeName = active.getName().toUpperCase();
  var activeType = _getSheetType(activeName);
  if (activeType) fn(active, activeType);
  SpreadsheetApp.flush();

  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getSheetId() === active.getSheetId()) continue;
    var name = sheets[i].getName().toUpperCase();
    var type = _getSheetType(name);
    if (type) fn(sheets[i], type);
  }
  if (toastMsg) ss.toast(toastMsg, 'View', 3);
}

/**
 * Batch show/hide columns from a sorted list of 1-indexed column numbers.
 * Groups contiguous columns into single showColumns/hideColumns calls
 * to minimize API round-trips (critical for staying under the 6-min limit).
 */
function _batchColumns(sheet, cols, show) {
  if (!cols.length) return;
  cols.sort(function(a, b) { return a - b; });
  var start = cols[0];
  var count = 1;
  for (var i = 1; i < cols.length; i++) {
    if (cols[i] === start + count) {
      count++;
    } else {
      try {
        if (show) sheet.showColumns(start, count);
        else      sheet.hideColumns(start, count);
      } catch (e) { Logger.log('_batchColumns error: ' + e); }
      start = cols[i];
      count = 1;
    }
  }
  try {
    if (show) sheet.showColumns(start, count);
    else      sheet.hideColumns(start, count);
  } catch (e) { Logger.log('_batchColumns error: ' + e); }
}

/**
 * Detect the current percentile/advanced state from the actual sheet.
 * Checks whether a known percentile column is visible (for pct state)
 * and whether the subsection row is visible (for advanced state).
 * This is more reliable than reading document properties, which can
 * become stale after a manual Python sync (./sync_sheets.sh).
 */
function _detectToggleState(sheet, sheetType) {
  var config   = loadConfig();
  var rangeKey = _getRangeKey(sheetType);
  var colMeta  = (config.column_metadata || {})[rangeKey] || [];

  // Detect percentile state: find the first stats pct column and check visibility
  var showPct = false;
  for (var i = 0; i < colMeta.length; i++) {
    if (colMeta[i].stats && colMeta[i].pct) {
      try { showPct = !sheet.isColumnHiddenByUser(colMeta[i].col); }
      catch (e) { /* fall through */ }
      break;
    }
  }

  // Detect advanced state: subsection row is visible iff advanced is on
  var showAdv = false;
  var subRow  = config.subsection_row_index || 2;
  try { showAdv = !sheet.isRowHiddenByUser(subRow); }
  catch (e) { /* fall through */ }

  return { showPct: showPct, showAdv: showAdv };
}

/**
 * Apply ALL vertical borders (both section and subsection) on a single sheet.
 * Config-driven: reads section_boundaries and subsection_boundaries from the API.
 *
 * Section borders:     Always present. Span row 1 through maxRows.
 *                      White in headers, black in data.
 * Subsection borders:  Only when showAdv=true. Span from subsection row through maxRows.
 *                      White in headers, black in data.
 *
 * When showPct=true AND a boundary has hp=true (has_percentile), the base
 * column is hidden so the border shifts +1 to the pct column.  Stale borders
 * on the "other" column are cleared to avoid ghosts after toggling.
 *
 * @param {Sheet}   sheet
 * @param {string}  sheetType
 * @param {boolean} showAdv   - Whether advanced/subsection borders are visible
 * @param {boolean} showPct   - Whether percentile columns are shown (shifts hp borders)
 */
function _applyVerticalBorders(sheet, sheetType, showAdv, showPct) {
  var config     = loadConfig();
  var rangeKey   = _getRangeKey(sheetType);
  var layout     = config.layout || {};
  var headerRows = layout.header_row_count || 4;
  var subRow     = config.subsection_row_index || 2;  // 1-indexed
  var maxRows    = sheet.getMaxRows();
  var maxCols    = sheet.getMaxColumns();

  /**
   * Core border setter for a single boundary.
   * @param {number}  baseCol    - 1-indexed base column of the boundary
   * @param {boolean} hasPercentile - Whether this boundary has a pct column at +1
   * @param {number}  startRow   - 1-indexed first row of the border
   * @param {boolean} shouldShow - Whether to apply or remove the border
   */
  function _setBoundaryBorder(baseCol, hasPercentile, startRow, shouldShow) {
    // For data rows the border shifts to the pct column when percentiles are shown.
    // For header rows (rows 1-2 which have merged cells) the border MUST stay on
    // baseCol — the merge anchor — regardless of showPct.  Setting a border on any
    // other column inside a merged range is invisible in Google Sheets.  Python also
    // always places borders at baseCol for this reason, letting the hidden-column
    // logic make the merge's visual left edge carry the border automatically.
    var dataCol = (showPct && hasPercentile) ? baseCol + 1 : baseCol;
    if (baseCol > maxCols) return;
    var firstDataRow = headerRows + 1;
    try {
      if (shouldShow) {
        // Header rows: always use baseCol (merge anchor in rows 1-2).
        // The border stays here even when baseCol is hidden — Google Sheets renders
        // it at the visual left edge of the merged cell.
        if (startRow <= headerRows && baseCol <= maxCols) {
          sheet.getRange(startRow, baseCol, headerRows - startRow + 1, 1)
               .setBorder(null, true, null, null, null, null,
                          '#FFFFFF', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
        }
        // Data rows: use dataCol (shifts to pct column when showPct && hp).
        if (maxRows >= firstDataRow && dataCol <= maxCols) {
          sheet.getRange(firstDataRow, dataCol, maxRows - firstDataRow + 1, 1)
               .setBorder(null, true, null, null, null, null,
                          '#000000', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
          // Clear stale border from data rows only (never touch header rows here).
          if (hasPercentile) {
            var otherDataCol = showPct ? baseCol : baseCol + 1;
            if (otherDataCol !== dataCol && otherDataCol > 0 && otherDataCol <= maxCols) {
              sheet.getRange(firstDataRow, otherDataCol, maxRows - firstDataRow + 1, 1)
                   .setBorder(null, false, null, null, null, null);
            }
          }
        }
      } else {
        // Remove border from BOTH base and pct columns (all rows)
        sheet.getRange(startRow, baseCol, maxRows - startRow + 1, 1)
             .setBorder(null, false, null, null, null, null);
        if (hasPercentile && baseCol + 1 <= maxCols) {
          sheet.getRange(startRow, baseCol + 1, maxRows - startRow + 1, 1)
               .setBorder(null, false, null, null, null, null);
        }
      }
    } catch (e) {
      Logger.log('Border error col ' + baseCol + ': ' + e);
    }
  }

  // --- Section borders: always present, full height (row 1 through maxRows) ---
  var secBounds = (config.section_boundaries || {})[rangeKey] || [];
  for (var s = 0; s < secBounds.length; s++) {
    var sec = secBounds[s];
    _setBoundaryBorder(sec.col, sec.hp, 1, true);  // always on, starts at row 1
  }

  // --- Subsection borders: only when advanced is visible, starts at subRow ---
  var subBounds = (config.subsection_boundaries || {})[rangeKey] || [];
  for (var b = 0; b < subBounds.length; b++) {
    var sub = subBounds[b];
    _setBoundaryBorder(sub.col, sub.hp, subRow, showAdv);
  }
}

function _setAdvancedStats(newAdvancedVisible) {
  var config = loadConfig();
  var props  = PropertiesService.getDocumentProperties();
  props.setProperty('SHOW_ADVANCED', newAdvancedVisible ? 'true' : 'false');

  var subRow = config.subsection_row_index || 2;  // 1-indexed

  // Detect actual percentile state from the active sheet and sync property
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var active = ss.getActiveSheet();
  var activeType = _getSheetType(active.getName().toUpperCase());
  if (activeType) {
    var detected = _detectToggleState(active, activeType);
    props.setProperty('SHOW_PERCENTILES', detected.showPct ? 'true' : 'false');
  }

  _applyToAllSheets(function(sheet, sheetType) {
    var rangeKey = _getRangeKey(sheetType);

    // Show/hide subsection header row
    try {
      if (newAdvancedVisible) sheet.showRows(subRow, 1);
      else                     sheet.hideRows(subRow, 1);
    } catch (e) { Logger.log('Subsection row toggle error: ' + e); }

    // Re-apply column visibility (adv/basic + pct swap) — batched
    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);

    // Vertical borders — uses shared helper that handles section + subsection + pct offset
    var curPct = props.getProperty('SHOW_PERCENTILES') === 'true';
    _applyVerticalBorders(sheet, sheetType, newAdvancedVisible, curPct);
  }, newAdvancedVisible ? 'Advanced stats shown' : 'Basic stats shown');
}

/**
 * Show/Hide percentile columns across all sheets.
 * Applies to ALL columns regardless of advanced/basic mode.
 * When shown, base value columns are hidden; when hidden, percentile columns hidden.
 * Also updates section headers: "Stats" ↔ "Percentiles"
 */
function showPercentiles() { _setPercentiles(true); }
function hidePercentiles() { _setPercentiles(false); }

/** Keep legacy toggle for backward compatibility */
function togglePercentiles() {
  var props = PropertiesService.getDocumentProperties();
  var current = props.getProperty('SHOW_PERCENTILES') === 'true';
  _setPercentiles(!current);
}

function _setPercentiles(showPct) {
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('SHOW_PERCENTILES', showPct ? 'true' : 'false');

  // Detect actual advanced state from the active sheet and sync property
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var active = ss.getActiveSheet();
  var activeType = _getSheetType(active.getName().toUpperCase());
  if (activeType) {
    var detected = _detectToggleState(active, activeType);
    props.setProperty('SHOW_ADVANCED', detected.showAdv ? 'true' : 'false');
  }

  _applyToAllSheets(function(sheet, sheetType) {
    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);
    _updateSectionHeadersForPercentiles(sheet, showPct);
    // Re-apply all vertical borders: in pct mode boundaries shift to pct column (+1)
    var curAdv = props.getProperty('SHOW_ADVANCED') === 'true';
    _applyVerticalBorders(sheet, sheetType, curAdv, showPct);
  }, showPct ? 'Percentiles shown' : 'Values shown');
}

/**
 * Update section headers to say "Percentiles" or "Stats" based on toggle.
 */
function _updateSectionHeadersForPercentiles(sheet, showPct) {
  try {
    var lastCol = sheet.getMaxColumns();
    if (lastCol < 1) return;
    var values = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    // Write only the cells that change — setValues on the full range would
    // clear cell formatting (including borders) on every cell it touches.
    for (var i = 0; i < values.length; i++) {
      var text = String(values[i]);
      if (!text) continue;
      if (showPct && text.indexOf(' Stats ') !== -1) {
        sheet.getRange(1, i + 1).setValue(text.replace(' Stats ', ' Percentiles '));
      } else if (!showPct && text.indexOf(' Percentiles ') !== -1) {
        sheet.getRange(1, i + 1).setValue(text.replace(' Percentiles ', ' Stats '));
      }
    }
  } catch (e) {
    Logger.log('_updateSectionHeadersForPercentiles error: ' + e);
  }
}

// ============================================================
// HISTORICAL / POSTSEASON TIMEFRAME DIALOG
// ============================================================

/**
 * Determine sheet type: 'team', 'players', 'teams', or null.
 */
function _getSheetType(sheetName) {
  var config = loadConfig();
  var nbaTeams = config.nba_teams || {};
  var upper = sheetName.toUpperCase();
  if (nbaTeams.hasOwnProperty(upper)) return 'team';
  if (upper === 'NBA' || upper === 'PLAYERS') return 'players';
  if (upper === 'TEAMS') return 'teams';
  return null;
}

/** Map sheet type to the config range key. */
function _getRangeKey(sheetType) {
  if (sheetType === 'team')    return 'team_sheet';
  if (sheetType === 'players') return 'nba_sheet';
  if (sheetType === 'teams')   return 'teams_sheet';
  return null;
}

/** Check if a sheet is a managed non-team sheet (Players, Teams, or legacy NBA). */
function _isNbaSheet(name) {
  return name === 'NBA' || name === 'PLAYERS' || name === 'TEAMS';
}

/** Get active team abbreviation if on a team sheet, else null. */
function _getActiveTeamAbbr() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var name = ss.getActiveSheet().getName().toUpperCase();
  var config = loadConfig();
  var nbaTeams = config.nba_teams || {};
  return nbaTeams.hasOwnProperty(name) ? name : null;
}

/** Apply show/hide to column ranges on a single sheet. */
function _applyRangeVisibility(sheet, rangeList, maxCols, visible) {
  for (var r = 0; r < rangeList.length; r++) {
    var rng = rangeList[r];
    if (rng.start > maxCols) continue;
    var count = Math.min(rng.count, maxCols - rng.start + 1);
    try {
      if (visible) sheet.showColumns(rng.start, count);
      else         sheet.hideColumns(rng.start, count);
    } catch (e) {
      Logger.log('_applyRangeVisibility error: ' + e);
    }
  }
}

// ============================================================
// HISTORICAL / POSTSEASON TIMEFRAME DIALOG
// ============================================================

function showHistoricalStatsDialog() {
  var template = HtmlService.createTemplateFromFile('HistoricalStatsDialog');
  var props = PropertiesService.getDocumentProperties();
  template.includeCurrentYear = props.getProperty('HIST_INCLUDE_CURRENT') || 'false';
  var html = template.evaluate()
    .setWidth(420)
    .setHeight(320);
  SpreadsheetApp.getUi().showModalDialog(html, 'Historical & Postseason Timeframe');
}

function parseHistoricalStatsInput(input) {
  if (!input) return null;
  var str = input.toString().trim();
  if (str.toLowerCase() === 'career' || str.toLowerCase() === 'c') return { mode: 'career' };
  if (/^\d+$/.test(str)) {
    var years = parseInt(str);
    if (years > 0 && years <= 30) return { mode: 'years', years: years };
    return null;
  }
  if (/^(\d{2,4})-(\d{2})$/.test(str)) return { mode: 'since_season', season: str };
  return null;
}

/**
 * Called by HistoricalStatsDialog — unified timeframe for both hist and post.
 */
function saveHistoricalStatsConfig(input, includeCurrentYear) {
  var result = parseHistoricalStatsInput(input);
  if (!result) return { success: false, error: 'Invalid input: ' + input };

  var props = PropertiesService.getDocumentProperties();
  props.setProperty('HIST_MODE', result.mode);
  props.setProperty('HIST_INCLUDE_CURRENT', includeCurrentYear ? 'true' : 'false');
  if (result.mode === 'years')        props.setProperty('HIST_YEARS', String(result.years));
  if (result.mode === 'since_season') props.setProperty('HIST_SEASON', result.season);

  triggerSync(null, { priorityTeam: _getActiveTeamAbbr() });
  return { success: true };
}

// ============================================================
// SYNC TRIGGER
// ============================================================

function triggerSync(mode, options) {
  options = options || {};
  var config  = loadConfig();
  var apiBase = config.api_base_url || getApiBaseUrl();
  var props   = PropertiesService.getDocumentProperties();

  if (mode) props.setProperty('STATS_MODE', mode);
  var statsMode = mode || props.getProperty('STATS_MODE');

  // If STATS_MODE property was never set (initial sync via CLI), detect from headers
  if (!statsMode) {
    var labels = {
      'per 100 Poss': 'per_100',
      'per Game':     'per_game',
      'per 36 Mins':  'per_36',
      'Totals':       'totals',
    };
    try {
      var active = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
      var lastCol = active.getMaxColumns();
      if (lastCol > 0) {
        var headerVals = active.getRange(1, 1, 1, lastCol).getValues()[0];
        for (var hi = 0; hi < headerVals.length; hi++) {
          var headerText = String(headerVals[hi]);
          for (var lbl in labels) {
            if (headerText.indexOf(lbl) !== -1) {
              statsMode = labels[lbl];
              props.setProperty('STATS_MODE', statsMode);
              break;
            }
          }
          if (statsMode) break;
        }
      }
    } catch (e) { Logger.log('Stats mode detection error: ' + e); }
    if (!statsMode) statsMode = 'per_100';  // ultimate fallback
  }

  var showPercentiles = (options.showPercentiles !== undefined)
    ? options.showPercentiles
    : (props.getProperty('SHOW_PERCENTILES') === 'true');

  var showAdvanced = (options.showAdvanced !== undefined)
    ? options.showAdvanced
    : (props.getProperty('SHOW_ADVANCED') === 'true');

  // Unified timeframe for both historical and postseason
  var histMode    = props.getProperty('HIST_MODE') || 'career';
  var histYears   = parseInt(props.getProperty('HIST_YEARS') || '25');
  var histSeason  = props.getProperty('HIST_SEASON') || null;
  var includeCurr = props.getProperty('HIST_INCLUDE_CURRENT') === 'true';

  var timeframe = {
    mode:            histMode,
    years:           histYears,
    season:          histSeason,
    include_current: includeCurr,
  };

  var payload = {
    stats_mode:       statsMode,
    mode:             histMode,               // 'career', 'years', or 'seasons'
    years:            histYears,
    include_current:  includeCurr,
    show_percentiles: showPercentiles,
    show_advanced:    showAdvanced,
    priority_team:    options.priorityTeam || null,
    data_only:        true,  // mode/timeframe switches use fast sync (skip structural formatting)
  };
  if (histSeason) payload.seasons = [histSeason];

  var url = apiBase + '/api/sync-historical-stats';

  try {
    SpreadsheetApp.getActiveSpreadsheet().toast('Syncing stats...', 'Update', 3);
    var response = UrlFetchApp.fetch(url, {
      method:             'post',
      contentType:        'application/json',
      payload:            JSON.stringify(payload),
      muteHttpExceptions: true
    });
    var code = response.getResponseCode();
    if (code === 200 || code === 202) {
      SpreadsheetApp.getActiveSpreadsheet().toast('Sync triggered - data will refresh shortly.', 'Update', 4);
    } else {
      var errMsg = String(code);
      try { errMsg = JSON.parse(response.getContentText()).error || errMsg; } catch (_) {}
      SpreadsheetApp.getActiveSpreadsheet().toast('Sync error: ' + errMsg, 'Error', 6);
    }
  } catch (err) {
    SpreadsheetApp.getActiveSpreadsheet().toast('Network error: ' + err.message, 'Error', 6);
    Logger.log('triggerSync error: ' + err);
  }
}

// ============================================================
// PLAYER FIELD UPDATE
// ============================================================

function getPlayerIdByName(playerName, teamAbbr) {
  var config   = loadConfig();
  var nbaTeams = config.nba_teams || {};
  var teamId   = nbaTeams[teamAbbr];
  if (!teamId) return null;
  var url = getApiBaseUrl() + '/api/team/' + teamId + '/players';
  try {
    var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    var data     = JSON.parse(response.getContentText());
    if (response.getResponseCode() === 200 && data.players) {
      var player = data.players.find(function(p) { return p.name === playerName; });
      return player ? player.player_id : null;
    }
  } catch (err) { Logger.log('getPlayerIdByName error: ' + err); }
  return null;
}

function updatePlayerField(playerId, fieldName, fieldValue) {
  var payload = {};
  payload[fieldName] = fieldValue;
  var response = UrlFetchApp.fetch(getApiBaseUrl() + '/api/player/' + playerId, {
    method: 'patch', contentType: 'application/json',
    payload: JSON.stringify(payload), muteHttpExceptions: true
  });
  var data = JSON.parse(response.getContentText());
  if (response.getResponseCode() !== 200) throw new Error(data.error || 'Unknown error');
  return data;
}

// ============================================================
// TEAM FIELD UPDATE
// ============================================================

function updateTeamField(teamId, fieldName, fieldValue) {
  var payload = {};
  payload[fieldName] = fieldValue;
  var response = UrlFetchApp.fetch(getApiBaseUrl() + '/api/teams/' + teamId, {
    method: 'put', contentType: 'application/json',
    payload: JSON.stringify(payload), muteHttpExceptions: true
  });
  var data = JSON.parse(response.getContentText());
  if (response.getResponseCode() !== 200) throw new Error(data.error || 'Unknown error');
  return data;
}

// ============================================================
// WINGSPAN PARSER
// ============================================================

function parseWingspan(value) {
  if (!value) return null;
  var str = value.toString().trim();
  var feetInches = str.match(/^(\d+)'(\d+)"?$/);
  if (feetInches) return parseInt(feetInches[1]) * 12 + parseInt(feetInches[2]);
  var inches = parseInt(str);
  if (!isNaN(inches) && inches > 0 && inches < 120) return inches;
  return null;
}

// ============================================================
// PERCENTILE COLOR HELPER
// ============================================================

function getPercentileColor(percentile) {
  var colors = getColors();
  var red = colors.red, yellow = colors.yellow, green = colors.green;
  var r, g, b;
  if (percentile < 50) {
    var t = percentile / 50;
    r = Math.round(red.r + (yellow.r - red.r) * t);
    g = Math.round(red.g + (yellow.g - red.g) * t);
    b = Math.round(red.b + (yellow.b - red.b) * t);
  } else {
    var t2 = (percentile - 50) / 50;
    r = Math.round(yellow.r + (green.r - yellow.r) * t2);
    g = Math.round(yellow.g + (green.g - yellow.g) * t2);
    b = Math.round(yellow.b + (green.b - yellow.b) * t2);
  }
  return '#' + r.toString(16).padStart(2, '0')
             + g.toString(16).padStart(2, '0')
             + b.toString(16).padStart(2, '0');
}

// ============================================================
// SECTION VISIBILITY TOGGLES
// ============================================================

/**
 * Re-hide columns that must always be hidden on certain sheet types.
 * Called after any toggle that shows columns (e.g. player_info section
 * on team sheets should never reveal the team column).
 */
function _rehideAlwaysHidden(sheet, sheetType) {
  var config     = loadConfig();
  var rangeKey   = _getRangeKey(sheetType);
  var hiddenCols = (config.always_hidden_columns || {})[rangeKey] || [];
  var maxCols    = sheet.getMaxColumns();
  var toHide = [];
  for (var h = 0; h < hiddenCols.length; h++) {
    if (hiddenCols[h] <= maxCols) toHide.push(hiddenCols[h]);
  }
  _batchColumns(sheet, toHide, false);
}

/**
 * Re-apply current toggle states (percentiles, advanced/basic) after any toggle.
 * Ensures toggles are DRY — showing a section doesn't reveal columns that should
 * be hidden by another toggle (e.g. percentile columns when in values mode,
 * basic columns when advanced is shown).
 *
 * Single-mode architecture: only one set of stat columns exists per sheet,
 * so no mode-based column hiding is needed.
 *
 * Uses column_metadata for per-column precision when available,
 * falling back to range-based approach otherwise.
 *
 * @param {Sheet}  sheet
 * @param {string} sheetType
 */
function _reapplyToggles(sheet, sheetType) {
  var config   = loadConfig();
  var props    = PropertiesService.getDocumentProperties();
  var rangeKey = _getRangeKey(sheetType);
  var maxCols  = sheet.getMaxColumns();

  var showPct = props.getProperty('SHOW_PERCENTILES') === 'true';
  var showAdv = props.getProperty('SHOW_ADVANCED') === 'true';

  // --- column_metadata path (per-column flags, batched) ---
  var colMeta = (config.column_metadata || {})[rangeKey];
  if (colMeta && colMeta.length > 0) {
    var showList = [];
    var hideList = [];

    // Map section context names → SECTION_VIS_ property keys so that the
    // isStats branch can honour section-level hide state (current/historical/postseason).
    var _secVisMap = {
      'current_stats':    'SECTION_VIS_CURRENT',
      'historical_stats': 'SECTION_VIS_HISTORICAL',
      'postseason_stats': 'SECTION_VIS_POSTSEASON',
      'analysis':         'SECTION_VIS_NOTES',
      'player_info':      'SECTION_VIS_PLAYER_INFO',
    };

    for (var i = 0; i < colMeta.length; i++) {
      var meta = colMeta[i];
      var colIdx = meta.col;
      if (colIdx > maxCols) continue;

      var isPct      = meta.pct || false;
      var isAdvanced = meta.adv || false;
      var isBasic    = meta.bas || false;
      var isStats    = meta.stats || false;
      var hasPercentile = meta.hp || false;
      var secName    = meta.sec || '';

      var shouldShow = true;

      if (isStats) {
        // Respect section-level visibility: if the section is hidden, hide this column too
        var secVisKey = _secVisMap[secName];
        if (secVisKey && props.getProperty(secVisKey) === 'false') {
          shouldShow = false;
        } else {
          if (isAdvanced && !showAdv) shouldShow = false;
          if (isBasic && showAdv)     shouldShow = false;
          if (shouldShow && isPct && !showPct) shouldShow = false;
          if (shouldShow && !isPct && hasPercentile && showPct) shouldShow = false;
        }
      } else if (isPct) {
        if (!showPct) {
          shouldShow = false;
        } else if (secName && props.getProperty('SECTION_VIS_' + secName.toUpperCase()) === 'false') {
          shouldShow = false;
        }
      } else if (hasPercentile) {
        var secHidden = secName && props.getProperty('SECTION_VIS_' + secName.toUpperCase()) === 'false';
        if (secHidden) continue;
        if (showPct) shouldShow = false;
      } else {
        continue;
      }

      if (shouldShow) showList.push(colIdx);
      else            hideList.push(colIdx);
    }

    _batchColumns(sheet, showList, true);
    _batchColumns(sheet, hideList, false);
    return;
  }

  // --- Legacy fallback: range-based approach ---
  var advR = (config.advanced_column_ranges || {})[rangeKey] || [];
  var basR = (config.basic_column_ranges || {})[rangeKey] || [];
  _applyRangeVisibility(sheet, advR, maxCols, showAdv);
  _applyRangeVisibility(sheet, basR, maxCols, !showAdv);

  var pctR = (config.percentile_column_ranges || {})[rangeKey] || [];
  var valR = (config.base_value_column_ranges || {})[rangeKey] || [];
  _applyRangeVisibility(sheet, pctR, maxCols, showPct);
  _applyRangeVisibility(sheet, valR, maxCols, !showPct);

  // Re-enforce adv/basic after pct/val swap (ranges overlap)
  if (showAdv) _applyRangeVisibility(sheet, basR, maxCols, false);
  else         _applyRangeVisibility(sheet, advR, maxCols, false);
}

/**
 * Generic column-section visibility control.
 * @param {string}  sectionKey    - Config key for the section
 * @param {boolean} makeVisible   - true to show, false to hide
 * @param {string}  label         - Toast message
 */
function _setSectionVisibility(sectionKey, makeVisible, label) {
  var ss           = SpreadsheetApp.getActiveSpreadsheet();
  var config       = loadConfig();
  var columnRanges = config.column_ranges || {};
  var activeSheet  = ss.getActiveSheet();
  var sheets       = ss.getSheets();
  var updatedCount = 0;

  // Persist state so _reapplyToggles can respect section visibility
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('SECTION_VIS_' + sectionKey.toUpperCase(), makeVisible ? 'true' : 'false');

  function applyToSheet(sheet) {
    var name      = sheet.getName().toUpperCase();
    var sheetType = _getSheetType(name);
    if (!sheetType) return;
    var rangeKey = _getRangeKey(sheetType);
    var ranges   = (columnRanges[rangeKey] || {})[sectionKey] || null;
    if (!ranges) return;
    var maxCols = sheet.getMaxColumns();
    if (ranges.start > maxCols) return;
    var count = Math.min(ranges.count, maxCols - ranges.start + 1);
    try {
      if (makeVisible) sheet.showColumns(ranges.start, count);
      else             sheet.hideColumns(ranges.start, count);
      updatedCount++;
    } catch (e) {
      Logger.log('_setSectionVisibility error on ' + sheet.getName() + ': ' + e);
    }
    if (makeVisible) {
      _rehideAlwaysHidden(sheet, sheetType);
      _reapplyToggles(sheet, sheetType);
    }
  }

  applyToSheet(activeSheet);
  SpreadsheetApp.flush();

  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getSheetId() === activeSheet.getSheetId()) continue;
    applyToSheet(sheets[i]);
  }

  ss.toast(label + ' (' + updatedCount + ' sheets)', 'Section Visibility', 3);
}

/** Keep legacy toggles for backward compat */
function _toggleSection(sectionKey, labelOn, labelOff) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var config = loadConfig();
  var columnRanges = config.column_ranges || {};
  var activeSheet = ss.getActiveSheet();
  var activeName  = activeSheet.getName().toUpperCase();
  var activeType  = _getSheetType(activeName);
  var activeKey   = _getRangeKey(activeType);
  var detectRange = (columnRanges[activeKey] || {})[sectionKey] || null;
  var newVisible  = true;
  if (detectRange && detectRange.start <= activeSheet.getMaxColumns()) {
    try { newVisible = activeSheet.isColumnHiddenByUser(detectRange.start); }
    catch (e) { Logger.log('_toggleSection detect error: ' + e); }
  }
  _setSectionVisibility(sectionKey, newVisible, newVisible ? labelOn : labelOff);
}

// Explicit show/hide for each section
function showCurrentStats()    { _setSectionVisibility('current',     true,  'Current stats shown');    }
function hideCurrentStats()    { _setSectionVisibility('current',     false, 'Current stats hidden');   }
function showHistoricalStats() { _setSectionVisibility('historical',  true,  'Historical stats shown'); }
function hideHistoricalStats() { _setSectionVisibility('historical',  false, 'Historical stats hidden');}
function showPostseasonStats() { _setSectionVisibility('postseason',  true,  'Postseason stats shown'); }
function hidePostseasonStats() { _setSectionVisibility('postseason',  false, 'Postseason stats hidden');}
function showPlayerInfo()      { _setSectionVisibility('player_info', true,  'Player info shown');      }
function hidePlayerInfo()      { _setSectionVisibility('player_info', false, 'Player info hidden');     }
function showAnalysis()        { _setSectionVisibility('notes',       true,  'Analysis shown');         }
function hideAnalysis()        { _setSectionVisibility('notes',       false, 'Analysis hidden');        }

// Legacy toggle wrappers
function toggleCurrentStats()    { _toggleSection('current',     'Current stats shown',    'Current stats hidden');    }
function toggleHistoricalStats() { _toggleSection('historical',  'Historical stats shown', 'Historical stats hidden'); }
function togglePostseasonStats() { _toggleSection('postseason',  'Postseason stats shown', 'Postseason stats hidden'); }
function togglePlayerInfo()      { _toggleSection('player_info', 'Player info shown',      'Player info hidden');      }
function toggleAnalysis()        { _toggleSection('notes',       'Analysis shown',         'Analysis hidden');         }

function showAllSections() {
  var sections = ['current', 'historical', 'postseason', 'player_info', 'notes'];
  var ss       = SpreadsheetApp.getActiveSpreadsheet();
  var config   = loadConfig();
  var colRange = config.column_ranges || {};
  var sheets   = ss.getSheets();
  for (var i = 0; i < sheets.length; i++) {
    var sheet     = sheets[i];
    var name      = sheet.getName().toUpperCase();
    var sheetType = _getSheetType(name);
    if (!sheetType) continue;
    var rangeKey = _getRangeKey(sheetType);
    var ranges   = colRange[rangeKey] || {};
    for (var s = 0; s < sections.length; s++) {
      var r = ranges[sections[s]];
      if (r) sheet.showColumns(r.start, r.count);
    }
    _rehideAlwaysHidden(sheet, sheetType);
  }
  ss.toast('All sections shown', 'Section Visibility', 3);
}
