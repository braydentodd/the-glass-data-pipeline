/**
 * apps-script/Code.js
 *
 * Pure UI layer for The Glass NBA stats spreadsheet.
 * ALL data calculations are performed by the Python backend (src/sheets.py).
 *
 * Responsibilities:
 *   - Load config from /api/config (single source of truth)
 *   - Menu creation and user interactions
 *   - Trigger Python-side syncs via the API
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
    .addItem('Totals',              'switchToTotals')
    .addItem('Per Game',            'switchToPerGame')
    .addItem('Per 36 Minutes',      'switchToPer36')
    .addItem('Per 100 Possessions', 'switchToPer100')
    .addSeparator()
    .addItem('Toggle Advanced Stats', 'toggleAdvancedStats')
    .addItem('Toggle Percentiles',    'togglePercentiles')
    .addSeparator()
    .addItem('Toggle Player Info',      'togglePlayerInfo')
    .addItem('Toggle Analysis',         'toggleAnalysis')
    .addItem('Toggle Current Stats',    'toggleCurrentStats')
    .addItem('Toggle Historical Stats', 'toggleHistoricalStats')
    .addItem('Toggle Postseason Stats', 'togglePostseasonStats')
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

function switchToTotals()  { triggerSync('totals',   {priorityTeam: _getActiveTeamAbbr()}); }
function switchToPerGame() { triggerSync('per_game', {priorityTeam: _getActiveTeamAbbr()}); }
function switchToPer36()   { triggerSync('per_36',   {priorityTeam: _getActiveTeamAbbr()}); }
function switchToPer100()  { triggerSync('per_100',  {priorityTeam: _getActiveTeamAbbr()}); }

// ============================================================
// ADVANCED STATS & PERCENTILES TOGGLE
// ============================================================

/**
 * Toggle advanced stat columns across all sheets.
 * Active sheet is processed first for immediate visual feedback.
 * Manages subsection borders: shown with advanced, hidden with basic.
 */
function toggleAdvancedStats() {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var config = loadConfig();
  var advRanges   = config.advanced_column_ranges || {};
  var basicRanges = config.basic_column_ranges || {};

  // Detect current state from active sheet
  var active     = ss.getActiveSheet();
  var activeName = active.getName().toUpperCase();
  var activeType = _getSheetType(activeName);
  var activeKey  = _getRangeKey(activeType);
  var detectR    = (advRanges[activeKey] || []);
  var newAdvancedVisible = true;
  if (detectR.length > 0 && detectR[0].start <= active.getMaxColumns()) {
    try { newAdvancedVisible = active.isColumnHiddenByUser(detectR[0].start); }
    catch (e) { Logger.log('toggleAdvancedStats detect error: ' + e); }
  }

  // Save state to DocumentProperties for DRY toggle management
  PropertiesService.getDocumentProperties().setProperty(
    'SHOW_ADVANCED', newAdvancedVisible ? 'true' : 'false'
  );

  // Apply column visibility + subsection borders (active sheet first)
  var boundaries = config.subsection_boundaries || {};
  var sheets = ss.getSheets();

  function applyToSheet(sheet) {
    var name      = sheet.getName().toUpperCase();
    var sheetType = _getSheetType(name);
    if (!sheetType) return;
    var rangeKey = _getRangeKey(sheetType);
    var maxCols  = sheet.getMaxColumns();

    // Show/hide advanced columns
    var advR = advRanges[rangeKey] || [];
    _applyRangeVisibility(sheet, advR, maxCols, newAdvancedVisible);

    // Hide/show basic columns (swap behavior)
    var basR = basicRanges[rangeKey] || [];
    _applyRangeVisibility(sheet, basR, maxCols, !newAdvancedVisible);

    // Show/hide subsection header row
    var subRow = config.subsection_row_index || 2;
    try {
      if (newAdvancedVisible) sheet.showRows(subRow, 1);
      else                     sheet.hideRows(subRow, 1);
    } catch (e) {
      Logger.log('Subsection row toggle error on ' + name + ': ' + e);
    }

    // Subsection borders
    var layout = config.layout || {};
    var headerRows = layout.header_row_count || 4;
    var maxRows = sheet.getMaxRows();
    var borderCols = (boundaries[rangeKey] || []);
    var statsRange = (config.stats_section_ranges || {})[rangeKey] || {};

    // White borders on subsection + column name header rows — ONLY for stats section
    if (newAdvancedVisible && statsRange.start && statsRange.end) {
      try {
        var statsColCount = statsRange.end - statsRange.start + 1;
        // Apply white borders to subsection row AND column name row (rows 2-3)
        var headerBorderRange = sheet.getRange(subRow, statsRange.start, headerRows - 1, statsColCount);
        headerBorderRange.setBorder(true, true, true, true, true, true, '#FFFFFF',
          SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
      } catch (e) {
        Logger.log('Subsection stats border error on ' + name + ': ' + e);
      }
    }

    // Add or remove subsection vertical borders
    for (var b = 0; b < borderCols.length; b++) {
      var col = borderCols[b];
      if (col > maxCols) continue;
      try {
        if (newAdvancedVisible) {
          // White SOLID_MEDIUM left border on header rows (subsection row through filter row)
          var hdrBorderRange = sheet.getRange(subRow, col, headerRows - 1, 1);
          hdrBorderRange.setBorder(null, true, null, null, null, null, '#FFFFFF',
            SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
          // Black SOLID_MEDIUM left border on all data rows
          var dataRange = sheet.getRange(headerRows + 1, col, maxRows - headerRows, 1);
          dataRange.setBorder(null, true, null, null, null, null, '#000000',
            SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
        } else {
          // Remove borders from subheader through all data rows
          var fullRange = sheet.getRange(subRow, col, maxRows - subRow + 1, 1);
          fullRange.setBorder(null, false, null, null, null, null);
        }
      } catch (e) {
        Logger.log('Border error on ' + name + ' col ' + col + ': ' + e);
      }
    }

    // Re-hide always-hidden columns and respect current toggle states
    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);
  }

  applyToSheet(active);
  SpreadsheetApp.flush();

  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getSheetId() === active.getSheetId()) continue;
    applyToSheet(sheets[i]);
  }

  ss.toast(newAdvancedVisible ? 'Advanced stats shown' : 'Basic stats shown', 'View', 3);
}

/**
 * Toggle percentile columns across all sheets.
 * Swaps between value view and percentile view:
 *   - Percentile ON  → show percentile columns, hide base value columns
 *   - Percentile OFF → show base value columns, hide percentile columns
 * Active sheet is processed first for immediate visual feedback.
 */
function togglePercentiles() {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var config = loadConfig();

  var pctRanges = config.percentile_column_ranges || {};
  var valRanges = config.base_value_column_ranges || {};

  // Detect: are percentiles currently visible?
  var active     = ss.getActiveSheet();
  var activeName = active.getName().toUpperCase();
  var activeType = _getSheetType(activeName);
  var activeKey  = _getRangeKey(activeType);
  var detectR    = (pctRanges[activeKey] || []);
  var percentilesCurrentlyVisible = false;
  if (detectR.length > 0 && detectR[0].start <= active.getMaxColumns()) {
    try { percentilesCurrentlyVisible = !active.isColumnHiddenByUser(detectR[0].start); }
    catch (e) { Logger.log('togglePercentiles detect error: ' + e); }
  }

  var showPercentiles = !percentilesCurrentlyVisible;
  var sheets = ss.getSheets();

  function applyToSheet(sheet) {
    var name      = sheet.getName().toUpperCase();
    var sheetType = _getSheetType(name);
    if (!sheetType) return;
    var rangeKey = _getRangeKey(sheetType);
    var pctR = pctRanges[rangeKey] || [];
    var valR = valRanges[rangeKey] || [];

    var maxCols = sheet.getMaxColumns();
    _applyRangeVisibility(sheet, pctR, maxCols, showPercentiles);
    _applyRangeVisibility(sheet, valR, maxCols, !showPercentiles);

    // Re-hide always-hidden columns and respect all toggle states
    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);
  }

  applyToSheet(active);
  SpreadsheetApp.flush();

  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getSheetId() === active.getSheetId()) continue;
    applyToSheet(sheets[i]);
  }

  PropertiesService.getDocumentProperties().setProperty(
    'SHOW_PERCENTILES', showPercentiles ? 'true' : 'false'
  );
  ss.toast(showPercentiles ? 'Percentiles shown' : 'Values shown', 'View', 3);
}

/**
 * Apply show/hide to a list of column ranges across all sheets.
 * Active sheet is processed first for immediate visual feedback.
 */
function _applyColumnRangeList(allRanges, visible) {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var active = ss.getActiveSheet();
  var sheets = ss.getSheets();

  function applyToSheet(sheet) {
    var name      = sheet.getName().toUpperCase();
    var sheetType = _getSheetType(name);
    if (!sheetType) return;
    var rangeKey  = _getRangeKey(sheetType);
    var rangeList = allRanges[rangeKey] || [];
    _applyRangeVisibility(sheet, rangeList, sheet.getMaxColumns(), visible);
  }

  applyToSheet(active);
  SpreadsheetApp.flush();

  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getSheetId() === active.getSheetId()) continue;
    applyToSheet(sheets[i]);
  }
}

// ============================================================
// TOGGLE HELPERS
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

/**
 * Apply column visibility + subsection border management.
 * Used by toggleAdvancedStats to add/remove subsection borders,
 * show/hide the subsection header row, and apply white borders
 * on subheader cells.
 * Active sheet processed first for immediate feedback.
 *
 * @param {Object} allRanges  - advanced_column_ranges from config (keyed by range key)
 * @param {boolean} visible   - whether to show (true) or hide (false)
 * @param {Object} boundaries - subsection_boundaries from config (keyed by range key)
 */
function _applyColumnRangeListWithBorders(allRanges, visible, boundaries) {
  var ss       = SpreadsheetApp.getActiveSpreadsheet();
  var config   = loadConfig();
  var layout   = config.layout || {};
  var subRow   = config.subsection_row_index || 2;  // 1-indexed
  var colNameRow = subRow + 1;  // Column name header row
  var statsSectionRanges = config.stats_section_ranges || {};
  var active   = ss.getActiveSheet();
  var sheets   = ss.getSheets();

  function applyToSheet(sheet) {
    var name      = sheet.getName().toUpperCase();
    var sheetType = _getSheetType(name);
    if (!sheetType) return;
    var rangeKey  = _getRangeKey(sheetType);
    var rangeList = allRanges[rangeKey] || [];
    var borderCols = (boundaries[rangeKey] || []);
    var statsRange = statsSectionRanges[rangeKey] || {};

    var maxCols = sheet.getMaxColumns();
    var maxRows = sheet.getMaxRows();
    _applyRangeVisibility(sheet, rangeList, maxCols, visible);

    // Show/hide subsection header row
    try {
      if (visible) sheet.showRows(subRow, 1);
      else         sheet.hideRows(subRow, 1);
    } catch (e) {
      Logger.log('Subsection row toggle error on ' + name + ': ' + e);
    }

    // White borders on subsection header row — ONLY for stats section columns
    // (entities, player_info, analysis sections should NOT have borders)
    if (visible && statsRange.start && statsRange.end) {
      try {
        var statsColCount = statsRange.end - statsRange.start + 1;
        var subStatsRange = sheet.getRange(subRow, statsRange.start, 1, statsColCount);
        subStatsRange.setBorder(true, true, true, true, true, true, '#FFFFFF',
          SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
      } catch (e) {
        Logger.log('Subsection stats border error on ' + name + ': ' + e);
      }
    }

    // Add or remove subsection borders: full header (subheader + col names) + all data rows
    var headerRows = layout.header_row_count || 4;
    for (var b = 0; b < borderCols.length; b++) {
      var col = borderCols[b];
      if (col > maxCols) continue;
      try {
        if (visible) {
          // White SOLID_MEDIUM left border on header rows (subheader + column name rows)
          var headerBorderRange = sheet.getRange(subRow, col, colNameRow - subRow + 1, 1);
          headerBorderRange.setBorder(null, true, null, null, null, null, '#FFFFFF',
            SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
          // Black SOLID_MEDIUM left border on all data rows (including team/opp/summary)
          var dataRange = sheet.getRange(headerRows + 1, col, maxRows - headerRows, 1);
          dataRange.setBorder(null, true, null, null, null, null, '#000000',
            SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
        } else {
          // Remove borders from subheader through all data rows
          var fullRange = sheet.getRange(subRow, col, maxRows - subRow + 1, 1);
          fullRange.setBorder(null, false, null, null, null, null);
        }
      } catch (e) {
        Logger.log('Border error on ' + name + ' col ' + col + ': ' + e);
      }
    }

    // Re-hide always-hidden columns and respect current toggle states
    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);
  }

  applyToSheet(active);
  SpreadsheetApp.flush();

  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getSheetId() === active.getSheetId()) continue;
    applyToSheet(sheets[i]);
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

  triggerSync(null);
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
  var statsMode = mode || props.getProperty('STATS_MODE') || 'per_100';

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
  for (var h = 0; h < hiddenCols.length; h++) {
    var col = hiddenCols[h];
    if (col <= maxCols) {
      try { sheet.hideColumns(col, 1); }
      catch (e) { Logger.log('_rehideAlwaysHidden error: ' + e); }
    }
  }
}

/**
 * Re-apply current toggle states (percentiles, advanced/basic) after any section toggle.
 * Ensures toggles are DRY — showing a section doesn't reveal columns that should
 * be hidden by another toggle (e.g. percentile columns when in values mode,
 * basic columns when advanced is shown).
 */
function _reapplyToggles(sheet, sheetType) {
  var config   = loadConfig();
  var props    = PropertiesService.getDocumentProperties();
  var rangeKey = _getRangeKey(sheetType);
  var maxCols  = sheet.getMaxColumns();

  // Re-hide percentile or base-value columns based on current SHOW_PERCENTILES state
  var showPct  = props.getProperty('SHOW_PERCENTILES') === 'true';
  var pctR     = (config.percentile_column_ranges || {})[rangeKey] || [];
  var valR     = (config.base_value_column_ranges || {})[rangeKey] || [];
  _applyRangeVisibility(sheet, pctR, maxCols, showPct);
  _applyRangeVisibility(sheet, valR, maxCols, !showPct);

  // Re-apply advanced/basic swap based on SHOW_ADVANCED state
  var showAdv  = props.getProperty('SHOW_ADVANCED') === 'true';
  var advR = (config.advanced_column_ranges || {})[rangeKey] || [];
  var basR = (config.basic_column_ranges || {})[rangeKey] || [];
  _applyRangeVisibility(sheet, advR, maxCols, showAdv);
  _applyRangeVisibility(sheet, basR, maxCols, !showAdv);
}

/**
 * Generic column-section visibility toggle.
 * Detects current visibility state from the active sheet.
 * After showing, re-hides always-hidden columns (e.g. team column on team sheets).
 */
function _toggleSection(sectionKey, labelOn, labelOff) {
  var ss           = SpreadsheetApp.getActiveSpreadsheet();
  var config       = loadConfig();
  var columnRanges = config.column_ranges || {};

  // Detect current state from active sheet
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

  var sheets       = ss.getSheets();
  var updatedCount = 0;

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
      if (newVisible) sheet.showColumns(ranges.start, count);
      else            sheet.hideColumns(ranges.start, count);
      updatedCount++;
    } catch (e) {
      Logger.log('_toggleSection error on ' + sheet.getName() + ': ' + e);
    }
    // Re-hide always-hidden columns and respect current toggle states
    if (newVisible) {
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

  ss.toast(
    newVisible ? (labelOn  + ' (' + updatedCount + ' sheets)')
               : (labelOff + ' (' + updatedCount + ' sheets)'),
    'Section Visibility', 3
  );
}

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
