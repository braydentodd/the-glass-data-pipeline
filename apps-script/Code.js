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
var BOOTSTRAP_URL = 'http://150.136.255.23:5001/api/config';

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
  ui.createMenu('Stats')
    .addItem('Totals',              'switchToTotals')
    .addItem('Per Game',            'switchToPerGame')
    .addItem('Per 36 Minutes',      'switchToPer36')
    .addItem('Per 100 Possessions', 'switchToPer100')
    .addSeparator()
    .addItem('Toggle Advanced Stats', 'toggleAdvancedStats')
    .addItem('Toggle Percentiles',    'togglePercentiles')
    .addSeparator()
    .addItem('Historical Timeframe...', 'showHistoricalStatsDialog')
    .addSeparator()
    .addSubMenu(
      ui.createMenu('Sections')
        .addItem('Toggle Player Info',      'togglePlayerInfo')
        .addItem('Toggle Analysis',         'toggleAnalysis')
        .addItem('Toggle Current Stats',    'toggleCurrentStats')
        .addItem('Toggle Historical Stats', 'toggleHistoricalStats')
        .addItem('Toggle Postseason Stats', 'togglePostseasonStats')
        .addSeparator()
        .addItem('Show All Sections',       'showAllSections')
    )
    .addToUi();
}

// ============================================================
// EDIT TRIGGER - wingspan / notes write-back to DB
// ============================================================

function onEditInstallable(e) {
  var sheet     = e.range.getSheet();
  var sheetName = sheet.getName().toUpperCase();
  var config    = loadConfig();
  var nbaTeams  = config.nba_teams || {};

  if (!nbaTeams.hasOwnProperty(sheetName) && sheetName !== 'NBA') return;

  var colIndices = config.column_indices || {};
  var editedCol  = e.range.getColumn();
  var editedRow  = e.range.getRow();

  // Skip header rows (config-driven layout)
  var layout = config.layout || {};
  if (editedRow <= (layout.header_row_count || 4)) return;

  var wingspanCol = colIndices.wingspan;
  var notesCol    = colIndices.notes;
  if (!wingspanCol && !notesCol) return;

  var playerName = sheet.getRange(editedRow, 1).getValue();
  if (!playerName) return;

  var teamAbbr = (sheetName === 'NBA')
    ? sheet.getRange(editedRow, colIndices.team || 2).getValue()
    : sheetName;

  if (editedCol === wingspanCol) {
    var wingspan = parseWingspan(e.range.getValue());
    if (wingspan === null) {
      SpreadsheetApp.getActiveSpreadsheet().toast(
        "Invalid wingspan. Use feet'inches (e.g. 6'8) or total inches.",
        'Error', 5
      );
      return;
    }
    var playerId = getPlayerIdByName(playerName, teamAbbr);
    if (playerId) {
      try { updatePlayerField(playerId, 'wingspan', wingspan); }
      catch (err) { SpreadsheetApp.getActiveSpreadsheet().toast('Error saving wingspan: ' + err.message, 'Error', 5); }
    }
  } else if (editedCol === notesCol) {
    var playerId2 = getPlayerIdByName(playerName, teamAbbr);
    if (playerId2) {
      try { updatePlayerField(playerId2, 'notes', e.range.getValue()); }
      catch (err) { SpreadsheetApp.getActiveSpreadsheet().toast('Error saving note: ' + err.message, 'Error', 5); }
    }
  }
}

// ============================================================
// MODE SWITCHING
// ============================================================

function switchToTotals()  { triggerSync('totals');   }
function switchToPerGame() { triggerSync('per_game'); }
function switchToPer36()   { triggerSync('per_36');   }
function switchToPer100()  { triggerSync('per_100');  }

// ============================================================
// ADVANCED STATS & PERCENTILES TOGGLE
// ============================================================

/**
 * Toggle advanced stat columns across all sheets.
 * Detects current state from the active sheet.
 */
function toggleAdvancedStats() {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var config = loadConfig();
  var ranges = config.advanced_column_ranges || {};
  var teamR  = ranges.team_sheet || [];
  var nbaR   = ranges.nba_sheet  || [];

  if (!teamR.length && !nbaR.length) {
    ss.toast('Advanced column ranges not configured', 'Error', 3);
    return;
  }

  // Detect current state from active sheet
  var active     = ss.getActiveSheet();
  var activeName = active.getName().toUpperCase();
  var nbaTeams   = config.nba_teams || {};
  var detectR    = nbaTeams.hasOwnProperty(activeName) ? teamR : nbaR;
  var newVisible = true;
  if (detectR.length > 0 && detectR[0].start <= active.getMaxColumns()) {
    try { newVisible = active.isColumnHiddenByUser(detectR[0].start); }
    catch (e) { Logger.log('toggleAdvancedStats detect error: ' + e); }
  }

  _applyColumnRangeList(teamR, nbaR, newVisible);
  ss.toast(newVisible ? 'Advanced stats shown' : 'Advanced stats hidden', 'View', 3);
}

/**
 * Toggle percentile columns across all sheets.
 * Detects current state from the active sheet.
 */
function togglePercentiles() {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var config = loadConfig();
  var ranges = config.percentile_column_ranges || {};
  var teamR  = ranges.team_sheet || [];
  var nbaR   = ranges.nba_sheet  || [];

  if (!teamR.length && !nbaR.length) {
    ss.toast('Percentile column ranges not configured', 'Error', 3);
    return;
  }

  var active     = ss.getActiveSheet();
  var activeName = active.getName().toUpperCase();
  var nbaTeams   = config.nba_teams || {};
  var detectR    = nbaTeams.hasOwnProperty(activeName) ? teamR : nbaR;
  var newVisible = true;
  if (detectR.length > 0 && detectR[0].start <= active.getMaxColumns()) {
    try { newVisible = active.isColumnHiddenByUser(detectR[0].start); }
    catch (e) { Logger.log('togglePercentiles detect error: ' + e); }
  }

  _applyColumnRangeList(teamR, nbaR, newVisible);
  ss.toast(newVisible ? 'Percentiles shown' : 'Percentiles hidden', 'View', 3);
}

/**
 * Apply show/hide to a list of column ranges across all sheets.
 */
function _applyColumnRangeList(teamRanges, nbaRanges, visible) {
  var ss       = SpreadsheetApp.getActiveSpreadsheet();
  var config   = loadConfig();
  var nbaTeams = config.nba_teams || {};
  var sheets   = ss.getSheets();

  for (var i = 0; i < sheets.length; i++) {
    var sheet = sheets[i];
    var name  = sheet.getName().toUpperCase();
    var rangeList;
    if (nbaTeams.hasOwnProperty(name))      rangeList = teamRanges;
    else if (name === 'NBA')                rangeList = nbaRanges;
    else continue;

    var maxCols = sheet.getMaxColumns();
    for (var r = 0; r < rangeList.length; r++) {
      var rng = rangeList[r];
      // Guard against ranges that exceed the sheet's column count
      if (rng.start > maxCols) continue;
      var count = Math.min(rng.count, maxCols - rng.start + 1);
      try {
        if (visible) sheet.showColumns(rng.start, count);
        else         sheet.hideColumns(rng.start, count);
      } catch (e) {
        Logger.log('_applyColumnRangeList error on sheet ' + name + ': ' + e);
      }
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
 * Generic column-section visibility toggle.
 * Detects current visibility state from the active sheet.
 */
function _toggleSection(sectionKey, labelOn, labelOff) {
  var ss           = SpreadsheetApp.getActiveSpreadsheet();
  var config       = loadConfig();
  var nbaTeams     = config.nba_teams || {};
  var columnRanges = config.column_ranges || {};
  var teamRanges   = ((columnRanges.team_sheet || {})[sectionKey]) || null;
  var nbaRanges    = ((columnRanges.nba_sheet  || {})[sectionKey]) || null;

  // Detect current state from active sheet
  var activeSheet = ss.getActiveSheet();
  var activeName  = activeSheet.getName().toUpperCase();
  var detectRange = nbaTeams.hasOwnProperty(activeName) ? teamRanges : nbaRanges;
  var newVisible  = true;
  if (detectRange && detectRange.start <= activeSheet.getMaxColumns()) {
    try { newVisible = activeSheet.isColumnHiddenByUser(detectRange.start); }
    catch (e) { Logger.log('_toggleSection detect error: ' + e); }
  }

  var sheets       = ss.getSheets();
  var updatedCount = 0;

  function applyToSheet(sheet, ranges) {
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
  }

  if (nbaTeams.hasOwnProperty(activeName))       applyToSheet(activeSheet, teamRanges);
  else if (activeName === 'NBA')                  applyToSheet(activeSheet, nbaRanges);

  for (var i = 0; i < sheets.length; i++) {
    var sheet = sheets[i];
    if (sheet.getSheetId() === activeSheet.getSheetId()) continue;
    var name = sheet.getName().toUpperCase();
    if (nbaTeams.hasOwnProperty(name))       applyToSheet(sheet, teamRanges);
    else if (name === 'NBA')                 applyToSheet(sheet, nbaRanges);
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
  var nbaTeams = config.nba_teams || {};
  var colRange = config.column_ranges || {};
  var sheets   = ss.getSheets();
  for (var i = 0; i < sheets.length; i++) {
    var sheet = sheets[i];
    var name  = sheet.getName().toUpperCase();
    var ranges;
    if (nbaTeams.hasOwnProperty(name))       ranges = colRange.team_sheet || {};
    else if (name === 'NBA')                 ranges = colRange.nba_sheet  || {};
    else continue;
    for (var s = 0; s < sections.length; s++) {
      var r = ranges[sections[s]];
      if (r) sheet.showColumns(r.start, r.count);
    }
  }
  ss.toast('All sections shown', 'Section Visibility', 3);
}
