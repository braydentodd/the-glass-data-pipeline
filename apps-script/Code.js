/**
 * apps-script/Code.js
 *
 * Unified, config-driven UI layer for The Glass stats spreadsheets.
 * Supports NBA and NCAA via a separate LeagueConfig.js file.
 * ALL data calculations are performed by the Python backend.
 *
 * Deployment:
 *   1. Copy this file + the league's config file (config/NBA.js or config/NCAA.js)
 *      to the Apps Script project
 *   2. Code.js is IDENTICAL for both leagues — never edit it per-league
 *
 * Responsibilities:
 *   - Load config from the API (single source of truth)
 *   - Menu creation and user interactions
 *   - Trigger Python-side syncs via the API (including stat mode switching)
 *   - Write-back editable fields to the DB via PATCH/PUT
 *   - Column visibility toggles (sections, advanced stats)
 *
 * Explicitly NOT done here:
 *   - Stat calculations of any kind
 *   - Hardcoded team lists or column indices
 */

// ============================================================
// LEAGUE CONFIG
// ============================================================
//
// The LEAGUE global is defined in a separate LeagueConfig.js file
// (NbaLeagueConfig.js or NcaaLeagueConfig.js). Apps Script loads
// all project files into one shared scope, so Code.js sees it
// automatically. This file is IDENTICAL across both deployments.
//

// ============================================================
// CONFIG
// ============================================================

/** Server base URL — single place to change if server moves. */
var API_BASE = 'http://150.136.255.23:5000';

var CONFIG = null;

/**
 * Load config from the Python API. Cached for the lifetime of the script run.
 * Falls back to safe defaults so the spreadsheet never hard-breaks.
 */
function loadConfig() {
  if (CONFIG) return CONFIG;

  var league = LEAGUE;
  var url = API_BASE + league.configEndpoint;

  try {
    var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (response.getResponseCode() === 200) {
      CONFIG = JSON.parse(response.getContentText());
      return CONFIG;
    }
    Logger.log('loadConfig: API returned ' + response.getResponseCode());
  } catch (e) {
    Logger.log('loadConfig: failed to reach API - ' + e);
  }

  // Minimal fallback so the spreadsheet never hard-breaks
  CONFIG = {
    api_base_url: API_BASE,
    column_ranges: { team_sheet: {} },
    column_indices: {},
    layout: { header_row_count: 4 },
    colors: {
      red:    { r: 238, g: 75,  b: 43  },
      yellow: { r: 252, g: 245, b: 95  },
      green:  { r: 76,  g: 187, b: 23  }
    }
  };
  CONFIG[league.teamsKey] = {};
  CONFIG.column_ranges[league.playersRangeKey] = {};
  return CONFIG;
}

function getApiBaseUrl() {
  return loadConfig().api_base_url || API_BASE;
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
  var league = LEAGUE;
  var tfMenu = _buildTimeframeMenu();

  var menu = ui.createMenu('Display Settings')
    .addSubMenu(tfMenu)
    .addSubMenu(ui.createMenu('Stats Mode')
      .addItem('Per 100 Possessions', 'switchToPer100')
      .addItem('Per 48 Minutes',      'switchToPer48')
      .addItem('Per Game',            'switchToPerGame'));

  if (league.hasAdvancedStats) {
    menu.addSubMenu(ui.createMenu('Advanced Stats')
      .addItem('Show', 'showAdvancedStats')
      .addItem('Hide', 'hideAdvancedStats'));
  }

  menu.addSeparator()
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
      .addItem('Hide', 'hidePostseasonStats'));

  menu.addToUi();
}

function _buildTimeframeMenu() {
  var ui = SpreadsheetApp.getUi();
  var menu = ui.createMenu('Historical Timeframe');

  for (var years = 1; years <= 23; years++) {
    var label = 'Last ' + years + ' Season' + (years > 1 ? 's' : '');
    var includeFn = 'setTimeframe' + years + 'IncludeCurrent';
    var excludeFn = 'setTimeframe' + years + 'ExcludeCurrent';
    menu.addSubMenu(
      ui.createMenu(label)
        .addItem('Include Current Season', includeFn)
        .addItem('Exclude Current Season', excludeFn)
    );
  }

  return menu;
}

// ============================================================
// EDIT TRIGGER — config-driven write-back to DB
// ============================================================

function onEditInstallable(e) {
  var sheet     = e.range.getSheet();
  var sheetName = sheet.getName().toUpperCase();
  var config    = loadConfig();
  var league    = LEAGUE;
  var teams     = config[league.teamsKey] || {};
  var sheetType = _getSheetType(sheetName);

  if (!sheetType) return;

  var layout    = config.layout || {};
  var editedRow = e.range.getRow();
  if (editedRow <= (layout.header_row_count || 4)) return;

  var editedCol = e.range.getColumn();
  var ss        = SpreadsheetApp.getActiveSpreadsheet();

  // ---- Teams sheet: every row is a team ----
  if (sheetType === 'teams') {
    var teamsEditable = config.teams_editable_columns || [];
    var matched = null;
    for (var i = 0; i < teamsEditable.length; i++) {
      if (teamsEditable[i].col_index === editedCol) { matched = teamsEditable[i]; break; }
    }
    if (!matched) return;

    var entityName = sheet.getRange(editedRow, 1).getValue();
    if (!entityName) return;

    var nameToAbbr = config.team_name_to_abbr || {};
    var teamAbbr = teams.hasOwnProperty(String(entityName).toUpperCase())
      ? String(entityName).toUpperCase()
      : nameToAbbr[entityName];
    if (!teamAbbr) {
      ss.toast('Could not identify team: ' + entityName, 'Error', 3);
      return;
    }
    var teamId = teams[teamAbbr];
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

  // ---- Team/Players sheets ----
  var editableColumns = config.editable_columns || [];
  var isPlayersSheet  = (sheetType === 'players');
  var matched         = null;

  for (var i = 0; i < editableColumns.length; i++) {
    var ec     = editableColumns[i];
    var colIdx = isPlayersSheet ? ec[league.editColIndexKey] : ec.team_col_index;
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

  // ---- TEAM row ----
  if (entityName === 'TEAM') {
    if (matched.col_key !== 'notes') {
      ss.toast('Only notes can be edited for teams', 'Info', 3);
      return;
    }
    var teamId = teams[teamAbbr];
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

  // ---- Player row ----
  if (league.hasWingspan && matched.col_key === 'wingspan') {
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

function switchToPerGame() { _switchStatMode('per_game'); }
function switchToPer48()   { _switchStatMode('per_48'); }
function switchToPer100()  { _switchStatMode('per_100'); }

/**
 * Switch stat mode by triggering a re-sync from the Python backend.
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

  var sheets = ss.getSheets();
  for (var i = 0; i < sheets.length; i++) {
    var name = sheets[i].getName().toUpperCase();
    if (_getSheetType(name)) {
      _updateSectionHeaders(sheets[i], newMode);
    }
  }
  SpreadsheetApp.flush();

  triggerSync(newMode, { priorityTeam: _getActiveTeamAbbr() });
}

/**
 * Update section header text (row 1) to reflect the current stat mode.
 * Replaces mode labels in stats section headers.
 */
function _updateSectionHeaders(sheet, newMode) {
  var labels = {
    'per_100':  'per 100 Poss',
    'per_game': 'per Game',
    'per_48':   'per 48 Mins',
  };
  var allLabels = Object.values(labels);
  var newLabel = labels[newMode] || '';

  try {
    var lastCol = sheet.getMaxColumns();
    if (lastCol < 1) return;
    var values = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
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
// ADVANCED STATS TOGGLE  (NBA only — menu item is gated)
// ============================================================

function showAdvancedStats() { _setAdvancedStats(true); }
function hideAdvancedStats() { _setAdvancedStats(false); }

function _setAdvancedStats(newAdvancedVisible) {
  var config = loadConfig();
  var props  = PropertiesService.getDocumentProperties();
  props.setProperty('SHOW_ADVANCED', newAdvancedVisible ? 'true' : 'false');

  var subRow = config.subsection_row_index || 2;

  _applyToAllSheets(function(sheet, sheetType) {
    try {
      if (newAdvancedVisible) sheet.showRows(subRow, 1);
      else                     sheet.hideRows(subRow, 1);
    } catch (e) { Logger.log('Subsection row toggle error: ' + e); }

    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);
    _applyVerticalBorders(sheet, sheetType, newAdvancedVisible);
  }, newAdvancedVisible ? 'Advanced stats shown' : 'Basic stats shown');
}

// ============================================================
// HISTORICAL / POSTSEASON TIMEFRAME  (menu-driven)
// ============================================================

/**
 * Set historical/postseason timeframe and trigger sync.
 * Called by timeframe menu handlers.
 */
function _setTimeframe(mode, years) {
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('HIST_MODE', mode);
  if (years) props.setProperty('HIST_YEARS', String(years));
  var label = years + ' season' + (years > 1 ? 's' : '');
  SpreadsheetApp.getActiveSpreadsheet().toast('Timeframe set to ' + label, 'Updated', 3);
  triggerSync(null, { priorityTeam: _getActiveTeamAbbr() });
}

function _setIncludeCurrentSeason(include) {
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('HIST_INCLUDE_CURRENT', include ? 'true' : 'false');
  var msg = include ? 'Current season included' : 'Current season excluded';
  SpreadsheetApp.getActiveSpreadsheet().toast(msg, 'Updated', 3);
  triggerSync(null, { priorityTeam: _getActiveTeamAbbr() });
}

// Menu handlers — one per timeframe option (1..23, each with include/exclude)
function _setTimeframeWithCurrent(years, includeCurrent) {
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('HIST_INCLUDE_CURRENT', includeCurrent ? 'true' : 'false');
  _setTimeframe('years', years);
}

function setTimeframe1IncludeCurrent()  { _setTimeframeWithCurrent(1, true); }
function setTimeframe1ExcludeCurrent()  { _setTimeframeWithCurrent(1, false); }
function setTimeframe2IncludeCurrent()  { _setTimeframeWithCurrent(2, true); }
function setTimeframe2ExcludeCurrent()  { _setTimeframeWithCurrent(2, false); }
function setTimeframe3IncludeCurrent()  { _setTimeframeWithCurrent(3, true); }
function setTimeframe3ExcludeCurrent()  { _setTimeframeWithCurrent(3, false); }
function setTimeframe4IncludeCurrent()  { _setTimeframeWithCurrent(4, true); }
function setTimeframe4ExcludeCurrent()  { _setTimeframeWithCurrent(4, false); }
function setTimeframe5IncludeCurrent()  { _setTimeframeWithCurrent(5, true); }
function setTimeframe5ExcludeCurrent()  { _setTimeframeWithCurrent(5, false); }
function setTimeframe6IncludeCurrent()  { _setTimeframeWithCurrent(6, true); }
function setTimeframe6ExcludeCurrent()  { _setTimeframeWithCurrent(6, false); }
function setTimeframe7IncludeCurrent()  { _setTimeframeWithCurrent(7, true); }
function setTimeframe7ExcludeCurrent()  { _setTimeframeWithCurrent(7, false); }
function setTimeframe8IncludeCurrent()  { _setTimeframeWithCurrent(8, true); }
function setTimeframe8ExcludeCurrent()  { _setTimeframeWithCurrent(8, false); }
function setTimeframe9IncludeCurrent()  { _setTimeframeWithCurrent(9, true); }
function setTimeframe9ExcludeCurrent()  { _setTimeframeWithCurrent(9, false); }
function setTimeframe10IncludeCurrent() { _setTimeframeWithCurrent(10, true); }
function setTimeframe10ExcludeCurrent() { _setTimeframeWithCurrent(10, false); }
function setTimeframe11IncludeCurrent() { _setTimeframeWithCurrent(11, true); }
function setTimeframe11ExcludeCurrent() { _setTimeframeWithCurrent(11, false); }
function setTimeframe12IncludeCurrent() { _setTimeframeWithCurrent(12, true); }
function setTimeframe12ExcludeCurrent() { _setTimeframeWithCurrent(12, false); }
function setTimeframe13IncludeCurrent() { _setTimeframeWithCurrent(13, true); }
function setTimeframe13ExcludeCurrent() { _setTimeframeWithCurrent(13, false); }
function setTimeframe14IncludeCurrent() { _setTimeframeWithCurrent(14, true); }
function setTimeframe14ExcludeCurrent() { _setTimeframeWithCurrent(14, false); }
function setTimeframe15IncludeCurrent() { _setTimeframeWithCurrent(15, true); }
function setTimeframe15ExcludeCurrent() { _setTimeframeWithCurrent(15, false); }
function setTimeframe16IncludeCurrent() { _setTimeframeWithCurrent(16, true); }
function setTimeframe16ExcludeCurrent() { _setTimeframeWithCurrent(16, false); }
function setTimeframe17IncludeCurrent() { _setTimeframeWithCurrent(17, true); }
function setTimeframe17ExcludeCurrent() { _setTimeframeWithCurrent(17, false); }
function setTimeframe18IncludeCurrent() { _setTimeframeWithCurrent(18, true); }
function setTimeframe18ExcludeCurrent() { _setTimeframeWithCurrent(18, false); }
function setTimeframe19IncludeCurrent() { _setTimeframeWithCurrent(19, true); }
function setTimeframe19ExcludeCurrent() { _setTimeframeWithCurrent(19, false); }
function setTimeframe20IncludeCurrent() { _setTimeframeWithCurrent(20, true); }
function setTimeframe20ExcludeCurrent() { _setTimeframeWithCurrent(20, false); }
function setTimeframe21IncludeCurrent() { _setTimeframeWithCurrent(21, true); }
function setTimeframe21ExcludeCurrent() { _setTimeframeWithCurrent(21, false); }
function setTimeframe22IncludeCurrent() { _setTimeframeWithCurrent(22, true); }
function setTimeframe22ExcludeCurrent() { _setTimeframeWithCurrent(22, false); }
function setTimeframe23IncludeCurrent() { _setTimeframeWithCurrent(23, true); }
function setTimeframe23ExcludeCurrent() { _setTimeframeWithCurrent(23, false); }

// ============================================================
// SYNC TRIGGER
// ============================================================

function triggerSync(mode, options) {
  options = options || {};
  var config  = loadConfig();
  var league  = LEAGUE;
  var apiBase = config.api_base_url || getApiBaseUrl();
  var props   = PropertiesService.getDocumentProperties();

  if (mode) props.setProperty('STATS_MODE', mode);
  var statsMode = mode || props.getProperty('STATS_MODE');

  // If STATS_MODE was never set (initial sync via CLI), detect from headers
  if (!statsMode) {
    var labels = {
      'per 100 Poss': 'per_100',
      'per Game':     'per_game',
      'per 48 Mins':  'per_48',
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
    if (!statsMode) statsMode = 'per_100';
  }

  // Build sync payload — league-aware
  var payload = {
    stats_mode:    statsMode,
    priority_team: options.priorityTeam || null,
    data_only:     true,
  };

  // Historical timeframe — always read from document properties
  var showAdvanced = league.hasAdvancedStats
    ? ((options.showAdvanced !== undefined)
        ? options.showAdvanced
        : (props.getProperty('SHOW_ADVANCED') === 'true'))
    : false;

  payload.mode            = props.getProperty('HIST_MODE') || 'years';
  payload.years           = parseInt(props.getProperty('HIST_YEARS') || '3');
  payload.include_current = props.getProperty('HIST_INCLUDE_CURRENT') === 'true';
  payload.show_advanced   = showAdvanced;

  var histSeason = props.getProperty('HIST_SEASON') || null;
  if (histSeason) payload.seasons = [histSeason];

  var url = apiBase + league.syncEndpoint;

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
// PLAYER / TEAM FIELD UPDATES
// ============================================================

function getPlayerIdByName(playerName, teamAbbr) {
  var config = loadConfig();
  var league = LEAGUE;
  var teams  = config[league.teamsKey] || {};
  var teamId = teams[teamAbbr];
  if (!teamId) return null;
  var url = getApiBaseUrl() + league.apiPrefix + '/team/' + teamId + '/players';
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
  var league  = LEAGUE;
  var payload = {};
  payload[fieldName] = fieldValue;
  var response = UrlFetchApp.fetch(getApiBaseUrl() + league.apiPrefix + '/player/' + playerId, {
    method: 'patch', contentType: 'application/json',
    payload: JSON.stringify(payload), muteHttpExceptions: true
  });
  var data = JSON.parse(response.getContentText());
  if (response.getResponseCode() !== 200) throw new Error(data.error || 'Unknown error');
  return data;
}

function updateTeamField(teamId, fieldName, fieldValue) {
  var league  = LEAGUE;
  var payload = {};
  payload[fieldName] = fieldValue;
  var response = UrlFetchApp.fetch(getApiBaseUrl() + league.apiPrefix + '/teams/' + teamId, {
    method: 'put', contentType: 'application/json',
    payload: JSON.stringify(payload), muteHttpExceptions: true
  });
  var data = JSON.parse(response.getContentText());
  if (response.getResponseCode() !== 200) throw new Error(data.error || 'Unknown error');
  return data;
}

// ============================================================
// WINGSPAN PARSER  (NBA only — guarded in onEditInstallable)
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
// SHEET TYPE & RANGE HELPERS
// ============================================================

/** Determine sheet type: 'team', 'players', 'teams', or null. */
function _getSheetType(sheetName) {
  var config = loadConfig();
  var league = LEAGUE;
  var teams  = config[league.teamsKey] || {};
  var upper  = sheetName.toUpperCase();
  if (teams.hasOwnProperty(upper)) return 'team';
  if (league.playersSheetNames.indexOf(upper) !== -1) return 'players';
  if (upper === 'TEAMS') return 'teams';
  return null;
}

/** Map sheet type to the config range key. */
function _getRangeKey(sheetType) {
  var league = LEAGUE;
  if (sheetType === 'team')    return 'team_sheet';
  if (sheetType === 'players') return league.playersRangeKey;
  if (sheetType === 'teams')   return 'teams_sheet';
  return null;
}

/** Get active team abbreviation if on a team sheet, else null. */
function _getActiveTeamAbbr() {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var name   = ss.getActiveSheet().getName().toUpperCase();
  var config = loadConfig();
  var league = LEAGUE;
  var teams  = config[league.teamsKey] || {};
  return teams.hasOwnProperty(name) ? name : null;
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

// ============================================================
// VERTICAL BORDERS
// ============================================================

/**
 * Apply ALL vertical borders (both section and subsection) on a single sheet.
 * Config-driven: reads section_boundaries and subsection_boundaries from the API.
 *
 * Section borders:     Always present. Span row 1 through maxRows.
 *                      White in headers, black in data.
 * Subsection borders:  Only when showAdv=true. Span from subsection row through maxRows.
 *                      White in headers, black in data.
 *
 * @param {Sheet}   sheet
 * @param {string}  sheetType
 * @param {boolean} showAdv   - Whether advanced/subsection borders are visible
 */
function _applyVerticalBorders(sheet, sheetType, showAdv) {
  var config     = loadConfig();
  var rangeKey   = _getRangeKey(sheetType);
  var layout     = config.layout || {};
  var headerRows = layout.header_row_count || 4;
  var subRow     = config.subsection_row_index || 2;
  var maxRows    = sheet.getMaxRows();
  var maxCols    = sheet.getMaxColumns();

  function _setBoundaryBorder(baseCol, startRow, shouldShow) {
    if (baseCol > maxCols) return;
    var firstDataRow = headerRows + 1;
    try {
      if (shouldShow) {
        if (startRow <= headerRows && baseCol <= maxCols) {
          sheet.getRange(startRow, baseCol, headerRows - startRow + 1, 1)
               .setBorder(null, true, null, null, null, null,
                          '#FFFFFF', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
        }
        if (maxRows >= firstDataRow && baseCol <= maxCols) {
          sheet.getRange(firstDataRow, baseCol, maxRows - firstDataRow + 1, 1)
               .setBorder(null, true, null, null, null, null,
                          '#000000', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
        }
      } else {
        sheet.getRange(startRow, baseCol, maxRows - startRow + 1, 1)
             .setBorder(null, false, null, null, null, null);
      }
    } catch (e) {
      Logger.log('Border error col ' + baseCol + ': ' + e);
    }
  }

  // Section borders — always present, full height (row 1 through maxRows)
  var secBounds = (config.section_boundaries || {})[rangeKey] || [];
  for (var s = 0; s < secBounds.length; s++) {
    _setBoundaryBorder(secBounds[s].col, 1, true);
  }

  // Subsection borders — only when advanced is visible, starts at subRow
  var subBounds = (config.subsection_boundaries || {})[rangeKey] || [];
  for (var b = 0; b < subBounds.length; b++) {
    _setBoundaryBorder(subBounds[b].col, subRow, showAdv);
  }
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
 * Re-apply current toggle states (advanced/basic) after any toggle.
 * Ensures toggles are DRY — showing a section doesn't reveal columns that should
 * be hidden by another toggle (e.g. basic columns when advanced is shown).
 *
 * Single-mode architecture: only one set of stat columns exists per sheet,
 * so no mode-based column hiding is needed.
 * Percentile companions are always visible (inline, not toggled).
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
  var league   = LEAGUE;

  var showAdv = league.hasAdvancedStats
    ? (props.getProperty('SHOW_ADVANCED') === 'true')
    : false;

  // --- column_metadata path (per-column flags, batched) ---
  var colMeta = (config.column_metadata || {})[rangeKey];
  if (colMeta && colMeta.length > 0) {
    var showList = [];
    var hideList = [];

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

      var isAdvanced = meta.adv || false;
      var isBasic    = meta.bas || false;
      var isStats    = meta.stats || false;
      var secName    = meta.sec || '';

      if (!isStats) continue;

      var shouldShow = true;

      // Respect section-level visibility
      var secVisKey = _secVisMap[secName];
      if (secVisKey && props.getProperty(secVisKey) === 'false') {
        shouldShow = false;
      } else {
        if (isAdvanced && !showAdv) shouldShow = false;
        if (isBasic && showAdv)     shouldShow = false;
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

  var props = PropertiesService.getDocumentProperties();
  props.setProperty('SECTION_VIS_' + sectionKey.toUpperCase(), makeVisible ? 'true' : 'false');

  function applyToSheet(sheet) {
    var name      = sheet.getName().toUpperCase();
    var sheetType = _getSheetType(name);
    if (!sheetType) return;
    var rangeKey = _getRangeKey(sheetType);
    var maxCols = sheet.getMaxColumns();

    // Preferred path: section-aware per-column metadata
    var secMap = {
      current: 'current_stats',
      historical: 'historical_stats',
      postseason: 'postseason_stats',
      player_info: 'player_info',
      notes: 'analysis'
    };
    var targetSec = secMap[sectionKey] || sectionKey;
    var colMeta = ((config.column_metadata || {})[rangeKey] || []);
    var sectionCols = [];
    for (var m = 0; m < colMeta.length; m++) {
      var meta = colMeta[m];
      if (meta.sec === targetSec && meta.col <= maxCols) {
        sectionCols.push(meta.col);
      }
    }

    if (sectionCols.length > 0) {
      _batchColumns(sheet, sectionCols, makeVisible);
      updatedCount++;
    } else {
      // Fallback path: legacy contiguous range map
      var ranges = (columnRanges[rangeKey] || {})[sectionKey] || null;
      if (!ranges) return;
      if (ranges.start > maxCols) return;
      var count = Math.min(ranges.count, maxCols - ranges.start + 1);
      try {
        if (makeVisible) sheet.showColumns(ranges.start, count);
        else             sheet.hideColumns(ranges.start, count);
        updatedCount++;
      } catch (e) {
        Logger.log('_setSectionVisibility error on ' + sheet.getName() + ': ' + e);
      }
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
