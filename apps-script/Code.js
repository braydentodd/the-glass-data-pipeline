/**
 * apps-script/Code.js
 *
 * Unified, config-driven UI layer for The Glass stats spreadsheets.
 * ALL data calculations are performed by the Python backend.
 *
 * Deployment:
 *   1. Copy this file into the Apps Script project for each league's spreadsheet.
 *   2. Code.js is IDENTICAL for all leagues — never edit it per-league.
 *   3. Set two Script Properties (File > Project Properties > Script Properties):
 *        LEAGUE        — lowercase league slug (e.g. 'nba', 'ncaa')
 *        API_BASE_URL  — server base URL (e.g. 'http://150.136.255.23:5000')
 *   4. Create an installable onOpen trigger (Edit > Current project's triggers)
 *      pointing to the `onOpen` function so the full menu loads on open.
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
 *   - Percentile coloring (handled by Python sync)
 */

// ============================================================
// BOOTSTRAP — Script Properties
// ============================================================

var LEAGUE_SLUG = (function() {
  var league = PropertiesService.getScriptProperties().getProperty('LEAGUE');
  if (!league) {
    throw new Error(
      'Script Property "LEAGUE" is not set. ' +
      'Go to File > Project Properties > Script Properties and add LEAGUE (e.g. "nba").'
    );
  }
  return league.toLowerCase();
})();

// ============================================================
// CONFIG
// ============================================================

var CONFIG = null;

function _getScriptProperty(key) {
  var val = PropertiesService.getScriptProperties().getProperty(key);
  if (!val) {
    throw new Error(
      'Script Property "' + key + '" is not set. ' +
      'Go to File > Project Properties > Script Properties to add it.'
    );
  }
  return val;
}

/**
 * Load config from the Python API. Cached for the lifetime of the script run.
 * All league-specific metadata is provided by the API response.
 */
function loadConfig() {
  if (CONFIG) return CONFIG;

  var apiBase = _getScriptProperty('API_BASE_URL');
  var url = apiBase + '/api/config?league=' + LEAGUE_SLUG;

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

  throw new Error('Could not load config from API (' + url + '). All operations require the API.');
}

function getApiBaseUrl() {
  return loadConfig().api_base_url || _getScriptProperty('API_BASE_URL');
}

function getColors() {
  return loadConfig().colors;
}

// ============================================================
// DYNAMIC FUNCTION REGISTRATION (V8 globalThis)
// ============================================================

// Timeframe handlers — years 1..23 is a fixed range, no config needed.
(function() {
  for (var y = 1; y <= 23; y++) {
    (function(years) {
      globalThis['setTimeframe' + years + 'IncludeCurrent'] = function() { _setTimeframeWithCurrent(years, true); };
      globalThis['setTimeframe' + years + 'ExcludeCurrent'] = function() { _setTimeframeWithCurrent(years, false); };
    })(y);
  }
})();

// Section show/hide + stat mode switchers — requires config.
// Wrapped in try-catch: succeeds in authorized contexts (menu clicks,
// installable triggers), silently fails in simple trigger context.
(function() {
  try {
    var config = loadConfig();

    var sections = config.sections || {};
    Object.keys(sections).forEach(function(key) {
      if (!sections[key].toggleable) return;
      var displayName = sections[key].display_name || key;
      globalThis['show_' + key] = function() { _setSectionVisibility(key, true, displayName + ' shown'); };
      globalThis['hide_' + key] = function() { _setSectionVisibility(key, false, displayName + ' hidden'); };
    });

    (config.stat_modes || []).forEach(function(mode) {
      globalThis['switchTo_' + mode] = function() { _switchStatMode(mode); };
    });
  } catch (e) {
    // Expected in simple trigger context (onOpen without installable trigger).
    // Functions will be registered on the next authorized execution.
  }
})();

// ============================================================
// MENU
// ============================================================

/**
 * Build the full menu from config. Works as an installable onOpen trigger.
 * If loadConfig() fails (simple trigger context), shows a minimal fallback.
 */
function onOpen() {
  var ui = SpreadsheetApp.getUi();

  try {
    var config = loadConfig();

    var menu = ui.createMenu('Display Settings')
      .addSubMenu(_buildTimeframeMenu());

    // Stats Mode — driven by config.stat_modes + config.stat_mode_labels
    var modeMenu = ui.createMenu('Stats Mode');
    var modeLabels = config.stat_mode_labels;
    (config.stat_modes || []).forEach(function(mode) {
      modeMenu.addItem(modeLabels[mode] || mode, 'switchTo_' + mode);
    });
    menu.addSubMenu(modeMenu);

    // Advanced stats toggle (subsection row, not a section)
    menu.addSubMenu(ui.createMenu('Advanced Stats')
      .addItem('Show', 'showAdvancedStats')
      .addItem('Hide', 'hideAdvancedStats'));

    menu.addSeparator();

    // Section toggles — driven by config.sections with toggleable flag
    var sections = config.sections || {};
    Object.keys(sections).forEach(function(key) {
      if (!sections[key].toggleable) return;
      menu.addSubMenu(ui.createMenu(sections[key].display_name)
        .addItem('Show', 'show_' + key)
        .addItem('Hide', 'hide_' + key));
    });

    menu.addToUi();
  } catch (e) {
    // Simple trigger context — UrlFetchApp not available.
    // Set up an installable onOpen trigger to get the full menu.
    ui.createMenu('Display Settings')
      .addItem('Menu unavailable — set up installable onOpen trigger', 'onOpen')
      .addToUi();
  }
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
  var league    = config.league;
  var teams     = config[league.teams_key] || {};
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
    var colIdx = isPlayersSheet ? ec[league.edit_col_index_key] : ec.team_col_index;
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
    if (matched.team_row_calc !== 'editable') {
      ss.toast(matched.display_name + ' is not editable for teams', 'Info', 3);
      return;
    }
    var teamId = teams[teamAbbr];
    if (!teamId) {
      ss.toast('Could not find team: ' + teamAbbr, 'Error', 3);
      return;
    }
    var teamDbField = matched.db_field;
    if (matched.format === 'measurement') {
      value = parseMeasurementInput(value);
      if (value === null) {
        ss.toast("Invalid measurement. Use feet'inches (e.g. 6'8) or total inches.", 'Error', 5);
        return;
      }
    }
    try {
      updateTeamField(teamId, teamDbField, value);
      ss.toast(matched.display_name + ' saved for ' + teamAbbr, 'Saved \u2713', 3);
    } catch (err) {
      ss.toast('Error saving ' + matched.display_name + ': ' + err.message, 'Error', 5);
    }
    return;
  }

  // ---- OPPONENTS row: not editable ----
  if (entityName === 'OPPONENTS') {
    ss.toast('Opponent rows cannot be edited', 'Info', 3);
    return;
  }

  // ---- Player row ----
  if (matched.format === 'measurement') {
    value = parseMeasurementInput(value);
    if (value === null) {
      ss.toast(`Invalid measurement. Use [feet]'[inches]" (e.g. 6'8").`, 'Error', 5);
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

  triggerSync(newMode, { prioritySheet: _getActiveSheetKey() });
}

/**
 * Update section header text (row 1) to reflect the current stat mode.
 * Replaces mode labels in stats section headers.
 */
function _updateSectionHeaders(sheet, newMode) {
  var config = loadConfig();
  var labels = config.stat_mode_labels;
  var allLabels = [];
  for (var key in labels) { allLabels.push(labels[key]); }
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
// ADVANCED STATS TOGGLE
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
  if (years) props.setProperty('HIST_SEASONS_COUNT', String(years));
  var label = years + ' season' + (years > 1 ? 's' : '');
  SpreadsheetApp.getActiveSpreadsheet().toast('Timeframe set to ' + label, 'Updated', 3);
  triggerSync(null, { prioritySheet: _getActiveSheetKey() });
}

function _setIncludeCurrentSeason(include) {
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('HIST_INCLUDE_CURRENT', include ? 'true' : 'false');
  var msg = include ? 'Current season included' : 'Current season excluded';
  SpreadsheetApp.getActiveSpreadsheet().toast(msg, 'Updated', 3);
  triggerSync(null, { prioritySheet: _getActiveSheetKey() });
}

function _setTimeframeWithCurrent(years, includeCurrent) {
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('HIST_INCLUDE_CURRENT', includeCurrent ? 'true' : 'false');
  _setTimeframe('seasons', years);
}

// ============================================================
// SYNC TRIGGER
// ============================================================

function triggerSync(mode, options) {
  options = options || {};
  var config  = loadConfig();
  var league  = config.league;
  var apiBase = config.api_base_url || getApiBaseUrl();
  var props   = PropertiesService.getDocumentProperties();

  if (mode) props.setProperty('STATS_MODE', mode);
  var statsMode = mode || props.getProperty('STATS_MODE');

  // If STATS_MODE was never set (initial sync via CLI), detect from headers
  if (!statsMode) {
    var modeLabels = config.stat_mode_labels;
    var labelToMode = {};
    for (var mKey in modeLabels) { labelToMode[modeLabels[mKey]] = mKey; }
    try {
      var active = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
      var lastCol = active.getMaxColumns();
      if (lastCol > 0) {
        var headerVals = active.getRange(1, 1, 1, lastCol).getValues()[0];
        for (var hi = 0; hi < headerVals.length; hi++) {
          var headerText = String(headerVals[hi]);
          for (var lbl in labelToMode) {
            if (headerText.indexOf(lbl) !== -1) {
              statsMode = labelToMode[lbl];
              props.setProperty('STATS_MODE', statsMode);
              break;
            }
          }
          if (statsMode) break;
        }
      }
    } catch (e) { Logger.log('Stats mode detection error: ' + e); }
    if (!statsMode) statsMode = config.default_stat_mode || 'per_100';
  }

  // Reliably detect advanced stats state directly from the sheet UI
  var subRow = config.subsection_row_index || 2;
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var checkSheet = ss.getActiveSheet();
  
  // If the subsection row is visible, advanced stats are shown. If it throws, default to property.
  var actuallyVisible = true;
  try {
      actuallyVisible = !checkSheet.isRowHiddenByUser(subRow);
  } catch(e) {
      actuallyVisible = (props.getProperty('SHOW_ADVANCED') === 'true');
  }

  var showAdvanced = (options.showAdvanced !== undefined)
    ? options.showAdvanced
    : actuallyVisible;

  var previousAdvanced = (props.getProperty('SHOW_ADVANCED') === 'true');
  var advancedToggled = (showAdvanced !== previousAdvanced);

  // Build sync payload — league-aware
  var payload = {
    stats_mode:    statsMode,
    priority_team: options.prioritySheet || null,
    partial_update:     !advancedToggled,
  };
  if (options.syncSection) {
    payload.sync_section = options.syncSection;
  }

  // Keep document properties in sync with UI
  props.setProperty('SHOW_ADVANCED', showAdvanced ? 'true' : 'false');

  payload.mode            = props.getProperty('HIST_MODE') || 'seasons';
  payload.seasons_count           = parseInt(props.getProperty('HIST_SEASONS_COUNT') || '3');
  payload.include_current = props.getProperty('HIST_INCLUDE_CURRENT') === 'true';
  payload.show_advanced   = showAdvanced;
  payload.league          = league.slug;

  var histSeason = props.getProperty('HIST_SEASON') || null;
  if (histSeason) payload.seasons = [histSeason];

  var url = apiBase + league.sync_endpoint;

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
  var league = config.league;
  var teams  = config[league.teams_key] || {};
  var teamId = teams[teamAbbr];
  if (!teamId) return null;
  var url = getApiBaseUrl() + league.api_prefix + '/team/' + teamId + '/players?league=' + league.slug;
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
  var league  = loadConfig().league;
  var payload = {};
  payload[fieldName] = fieldValue;
  var url = getApiBaseUrl() + league.api_prefix + '/player/' + playerId + '?league=' + league.slug;
  var response = UrlFetchApp.fetch(url, {
    method: 'patch', contentType: 'application/json',
    payload: JSON.stringify(payload), muteHttpExceptions: true
  });
  var data = JSON.parse(response.getContentText());
  if (response.getResponseCode() !== 200) throw new Error(data.error || 'Unknown error');
  return data;
}

function updateTeamField(teamId, fieldName, fieldValue) {
  var league  = loadConfig().league;
  var payload = {};
  payload[fieldName] = fieldValue;
  var url = getApiBaseUrl() + league.api_prefix + '/teams/' + teamId + '?league=' + league.slug;
  var response = UrlFetchApp.fetch(url, {
    method: 'put', contentType: 'application/json',
    payload: JSON.stringify(payload), muteHttpExceptions: true
  });
  var data = JSON.parse(response.getContentText());
  if (response.getResponseCode() !== 200) throw new Error(data.error || 'Unknown error');
  return data;
}

// ============================================================
// MEASUREMENT FORMAT PARSER — converts feet'inches to total inches
// Used for any editable column with format: 'measurement'
// ============================================================

function parseMeasurementInput(value) {
  if (!value) return null;
  var str = value.toString().trim();
  var feetInches = str.match(/^(\d+)'(\d+)"?$/);
  if (feetInches) return parseInt(feetInches[1]) * 12 + parseInt(feetInches[2]);
  var inches = parseInt(str);
  if (!isNaN(inches) && inches > 0 && inches < 120) return inches;
  return null;
}

// ============================================================
// SHEET TYPE & RANGE HELPERS
// ============================================================

/** Determine sheet type: 'team', 'players', 'teams', or null. */
function _getSheetType(sheetName) {
  var config = loadConfig();
  var league = config.league;
  var teams  = config[league.teams_key] || {};
  var upper  = sheetName.toUpperCase();
  if (teams.hasOwnProperty(upper)) return 'team';
  if (league.players_sheet_names.indexOf(upper) !== -1) return 'players';
  if (upper === 'TEAMS') return 'teams';
  return null;
}

/** Map sheet type to the config range key. */
function _getRangeKey(sheetType) {
  var league = loadConfig().league;
  if (sheetType === 'team')    return 'team_sheet';
  if (sheetType === 'players') return league.players_range_key;
  if (sheetType === 'teams')   return 'teams_sheet';
  return null;
}

/**
 * Get the active sheet's key for priority syncing.
 * Returns the team abbreviation for team sheets, or the sheet name
 * for players/teams sheets. Null for unrecognized sheets.
 */
function _getActiveSheetKey() {
  var name = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet().getName().toUpperCase();
  return _getSheetType(name) ? name : null;
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

  function _setBoundaryBorder(baseCol, startRow, shouldShow, isSectionBorder) {
    if (baseCol > maxCols) return;
    var firstDataRow = headerRows + 1;
    var borderWeight = isSectionBorder ? SpreadsheetApp.BorderStyle.SOLID_MEDIUM : SpreadsheetApp.BorderStyle.SOLID;
    
    try {
      if (shouldShow) {
        if (startRow <= headerRows && baseCol <= maxCols) {
          sheet.getRange(startRow, baseCol, headerRows - startRow + 1, 1)
               .setBorder(null, true, null, null, null, null,
                          '#FFFFFF', borderWeight);
        }
        if (maxRows >= firstDataRow && baseCol <= maxCols) {
          sheet.getRange(firstDataRow, baseCol, maxRows - firstDataRow + 1, 1)
               .setBorder(null, true, null, null, null, null,
                          '#000000', borderWeight);
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
  var secBoundCols = [];
  for (var s = 0; s < secBounds.length; s++) {
    secBoundCols.push(secBounds[s].col);
    _setBoundaryBorder(secBounds[s].col, 1, true, true);
  }

  // Subsection borders — only when advanced is visible, starts at subRow
  var subBounds = (config.subsection_boundaries || {})[rangeKey] || [];
  for (var b = 0; b < subBounds.length; b++) {
    var col = subBounds[b].col;
    if (showAdv) {
      // Don't draw a subsection border if a section border already exists here
      if (secBoundCols.indexOf(col) === -1) {
        _setBoundaryBorder(col, subRow, true, false);
      }
    } else {
      // When hiding advanced stats, if there is no section border here, we must clear the orphaned border
      if (secBoundCols.indexOf(col) === -1) {
        _setBoundaryBorder(col, subRow, false, false);
      }
    }
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
 * Uses column_metadata for per-column precision.
 *
 * @param {Sheet}  sheet
 * @param {string} sheetType
 */
function _reapplyToggles(sheet, sheetType) {
  var config   = loadConfig();
  var props    = PropertiesService.getDocumentProperties();
  var rangeKey = _getRangeKey(sheetType);
  var maxCols  = sheet.getMaxColumns();

  var showAdv = (props.getProperty('SHOW_ADVANCED') === 'true');

  var colMeta = (config.column_metadata || {})[rangeKey] || [];
  var showList = [];
  var hideList = [];

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
    var secVisKey = secName ? 'SECTION_VIS_' + secName.toUpperCase() : null;
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
}

/**
 * Generic column-section visibility control.
 * @param {string}  sectionKey    - Config key for the section
 * @param {boolean} makeVisible   - true to show, false to hide
 * @param {string}  label         - Toast message
 */
function _setSectionVisibility(sectionKey, makeVisible, label) {
  var ss          = SpreadsheetApp.getActiveSpreadsheet();
  var config      = loadConfig();
  var activeSheet = ss.getActiveSheet();
  var sheets      = ss.getSheets();
  var updatedCount = 0;

  var props = PropertiesService.getDocumentProperties();
  props.setProperty('SECTION_VIS_' + sectionKey.toUpperCase(), makeVisible ? 'true' : 'false');

  function applyToSheet(sheet) {
    var name      = sheet.getName().toUpperCase();
    var sheetType = _getSheetType(name);
    if (!sheetType) return;
    var rangeKey = _getRangeKey(sheetType);
    var maxCols  = sheet.getMaxColumns();

    var colMeta = ((config.column_metadata || {})[rangeKey] || []);
    var sectionCols = [];
    for (var m = 0; m < colMeta.length; m++) {
      var meta = colMeta[m];
      if (meta.sec === sectionKey && meta.col <= maxCols) {
        sectionCols.push(meta.col);
      }
    }

    if (sectionCols.length > 0) {
      _batchColumns(sheet, sectionCols, makeVisible);
      updatedCount++;
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

function showAllSections() {
  var config   = loadConfig();
  var sections = config.sections || {};
  var props    = PropertiesService.getDocumentProperties();

  var toggleableKeys = Object.keys(sections).filter(function(key) {
    return sections[key].toggleable;
  });

  toggleableKeys.forEach(function(key) {
    props.setProperty('SECTION_VIS_' + key.toUpperCase(), 'true');
  });

  _applyToAllSheets(function(sheet, sheetType) {
    var rangeKey = _getRangeKey(sheetType);
    var maxCols  = sheet.getMaxColumns();
    var colMeta  = ((config.column_metadata || {})[rangeKey] || []);
    var showCols = [];

    for (var i = 0; i < colMeta.length; i++) {
      if (colMeta[i].col <= maxCols && toggleableKeys.indexOf(colMeta[i].sec) !== -1) {
        showCols.push(colMeta[i].col);
      }
    }

    _batchColumns(sheet, showCols, true);
    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);
  }, 'All sections shown');
}
