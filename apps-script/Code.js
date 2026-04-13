/**
 * apps-script/Code.js
 *
 * Unified, config-driven UI layer for The Glass stats spreadsheets.
 * ALL data calculations and syncing are performed by the Python backend CLI.
 * This file handles display settings, mode switching, and linked editable cells.
 *
 * Deployment:
 *   1. Generate config:  python -m src.sheets.config_export --league nba
 *   2. Push via clasp:   clasp push
 *   Config is loaded from the generated JS file (e.g. config/NBA_generated.js).
 *
 * Responsibilities:
 *   - Menu creation and user interactions
 *   - Stat rate switching via column show/hide (instant, no API)
 *   - Section and advanced stats visibility toggles
 *   - Linked editable cells — edits propagate across all tabs for the same entity
 *   - Vertical border management
 *
 * Explicitly NOT done here:
 *   - Stat calculations of any kind
 *   - API calls (removed — all data comes from Python CLI sync)
 *   - Percentile coloring (handled by Python sync)
 */

// ============================================================
// CONFIG — loaded from generated JS file (no API)
// ============================================================

function _getConfig() {
  if (typeof CONFIG === 'undefined') {
    throw new Error(
      'CONFIG is not defined. Run config_export.py and clasp push to generate it.'
    );
  }
  return CONFIG;
}

// ============================================================
// DYNAMIC FUNCTION REGISTRATION (V8 globalThis)
// ============================================================

(function() {
  try {
    var config = _getConfig();

    // Section show/hide handlers
    var sections = config.sections || {};
    Object.keys(sections).forEach(function(key) {
      if (!sections[key].toggleable) return;
      var displayName = sections[key].display_name || key;
      globalThis['show_' + key] = function() { _setSectionVisibility(key, true, displayName + ' shown'); };
      globalThis['hide_' + key] = function() { _setSectionVisibility(key, false, displayName + ' hidden'); };
    });

    // Stat rate switchers
    (config.stat_rates || []).forEach(function(rate) {
      globalThis['switchTo_' + rate] = function() { _switchStatRate(rate); };
    });

    // Timeframe handlers
    var maxYears = config.max_historical_timeframe || 20;
    for (var y = 1; y <= maxYears; y++) {
      (function(years) {
        globalThis['setTimeframe' + years] = function() { _setTimeframe(years); };
      })(y);
    }
  } catch (e) {
    // Simple trigger context — functions registered on next authorized execution.
  }
})();

// ============================================================
// MENU
// ============================================================

function onOpen() {
  var ui = SpreadsheetApp.getUi();

  try {
    var config = _getConfig();

    var menu = ui.createMenu('Display Settings');
    var menuConfig = config.menu || {};

    // --- Timeframe submenu ---
    var timeConfig = menuConfig.historical_timeframe || {};
    var timeMenu = ui.createMenu(timeConfig.display_name || 'Historical Timeframe');
    var maxYears = config.max_historical_timeframe || 20;
    for (var y = 1; y <= maxYears; y++) {
      timeMenu.addItem('Previous ' + y + ' Season' + (y > 1 ? 's' : ''), 'setTimeframe' + y);
    }
    menu.addSubMenu(timeMenu);

    // --- Stats Rate submenu ---
    var rateConfig = menuConfig.stats_rate || {};
    var rateMenu = ui.createMenu(rateConfig.display_name || 'Stats Rate');
    var rateLabels = config.stat_rate_labels || {};
    (config.stat_rates || []).forEach(function(rate) {
      rateMenu.addItem(rateLabels[rate] || rate, 'switchTo_' + rate);
    });
    menu.addSubMenu(rateMenu);

    // --- Stats Mode submenu (advanced/basic toggle) ---
    var modeConfig = menuConfig.stats_mode || {};
    menu.addSubMenu(ui.createMenu(modeConfig.display_name || 'Stats Mode')
      .addItem(modeConfig.show_label || 'Show Advanced', 'showAdvancedStats')
      .addItem(modeConfig.hide_label || 'Show Basic', 'hideAdvancedStats'));

    menu.addSeparator();

    // --- Section toggles ---
    var sections = config.sections || {};
    Object.keys(sections).forEach(function(key) {
      if (!sections[key].toggleable) return;
      menu.addSubMenu(ui.createMenu(sections[key].display_name)
        .addItem('Show', 'show_' + key)
        .addItem('Hide', 'hide_' + key));
    });

    menu.addToUi();
  } catch (e) {
    ui.createMenu('Display Settings')
      .addItem('Config not loaded — run config_export.py + clasp push', 'onOpen')
      .addToUi();
  }
}

// ============================================================
// RATE SWITCHING — instant column show/hide
// ============================================================

/**
 * Switch stat rate by showing/hiding pre-computed column groups.
 * All rates' data is already written by the Python sync — switching
 * is purely a column visibility change (instant, no API call).
 */
function _switchStatRate(newRate) {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var config = _getConfig();
  var props  = PropertiesService.getDocumentProperties();

  var currentRate = props.getProperty('STATS_RATE') || config.default_stat_rate;
  if (newRate === currentRate) {
    ss.toast('Already in ' + (config.stat_rate_labels[newRate] || newRate) + ' mode', 'Rate', 2);
    return;
  }

  props.setProperty('STATS_RATE', newRate);

  // Update section header labels
  var allLabels = [];
  var labels = config.stat_rate_labels || {};
  for (var key in labels) { allLabels.push(labels[key]); }
  var newLabel = labels[newRate] || '';

  _applyToAllSheets(function(sheet, sheetType) {
    var rangeKey = _getRangeKey(sheetType);
    var rateRanges = (config.rate_column_ranges || {})[rangeKey] || {};

    // Show target rate columns, hide all other rate columns
    var statRates = config.stat_rates || [];
    for (var i = 0; i < statRates.length; i++) {
      var rate = statRates[i];
      var cols = rateRanges[rate] || [];
      if (cols.length > 0) {
        _batchColumns(sheet, cols, rate === newRate);
      }
    }

    // Re-apply advanced/section toggles on the newly visible columns
    _reapplyToggles(sheet, sheetType);
    _rehideAlwaysHidden(sheet, sheetType);

    // Update header text
    _updateSectionHeaders(sheet, allLabels, newLabel);
  }, (config.stat_rate_labels[newRate] || newRate) + ' mode');
}

/**
 * Replace any known rate label in section headers (row 1) with the new one.
 */
function _updateSectionHeaders(sheet, allLabels, newLabel) {
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
  var config = _getConfig();
  var props  = PropertiesService.getDocumentProperties();
  props.setProperty('SHOW_ADVANCED', newAdvancedVisible ? 'true' : 'false');

  var subRow = config.subsection_row_index || 2;

  _applyToAllSheets(function(sheet, sheetType) {
    try {
      if (newAdvancedVisible) sheet.showRows(subRow, 1);
      else                    sheet.hideRows(subRow, 1);
    } catch (e) { Logger.log('Subsection row toggle error: ' + e); }

    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);
    _applyVerticalBorders(sheet, sheetType, newAdvancedVisible);
  }, newAdvancedVisible ? 'Advanced stats shown' : 'Basic stats shown');
}

// ============================================================
// HISTORICAL TIMEFRAME (stored as document property for next sync)
// ============================================================

function _setTimeframe(years) {
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('HISTORICAL_TIMEFRAME', String(years));
  var label = years + ' season' + (years > 1 ? 's' : '');
  SpreadsheetApp.getActiveSpreadsheet().toast(
    'Timeframe set to ' + label + '. Run Python sync to apply.', 'Updated', 4
  );
}

// ============================================================
// EDIT TRIGGER — linked cells across tabs
// ============================================================

/**
 * Propagate editable cell changes across all tabs for the same entity.
 *
 * When a player's notes/hand/etc is edited on any tab (team sheet, Players,
 * or Teams), this finds the same entity on all other tabs and updates
 * the matching cell.
 *
 * The Players and Teams sheets serve as the authoritative source for
 * DB sync — the Python CLI reads editable values from those sheets.
 */
function onEditInstallable(e) {
  var sheet     = e.range.getSheet();
  var sheetName = sheet.getName().toUpperCase();
  var config    = _getConfig();
  var league    = config.league || {};
  var teams     = config[league.teams_key] || {};
  var sheetType = _getSheetType(sheetName);

  if (!sheetType) return;

  var layout    = config.layout || {};
  var editedRow = e.range.getRow();
  if (editedRow <= (layout.header_row_count || 4)) return;

  var editedCol = e.range.getColumn();
  var newValue  = e.range.getValue();
  var ss        = SpreadsheetApp.getActiveSpreadsheet();

  // --- Identify what was edited ---
  var editableColumns = config.editable_columns || [];
  var teamsEditable   = config.teams_editable_columns || [];
  var colIndices      = config.column_indices || {};
  var matched         = null;
  var entityType      = null;
  var isPlayersSheet  = (sheetType === 'players');

  if (sheetType === 'teams') {
    for (var i = 0; i < teamsEditable.length; i++) {
      if (teamsEditable[i].col_index === editedCol) { matched = teamsEditable[i]; break; }
    }
    if (!matched) return;
    entityType = 'team';
  } else {
    for (var i = 0; i < editableColumns.length; i++) {
      var ec     = editableColumns[i];
      var colIdx = isPlayersSheet
        ? ec[league.edit_col_index_key]
        : ec.team_col_index;
      if (colIdx === editedCol) { matched = ec; break; }
    }
    if (!matched) return;

    var entityName = sheet.getRange(editedRow, 1).getValue();
    if (!entityName) return;

    if (entityName === 'OPPONENTS') {
      ss.toast('Opponent rows cannot be edited', 'Info', 3);
      return;
    }
    entityType = (entityName === 'TEAM') ? 'team' : 'player';
  }

  // --- Validate measurement format ---
  if (matched.format === 'measurement') {
    var parsed = parseMeasurementInput(newValue);
    if (parsed === null) {
      ss.toast("Invalid measurement. Use feet'inches (e.g. 6'8) or total inches.", 'Error', 5);
      return;
    }
    newValue = parsed;
    e.range.setValue(parsed);
  }

  // --- Propagate to other sheets ---
  var entityName = sheet.getRange(editedRow, 1).getValue();
  var idCol      = colIndices.player_id || 1;
  var entityId   = (entityType === 'player')
    ? sheet.getRange(editedRow, idCol).getValue()
    : null;
  var teamAbbr   = null;

  if (sheetType === 'team') {
    teamAbbr = sheetName;
  } else if (isPlayersSheet && entityType === 'player') {
    teamAbbr = sheet.getRange(editedRow, colIndices.team || 2).getValue();
  } else if (sheetType === 'teams') {
    teamAbbr = _teamNameToAbbr(entityName, config);
  }

  var allSheets = ss.getSheets();
  var sourceId  = sheet.getSheetId();

  for (var s = 0; s < allSheets.length; s++) {
    var targetSheet = allSheets[s];
    if (targetSheet.getSheetId() === sourceId) continue;

    var targetName = targetSheet.getName().toUpperCase();
    var targetType = _getSheetType(targetName);
    if (!targetType) continue;

    var targetCol = _getEditableColForSheet(matched, targetType, league);
    if (!targetCol) continue;

    _propagateValue(targetSheet, targetType, entityType, entityId,
                    entityName, teamAbbr, targetCol, newValue, config);
  }

  var displayName = matched.display_name || matched.db_field || '';
  ss.toast(displayName + ' updated for ' + entityName, 'Saved', 3);
}

/**
 * Get the column index for an editable field on a given sheet type.
 */
function _getEditableColForSheet(matched, sheetType, league) {
  if (sheetType === 'teams') return matched.col_index || null;
  if (sheetType === 'players') return matched[league.edit_col_index_key] || null;
  if (sheetType === 'team') return matched.team_col_index || null;
  return null;
}

/**
 * Write a value to the matching entity row on a target sheet.
 */
function _propagateValue(sheet, sheetType, entityType, entityId,
                         entityName, teamAbbr, targetCol, value, config) {
  var layout = config.layout || {};
  var dataStart = layout.data_start_row || 4;
  var lastRow = sheet.getLastRow();
  if (lastRow < dataStart) return;

  var colIndices = config.column_indices || {};
  var targetName = sheet.getName().toUpperCase();

  if (entityType === 'player' && entityId) {
    var idCol = colIndices.player_id || 1;
    var ids = sheet.getRange(dataStart + 1, idCol, lastRow - dataStart, 1).getValues();
    for (var r = 0; r < ids.length; r++) {
      if (String(ids[r][0]) === String(entityId)) {
        sheet.getRange(dataStart + 1 + r, targetCol).setValue(value);
        return;
      }
    }
  } else if (entityType === 'team') {
    var names = sheet.getRange(dataStart + 1, 1, lastRow - dataStart, 1).getValues();
    for (var r = 0; r < names.length; r++) {
      var rowName = String(names[r][0]).toUpperCase();
      if (rowName === 'TEAM' && sheetType === 'team') {
        if (targetName === (teamAbbr || '').toUpperCase()) {
          sheet.getRange(dataStart + 1 + r, targetCol).setValue(value);
          return;
        }
      } else if (sheetType === 'teams') {
        var rowAbbr = _teamNameToAbbr(names[r][0], config);
        if (rowAbbr && rowAbbr === (teamAbbr || '').toUpperCase()) {
          sheet.getRange(dataStart + 1 + r, targetCol).setValue(value);
          return;
        }
      }
    }
  }
}

/**
 * Resolve a team display name to its abbreviation using config lookup.
 */
function _teamNameToAbbr(name, config) {
  if (!name) return null;
  var upper = String(name).toUpperCase();
  var league = config.league || {};
  var teams = config[league.teams_key] || {};
  if (teams.hasOwnProperty(upper)) return upper;
  var map = config.team_name_to_abbr || {};
  return map[name] || null;
}

// ============================================================
// MEASUREMENT FORMAT PARSER
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

function _getSheetType(sheetName) {
  var config = _getConfig();
  var league = config.league || {};
  var teams  = config[league.teams_key] || {};
  var upper  = sheetName.toUpperCase();
  if (teams.hasOwnProperty(upper)) return 'team';
  if ((league.players_sheet_names || []).indexOf(upper) !== -1) return 'players';
  if (upper === 'TEAMS') return 'teams';
  return null;
}

function _getRangeKey(sheetType) {
  var league = _getConfig().league || {};
  if (sheetType === 'team')    return 'team_sheet';
  if (sheetType === 'players') return league.players_range_key;
  if (sheetType === 'teams')   return 'teams_sheet';
  return null;
}

// ============================================================
// DRY HELPERS
// ============================================================

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
  if (toastMsg) SpreadsheetApp.getActiveSpreadsheet().toast(toastMsg, 'View', 3);
}

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
// SECTION VISIBILITY TOGGLES
// ============================================================

function _rehideAlwaysHidden(sheet, sheetType) {
  var config     = _getConfig();
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
 * Re-apply current toggle states after any visibility change.
 * Respects: active stat rate, advanced/basic toggle, section visibility.
 */
function _reapplyToggles(sheet, sheetType) {
  var config   = _getConfig();
  var props    = PropertiesService.getDocumentProperties();
  var rangeKey = _getRangeKey(sheetType);
  var maxCols  = sheet.getMaxColumns();

  var showAdv = (props.getProperty('SHOW_ADVANCED') === 'true');
  var activeRate = props.getProperty('STATS_RATE') || config.default_stat_rate;

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

    // Determine base section and rate from composite key
    var baseSection = secName;
    var colRate = null;
    if (secName.indexOf('__') !== -1) {
      var parts = secName.split('__');
      baseSection = parts[0];
      colRate = parts[1];
    }

    var shouldShow = true;

    // Hide columns that belong to a non-active stat rate
    if (colRate && colRate !== activeRate) {
      shouldShow = false;
    } else {
      // Respect section-level visibility
      var secVisKey = baseSection ? 'SECTION_VIS_' + baseSection.toUpperCase() : null;
      if (secVisKey && props.getProperty(secVisKey) === 'false') {
        shouldShow = false;
      } else {
        if (isAdvanced && !showAdv) shouldShow = false;
        if (isBasic && showAdv)     shouldShow = false;
      }
    }

    if (shouldShow) showList.push(colIdx);
    else            hideList.push(colIdx);
  }

  _batchColumns(sheet, showList, true);
  _batchColumns(sheet, hideList, false);
}

function _setSectionVisibility(sectionKey, makeVisible, label) {
  var ss     = SpreadsheetApp.getActiveSpreadsheet();
  var config = _getConfig();
  var props  = PropertiesService.getDocumentProperties();
  props.setProperty('SECTION_VIS_' + sectionKey.toUpperCase(), makeVisible ? 'true' : 'false');

  var updatedCount = 0;

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
      var baseSec = meta.sec || '';
      if (baseSec.indexOf('__') !== -1) baseSec = baseSec.split('__')[0];
      if (baseSec === sectionKey && meta.col <= maxCols) {
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

  var activeSheet = ss.getActiveSheet();
  applyToSheet(activeSheet);
  SpreadsheetApp.flush();

  var sheets = ss.getSheets();
  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getSheetId() === activeSheet.getSheetId()) continue;
    applyToSheet(sheets[i]);
  }

  ss.toast(label + ' (' + updatedCount + ' sheets)', 'Section Visibility', 3);
}

function showAllSections() {
  var config   = _getConfig();
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
      var baseSec = colMeta[i].sec || '';
      if (baseSec.indexOf('__') !== -1) baseSec = baseSec.split('__')[0];
      if (colMeta[i].col <= maxCols && toggleableKeys.indexOf(baseSec) !== -1) {
        showCols.push(colMeta[i].col);
      }
    }

    _batchColumns(sheet, showCols, true);
    _rehideAlwaysHidden(sheet, sheetType);
    _reapplyToggles(sheet, sheetType);
  }, 'All sections shown');
}

// ============================================================
// VERTICAL BORDERS
// ============================================================

function _applyVerticalBorders(sheet, sheetType, showAdv) {
  var config     = _getConfig();
  var rangeKey   = _getRangeKey(sheetType);
  var layout     = config.layout || {};
  var headerRows = layout.header_row_count || 4;
  var subRow     = config.subsection_row_index || 2;
  var maxRows    = sheet.getMaxRows();
  var maxCols    = sheet.getMaxColumns();

  function _setBoundaryBorder(baseCol, startRow, shouldShow, isSectionBorder) {
    if (baseCol > maxCols) return;
    var firstDataRow = headerRows + 1;
    var borderWeight = isSectionBorder
      ? SpreadsheetApp.BorderStyle.SOLID_MEDIUM
      : SpreadsheetApp.BorderStyle.SOLID;

    try {
      if (shouldShow) {
        if (startRow <= headerRows && baseCol <= maxCols) {
          sheet.getRange(startRow, baseCol, headerRows - startRow + 1, 1)
               .setBorder(null, true, null, null, null, null, '#FFFFFF', borderWeight);
        }
        if (maxRows >= firstDataRow && baseCol <= maxCols) {
          sheet.getRange(firstDataRow, baseCol, maxRows - firstDataRow + 1, 1)
               .setBorder(null, true, null, null, null, null, '#000000', borderWeight);
        }
      } else {
        sheet.getRange(startRow, baseCol, maxRows - startRow + 1, 1)
             .setBorder(null, false, null, null, null, null);
      }
    } catch (e) {
      Logger.log('Border error col ' + baseCol + ': ' + e);
    }
  }

  var secBounds = (config.section_boundaries || {})[rangeKey] || [];
  var secBoundCols = [];
  for (var s = 0; s < secBounds.length; s++) {
    secBoundCols.push(secBounds[s].col);
    _setBoundaryBorder(secBounds[s].col, 1, true, true);
  }

  var subBounds = (config.subsection_boundaries || {})[rangeKey] || [];
  for (var b = 0; b < subBounds.length; b++) {
    var col = subBounds[b].col;
    if (showAdv) {
      if (secBoundCols.indexOf(col) === -1) {
        _setBoundaryBorder(col, subRow, true, false);
      }
    } else {
      if (secBoundCols.indexOf(col) === -1) {
        _setBoundaryBorder(col, subRow, false, false);
      }
    }
  }
}
