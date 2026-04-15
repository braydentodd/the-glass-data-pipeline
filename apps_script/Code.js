/**
 * apps_script/Code.js
 *
 * Config-driven UI and edit propagation for The Glass Sheets workbook.
 * The generated config is the source of truth for menu entries, sheet names,
 * editable fields, and column visibility metadata.
 */

function _getConfig() {
  if (typeof CONFIG === 'undefined') {
    throw new Error('CONFIG is not defined. Run the export step and clasp push to generate it.');
  }
  return CONFIG;
}

function _getRootScope() {
  return typeof globalThis !== 'undefined' ? globalThis : this;
}

function _ensureMenuActions() {
  if (typeof CONFIG === 'undefined') {
    return;
  }

  var root = _getRootScope();
  var config = _getConfig();
  var actionMap = {};

  actionMap.showAdvancedStats = function() { _setAdvancedStats(true); };
  actionMap.hideAdvancedStats = function() { _setAdvancedStats(false); };

  (config.supported_historical_timeframes || []).forEach(function(years) {
    actionMap['setTimeframe' + years] = function() {
      _setTimeframe(years);
    };
  });

  (config.stat_rates || []).forEach(function(rate) {
    actionMap['switchTo_' + rate] = function() {
      _switchStatRate(rate);
    };
  });

  Object.keys(config.sections || {}).forEach(function(sectionKey) {
    var section = config.sections[sectionKey];
    if (!section || !section.toggleable) {
      return;
    }

    actionMap['show_' + sectionKey] = function() {
      _setSectionVisibility(sectionKey, true);
    };
    actionMap['hide_' + sectionKey] = function() {
      _setSectionVisibility(sectionKey, false);
    };
  });

  Object.keys(actionMap).forEach(function(name) {
    if (typeof root[name] !== 'function') {
      root[name] = actionMap[name];
    }
  });
}

_ensureMenuActions();

function onOpen() {
  _ensureMenuActions();

  var ui = SpreadsheetApp.getUi();
  var config = _getConfig();
  var menu = _buildDisplayMenu(ui, config);

  try {
    menu.addToUi();
  } catch (err) {
    ui.createMenu('Display Settings')
      .addItem('Config not loaded', 'onOpen')
      .addToUi();
  }
}

function _buildDisplayMenu(ui, config) {
  var menuConfig = config.menu || {};
  var menu = ui.createMenu(menuConfig.display_name || 'Display Settings');

  var timeframeConfig = menuConfig.historical_timeframe || {};
  var supportedTimeframes = config.supported_historical_timeframes || [];
  if (supportedTimeframes.length) {
    var timeframeMenu = ui.createMenu(timeframeConfig.display_name || 'Historical Timeframe');
    supportedTimeframes.forEach(function(years) {
      timeframeMenu.addItem(_timeframeMenuLabel(years), 'setTimeframe' + years);
    });
    menu.addSubMenu(timeframeMenu);
  }

  var rateConfig = menuConfig.stats_rate || {};
  var rates = config.stat_rates || [];
  if (rates.length) {
    var rateMenu = ui.createMenu(rateConfig.display_name || 'Stats Rate');
    var rateLabels = config.stat_rate_labels || {};
    rates.forEach(function(rate) {
      rateMenu.addItem(rateLabels[rate] || rate, 'switchTo_' + rate);
    });
    menu.addSubMenu(rateMenu);
  }

  var statsModeConfig = menuConfig.stats_mode || {};
  menu.addSubMenu(
    ui.createMenu(statsModeConfig.display_name || 'Stats Mode')
      .addItem(statsModeConfig.show_label || 'Show Advanced', 'showAdvancedStats')
      .addItem(statsModeConfig.hide_label || 'Show Basic', 'hideAdvancedStats')
  );

  var sections = config.sections || {};
  var sectionKeys = Object.keys(sections).filter(function(sectionKey) {
    return sections[sectionKey] && sections[sectionKey].toggleable;
  });
  if (sectionKeys.length) {
    menu.addSeparator();
    sectionKeys.forEach(function(sectionKey) {
      var sectionConfig = sections[sectionKey];
      menu.addSubMenu(
        ui.createMenu(sectionConfig.display_name || sectionKey)
          .addItem('Show', 'show_' + sectionKey)
          .addItem('Hide', 'hide_' + sectionKey)
      );
    });
  }

  return menu;
}

function _timeframeMenuLabel(years) {
  return 'Previous ' + years + ' Season' + (years === 1 ? '' : 's');
}

function _switchStatRate(newRate) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var config = _getConfig();
  var props = PropertiesService.getDocumentProperties();
  var currentRate = props.getProperty('STATS_RATE') || config.default_stat_rate;

  if (newRate === currentRate) {
    ss.toast('Already in ' + (config.stat_rate_labels[newRate] || newRate) + ' mode', 'Rate', 2);
    return;
  }

  props.setProperty('STATS_RATE', newRate);
  _applyToAllSheets(function(sheet, sheetType) {
    _reapplyToggles(sheet, sheetType);
  }, (config.stat_rate_labels[newRate] || newRate) + ' mode');
}

function _setAdvancedStats(newAdvancedVisible) {
  PropertiesService.getDocumentProperties().setProperty('SHOW_ADVANCED', newAdvancedVisible ? 'true' : 'false');
  _applyToAllSheets(function(sheet, sheetType) {
    _reapplyToggles(sheet, sheetType);
  }, newAdvancedVisible ? 'Advanced stats shown' : 'Basic stats shown');
}

function _setTimeframe(years) {
  PropertiesService.getDocumentProperties().setProperty('HISTORICAL_TIMEFRAME', String(years));
  _applyToAllSheets(function(sheet, sheetType) {
    _reapplyToggles(sheet, sheetType);
  }, 'Timeframe swapped to ' + years + ' season' + (years === 1 ? '' : 's'));
}

function _setSectionVisibility(sectionKey, makeVisible) {
  PropertiesService.getDocumentProperties().setProperty(_getSectionVisibilityKey(sectionKey), makeVisible ? 'true' : 'false');
  _applyToAllSheets(function(sheet, sheetType) {
    _reapplyToggles(sheet, sheetType);
  }, sectionKey + (makeVisible ? ' shown' : ' hidden'));
}

function onEditInstallable(e) {
  if (!e || !e.range) {
    return;
  }

  var config = _getConfig();
  var sheet = e.range.getSheet();
  var sheetName = sheet.getName().toUpperCase();
  var sheetType = _getSheetType(sheetName);
  if (!sheetType) {
    return;
  }

  var layout = config.layout || {};
  var editedRow = e.range.getRow();
  if (editedRow <= (layout.header_row_count || 4)) {
    return;
  }

  var rangeKey = _getRangeKey(sheetType);
  var lookup = config.editable_lookup || {};
  var editedCol = e.range.getColumn();
  var matched = _findEditableMatch(lookup, rangeKey, editedCol);
  if (!matched) {
    return;
  }

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var rowLabel = String(sheet.getRange(editedRow, 1).getValue() || '').toUpperCase();
  if (!rowLabel) {
    return;
  }
  if (sheetType === 'players' && rowLabel === 'OPPONENTS') {
    ss.toast('Opponent rows cannot be edited', 'Info', 3);
    return;
  }

  var entityType = sheetType === 'teams' || rowLabel === 'TEAM' ? 'team' : 'player';
  var newValue = e.range.getValue();
  if (matched.format === 'measurement') {
    var parsed = parseMeasurementInput(newValue);
    if (parsed === null) {
      ss.toast("Invalid measurement. Use feet'inches (e.g. 6'8) or total inches.", 'Error', 5);
      return;
    }
    newValue = parsed;
    e.range.setValue(parsed);
  }

  var colIndices = config.column_indices || {};
  var entityId = entityType === 'player' ? sheet.getRange(editedRow, colIndices.player_id || 1).getValue() : null;
  var teamAbbr = _getTeamAbbrForRow(sheet, sheetType, editedRow, rowLabel, colIndices, config);

  var allSheets = ss.getSheets();
  var sourceId = sheet.getSheetId();
  for (var i = 0; i < allSheets.length; i++) {
    var targetSheet = allSheets[i];
    if (targetSheet.getSheetId() === sourceId) {
      continue;
    }

    var targetType = _getSheetType(targetSheet.getName());
    if (!targetType) {
      continue;
    }

    var targetRangeKey = _getRangeKey(targetType);
    var targetCol = matched.indices[targetRangeKey];
    if (!targetCol) {
      continue;
    }

    _propagateValue(targetSheet, targetType, entityType, entityId, teamAbbr, targetCol, newValue, config);
  }

  ss.toast((matched.display_name || matched.col_key || 'Field') + ' updated for ' + rowLabel, 'Saved', 3);
}

function _findEditableMatch(lookup, rangeKey, editedCol) {
  var keys = Object.keys(lookup || {});
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    var entry = lookup[key];
    if (entry && entry.indices && entry.indices[rangeKey] === editedCol) {
      return {
        col_key: key,
        display_name: entry.display_name,
        format: entry.format,
        indices: entry.indices,
      };
    }
  }
  return null;
}

function _getTeamAbbrForRow(sheet, sheetType, editedRow, rowLabel, colIndices, config) {
  if (sheetType === 'team') {
    return sheet.getName().toUpperCase();
  }

  if (sheetType === 'players') {
    if (rowLabel === 'TEAM') {
      return String(sheet.getRange(editedRow, colIndices.team || 2).getValue() || '').toUpperCase() || null;
    }
    return String(sheet.getRange(editedRow, colIndices.team || 2).getValue() || '').toUpperCase() || null;
  }

  if (sheetType === 'teams') {
    var teamName = String(sheet.getRange(editedRow, 1).getValue() || '').trim();
    if (!teamName) {
      return null;
    }
    var teamMap = config.team_name_to_abbr || {};
    return (teamMap[teamName] || teamMap[teamName.toUpperCase()] || teamName).toUpperCase();
  }

  return null;
}

function _propagateValue(sheet, sheetType, entityType, entityId, teamAbbr, targetCol, value, config) {
  var layout = config.layout || {};
  var dataStart = layout.data_start_row || 5;
  var lastRow = sheet.getLastRow();
  if (lastRow < dataStart) {
    return;
  }

  if (entityType === 'player' && entityId) {
    var idCol = config.column_indices && config.column_indices.player_id ? config.column_indices.player_id : 1;
    var ids = sheet.getRange(dataStart, idCol, lastRow - dataStart + 1, 1).getValues();
    for (var i = 0; i < ids.length; i++) {
      if (String(ids[i][0]) === String(entityId)) {
        sheet.getRange(dataStart + i, targetCol).setValue(value);
        return;
      }
    }
    return;
  }

  if (entityType !== 'team' || !teamAbbr) {
    return;
  }

  if (sheetType === 'team') {
    if (sheet.getName().toUpperCase() === teamAbbr.toUpperCase()) {
      var teamRow = _findRowByLabel(sheet, dataStart, 'TEAM');
      if (teamRow !== null) {
        sheet.getRange(teamRow, targetCol).setValue(value);
      }
    }
    return;
  }

  if (sheetType === 'teams') {
    var teamMap = config.team_name_to_abbr || {};
    var names = sheet.getRange(dataStart, 1, lastRow - dataStart + 1, 1).getValues();
    for (var r = 0; r < names.length; r++) {
      var rowName = String(names[r][0] || '').trim();
      if (!rowName) {
        continue;
      }
      var rowAbbr = (teamMap[rowName] || teamMap[rowName.toUpperCase()] || rowName).toUpperCase();
      if (rowAbbr === teamAbbr.toUpperCase()) {
        sheet.getRange(dataStart + r, targetCol).setValue(value);
        return;
      }
    }
  }
}

function _findRowByLabel(sheet, startRow, label) {
  var lastRow = sheet.getLastRow();
  if (lastRow < startRow) {
    return null;
  }
  var values = sheet.getRange(startRow, 1, lastRow - startRow + 1, 1).getValues();
  for (var i = 0; i < values.length; i++) {
    if (String(values[i][0] || '').toUpperCase() === String(label).toUpperCase()) {
      return startRow + i;
    }
  }
  return null;
}

function parseMeasurementInput(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  var str = String(value).trim();
  var feetInches = str.match(/^(\d+)'(\d+)"?$/);
  if (feetInches) {
    return parseInt(feetInches[1], 10) * 12 + parseInt(feetInches[2], 10);
  }

  var inches = parseInt(str, 10);
  if (!isNaN(inches) && inches > 0 && inches < 120) {
    return inches;
  }

  return null;
}

function _getSheetType(sheetName) {
  var config = _getConfig();
  var upper = String(sheetName || '').toUpperCase();
  var sheetNames = config.sheet_names || {};

  if ((sheetNames.players || []).indexOf(upper) !== -1) {
    return 'players';
  }
  if ((sheetNames.teams || []).indexOf(upper) !== -1) {
    return 'teams';
  }

  var teamAbbrs = Object.values(config.team_name_to_abbr || {}).map(function(abbr) {
    return String(abbr || '').toUpperCase();
  });
  if (teamAbbrs.indexOf(upper) !== -1) {
    return 'team';
  }

  return null;
}

function _getRangeKey(sheetType) {
  if (sheetType === 'team') {
    return 'team_tab';
  }
  if (sheetType === 'players') {
    return 'all_players_tab';
  }
  if (sheetType === 'teams') {
    return 'all_teams_tab';
  }
  return null;
}

function _getSectionVisibilityKey(sectionKey) {
  return 'SECTION_VIS_' + String(sectionKey || '').toUpperCase();
}

function _applyToAllSheets(fn, toastMsg) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getSheets();

  for (var i = 0; i < sheets.length; i++) {
    var type = _getSheetType(sheets[i].getName());
    if (type) {
      fn(sheets[i], type);
    }
  }

  if (toastMsg) {
    ss.toast(toastMsg, 'Updated', 2);
  }
}

function _reapplyToggles(sheet, sheetType) {
  var config = _getConfig();
  var props = PropertiesService.getDocumentProperties();
  var rangeKey = _getRangeKey(sheetType);
  var metadata = (config.column_metadata || {})[rangeKey] || [];
  var ranges = _buildVisibilityRanges(metadata, props, config);

  _applyColumnRanges(sheet, ranges.visible, true);
  _applyColumnRanges(sheet, ranges.hidden, false);
}

function _buildVisibilityRanges(metadata, props, config) {
  var visible = [];
  var hidden = [];
  var currentRate = props.getProperty('STATS_RATE') || config.default_stat_rate;
  var showAdvanced = props.getProperty('SHOW_ADVANCED') === 'true';
  var timeframe = parseInt(props.getProperty('HISTORICAL_TIMEFRAME'), 10);
  if (isNaN(timeframe) || timeframe <= 0) {
    timeframe = config.default_historical_timeframe || 1;
  }

  var currentRun = null;
  var currentState = null;

  for (var i = 0; i < metadata.length; i++) {
    var block = metadata[i];
    var isVisible = _isColumnVisible(block, props, currentRate, showAdvanced, timeframe);

    if (currentRun === null) {
      currentRun = { start: block.start, count: block.count };
      currentState = isVisible;
      continue;
    }

    if (isVisible === currentState) {
      currentRun.count += block.count;
    } else {
      (currentState ? visible : hidden).push(currentRun);
      currentRun = { start: block.start, count: block.count };
      currentState = isVisible;
    }
  }

  if (currentRun !== null) {
    (currentState ? visible : hidden).push(currentRun);
  }

  return {
    visible: visible,
    hidden: hidden,
  };
}

function _isColumnVisible(meta, props, currentRate, showAdvanced, timeframe) {
  if (meta.is_separator) {
    return true;
  }

  var sectionVisible = props.getProperty(_getSectionVisibilityKey(meta.base_section)) !== 'false';
  if (!sectionVisible) {
    return false;
  }

  if (meta.advanced) {
    return showAdvanced;
  }

  if (meta.basic) {
    return !showAdvanced;
  }

  if (meta.is_stats_section) {
    if (meta.rate && meta.rate !== currentRate) {
      return false;
    }
    if (meta.timeframe && meta.timeframe !== timeframe) {
      return false;
    }
  }

  return true;
}

function _applyColumnRanges(sheet, ranges, shouldShow) {
  if (!ranges || !ranges.length) {
    return;
  }

  for (var i = 0; i < ranges.length; i++) {
    var range = ranges[i];
    if (!range || !range.count) {
      continue;
    }
    try {
      if (shouldShow) {
        sheet.showColumns(range.start, range.count);
      } else {
        sheet.hideColumns(range.start, range.count);
      }
    } catch (err) {
      // Some ranges may already be in the requested state.
    }
  }
}
