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



// ============================================================
// DYNAMIC FUNCTION BUILDER
// ============================================================
// We dynamically build the callbacks on globalThis. Because the config file is placed
// in the `_config` directory, it evaluates chronologically before `Code.js`, meaning 
// `CONFIG` is fully defined and available at script load time.

(function() {
  if (typeof CONFIG === 'undefined') return;

  var root = typeof globalThis !== 'undefined' ? globalThis : this;

  root.showAdvancedStats = function() { _setAdvancedStats(true); };
  root.hideAdvancedStats = function() { _setAdvancedStats(false); };
  root.resetDisplayToDefaults = function() { _resetToDefaults(); };

  (CONFIG.supported_historical_timeframes || []).forEach(function(years) {
    root['setTimeframe' + years] = function() { _setTimeframe(years); };
  });

  Object.keys(CONFIG.stat_rates || {}).forEach(function(rate) {
    root['switchTo_' + rate] = function() { _switchStatRate(rate); };
  });

  Object.keys(CONFIG.sections || {}).forEach(function(secKey) {
    root['show_' + secKey] = function() { _setSectionVisibility(secKey, true); };
    root['hide_' + secKey] = function() { _setSectionVisibility(secKey, false); };
  });
})();

function onOpen() {
  var ui = SpreadsheetApp.getUi();

  try {
    var config = _getConfig();
    var props = PropertiesService.getDocumentProperties();
    _checkEpochReset(config, props);
    var menu = _buildDisplayMenu(ui, config);
    menu.addToUi();
  } catch (err) {
    ui.createMenu('Display Settings')
      .addItem('Config not loaded', 'onOpen')
      .addToUi();
  }
}

function _checkEpochReset(config, props) {
  if (config.publish_epoch) {
    var currentEpoch = props.getProperty('PUBLISH_EPOCH');
    var publishEpochStr = String(config.publish_epoch);
    if (currentEpoch !== publishEpochStr) {
      props.deleteProperty('STATS_RATE');
      props.deleteProperty('SHOW_ADVANCED');
      props.deleteProperty('HISTORICAL_TIMEFRAME');
      
      var sections = config.sections || {};
      Object.keys(sections).forEach(function(secKey) {
        props.deleteProperty(_getSectionVisibilityKey(secKey));
      });
      
      props.setProperty('PUBLISH_EPOCH', publishEpochStr);
    }
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
  var ratesKeys = Object.keys(config.stat_rates || {});
  if (ratesKeys.length) {
    var rateMenu = ui.createMenu(rateConfig.display_name || 'Stats Rate');
    ratesKeys.forEach(function(rate) {
      var rObj = config.stat_rates[rate];
      var label = rate;
      if (rObj && rObj.short_label) {
        label = 'per ' + (rObj.rate ? rObj.rate + ' ' : '') + rObj.short_label;
      }
      rateMenu.addItem(label, 'switchTo_' + rate);
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
  _checkEpochReset(config, props);
  var currentRate = props.getProperty('STATS_RATE') || config.default_stat_rate;

  var rObj = config.stat_rates[newRate];
  var rateLabel = newRate;
  if (rObj && rObj.short_label) {
    rateLabel = 'per ' + (rObj.rate ? rObj.rate + ' ' : '') + rObj.short_label;
  }

  if (newRate === currentRate) {
    return;
  }

  props.setProperty('STATS_RATE', newRate);
  var predicate = function(b) { return b.is_stats_section && b.rate !== ""; };
  _applyToAllSheets(function(sheet, sheetType) {
    _reapplyToggles(sheet, sheetType, predicate);
  }, rateLabel + ' mode');
}

function _setAdvancedStats(newAdvancedVisible) {
  var config = _getConfig();
  var props = PropertiesService.getDocumentProperties();
  _checkEpochReset(config, props);
  
  props.setProperty('SHOW_ADVANCED', newAdvancedVisible ? 'true' : 'false');
  var predicate = function(b) { return b.advanced || b.basic; };
  _applyToAllSheets(function(sheet, sheetType) {
    _reapplyToggles(sheet, sheetType, predicate);
  }, newAdvancedVisible ? 'Advanced stats shown' : 'Basic stats shown');
}

function _setTimeframe(years) {
  var config = _getConfig();
  var props = PropertiesService.getDocumentProperties();
  _checkEpochReset(config, props);
  
  props.setProperty('HISTORICAL_TIMEFRAME', String(years));
  var predicate = function(b) { return b.is_stats_section && b.timeframe !== ""; };
  _applyToAllSheets(function(sheet, sheetType) {
    _reapplyToggles(sheet, sheetType, predicate);
  }, 'Timeframe swapped to ' + years + ' season' + (years === 1 ? '' : 's'));
}

function _setSectionVisibility(sectionKey, makeVisible) {
  var config = _getConfig();
  var props = PropertiesService.getDocumentProperties();
  _checkEpochReset(config, props);
  
  props.setProperty(_getSectionVisibilityKey(sectionKey), makeVisible ? 'true' : 'false');
  var predicate = function(b) { return b.base_section === sectionKey; };
  _applyToAllSheets(function(sheet, sheetType) {
    _reapplyToggles(sheet, sheetType, predicate);
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

    var editedRow = e.range.getRow();
  if (editedRow <= (config.header_row_count || 5)) {
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
    return;
  }

  var entityType = sheetType === 'teams' || rowLabel === 'TEAM' ? 'team' : 'player';
  var newValue = e.range.getValue();

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
    var dataStart = (config.header_row_count || 5) + 1;
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

function _applyToAllSheets(fn) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getSheets();
  var activeSheet = ss.getActiveSheet();

  // Apply to active sheet first for immediate visual feedback
  var activeType = _getSheetType(activeSheet.getName());
  if (activeType) {
    fn(activeSheet, activeType);
    SpreadsheetApp.flush(); // Force UI to redraw immediately before pausing for the rest
  }

  for (var i = 0; i < sheets.length; i++) {
    var sheet = sheets[i];
    if (sheet.getSheetId() === activeSheet.getSheetId()) {
      continue;
    }
    var type = _getSheetType(sheet.getName());
    if (type) {
      fn(sheet, type);
    }
  }
}

function _reapplyToggles(sheet, sheetType, predicate) {
  var config = _getConfig();
  var props = PropertiesService.getDocumentProperties().getProperties();
  var rangeKey = _getRangeKey(sheetType);
  var metadata = (config.column_metadata || {})[rangeKey] || [];
  var ranges = _buildVisibilityRanges(metadata, props, config, predicate);

  _applyColumnRanges(sheet, ranges.visible, true);
  _applyColumnRanges(sheet, ranges.hidden, false);
}

function _buildVisibilityRanges(metadata, props, config, predicate) {
  var visible = [];
  var hidden = [];
  var currentRate = props['STATS_RATE'] || config.default_stat_rate;
  var showAdvanced = props['SHOW_ADVANCED'] === 'true';
  var timeframe = parseInt(props['HISTORICAL_TIMEFRAME'], 10);
  if (isNaN(timeframe) || timeframe <= 0) {
    timeframe = config.default_historical_timeframe || 1;
  }

  var currentRun = null;
  var currentState = null;

  for (var i = 0; i < metadata.length; i++) {
    var b = metadata[i];
    var block = {
      start: b[0], count: b[1], base_section: b[2], rate: b[3],
      timeframe: b[4], advanced: !!b[5], basic: !!b[6],
      is_stats_section: !!b[7], is_separator: !!b[8]
    };

    if (predicate && !predicate(block)) {
      if (currentRun !== null) {
        (currentState ? visible : hidden).push(currentRun);
        currentRun = null;
      }
      continue;
    }

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
  var sectionVisible = props[_getSectionVisibilityKey(meta.base_section)] !== 'false';
  if (!sectionVisible) {
    return false;
  }

  if (meta.is_stats_section) {
    if (meta.rate && meta.rate !== currentRate) {
      return false;
    }
    if (meta.timeframe && meta.timeframe !== timeframe) {
      return false;
    }
  }

  if (meta.is_separator) {
    return true;
  }

  if (meta.advanced) {
    return showAdvanced;
  }

  if (meta.basic) {
    return !showAdvanced;
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
