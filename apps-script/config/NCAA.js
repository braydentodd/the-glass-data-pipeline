/**
 * apps-script/config/NCAA.js
 *
 * NCAA league configuration for Code.js.
 * Include this file alongside Code.js in the NCAA spreadsheet's Apps Script project.
 * Do NOT include NBA.js in the same project.
 */

var NCAA_CONFIG = {
  name:                'NCAA',
  configEndpoint:      '/api/ncaa/config',
  teamsKey:            'ncaa_teams',
  playersSheetNames:   ['NCAA', 'PLAYERS'],
  playersRangeKey:     'ncaa_sheet',
  editColIndexKey:     'ncaa_col_index',
  apiPrefix:           '/api/ncaa',
  syncEndpoint:        '/api/ncaa/sync',
  hasAdvancedStats:    false,
  hasWingspan:         false,
};

var LEAGUE = NCAA_CONFIG;
