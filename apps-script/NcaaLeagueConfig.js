/**
 * apps-script/NcaaLeagueConfig.js
 *
 * NCAA league configuration for Code.js.
 * Include this file alongside Code.js in the NCAA spreadsheet's Apps Script project.
 * Do NOT include NbaLeagueConfig.js in the same project.
 */

var LEAGUE = {
  name:                'NCAA',
  configEndpoint:      '/api/ncaa/config',
  teamsKey:            'ncaa_teams',
  playersSheetNames:   ['NCAA', 'PLAYERS'],
  playersRangeKey:     'ncaa_sheet',
  editColIndexKey:     'ncaa_col_index',
  apiPrefix:           '/api/ncaa',
  syncEndpoint:        '/api/ncaa/sync',
  hasAdvancedStats:    false,
  hasHistoricalDialog: false,
  hasWingspan:         false,
};
