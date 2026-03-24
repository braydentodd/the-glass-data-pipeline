/**
 * apps-script/config/NBA.js
 *
 * NBA league configuration for Code.js.
 * Include this file alongside Code.js in the NBA spreadsheet's Apps Script project.
 * Do NOT include NCAA.js in the same project.
 */

var LEAGUE = {
  name:                'NBA',
  configEndpoint:      '/api/config',
  teamsKey:            'nba_teams',
  playersSheetNames:   ['NBA', 'PLAYERS'],
  playersRangeKey:     'nba_sheet',
  editColIndexKey:     'nba_col_index',
  apiPrefix:           '/api',
  syncEndpoint:        '/api/sync-historical-stats',
  hasAdvancedStats:    true,
  hasWingspan:         true,
};
