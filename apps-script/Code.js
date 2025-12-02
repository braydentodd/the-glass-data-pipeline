/**
 * Configuration Management for The Glass Apps Script
 * 
 * Configuration is loaded dynamically from the Flask API at runtime.
 * The API serves configuration from src/config.py (single source of truth).
 * 
 * IMPORTANT: All configuration values come from Python config.py via API.
 * To change configuration, update src/config.py and redeploy the API.
 */

// Global configuration object - loaded on first use
let CONFIG = null;

/**
 * Load configuration from API endpoint
 * Configuration comes from Python's src/config.py
 */
function loadConfig() {
  if (CONFIG !== null) {
    return CONFIG;  // Return cached config
  }
  
  try {
    // Bootstrap: use this hardcoded URL ONLY to fetch the real config
    // After this initial fetch, all values come from the API
    const bootstrapUrl = 'http://150.136.255.23:5001/api/config';
    
    const response = UrlFetchApp.fetch(bootstrapUrl, { muteHttpExceptions: true });
    
    if (response.getResponseCode() !== 200) {
      throw new Error(`Config API returned ${response.getResponseCode()}`);
    }
    
    CONFIG = JSON.parse(response.getContentText());
    Logger.log('Configuration loaded from API successfully');
    return CONFIG;
    
  } catch (error) {
    Logger.log(`Error loading config from API: ${error}`);
    
    // FALLBACK ONLY - if API is unreachable, use these emergency defaults
    // These should match src/config.py values
    Logger.log('WARNING: Using fallback configuration - API unreachable');
    CONFIG = {
      api_base_url: 'http://150.136.255.23:5001',
      sheet_id: '1kqVNHu8cs4lFAEAflI4Ow77oEZEusX7_VpQ6xt8CgB4',
      nba_teams: {
        'ATL': 1610612737, 'BOS': 1610612738, 'BKN': 1610612751, 'CHA': 1610612766,
        'CHI': 1610612741, 'CLE': 1610612739, 'DAL': 1610612742, 'DEN': 1610612743,
        'DET': 1610612765, 'GSW': 1610612744, 'HOU': 1610612745, 'IND': 1610612754,
        'LAC': 1610612746, 'LAL': 1610612747, 'MEM': 1610612763, 'MIA': 1610612748,
        'MIL': 1610612749, 'MIN': 1610612750, 'NOP': 1610612740, 'NYK': 1610612752,
        'OKC': 1610612760, 'ORL': 1610612753, 'PHI': 1610612755, 'PHX': 1610612756,
        'POR': 1610612757, 'SAC': 1610612758, 'SAS': 1610612759, 'TOR': 1610612761,
        'UTA': 1610612762, 'WAS': 1610612764
      },
      stat_columns: [
        'games', 'minutes', 'possessions', 'points', 'ts_pct', 'fg2a', 'fg2_pct', 'fg3a', 'fg3_pct',
        'fta', 'ft_pct', 'assists', 'turnovers', 'oreb_pct', 'dreb_pct', 'steals', 
        'blocks', 'fouls', 'off_rating', 'def_rating'
      ],
      reverse_stats: ['turnovers', 'fouls'],
      column_indices: {
        wingspan: 7,
        notes: 8,
        player_id: 71,
        stats_start: 9
      },
      colors: {
        red: { r: 238, g: 75, b: 43 },
        yellow: { r: 252, g: 245, b: 95 },
        green: { r: 76, g: 187, b: 23 }
      }
    };
    return CONFIG;
  }
}

// Legacy constant accessors - these now fetch from loaded config
function getApiBaseUrl() {
  const config = loadConfig();
  return config.api_base_url;
}

function getSheetId() {
  const config = loadConfig();
  return config.sheet_id;
}

function getNbaTeams() {
  const config = loadConfig();
  return config.nba_teams;
}

function getStatColumns() {
  const config = loadConfig();
  return config.stat_columns;
}

function getReverseStats() {
  const config = loadConfig();
  return config.reverse_stats;
}

function getColumnIndices() {
  const config = loadConfig();
  return config.column_indices;
}

function getColors() {
  const config = loadConfig();
  return config.colors;
}

// Backward compatibility - these constants now load from config
const API_BASE_URL = getApiBaseUrl();
const SHEET_ID = getSheetId();
const NBA_TEAMS = getNbaTeams();
const STAT_COLUMNS = getStatColumns();
const REVERSE_STATS = getReverseStats();

/**
 * Get current NBA season based on date
 */
function getCurrentSeason() {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1; // 1-12
  
  // If after August (month 8), we're in the next season
  if (month > 8) {
    return `${year}-${(year + 1).toString().slice(-2)}`;
  } else {
    return `${year - 1}-${year.toString().slice(-2)}`;
  }
}

/**
 * Add custom menu when spreadsheet opens
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  
  // Initialize default settings if not already set
  const props = PropertiesService.getDocumentProperties();
  if (!props.getProperty('HISTORICAL_MODE')) {
    props.setProperty('HISTORICAL_MODE', 'career');
    props.setProperty('HISTORICAL_YEARS', '25');
    props.setProperty('INCLUDE_CURRENT_YEAR', 'false');
  }
  if (!props.getProperty('POSTSEASON_MODE')) {
    props.setProperty('POSTSEASON_MODE', 'career');
    props.setProperty('POSTSEASON_YEARS', '25');
  }
  
  ui.createMenu('Stats')
    .addSubMenu(ui.createMenu('Stats Config')
      .addItem('Totals', 'switchToTotals')
      .addItem('Per Game', 'switchToPerGame')
      .addItem('Per Minute', 'switchToPerMinute')
      .addItem('Per Possession', 'switchToPerPossession')
      .addSeparator()
      .addItem('Toggle Percentiles', 'togglePercentileDisplay')
      .addSeparator()
      .addItem('Historical Timeframe', 'showHistoricalStatsDialog'))
    .addSubMenu(ui.createMenu('Show/Hide')
      .addItem('Current', 'toggleCurrentStats')
      .addItem('Historical', 'toggleHistoricalStats')
      .addItem('Postseason', 'togglePostseasonStats'))
    .addToUi();
}

/**
 * Handle edits to wingspan and notes columns (installable version)
 * Writes changes back to the database via API
 */
function onEditInstallable(e) {
  const range = e.range;
  const sheet = range.getSheet();
  const sheetName = sheet.getName().toUpperCase();
  
  // Only process team sheets and NBA sheet
  if (!NBA_TEAMS.hasOwnProperty(sheetName) && sheetName !== 'NBA') {
    return;
  }
  
  const row = range.getRow();
  const col = range.getColumn();
  
  // Only process rows 4 and below (data rows)
  if (row < 4) {
    return;
  }
  
  // Get column indices from config
  const colIndices = getColumnIndices();
  
  // For NBA sheet, columns are shifted right by 1 due to Team column
  const isNBASheet = (sheetName === 'NBA');
  const wingspanCol = isNBASheet ? colIndices.wingspan + 1 : colIndices.wingspan;
  const notesCol = isNBASheet ? colIndices.notes + 1 : colIndices.notes;
  const playerIdCol = isNBASheet ? colIndices.player_id + 1 : colIndices.player_id;
  
  // Check if edited cell is in wingspan or notes column
  if (col !== wingspanCol && col !== notesCol) {
    return;
  }
  
  // Get player ID from hidden column
  const playerId = sheet.getRange(row, playerIdCol).getValue();
  
  if (!playerId) {
    const playerName = sheet.getRange(row, 1).getValue();
    return;
  }
  
  const playerName = sheet.getRange(row, 1).getValue();
  const newValue = range.getValue();
  
  // Determine which field was edited
  let fieldName, fieldValue, displayFieldName;
  if (col === wingspanCol) {
    // Wingspan column
    fieldName = 'wingspan_inches';
    displayFieldName = 'wingspan';
    
    // Allow empty values to clear the field
    if (!newValue || newValue === '') {
      fieldValue = null;
    } else {
      fieldValue = parseWingspan(newValue);
      if (fieldValue === null) {
        return;
      }
    }
  } else if (col === notesCol) {
    // Notes column
    fieldName = 'notes';
    displayFieldName = 'notes';
    fieldValue = newValue ? newValue.toString() : '';
  }
  
  // Call API to update player
  try {
    updatePlayerField(playerId, fieldName, fieldValue);
    
    // Show brief confirmation
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Updated ${displayFieldName} for ${playerName}`,
      'Saved to Database',
      2
    );
  } catch (error) {
    Logger.log(`Error updating player: ${error}`);
  }
}

/**
 * Switch all team sheets to Totals mode
 */
function switchToTotals() {
  SpreadsheetApp.getActiveSpreadsheet().toast(
    'Switching to Totals',
    'Updating Stats',
    5
  );
  updateAllSheets('totals', null);
}

/**
 * Switch all team sheets to Per Game mode
 */
function switchToPerGame() {
  SpreadsheetApp.getActiveSpreadsheet().toast(
    'Switching to Per Game',
    'Updating Stats',
    5
  );
  updateAllSheets('per_game', null);
}

/**
 * Switch all team sheets to Per Minute mode with user input
 */
function switchToPerMinute() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.prompt(
    'Per __ Minute Stats',
    'Enter the number of minutes to scale stats to:',
    ui.ButtonSet.OK_CANCEL
  );
  
  if (response.getSelectedButton() === ui.Button.OK) {
    const minutes = parseFloat(response.getResponseText());
    if (isNaN(minutes) || minutes <= 0) {
      return;
    }
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Switching to Per ${minutes} Minutes`,
      'Updating Stats',
      -1
    );
    updateAllSheets('per_minutes', minutes);
  }
}

/**
 * Switch all team sheets to Per Possession mode with user input
 */
function switchToPerPossession() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.prompt(
    'Per __ Possession Stats',
    'Enter the number of possessions to scale stats to:',
    ui.ButtonSet.OK_CANCEL
  );
  
  if (response.getSelectedButton() === ui.Button.OK) {
    const possessions = parseFloat(response.getResponseText());
    if (isNaN(possessions) || possessions <= 0) {
      return;
    }
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Switching to Per ${possessions} Possessions`,
      'Updating Stats',
      -1
    );
    updateAllSheets('per_possessions', possessions);
  }
}

/**
 * Toggle between showing stat values and percentile ranks
 */
function togglePercentileDisplay() {
  const ui = SpreadsheetApp.getUi();
  const props = PropertiesService.getDocumentProperties();
  const currentMode = props.getProperty('SHOW_PERCENTILES') || 'false';
  
  const newMode = (currentMode === 'true') ? 'false' : 'true';
  props.setProperty('SHOW_PERCENTILES', newMode);
  
  // Show loading message
  SpreadsheetApp.getActiveSpreadsheet().toast(
    'Toggling percentiles display',
    'Updating Stats',
    5
  );
  
  // Get the active sheet to set as priority team
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const activeSheet = ss.getActiveSheet();
  const activeSheetName = activeSheet.getName().toUpperCase();
  const priorityTeam = NBA_TEAMS.hasOwnProperty(activeSheetName) ? activeSheetName : null;
  
  // Check if historical or postseason stats are configured
  const historicalMode = props.getProperty('HISTORICAL_MODE');
  const postseasonMode = props.getProperty('POSTSEASON_MODE');
  
  if (historicalMode || postseasonMode) {
    // If historical or postseason stats are configured, trigger full sync
    // This ensures current, historical, and postseason stats all update together
    const historicalYears = props.getProperty('HISTORICAL_YEARS');
    const historicalSeasons = props.getProperty('HISTORICAL_SEASONS');
    const includeCurrent = props.getProperty('INCLUDE_CURRENT_YEAR') === 'true';
    
    let value;
    let syncMode = historicalMode || 'career';
    if (syncMode === 'seasons') {
      // Multiple specific seasons
      value = historicalSeasons || '';
    } else if (syncMode === 'career') {
      // Career mode - use all available years
      value = 25;
    } else {
      // Years mode - use specific number of years
      value = parseInt(historicalYears) || 3;
    }
    
    // Call full sync (updates current, historical, and postseason)
    syncFullStatsUpdate(syncMode, value, includeCurrent, priorityTeam);
  } else {
    // No historical/postseason configured, just update current season
    const statsMode = props.getProperty('STATS_MODE') || 'totals';
    const customValue = props.getProperty('STATS_CUSTOM_VALUE');
    updateAllSheets(statsMode, customValue ? parseFloat(customValue) : null);
  }
}

/**
 * Show dialog for historical stats configuration
 */
function showHistoricalStatsDialog() {
  const props = PropertiesService.getDocumentProperties();
  
  // Get current settings for display
  const currentMode = props.getProperty('HISTORICAL_MODE') || 'years';
  const currentYears = props.getProperty('HISTORICAL_YEARS') || '3';
  const currentSeasons = props.getProperty('HISTORICAL_SEASONS') || '';
  const includeCurrentYear = props.getProperty('INCLUDE_CURRENT_YEAR') || 'true';
  
  let currentValue = '';
  if (currentMode === 'career') {
    currentValue = 'Career';
  } else if (currentMode === 'seasons') {
    currentValue = currentSeasons;
  } else {
    currentValue = currentYears;
  }
  
  // Create and show the HTML dialog
  const htmlTemplate = HtmlService.createTemplateFromFile('HistoricalStatsDialog');
  htmlTemplate.currentValue = currentValue;
  htmlTemplate.includeCurrentYear = includeCurrentYear;
  
  const html = htmlTemplate.evaluate()
    .setWidth(450)
    .setHeight(280);
  
  SpreadsheetApp.getUi().showModalDialog(html, 'Previous Years Stats Configuration');
}

/**
 * Parse and validate historical stats input
 * Returns: { valid: boolean, mode: string, value: any, error: string }
 */
function parseHistoricalStatsInput(input) {
  Logger.log(`parseHistoricalStatsInput called with: "${input}"`);
  
  if (!input || input.trim() === '') {
    Logger.log('Input is empty');
    return { valid: false, error: 'Please enter a value' };
  }
  
  const trimmed = input.trim();
  Logger.log(`Trimmed input: "${trimmed}"`);
  
  // Check for Career mode
  if (trimmed.toLowerCase() === 'career' || trimmed.toLowerCase() === 'c') {
    Logger.log('Detected career mode');
    return { valid: true, mode: 'career', value: 25 };
  }
  
  // Check if it's a number (number of years)
  const numYears = parseInt(trimmed);
  if (!isNaN(numYears) && trimmed === numYears.toString()) {
    Logger.log(`Detected years mode: ${numYears}`);
    if (numYears < 1 || numYears > 25) {
      return { valid: false, error: 'Number of years must be between 1 and 25' };
    }
    return { valid: true, mode: 'years', value: numYears };
  }
  
  // Check if it's a season format (contains dash/slash or is 4 digits)
  // Valid formats: 2024-25, 1998-99, 98-99, 2024/25, 2024, 1998
  const seasonPattern = /^\d{2,4}([\-\/]\d{2,4})?$/;
  if (seasonPattern.test(trimmed)) {
    Logger.log('Detected season format');
    // Normalize to YYYY-YY format
    let normalized;
    
    if (trimmed.includes('-') || trimmed.includes('/')) {
      const parts = trimmed.split(/[\-\/]/);
      const firstPart = parts[0];
      const secondPart = parts[1];
      Logger.log(`Season parts: ${firstPart}, ${secondPart}`);
      
      // Handle 98-99 or 98/99 format
      if (firstPart.length === 2) {
        const prefix = parseInt(secondPart) < parseInt(firstPart) ? '20' : '19';
        normalized = `${prefix}${firstPart}-${secondPart}`;
      } else if (firstPart.length === 4 && secondPart.length === 4) {
        // Handle 2024-2025 or 2024/2025 format
        normalized = `${firstPart}-${secondPart.slice(-2)}`;
      } else {
        // Handle 2024-25 or 2024/25 format
        normalized = `${firstPart}-${secondPart}`;
      }
    } else {
      // Handle single year format (2024 or 1998)
      if (trimmed.length === 4) {
        const year = parseInt(trimmed);
        const nextYear = year + 1;
        normalized = `${year}-${nextYear.toString().slice(-2)}`;
      } else {
        Logger.log('Invalid single year format');
        return { valid: false, error: 'Invalid season format. Use: 2024-25, 98-99, or 2024' };
      }
    }
    
    Logger.log(`Normalized season: ${normalized}`);
    return { valid: true, mode: 'season', value: normalized };
  }
  
  Logger.log('No pattern matched');
  return { valid: false, error: 'Invalid format. Enter: number (1-25), season (2024-25), or "Career"' };
}

/**
 * Save historical stats configuration and trigger sync
 */
function saveHistoricalStatsConfig(input, includeCurrentYear) {
  const parsed = parseHistoricalStatsInput(input);
  
  if (!parsed.valid) {
    return { success: false, error: parsed.error };
  }
  
  const props = PropertiesService.getDocumentProperties();
  
  // Save the include current year setting
  props.setProperty('INCLUDE_CURRENT_YEAR', includeCurrentYear ? 'true' : 'false');
  
  if (parsed.mode === 'career') {
    props.setProperty('HISTORICAL_MODE', 'career');
    props.setProperty('HISTORICAL_YEARS', '25');
    props.deleteProperty('HISTORICAL_SEASONS');
  } else if (parsed.mode === 'years') {
    props.setProperty('HISTORICAL_MODE', 'years');
    props.setProperty('HISTORICAL_YEARS', parsed.value.toString());
    props.deleteProperty('HISTORICAL_SEASONS');
  } else if (parsed.mode === 'season') {
    props.setProperty('HISTORICAL_MODE', 'season');
    props.setProperty('HISTORICAL_SEASONS', parsed.value);
    props.deleteProperty('HISTORICAL_YEARS');
  }
  
  // Get the active sheet to set as priority team
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const activeSheet = ss.getActiveSheet();
  const activeSheetName = activeSheet.getName().toUpperCase();
  const priorityTeam = NBA_TEAMS.hasOwnProperty(activeSheetName) ? activeSheetName : null;
  
  // Execute sync directly (synchronous) - user waits but gets immediate results
  try {
    Logger.log(`Starting direct sync: mode=${parsed.mode}, value=${parsed.value}, includeCurrent=${includeCurrentYear}, priority=${priorityTeam}`);
    
    // Show loading message
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Updating historical stats timeframe',
      'Updating Stats',
      -1  // -1 means toast stays until manually dismissed or replaced
    );
    
    // Execute full sync WITHOUT sync_section parameter to update ALL sections with new timeframe
    // This ensures current, historical, and postseason all use the same timeframe
    syncFullStatsUpdate(parsed.mode, parsed.value, includeCurrentYear, priorityTeam);
    
    // Show success message
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Stats updated to ' + (parsed.mode === 'career' ? 'Career' : parsed.value + ' seasons') + ' successfully!',
      'Updating Stats',
      5
    );
    
    return { success: true };
  } catch (error) {
    Logger.log(`Sync error: ${error}`);
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Sync failed: ${error.message}`,
      'Error',
      5
    );
    return { success: false, error: error.message };
  }
}

/**
 * Save postseason stats configuration and trigger sync
 */
function savePostseasonStatsConfig(input) {
  const parsed = parseHistoricalStatsInput(input);
  
  if (!parsed.valid) {
    return { success: false, error: parsed.error };
  }
  
  const props = PropertiesService.getDocumentProperties();
  
  if (parsed.mode === 'career') {
    props.setProperty('POSTSEASON_MODE', 'career');
    props.setProperty('POSTSEASON_YEARS', '25');
    props.deleteProperty('POSTSEASON_SEASONS');
  } else if (parsed.mode === 'years') {
    props.setProperty('POSTSEASON_MODE', 'years');
    props.setProperty('POSTSEASON_YEARS', parsed.value.toString());
    props.deleteProperty('POSTSEASON_SEASONS');
  } else if (parsed.mode === 'season') {
    props.setProperty('POSTSEASON_MODE', 'season');
    props.setProperty('POSTSEASON_SEASONS', parsed.value);
    props.deleteProperty('POSTSEASON_YEARS');
  }
  
  // Get the active sheet to set as priority team
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const activeSheet = ss.getActiveSheet();
  const activeSheetName = activeSheet.getName().toUpperCase();
  const priorityTeam = NBA_TEAMS.hasOwnProperty(activeSheetName) ? activeSheetName : null;
  
  // Execute sync directly (synchronous) - user waits but gets immediate results
  try {
    Logger.log(`Starting postseason sync: mode=${parsed.mode}, value=${parsed.value}, priority=${priorityTeam}`);
    
    // Show loading message
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Postseason stats timeframe',
      'Updating Stats',
      -1  // -1 means toast stays until manually dismissed or replaced
    );
    
    // Execute the sync directly
    syncStatsSection('postseason', parsed.mode, parsed.value, false, priorityTeam);
    
    // Show success message
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Postseason stats updated to ' + (parsed.mode === 'career' ? 'Career' : parsed.value + ' seasons') + ' successfully!',
      'Updating Stats',
      5
    );
    
    return { success: true };
  } catch (error) {
    Logger.log(`Sync error: ${error}`);
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Sync failed: ${error.message}`,
      'Error',
      5
    );
    return { success: false, error: error.message };
  }
}

/**
 * Trigger stats sync via API for historical or postseason sections
 * @param {string} section - 'historical' or 'postseason'
 */
function syncStatsSection(section, mode, value, includeCurrentYear, priorityTeam) {
  const endpoint = section === 'postseason' ? 'sync-postseason-stats' : 'sync-historical-stats';
  const url = `${API_BASE_URL}/api/${endpoint}`;
  
  // Get current stats mode from document properties
  const props = PropertiesService.getDocumentProperties();
  const statsMode = props.getProperty('STATS_MODE') || 'per_36';
  const statsCustomValue = props.getProperty('STATS_CUSTOM_VALUE');
  const showPercentiles = props.getProperty('SHOW_PERCENTILES') === 'true';
  
  const payload = {
    mode: mode,
    include_current: includeCurrentYear,
    stats_mode: statsMode,  // Pass current stats mode to sync
    show_percentiles: showPercentiles,  // Pass percentile preference
    sync_section: section  // Tell API which section to update ('historical' or 'postseason')
  };
  
  // Add custom value if it exists
  if (statsCustomValue) {
    payload.stats_custom_value = parseFloat(statsCustomValue);
  }
  
  // Add priority team if specified
  if (priorityTeam) {
    payload.priority_team = priorityTeam;
  }
  
  // Add the appropriate value based on mode
  if (mode === 'years' || mode === 'career') {
    payload.years = parseInt(value);
  } else if (mode === 'season') {
    // Convert single season to array
    payload.seasons = [value];
  }
  
  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };
  
  const response = UrlFetchApp.fetch(url, options);
  const responseCode = response.getResponseCode();
  const responseText = response.getContentText();
  
  Logger.log(`API Response Code: ${responseCode}`);
  Logger.log(`API Response: ${responseText}`);
  
  if (responseCode !== 200) {
    let errorMsg = 'Unknown error from API';
    try {
      const data = JSON.parse(responseText);
      errorMsg = data.error || errorMsg;
      // Include stderr/stdout if available for debugging
      if (data.stderr) {
        Logger.log(`STDERR: ${data.stderr}`);
        errorMsg += `\n\nDetails: ${data.stderr.substring(0, 200)}`;
      }
      if (data.stdout) {
        Logger.log(`STDOUT: ${data.stdout}`);
      }
    } catch (e) {
      errorMsg = responseText.substring(0, 200);
    }
    throw new Error(errorMsg);
  }
  
  const data = JSON.parse(responseText);
  if (!data.success) {
    throw new Error(data.error || 'Sync failed');
  }
  
  return data;
}

/**
 * Trigger historical stats sync via API (legacy wrapper)
 */
function syncHistoricalStats(mode, value, includeCurrentYear, priorityTeam) {
  return syncStatsSection('historical', mode, value, includeCurrentYear, priorityTeam);
}

/**
 * Trigger FULL stats sync via API (updates current, historical, and postseason)
 * This is used when changing stats modes to ensure all sections update together
 */
function syncFullStatsUpdate(mode, value, includeCurrentYear, priorityTeam) {
  const url = `${API_BASE_URL}/api/sync-historical-stats`;
  
  // Get current stats mode from document properties
  const props = PropertiesService.getDocumentProperties();
  const statsMode = props.getProperty('STATS_MODE') || 'per_36';
  const statsCustomValue = props.getProperty('STATS_CUSTOM_VALUE');
  const showPercentiles = props.getProperty('SHOW_PERCENTILES') === 'true';
  
  const payload = {
    mode: mode,
    include_current: includeCurrentYear,
    stats_mode: statsMode,
    show_percentiles: showPercentiles
    // NOTE: NO sync_section parameter - this triggers a FULL sync of all sections
  };
  
  // Add custom value if it exists
  if (statsCustomValue) {
    payload.stats_custom_value = parseFloat(statsCustomValue);
  }
  
  // Add priority team if specified
  if (priorityTeam) {
    payload.priority_team = priorityTeam;
  }
  
  // Add the appropriate value based on mode
  if (mode === 'years' || mode === 'career') {
    payload.years = parseInt(value);
  } else if (mode === 'season' || mode === 'seasons') {
    // Handle both singular 'season' (single season string) and plural 'seasons' (comma-separated)
    if (typeof value === 'string' && value.includes(',')) {
      payload.seasons = value.split(',').map(s => s.trim());
    } else {
      payload.seasons = [value];  // Single season as array
    }
  }
  
  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };
  
  const response = UrlFetchApp.fetch(url, options);
  const responseCode = response.getResponseCode();
  const responseText = response.getContentText();
  
  Logger.log(`API Response Code: ${responseCode}`);
  Logger.log(`API Response: ${responseText}`);
  
  if (responseCode !== 200) {
    const errorData = JSON.parse(responseText);
    const errorMessage = errorData.error || 'Unknown error occurred';
    const stderr = errorData.stderr || '';
    const stdout = errorData.stdout || '';
    
    Logger.log(`STDERR: ${stderr}`);
    Logger.log(`STDOUT: ${stdout}`);
    
    throw new Error(`API Error (${responseCode}): ${errorMessage}`);
  }
  
  try {
    return JSON.parse(responseText);
  } catch (e) {
    Logger.log(`Response: ${responseText}`);
    return { success: true };
  }
}

/**
 * Update all team sheets with new stat mode
 */
function updateAllSheets(mode, customValue) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const activeSheet = ss.getActiveSheet();
  const activeSheetName = activeSheet.getName().toUpperCase();
  
  // Store mode in document properties
  const props = PropertiesService.getDocumentProperties();
  props.setProperty('STATS_MODE', mode);
  if (customValue !== null) {
    props.setProperty('STATS_CUSTOM_VALUE', customValue.toString());
  } else {
    props.deleteProperty('STATS_CUSTOM_VALUE');
  }
  
  // Check if historical or postseason stats are configured
  const historicalMode = props.getProperty('HISTORICAL_MODE');
  const postseasonMode = props.getProperty('POSTSEASON_MODE');
  
  if (historicalMode || postseasonMode) {
    // If historical or postseason stats are configured, trigger full sync
    // This ensures current, historical, and postseason stats all update together
    
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Stats Mode',
      'Updating Stats',
      -1
    );
    
    // Pass the active sheet as priority team if it's a team sheet
    const priorityTeam = NBA_TEAMS.hasOwnProperty(activeSheetName) ? activeSheetName : null;
    
    // When changing stats mode, we need to update ALL sections (current, historical, postseason)
    // This is done by calling the historical sync endpoint WITHOUT setting SYNC_SECTION
    // The Python script will update all three sections in one pass
    
    const historicalYears = props.getProperty('HISTORICAL_YEARS');
    const historicalSeasons = props.getProperty('HISTORICAL_SEASONS');
    const includeCurrent = props.getProperty('INCLUDE_CURRENT_YEAR') === 'true';
    
    let value;
    let syncMode = historicalMode || 'career';
    if (syncMode === 'seasons') {
      // Multiple specific seasons
      value = historicalSeasons || '';
    } else if (syncMode === 'career') {
      // Career mode - use all available years
      value = 25;
    } else {
      // Years mode - use specific number of years
      value = parseInt(historicalYears) || 3;
    }
    
    // Call full sync (updates current, historical, and postseason)
    syncFullStatsUpdate(syncMode, value, includeCurrent, priorityTeam);
    
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Stats updated successfully!',
      'Updating Stats',
      3
    );
    
  } else {
    // No historical stats configured, just update current season via API (fast)
    const sheets = ss.getSheets();
    let updatedCount = 0;
    
    // First, update the currently active sheet if it's a team sheet
    if (NBA_TEAMS.hasOwnProperty(activeSheetName)) {
      
      const teamId = NBA_TEAMS[activeSheetName];
      
      try {
        const stats = fetchTeamStats(teamId, mode, customValue);
        
        if (stats && stats.players) {
          Logger.log(`${activeSheetName}: Received ${stats.players.length} players from API`);
          updateSheetWithStats(activeSheet, stats, mode, customValue);
          updatedCount++;
        } else {
          Logger.log(`${activeSheetName}: No stats returned from API`);
        }
      } catch (error) {
        Logger.log(`Error updating ${activeSheetName}: ${error}`);
      }
    }
    
    // Then update all other team sheets
    for (const sheet of sheets) {
      const sheetName = sheet.getName().toUpperCase();
      
      // Skip non-team sheets and the sheet we already updated
      if (!NBA_TEAMS.hasOwnProperty(sheetName) || sheetName === activeSheetName) {
        continue;
      }
      
      const teamId = NBA_TEAMS[sheetName];
      
      try {
        const stats = fetchTeamStats(teamId, mode, customValue);
        
        if (stats && stats.players) {
          Logger.log(`${sheetName}: Received ${stats.players.length} players from API`);
          updateSheetWithStats(sheet, stats, mode, customValue);
          updatedCount++;
        } else {
          Logger.log(`${sheetName}: No stats returned from API`);
        }
      } catch (error) {
        Logger.log(`Error updating ${sheetName}: ${error}`);
      }
    }
  }
}

/**
 * Fetch team stats from API
 */
function fetchTeamStats(teamId, mode, customValue) {
  const url = `${API_BASE_URL}/api/stats`;
  
  const payload = {
    team_id: teamId,
    mode: mode,
    season: getCurrentSeason()
  };
  
  if (customValue !== null) {
    payload.custom_value = customValue;
  }
  
  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };
  
  try {
    const response = UrlFetchApp.fetch(url, options);
    const data = JSON.parse(response.getContentText());
    
    if (response.getResponseCode() === 200) {
      return data;
    } else {
      Logger.log(`API Error: ${data.error}`);
      return null;
    }
  } catch (error) {
    Logger.log(`Fetch Error: ${error}`);
    return null;
  }
}

/**
 * Update sheet with calculated stats and percentile colors
 */
function updateSheetWithStats(sheet, statsData, mode, customValue) {
  const players = statsData.players;
  
  // Check if we should show percentiles instead of values
  const props = PropertiesService.getDocumentProperties();
  const showPercentiles = props.getProperty('SHOW_PERCENTILES') === 'true';
  
  // Get current season
  const season = getCurrentSeason();
  
  // Format mode text for header
  let modeText = '';
  if (mode === 'totals') {
    modeText = 'Totals';
  } else if (mode === 'per_game') {
    modeText = 'Per Game';
  } else if (mode === 'per_minutes' && customValue) {
    modeText = `Per ${customValue} Minutes`;
  }
  
  // Get column indices from config
  const colIndices = getColumnIndices();
  
  // Update header with season and mode
  const headerText = `${season} Stats ${modeText}`;
  Logger.log(`Setting header to: ${headerText}`);
  sheet.getRange(1, colIndices.stats_start).setValue(headerText);
  
  // Start from row 4 for data
  const startRow = 4;
  const statsStartColumn = colIndices.stats_start;
  
  // Set row 3 height to 15 pixels
  sheet.setRowHeight(3, 15);
  
  // Clear ONLY stat columns (stats_start onwards), not player info columns (A-G)
  const lastRow = sheet.getLastRow();
  if (lastRow >= startRow) {
    const numStatCols = STAT_COLUMNS.length;
    sheet.getRange(startRow, statsStartColumn, lastRow - startRow + 1, numStatCols).clearContent();
  }
  
  // Write player data
  for (let i = 0; i < players.length; i++) {
    const player = players[i];
    const row = startRow + i;
    const stats = player.calculated_stats || {};
    const percentiles = player.percentiles || {};
    
    // Skip if player has no stats
    if (!stats || Object.keys(stats).length === 0) {
      continue;  // Player info already in sheet, stats remain empty
    }
    
    // Check if player has minutes - if not, skip all stats
    const minutes = stats['minutes'] || 0;
    if (!minutes || minutes === 0) {
      continue;  // Player has no minutes, leave all stats empty
    }
    
    // DON'T update Column A (player name) - it's already there from Python sync
    // DON'T update Columns B-G (jersey, exp, age, ht, ws, wt) - they're already there
    // ONLY update Column H (empty) and Columns I onwards (stats)
    
    // Columns I onwards: Stats with colors
    let col = statsStartColumn;
    for (const statName of STAT_COLUMNS) {
      const value = stats[statName];
      const percentile = percentiles[`${statName}_percentile`] || 0;
      
      const cell = sheet.getRange(row, col);
      
      // Get attempt values for shooting percentages
      const fg2a = stats['fg2a'] || 0;
      const fg3a = stats['fg3a'] || 0;
      const fta = stats['fta'] || 0;
      
      // Handle empty/zero values
      if (!value || value === 0) {
        // Shooting percentages: empty if no attempts, show 0 if attempts exist
        if (statName === 'ts_pct') {
          cell.clearContent();  // Always empty for TS% when 0
          col++;
          continue;
        } else if (statName === 'fg2_pct' && fg2a === 0) {
          cell.clearContent();  // Empty if no 2PA
          col++;
          continue;
        } else if (statName === 'fg3_pct' && fg3a === 0) {
          cell.clearContent();  // Empty if no 3PA
          col++;
          continue;
        } else if (statName === 'ft_pct' && fta === 0) {
          cell.clearContent();  // Empty if no FTA
          col++;
          continue;
        } else {
          // Other stats or shooting % with attempts show 0 and get color coded
          cell.setValue(0);
          if (statName !== 'games') {
            const color = getPercentileColor(percentile);
            cell.setBackground(color);
          }
          col++;
          continue;
        }
      }
      
      // Format value
      let displayValue;
      if (showPercentiles) {
        // Show percentile rank without % sign (e.g., "75.3" not "75.3%")
        // Percentile comes as 0-100 from API
        const rounded = Math.round(percentile * 10) / 10;
        displayValue = (rounded === Math.floor(rounded)) ? Math.floor(rounded) : rounded;
        Logger.log(`${statName} percentile: ${percentile} -> display: ${displayValue}`);
      } else if (statName.includes('pct')) {
        // Percentages: multiply by 100, remove .0 if whole number
        const pctValue = value * 100;
        const rounded = Math.round(pctValue * 10) / 10;
        displayValue = (rounded === Math.floor(rounded)) ? Math.floor(rounded) : rounded;
      } else if (statName === 'games') {
        displayValue = Math.round(value);
      } else {
        // Regular stats: remove .0 if whole number
        const rounded = Math.round(value * 10) / 10;
        displayValue = (rounded === Math.floor(rounded)) ? Math.floor(rounded) : rounded;
      }
      
      cell.setValue(displayValue);
      
      // Apply percentile color
      if (statName !== 'games') {
        const color = getPercentileColor(percentile);
        cell.setBackground(color);
      }
      
      col++;
    }
  }
  
  // Flush all pending updates
  SpreadsheetApp.flush();
  
  // Auto-resize column A (player names) to fit content
  // Set a reasonable max width to prevent overly wide columns
  sheet.autoResizeColumn(1);
  const currentWidth = sheet.getColumnWidth(1);
  if (currentWidth > 120) {
    sheet.setColumnWidth(1, 120);  // Cap at 120 pixels (reduced from 200)
  }
}

/**
 * Get RGB color based on percentile
 */
function getPercentileColor(percentile) {
  const colors = getColors();
  const red = colors.red;
  const yellow = colors.yellow;
  const green = colors.green;
  
  let r, g, b;
  
  if (percentile < 50) {
    // Interpolate between red and yellow
    const t = percentile / 50;
    r = Math.round(red.r + (yellow.r - red.r) * t);
    g = Math.round(red.g + (yellow.g - red.g) * t);
    b = Math.round(red.b + (yellow.b - red.b) * t);
  } else {
    // Interpolate between yellow and green
    const t = (percentile - 50) / 50;
    r = Math.round(yellow.r + (green.r - yellow.r) * t);
    g = Math.round(yellow.g + (green.g - yellow.g) * t);
    b = Math.round(yellow.b + (green.b - yellow.b) * t);
  }
  
  return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
}

/**
 * Parse wingspan from various formats
 * Accepts: "6'8", "6'8"", "80", 80
 * Returns: inches as integer, or null if invalid
 */
function parseWingspan(value) {
  if (!value) {
    return null;
  }
  
  const str = value.toString().trim();
  
  // Try to parse as feet'inches format (e.g., "6'8" or "6'8"")
  const feetInchesMatch = str.match(/^(\d+)'(\d+)"?$/);
  if (feetInchesMatch) {
    const feet = parseInt(feetInchesMatch[1]);
    const inches = parseInt(feetInchesMatch[2]);
    return feet * 12 + inches;
  }
  
  // Try to parse as plain number (inches)
  const inchesValue = parseInt(str);
  if (!isNaN(inchesValue) && inchesValue > 0 && inchesValue < 120) {
    return inchesValue;
  }
  
  return null;
}

/**
 * Get player ID by looking up player name and team
 * Returns player_id or null if not found
 */
function getPlayerIdByName(playerName, teamAbbr) {
  const teamId = NBA_TEAMS[teamAbbr];
  if (!teamId) {
    return null;
  }
  
  // Call API to get team roster
  const url = `${API_BASE_URL}/api/team/${teamId}/players`;
  
  try {
    const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    const data = JSON.parse(response.getContentText());
    
    if (response.getResponseCode() === 200 && data.players) {
      // Find player by name
      const player = data.players.find(p => p.name === playerName);
      return player ? player.player_id : null;
    }
  } catch (error) {
    Logger.log(`Error fetching player ID: ${error}`);
  }
  
  return null;
}

/**
 * Update a player field in the database via API
 */
function updatePlayerField(playerId, fieldName, fieldValue) {
  const apiBaseUrl = getApiBaseUrl();
  const url = `${apiBaseUrl}/api/player/${playerId}`;
  
  const payload = {};
  payload[fieldName] = fieldValue;
  
  const options = {
    method: 'patch',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };
  
  const response = UrlFetchApp.fetch(url, options);
  const data = JSON.parse(response.getContentText());
  
  if (response.getResponseCode() !== 200) {
    throw new Error(data.error || 'Unknown error');
  }
  
  return data;
}

/**
 * Section visibility toggle functions
 * Column ranges (1-indexed):
 * - Current Stats: I-AB (columns 9-28)
 * - Historical Stats: AC-AW (columns 29-49)
 * - Postseason Stats: AX-BR (columns 50-70)
 * - Hidden: BS (column 71)
 * For NBA sheet, all ranges shifted right by 1 due to Team column
 */

function toggleCurrentStats() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  const props = PropertiesService.getDocumentProperties();
  const currentVisible = props.getProperty('CURRENT_VISIBLE') !== 'false';  // Default true
  const newVisible = !currentVisible;
  
  Logger.log(`toggleCurrentStats: currentVisible=${currentVisible}, newVisible=${newVisible}`);
  
  // Get teams and column ranges from config
  const config = loadConfig();
  const nbaTeams = config.nba_teams || getNbaTeams();
  const columnRanges = config.column_ranges || {
    team_sheet: { current: { start: 9, count: 20 } },
    nba_sheet: { current: { start: 10, count: 20 } }
  };
  
  Logger.log(`NBA_TEAMS loaded: ${Object.keys(nbaTeams).length} teams`);
  
  let updatedCount = 0;
  
  // Get current sheet and prioritize it
  const currentSheet = ss.getActiveSheet();
  const currentSheetName = currentSheet.getName().toUpperCase();
  const isCurrentTeam = nbaTeams.hasOwnProperty(currentSheetName);
  const isCurrentNBA = (currentSheetName === 'NBA');
  
  // Process current sheet first if it's a team or NBA sheet
  if (isCurrentTeam) {
    const start = columnRanges.team_sheet.current.start;
    const count = columnRanges.team_sheet.current.count;
    Logger.log(`[PRIORITY] Toggling columns on ${currentSheetName}: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
    if (newVisible) {
      currentSheet.showColumns(start, count);
    } else {
      currentSheet.hideColumns(start, count);
    }
    updatedCount++;
  } else if (isCurrentNBA) {
    const start = columnRanges.nba_sheet.current.start;
    const count = columnRanges.nba_sheet.current.count;
    Logger.log(`[PRIORITY] Toggling columns on NBA: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
    if (newVisible) {
      currentSheet.showColumns(start, count);
    } else {
      currentSheet.hideColumns(start, count);
    }
    updatedCount++;
  }
  
  // Toggle visibility on all other team sheets and NBA sheet
  for (const sheet of sheets) {
    // Skip the current sheet since we already processed it
    if (sheet.getSheetId() === currentSheet.getSheetId()) {
      continue;
    }
    
    const sheetName = sheet.getName().toUpperCase();
    const isTeam = nbaTeams.hasOwnProperty(sheetName);
    const isNBA = (sheetName === 'NBA');
    Logger.log(`Checking sheet: ${sheetName}, is team: ${isTeam}, is NBA: ${isNBA}`);
    if (isTeam) {
      const start = columnRanges.team_sheet.current.start;
      const count = columnRanges.team_sheet.current.count;
      Logger.log(`Toggling columns on ${sheetName}: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
      if (newVisible) {
        sheet.showColumns(start, count);
      } else {
        sheet.hideColumns(start, count);
      }
      updatedCount++;
    } else if (isNBA) {
      const start = columnRanges.nba_sheet.current.start;
      const count = columnRanges.nba_sheet.current.count;
      Logger.log(`Toggling columns on NBA: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
      if (newVisible) {
        sheet.showColumns(start, count);
      } else {
        sheet.hideColumns(start, count);
      }
      updatedCount++;
    }
  }
  
  Logger.log(`Updated ${updatedCount} sheets`);
  props.setProperty('CURRENT_VISIBLE', newVisible.toString());
  SpreadsheetApp.getActiveSpreadsheet().toast(
    newVisible ? `Current stats shown on ${updatedCount} sheets` : `Current stats hidden on ${updatedCount} sheets`,
    'Column Visibility',
    3
  );
}

function toggleHistoricalStats() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  const props = PropertiesService.getDocumentProperties();
  const historicalVisible = props.getProperty('HISTORICAL_VISIBLE') !== 'false';  // Default true
  const newVisible = !historicalVisible;
  
  Logger.log(`toggleHistoricalStats: historicalVisible=${historicalVisible}, newVisible=${newVisible}`);
  
  // Get teams and column ranges from config
  const config = loadConfig();
  const nbaTeams = config.nba_teams || getNbaTeams();
  const columnRanges = config.column_ranges || {
    team_sheet: { historical: { start: 29, count: 21 } },
    nba_sheet: { historical: { start: 30, count: 21 } }
  };
  
  let updatedCount = 0;
  
  // Get current sheet and prioritize it
  const currentSheet = ss.getActiveSheet();
  const currentSheetName = currentSheet.getName().toUpperCase();
  const isCurrentTeam = nbaTeams.hasOwnProperty(currentSheetName);
  const isCurrentNBA = (currentSheetName === 'NBA');
  
  // Process current sheet first if it's a team or NBA sheet
  if (isCurrentTeam) {
    const start = columnRanges.team_sheet.historical.start;
    const count = columnRanges.team_sheet.historical.count;
    Logger.log(`[PRIORITY] Toggling columns on ${currentSheetName}: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
    if (newVisible) {
      currentSheet.showColumns(start, count);
    } else {
      currentSheet.hideColumns(start, count);
    }
    updatedCount++;
  } else if (isCurrentNBA) {
    const start = columnRanges.nba_sheet.historical.start;
    const count = columnRanges.nba_sheet.historical.count;
    Logger.log(`[PRIORITY] Toggling columns on NBA: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
    if (newVisible) {
      currentSheet.showColumns(start, count);
    } else {
      currentSheet.hideColumns(start, count);
    }
    updatedCount++;
  }
  
  // Toggle visibility on all other team sheets and NBA sheet
  for (const sheet of sheets) {
    // Skip the current sheet since we already processed it
    if (sheet.getSheetId() === currentSheet.getSheetId()) {
      continue;
    }
    
    const sheetName = sheet.getName().toUpperCase();
    const isTeam = nbaTeams.hasOwnProperty(sheetName);
    const isNBA = (sheetName === 'NBA');
    if (isTeam) {
      const start = columnRanges.team_sheet.historical.start;
      const count = columnRanges.team_sheet.historical.count;
      Logger.log(`Toggling columns on ${sheetName}: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
      if (newVisible) {
        sheet.showColumns(start, count);
      } else {
        sheet.hideColumns(start, count);
      }
      updatedCount++;
    } else if (isNBA) {
      const start = columnRanges.nba_sheet.historical.start;
      const count = columnRanges.nba_sheet.historical.count;
      Logger.log(`Toggling columns on NBA: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
      if (newVisible) {
        sheet.showColumns(start, count);
      } else {
        sheet.hideColumns(start, count);
      }
      updatedCount++;
    }
  }
  
  Logger.log(`Updated ${updatedCount} sheets`);
  props.setProperty('HISTORICAL_VISIBLE', newVisible.toString());
  SpreadsheetApp.getActiveSpreadsheet().toast(
    newVisible ? `Historical stats shown on ${updatedCount} sheets` : `Historical stats hidden on ${updatedCount} sheets`,
    'Column Visibility',
    3
  );
}

function togglePostseasonStats() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  const props = PropertiesService.getDocumentProperties();
  const postseasonVisible = props.getProperty('POSTSEASON_VISIBLE') !== 'false';  // Default true
  const newVisible = !postseasonVisible;
  
  Logger.log(`togglePostseasonStats: postseasonVisible=${postseasonVisible}, newVisible=${newVisible}`);
  
  // Get teams and column ranges from config
  const config = loadConfig();
  const nbaTeams = config.nba_teams || getNbaTeams();
  const columnRanges = config.column_ranges || {
    team_sheet: { postseason: { start: 50, count: 21 } },
    nba_sheet: { postseason: { start: 51, count: 21 } }
  };
  
  let updatedCount = 0;
  
  // Get current sheet and prioritize it
  const currentSheet = ss.getActiveSheet();
  const currentSheetName = currentSheet.getName().toUpperCase();
  const isCurrentTeam = nbaTeams.hasOwnProperty(currentSheetName);
  const isCurrentNBA = (currentSheetName === 'NBA');
  
  // Process current sheet first if it's a team or NBA sheet
  if (isCurrentTeam) {
    const start = columnRanges.team_sheet.postseason.start;
    const count = columnRanges.team_sheet.postseason.count;
    Logger.log(`[PRIORITY] Toggling columns on ${currentSheetName}: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
    if (newVisible) {
      currentSheet.showColumns(start, count);
    } else {
      currentSheet.hideColumns(start, count);
    }
    updatedCount++;
  } else if (isCurrentNBA) {
    const start = columnRanges.nba_sheet.postseason.start;
    const count = columnRanges.nba_sheet.postseason.count;
    Logger.log(`[PRIORITY] Toggling columns on NBA: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
    if (newVisible) {
      currentSheet.showColumns(start, count);
    } else {
      currentSheet.hideColumns(start, count);
    }
    updatedCount++;
  }
  
  // Toggle visibility on all other team sheets and NBA sheet
  for (const sheet of sheets) {
    // Skip the current sheet since we already processed it
    if (sheet.getSheetId() === currentSheet.getSheetId()) {
      continue;
    }
    
    const sheetName = sheet.getName().toUpperCase();
    const isTeam = nbaTeams.hasOwnProperty(sheetName);
    const isNBA = (sheetName === 'NBA');
    if (isTeam) {
      const start = columnRanges.team_sheet.postseason.start;
      const count = columnRanges.team_sheet.postseason.count;
      Logger.log(`Toggling columns on ${sheetName}: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
      if (newVisible) {
        sheet.showColumns(start, count);
      } else {
        sheet.hideColumns(start, count);
      }
      updatedCount++;
    } else if (isNBA) {
      const start = columnRanges.nba_sheet.postseason.start;
      const count = columnRanges.nba_sheet.postseason.count;
      Logger.log(`Toggling columns on NBA: ${newVisible ? 'show' : 'hide'} columns ${start}-${start+count-1}`);
      if (newVisible) {
        sheet.showColumns(start, count);
      } else {
        sheet.hideColumns(start, count);
      }
      updatedCount++;
    }
  }
  
  Logger.log(`Updated ${updatedCount} sheets`);
  props.setProperty('POSTSEASON_VISIBLE', newVisible.toString());
  SpreadsheetApp.getActiveSpreadsheet().toast(
    newVisible ? `Postseason stats shown on ${updatedCount} sheets` : `Postseason stats hidden on ${updatedCount} sheets`,
    'Column Visibility',
    3
  );
}