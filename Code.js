
// Configuration
const API_BASE_URL = 'http://150.136.255.23:5001';
const SHEET_ID = '1kqVNHu8cs4lFAEAflI4Ow77oEZEusX7_VpQ6xt8CgB4';

// NBA Team IDs mapping by abbreviation
const NBA_TEAMS = {
  'ATL': 1610612737,  // Atlanta Hawks
  'BOS': 1610612738,  // Boston Celtics
  'BKN': 1610612751,  // Brooklyn Nets
  'CHA': 1610612766,  // Charlotte Hornets
  'CHI': 1610612741,  // Chicago Bulls
  'CLE': 1610612739,  // Cleveland Cavaliers
  'DAL': 1610612742,  // Dallas Mavericks
  'DEN': 1610612743,  // Denver Nuggets
  'DET': 1610612765,  // Detroit Pistons
  'GSW': 1610612744,  // Golden State Warriors
  'HOU': 1610612745,  // Houston Rockets
  'IND': 1610612754,  // Indiana Pacers
  'LAC': 1610612746,  // LA Clippers
  'LAL': 1610612747,  // Los Angeles Lakers
  'MEM': 1610612763,  // Memphis Grizzlies
  'MIA': 1610612748,  // Miami Heat
  'MIL': 1610612749,  // Milwaukee Bucks
  'MIN': 1610612750,  // Minnesota Timberwolves
  'NOP': 1610612740,  // New Orleans Pelicans
  'NYK': 1610612752,  // New York Knicks
  'OKC': 1610612760,  // Oklahoma City Thunder
  'ORL': 1610612753,  // Orlando Magic
  'PHI': 1610612755,  // Philadelphia 76ers
  'PHX': 1610612756,  // Phoenix Suns
  'POR': 1610612757,  // Portland Trail Blazers
  'SAC': 1610612758,  // Sacramento Kings
  'SAS': 1610612759,  // San Antonio Spurs
  'TOR': 1610612761,  // Toronto Raptors
  'UTA': 1610612762,  // Utah Jazz
  'WAS': 1610612764   // Washington Wizards
};

// Stat columns in order
const STAT_COLUMNS = [
  'games', 'minutes', 'points', 'ts_pct', 'fg2a', 'fg2_pct', 'fg3a', 'fg3_pct',
  'fta', 'ft_pct', 'assists', 'turnovers', 'oreb_pct', 'dreb_pct', 'steals', 
  'blocks', 'fouls'
];

// Reverse stats (lower is better)
const REVERSE_STATS = ['turnovers', 'fouls'];

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
  ui.createMenu('Stats View')
    .addItem('Totals', 'switchToTotals')
    .addItem('Per Game', 'switchToPerGame')
    .addItem('Per Minute', 'switchToPerMinute')
    .addSeparator()
    .addItem('Show Percentiles', 'togglePercentileDisplay')
    .addSeparator()
    .addItem('Previous Years Stats', 'showHistoricalStatsDialog')
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
  
  // Only process team sheets
  if (!NBA_TEAMS.hasOwnProperty(sheetName)) {
    return;
  }
  
  const row = range.getRow();
  const col = range.getColumn();
  
  // Only process rows 4 and below (data rows)
  if (row < 4) {
    return;
  }
  
  // Column F = Wingspan (column 6), Column H = Notes (column 8)
  // Check if edited cell is in wingspan or notes column
  if (col !== 6 && col !== 8) {
    return;
  }
  
  // Get player ID from hidden column AR (column 44)
  const playerId = sheet.getRange(row, 44).getValue();
  
  if (!playerId) {
    const playerName = sheet.getRange(row, 1).getValue();
    SpreadsheetApp.getUi().alert(`Could not find player ID for ${playerName}. Please refresh the sheet.`);
    return;
  }
  
  const playerName = sheet.getRange(row, 1).getValue();
  const newValue = range.getValue();
  
  // Determine which field was edited
  let fieldName, fieldValue, displayFieldName;
  if (col === 6) {
    // Wingspan column
    fieldName = 'wingspan_inches';
    displayFieldName = 'wingspan';
    
    // Allow empty values to clear the field
    if (!newValue || newValue === '') {
      fieldValue = null;
    } else {
      fieldValue = parseWingspan(newValue);
      if (fieldValue === null) {
        SpreadsheetApp.getUi().alert('Invalid wingspan format. Please use format like 6\'8" or enter inches as a number.');
        return;
      }
    }
  } else if (col === 8) {
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
    SpreadsheetApp.getUi().alert(`Error updating database: ${error.message}`);
  }
}

/**
 * Switch all team sheets to Totals mode
 */
function switchToTotals() {
  updateAllSheets('totals', null);
  SpreadsheetApp.getUi().alert('Switched to Totals mode');
}

/**
 * Switch all team sheets to Per Game mode
 */
function switchToPerGame() {
  updateAllSheets('per_game', null);
  SpreadsheetApp.getUi().alert('Switched to Per Game mode');
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
      ui.alert('Invalid input. Please enter a positive number.');
      return;
    }
    updateAllSheets('per_minutes', minutes);
    ui.alert(`Switched to Per ${minutes} Minutes mode`);
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
  
  if (newMode === 'true') {
    // Trigger a refresh of all sheets
    const statsMode = props.getProperty('STATS_MODE') || 'totals';
    const customValue = props.getProperty('STATS_CUSTOM_VALUE');
    updateAllSheets(statsMode, customValue ? parseFloat(customValue) : null);
    ui.alert('Now showing percentile ranks instead of stat values.');
  } else {
    // Trigger a refresh of all sheets
    const statsMode = props.getProperty('STATS_MODE') || 'totals';
    const customValue = props.getProperty('STATS_CUSTOM_VALUE');
    updateAllSheets(statsMode, customValue ? parseFloat(customValue) : null);
    ui.alert('Now showing stat values.');
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
  if (!input || input.trim() === '') {
    return { valid: false, error: 'Please enter a value' };
  }
  
  const trimmed = input.trim();
  
  // Check for Career mode
  if (trimmed.toLowerCase() === 'career' || trimmed.toLowerCase() === 'c') {
    return { valid: true, mode: 'career', value: 25 };
  }
  
  // Check if it's a number (number of years)
  const numYears = parseInt(trimmed);
  if (!isNaN(numYears) && trimmed === numYears.toString()) {
    if (numYears < 1 || numYears > 25) {
      return { valid: false, error: 'Number of years must be between 1 and 25' };
    }
    return { valid: true, mode: 'years', value: numYears };
  }
  
  // Check if it's a season format (contains dash/slash or is 4 digits)
  // Valid formats: 2024-25, 1998-99, 98-99, 2024/25, 2024, 1998
  const seasonPattern = /^\d{2,4}([\-\/]\d{2,4})?$/;
  if (seasonPattern.test(trimmed)) {
    // Normalize to YYYY-YY format
    let normalized;
    
    if (trimmed.includes('-') || trimmed.includes('/')) {
      const parts = trimmed.split(/[\-\/]/);
      const firstPart = parts[0];
      const secondPart = parts[1];
      
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
        return { valid: false, error: 'Invalid season format. Use: 2024-25, 98-99, or 2024' };
      }
    }
    
    return { valid: true, mode: 'season', value: normalized };
  }
  
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
  
  // Trigger the sync via API in background
  try {
    syncHistoricalStats(parsed.mode, parsed.value, includeCurrentYear, null);
    
    // Show toast notification immediately (dialog will close first)
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Syncing previous years stats in the background. Sheets will update in a few minutes.',
      'Background Sync Started',
      3
    );
    
    return { success: true };
  } catch (error) {
    // Configuration saved but sync failed
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
 * Trigger historical stats sync via API
 */
function syncHistoricalStats(mode, value, includeCurrentYear, priorityTeam) {
  const url = `${API_BASE_URL}/api/sync-historical-stats`;
  
  // Get current stats mode from document properties
  const props = PropertiesService.getDocumentProperties();
  const statsMode = props.getProperty('STATS_MODE') || 'per_36';
  const statsCustomValue = props.getProperty('STATS_CUSTOM_VALUE');
  
  const payload = {
    mode: mode,
    include_current: includeCurrentYear,
    stats_mode: statsMode  // Pass current stats mode to sync
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
  
  // Check if historical stats are configured
  const historicalMode = props.getProperty('HISTORICAL_MODE');
  
  if (historicalMode) {
    // If historical stats are configured, trigger full sync (no quick update)
    // This ensures both current and historical stats update together without header duplication
    
    const historicalYears = props.getProperty('HISTORICAL_YEARS');
    const historicalSeasons = props.getProperty('HISTORICAL_SEASONS');
    const includeCurrent = props.getProperty('INCLUDE_CURRENT_YEAR') === 'true';
    
    let value;
    if (historicalMode === 'season') {
      value = historicalSeasons;
    } else {
      value = parseInt(historicalYears) || 3;
    }
    
    // Show appropriate message based on whether user is on a team sheet
    if (NBA_TEAMS.hasOwnProperty(activeSheetName)) {
      SpreadsheetApp.getActiveSpreadsheet().toast(
        `Updating all teams (current + historical) starting with ${activeSheetName}...\nThis will take a few minutes.`,
        'Full Sync Running',
        5
      );
    } else {
      SpreadsheetApp.getActiveSpreadsheet().toast(
        'Updating all teams (current + historical)...\nThis will take a few minutes.',
        'Full Sync Running',
        5
      );
    }
    
    // Pass the active sheet as priority team if it's a team sheet
    const priorityTeam = NBA_TEAMS.hasOwnProperty(activeSheetName) ? activeSheetName : null;
    syncHistoricalStats(historicalMode, value, includeCurrent, priorityTeam);
    
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
          SpreadsheetApp.getActiveSpreadsheet().toast(`${activeSheetName} updated`, 'Current Sheet Complete', 2);
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
      
      SpreadsheetApp.getActiveSpreadsheet().toast(`Updating ${sheetName}...`, 'Please wait', -1);
      
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
    
    SpreadsheetApp.getActiveSpreadsheet().toast(`Updated ${updatedCount} teams`, 'Complete', 3);
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
  
  // Update header with season and mode
  const headerText = `${season} Stats ${modeText}`;
  Logger.log(`Setting header to: ${headerText}`);
  sheet.getRange(1, 9).setValue(headerText);
  
  // Start from row 4, column 1 (A4) for player names, column 9 (I4) for stats
  const startRow = 4;
  const statsStartColumn = 9;  // Column I
  
  // Set row 3 height to 15 pixels
  sheet.setRowHeight(3, 15);
  
  // Clear ONLY stat columns (Column I onwards), not player info columns (A-G)
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
}

/**
 * Get RGB color based on percentile
 */
function getPercentileColor(percentile) {
  // Red at 0%, Yellow at 50%, Green at 100%
  const red = { r: 238, g: 75, b: 43 };    // #EE4B2B
  const yellow = { r: 252, g: 245, b: 95 }; // #FCF55F
  const green = { r: 76, g: 187, b: 23 };   // #4CBB17
  
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
  const url = `${API_BASE_URL}/api/player/${playerId}`;
  
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
