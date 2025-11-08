/**
 * Google Apps Script for The Glass - Interactive Stat Switching
 * 
 * This script adds a custom menu to Google Sheets with options to switch
 * between different stat modes (totals, per-game, per-100, per-36, etc.)
 * 
 * Installation:
 * 1. In Google Sheets, go to Extensions > Apps Script
 * 2. Replace the default code with this script
 * 3. Update API_BASE_URL with your OCI server URL
 * 4. Save and authorize the script
 * 5. Refresh the spreadsheet to see the "📊 Stats Mode" menu
 */

// Configuration
const API_BASE_URL = 'http://YOUR_OCI_IP:5000';  // Update with your OCI server IP
const SHEET_ID = '1kqVNHu8cs4lFAEAflI4Ow77oEZEusX7_VpQ6xt8CgB4';

// NBA Team IDs mapping
const NBA_TEAMS = {
  'Atlanta Hawks': 1610612737,
  'Boston Celtics': 1610612738,
  'Brooklyn Nets': 1610612751,
  'Charlotte Hornets': 1610612766,
  'Chicago Bulls': 1610612741,
  'Cleveland Cavaliers': 1610612739,
  'Dallas Mavericks': 1610612742,
  'Denver Nuggets': 1610612743,
  'Detroit Pistons': 1610612765,
  'Golden State Warriors': 1610612744,
  'Houston Rockets': 1610612745,
  'Indiana Pacers': 1610612754,
  'LA Clippers': 1610612746,
  'Los Angeles Lakers': 1610612747,
  'Memphis Grizzlies': 1610612763,
  'Miami Heat': 1610612748,
  'Milwaukee Bucks': 1610612749,
  'Minnesota Timberwolves': 1610612750,
  'New Orleans Pelicans': 1610612740,
  'New York Knicks': 1610612752,
  'Oklahoma City Thunder': 1610612760,
  'Orlando Magic': 1610612753,
  'Philadelphia 76ers': 1610612755,
  'Phoenix Suns': 1610612756,
  'Portland Trail Blazers': 1610612757,
  'Sacramento Kings': 1610612758,
  'San Antonio Spurs': 1610612759,
  'Toronto Raptors': 1610612761,
  'Utah Jazz': 1610612762,
  'Washington Wizards': 1610612764
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
 * Add custom menu when spreadsheet opens
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('📊 Stats Mode')
    .addItem('📈 Totals', 'switchToTotals')
    .addItem('🎮 Per Game', 'switchToPerGame')
    .addItem('💯 Per 100 Possessions', 'switchToPer100')
    .addItem('⏱️ Per 36 Minutes', 'switchToPer36')
    .addSeparator()
    .addItem('⚙️ Custom Per Minutes...', 'showCustomMinutesDialog')
    .addItem('⚙️ Custom Per Possessions...', 'showCustomPossessionsDialog')
    .addSeparator()
    .addItem('ℹ️ Current Mode', 'showCurrentMode')
    .addToUi();
}

/**
 * Switch all team sheets to Totals mode
 */
function switchToTotals() {
  updateAllSheets('totals', null);
  SpreadsheetApp.getUi().alert('✅ Switched to Totals mode');
}

/**
 * Switch all team sheets to Per Game mode
 */
function switchToPerGame() {
  updateAllSheets('per_game', null);
  SpreadsheetApp.getUi().alert('✅ Switched to Per Game mode');
}

/**
 * Switch all team sheets to Per 100 Possessions mode
 */
function switchToPer100() {
  updateAllSheets('per_100', null);
  SpreadsheetApp.getUi().alert('✅ Switched to Per 100 Possessions mode');
}

/**
 * Switch all team sheets to Per 36 Minutes mode
 */
function switchToPer36() {
  updateAllSheets('per_36', null);
  SpreadsheetApp.getUi().alert('✅ Switched to Per 36 Minutes mode');
}

/**
 * Show dialog for custom per-minutes value
 */
function showCustomMinutesDialog() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.prompt(
    'Custom Per Minutes',
    'Enter the number of minutes to scale stats to (e.g., 30, 32, 40):',
    ui.ButtonSet.OK_CANCEL
  );
  
  if (response.getSelectedButton() === ui.Button.OK) {
    const minutes = parseFloat(response.getResponseText());
    if (isNaN(minutes) || minutes <= 0) {
      ui.alert('❌ Invalid input. Please enter a positive number.');
      return;
    }
    updateAllSheets('per_minutes', minutes);
    ui.alert(`✅ Switched to Per ${minutes} Minutes mode`);
  }
}

/**
 * Show dialog for custom per-possessions value
 */
function showCustomPossessionsDialog() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.prompt(
    'Custom Per Possessions',
    'Enter the number of possessions to scale stats to (e.g., 75, 100, 150):',
    ui.ButtonSet.OK_CANCEL
  );
  
  if (response.getSelectedButton() === ui.Button.OK) {
    const possessions = parseFloat(response.getResponseText());
    if (isNaN(possessions) || possessions <= 0) {
      ui.alert('❌ Invalid input. Please enter a positive number.');
      return;
    }
    updateAllSheets('per_possessions', possessions);
    ui.alert(`✅ Switched to Per ${possessions} Possessions mode`);
  }
}

/**
 * Show current mode stored in document properties
 */
function showCurrentMode() {
  const props = PropertiesService.getDocumentProperties();
  const mode = props.getProperty('STATS_MODE') || 'per_100';
  const customValue = props.getProperty('STATS_CUSTOM_VALUE');
  
  let modeText = mode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  if (customValue) {
    modeText += ` (${customValue})`;
  }
  
  SpreadsheetApp.getUi().alert(`Current mode: ${modeText}`);
}

/**
 * Update all team sheets with new stat mode
 */
function updateAllSheets(mode, customValue) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  
  // Store mode in document properties
  const props = PropertiesService.getDocumentProperties();
  props.setProperty('STATS_MODE', mode);
  if (customValue !== null) {
    props.setProperty('STATS_CUSTOM_VALUE', customValue.toString());
  } else {
    props.deleteProperty('STATS_CUSTOM_VALUE');
  }
  
  // Show progress
  SpreadsheetApp.getActiveSpreadsheet().toast('Updating sheets...', '⏳ Please wait', -1);
  
  let updatedCount = 0;
  
  // Update each team sheet
  for (const sheet of sheets) {
    const sheetName = sheet.getName();
    
    // Skip non-team sheets
    if (!NBA_TEAMS.hasOwnProperty(sheetName)) {
      continue;
    }
    
    const teamId = NBA_TEAMS[sheetName];
    
    try {
      // Call API to get stats
      const stats = fetchTeamStats(teamId, mode, customValue);
      
      if (stats && stats.players) {
        // Update sheet with new stats
        updateSheetWithStats(sheet, stats, mode, customValue);
        updatedCount++;
      }
    } catch (error) {
      Logger.log(`Error updating ${sheetName}: ${error}`);
    }
  }
  
  SpreadsheetApp.getActiveSpreadsheet().toast(`Updated ${updatedCount} teams`, '✅ Complete', 3);
}

/**
 * Fetch team stats from API
 */
function fetchTeamStats(teamId, mode, customValue) {
  const url = `${API_BASE_URL}/api/stats`;
  
  const payload = {
    team_id: teamId,
    mode: mode,
    season: '2024-25'
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
  
  // Update header to show current mode
  let headerText = mode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  if (customValue !== null) {
    headerText += ` (${customValue})`;
  }
  sheet.getRange(1, 2).setValue(`24-25 Stats - ${headerText}`);
  
  // Start from row 3 (after headers)
  const startRow = 3;
  
  // Clear existing data (keep headers)
  const lastRow = sheet.getLastRow();
  if (lastRow >= startRow) {
    sheet.getRange(startRow, 1, lastRow - startRow + 1, sheet.getLastColumn()).clearContent();
  }
  
  // Write player data
  for (let i = 0; i < players.length; i++) {
    const player = players[i];
    const row = startRow + i;
    const stats = player.calculated_stats;
    const percentiles = player.percentiles;
    
    // Column A: Player name
    sheet.getRange(row, 1).setValue(player.player_name);
    
    // Columns B onwards: Stats with colors
    let col = 2;
    for (const statName of STAT_COLUMNS) {
      const value = stats[statName];
      const percentile = percentiles[`${statName}_percentile`] || 0;
      
      // Format value
      let displayValue = value;
      if (statName.includes('pct')) {
        // Percentages: multiply by 100 and add %
        displayValue = `${(value * 100).toFixed(1)}%`;
      } else if (statName === 'games') {
        displayValue = Math.round(value);
      } else {
        displayValue = value.toFixed(1);
      }
      
      const cell = sheet.getRange(row, col);
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
