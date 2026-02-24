"""Quick test of lib/sheets.py"""
from lib.sheets import (
    evaluate_formula, calculate_entity_stats, get_reverse_stats,
    get_editable_fields, build_sheet_columns, build_headers,
    get_columns_by_filters, get_percentile_rank, format_height,
    format_stat_value, clear_cache,
    _COMPILED_FORMULAS, _STAT_TABLE_FIELDS, SheetsConfigurationError
)

print('=== Import Success ===')
print(f'Compiled formulas: {len(_COMPILED_FORMULAS)}')
print(f'Stat DB fields: {len(_STAT_TABLE_FIELDS)}')
print(f'Sample DB fields: {sorted(list(_STAT_TABLE_FIELDS))[:10]}')
print(f'Reverse stats: {get_reverse_stats()}')
print(f'Editable fields: {get_editable_fields()}')
print()

# Test formula evaluation with mock data
mock_player = {
    'games': 50, 'minutes_x10': 15000, 'possessions': 3500,
    '2fgm': 200, '2fga': 400, '3fgm': 100, '3fga': 300,
    'ftm': 150, 'fta': 200, 'name': 'Test Player',
    'player_id': 1, 'team_abbr': 'BOS',
    'assists': 250, 'turnovers': 100,
}

print('=== Per Game Mode ===')
stats = calculate_entity_stats(mock_player, 'player', 'per_game')
print(f'Points per game: {stats.get("points")}')  # (200*2 + 100*3 + 150) / 50 = 17
print(f'FTR: {stats.get("free_throw_rate")}')  # 200 / (400+300) = 0.286
print(f'Assists per game: {stats.get("assists")}')  # 250 / 50 = 5.0
print(f'Minutes per game: {stats.get("minutes")}')  # 15000/10 / 50 = 30.0
print()

print('=== Totals Mode ===')
stats_totals = calculate_entity_stats(mock_player, 'player', 'totals')
print(f'Points total: {stats_totals.get("points")}')  # 200*2 + 100*3 + 150 = 850
print(f'FTR in totals (should be FTA=200): {stats_totals.get("free_throw_rate")}')
print(f'Assists total: {stats_totals.get("assists")}')  # 250
print()

print('=== Per 36 Mode ===')
stats_per36 = calculate_entity_stats(mock_player, 'player', 'per_36')
print(f'Minutes per_36 (should be per_game=30): {stats_per36.get("minutes")}')
print(f'Points per_36: {stats_per36.get("points")}')  # 850 * 36 / 1500 = 20.4
print()

print('=== Column Helpers ===')
columns = build_sheet_columns(entity='player', stat_mode='both', show_percentiles=False)
print(f'Total columns: {len(columns)}')
player_cols = get_columns_by_filters(section='current_stats', entity='player')
print(f'Current stats player columns: {len(player_cols)}')
print()

print('=== Format Helpers ===')
print(f'format_height(80): {format_height(80)}')
print(f'format_height(75): {format_height(75)}')

print()
print('=== Percentile ===')
sorted_vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
print(f'Percentile of 5 in [1-10]: {get_percentile_rank(5, sorted_vals)}')
print(f'Percentile of 5 reversed: {get_percentile_rank(5, sorted_vals, reverse=True)}')
print(f'Percentile of 10: {get_percentile_rank(10, sorted_vals)}')
print(f'Percentile of 1: {get_percentile_rank(1, sorted_vals)}')

print()
print('✅ All tests passed!')
