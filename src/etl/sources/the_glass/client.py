"""
Sheet-to-DB Editable Sync — read editable fields from Google Sheets, write to DB.

The Players and Teams sheets are the authoritative source for user-edited
values (wingspan, hand, notes).  This CLI reads those sheets and writes
the values back to the PostgreSQL database.

Usage:
    python -m etl.sources.the_glass.client --league nba [--dry-run]
"""

import argparse
import logging
from typing import Dict, List, Tuple

from dotenv import load_dotenv

from src.db import db_connection, quote_col
from src.publish.config import (
    GOOGLE_SHEETS_CONFIG, SHEETS_COLUMNS, SHEET_FORMATTING
)
from src.publish.core.layout import build_sheet_columns, get_column_index
from src.publish.destinations.sheets.client import get_sheets_client

load_dotenv()

logger = logging.getLogger(__name__)


def _get_editable_defs() -> List[dict]:
    """Return list of editable column definitions with their DB field names."""
    defs = []
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('editable', False):
            continue
        player_field = col_def.get('values', {}).get('player')
        if player_field and isinstance(player_field, str):
            # Strip formula braces: '{notes}' → 'notes'
            db_field = player_field.strip('{}')
            defs.append({
                'col_key': col_key,
                'db_field': db_field,
                'format': col_def.get('format', 'text'),
                'entity_types': _get_entity_types(col_def),
            })
    return defs


def _get_entity_types(col_def: dict) -> List[str]:
    """Determine which entity types an editable field applies to."""
    types = []
    values = col_def.get('values', {})
    if values.get('player'):
        types.append('player')
    if values.get('team') and values['team'] != 'TEAM':
        types.append('team')
    return types


def _read_sheet_data(worksheet, header_rows: int) -> Tuple[List[str], List[List]]:
    """Read all data from a worksheet, returning headers and data rows."""
    all_values = worksheet.get_all_values()
    if len(all_values) <= header_rows:
        return [], []
    headers = all_values[header_rows - 1]
    data_rows = all_values[header_rows:]
    return headers, data_rows


def sync_edits(league: str, dry_run: bool = False) -> Dict[str, int]:
    """Read editable fields from Sheets and write to the database.

    Returns dict with counts: {'players_updated': N, 'teams_updated': N}
    """
    google_config = GOOGLE_SHEETS_CONFIG.get(league)
    if not google_config:
        raise ValueError(f"No Google Sheets config for league: {league}")

    editable_defs = _get_editable_defs()
    if not editable_defs:
        logger.info('No editable fields defined')
        return {'players_updated': 0, 'teams_updated': 0}

    # Build column lists to find column positions
    players_columns = build_sheet_columns(
        entity='player', stats_mode='both', sheet_type='players'
    )
    teams_columns = build_sheet_columns(
        entity='team', stats_mode='both', sheet_type='teams'
    )

    header_rows = SHEET_FORMATTING.get('header_row_count', 4)

    # Resolve column indices for each editable field (0-indexed)
    player_col_map = {}
    team_col_map = {}
    for edef in editable_defs:
        key = edef['col_key']
        if 'player' in edef['entity_types']:
            idx = get_column_index(key, players_columns)
            if idx is not None:
                player_col_map[key] = {'col_idx': idx, **edef}
        if 'team' in edef['entity_types']:
            idx = get_column_index(key, teams_columns)
            if idx is not None:
                team_col_map[key] = {'col_idx': idx, **edef}

    # Find player_id column on Players sheet
    pid_idx = get_column_index('player_id', players_columns)
    if pid_idx is None:
        raise RuntimeError("Could not find player_id column in Players sheet layout")

    # League name maps directly to DB schema
    db_schema = league

    # Open spreadsheet
    client = get_sheets_client(google_config)
    spreadsheet = client.open_by_key(google_config['spreadsheet_id'])

    results = {'players_updated': 0, 'teams_updated': 0}

    # ---- Sync player editable fields from Players sheet ----
    if player_col_map:
        players_sheet_name = league.upper()
        try:
            ws = spreadsheet.worksheet(players_sheet_name)
        except Exception:
            ws = spreadsheet.worksheet('PLAYERS')

        _, data_rows = _read_sheet_data(ws, header_rows)
        logger.info('Read %d data rows from %s sheet', len(data_rows), players_sheet_name)

        updates = []
        for row in data_rows:
            if len(row) <= pid_idx:
                continue
            player_id = row[pid_idx]
            if not player_id:
                continue
            try:
                player_id = int(player_id)
            except (ValueError, TypeError):
                continue

            fields = {}
            for key, mapping in player_col_map.items():
                col_idx = mapping['col_idx']
                if col_idx < len(row):
                    value = row[col_idx]
                    if value == '':
                        value = None
                    fields[mapping['db_field']] = value
            if fields:
                updates.append((player_id, fields))

        if updates:
            results['players_updated'] = _write_player_updates(
                db_schema, updates, dry_run
            )

    # ---- Sync team editable fields from Teams sheet ----
    if team_col_map:
        try:
            ws = spreadsheet.worksheet('TEAMS')
        except Exception:
            logger.warning('TEAMS worksheet not found — skipping team edits')
            return results

        _, data_rows = _read_sheet_data(ws, header_rows)
        logger.info('Read %d data rows from TEAMS sheet', len(data_rows))

        from src.publish.core.queries import get_teams_from_db

        teams_db = get_teams_from_db(db_schema)
        name_to_id = {name: tid for tid, (abbr, name) in teams_db.items()}
        abbr_to_id = {abbr: tid for tid, (abbr, name) in teams_db.items()}

        updates = []
        for row in data_rows:
            if not row:
                continue
            entity_name = row[0]
            if not entity_name:
                continue

            team_id = name_to_id.get(entity_name) or abbr_to_id.get(entity_name.upper())
            if not team_id:
                logger.debug('Could not resolve team: %s', entity_name)
                continue

            fields = {}
            for key, mapping in team_col_map.items():
                col_idx = mapping['col_idx']
                if col_idx < len(row):
                    value = row[col_idx]
                    if value == '':
                        value = None
                    fields[mapping['db_field']] = value
            if fields:
                updates.append((team_id, fields))

        if updates:
            results['teams_updated'] = _write_team_updates(
                db_schema, updates, dry_run
            )

    return results


def _write_player_updates(db_schema: str, updates: List[Tuple[int, dict]],
                          dry_run: bool) -> int:
    """Write player field updates to the database."""
    count = 0
    with db_connection() as conn:
        with conn.cursor() as cur:
            for player_id, fields in updates:
                set_clause = ', '.join(
                    f'{quote_col(f)} = %s' for f in fields
                )
                values = list(fields.values()) + [player_id]
                sql = (
                    f'UPDATE {db_schema}.players '
                    f'SET {set_clause}, updated_at = NOW() '
                    f'WHERE player_id = %s'
                )
                if dry_run:
                    logger.info('[DRY RUN] %s | params=%s', sql, values)
                else:
                    cur.execute(sql, values)
                count += 1
    action = 'Would update' if dry_run else 'Updated'
    logger.info('%s %d player records', action, count)
    return count


def _write_team_updates(db_schema: str, updates: List[Tuple[int, dict]],
                        dry_run: bool) -> int:
    """Write team field updates to the database."""
    count = 0
    with db_connection() as conn:
        with conn.cursor() as cur:
            for team_id, fields in updates:
                set_clause = ', '.join(
                    f'{quote_col(f)} = %s' for f in fields
                )
                values = list(fields.values()) + [team_id]
                sql = (
                    f'UPDATE {db_schema}.teams '
                    f'SET {set_clause}, updated_at = NOW() '
                    f'WHERE team_id = %s'
                )
                if dry_run:
                    logger.info('[DRY RUN] %s | params=%s', sql, values)
                else:
                    cur.execute(sql, values)
                count += 1
    action = 'Would update' if dry_run else 'Updated'
    logger.info('%s %d team records', action, count)
    return count


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description='Sync editable fields from Sheets to DB')
    parser.add_argument('--league', choices=['nba', 'ncaa'], required=True)
    parser.add_argument('--dry-run', action='store_true',
                        help='Log SQL without executing')
    args = parser.parse_args()

    results = sync_edits(args.league, dry_run=args.dry_run)
    print(f"Players updated: {results['players_updated']}")
    print(f"Teams updated: {results['teams_updated']}")


if __name__ == '__main__':
    main()
