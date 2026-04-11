"""
The Glass - Shared Sheets Sync Utilities

Common Google Sheets operations shared between NBA and NCAA sync pipelines.
Contains gspread client, worksheet management, formatting application,
and data writing helpers.
"""

import logging
from decimal import Decimal
from typing import Callable, Optional

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


# ============================================================================
# GOOGLE SHEETS CLIENT
# ============================================================================

def get_sheets_client(google_sheets_config: dict):
    """Initialise Google Sheets API client from service-account credentials.

    Args:
        google_sheets_config: Dict with 'credentials_file' and 'scopes' keys.
    """
    creds = Credentials.from_service_account_file(
        google_sheets_config['credentials_file'],
        scopes=google_sheets_config['scopes'],
    )
    return gspread.authorize(creds)


# ============================================================================
# WORKSHEET MANAGEMENT
# ============================================================================

def get_or_create_worksheet(spreadsheet, title: str, rows: int = 200,
                            cols: int = 200, clear: bool = True):
    """Get existing worksheet or create a new one, clearing if it exists."""
    try:
        ws = spreadsheet.worksheet(title)
        if clear:
            ws.clear()
        return ws
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


# ============================================================================
# FORMATTING
# ============================================================================

def apply_sheet_formatting(worksheet, columns_list, header_merges: list,
                           n_data_rows: int, team_name: str,
                           percentile_cells: list, n_player_rows: int,
                           tab_type: str = 'team',
                           show_advanced: bool = False,
                           partial_update: bool = False,
                           build_fn: Optional[Callable] = None):
    """
    Apply ALL Google Sheets formatting via batch API requests.
    Delegates to the league-specific build_formatting_requests function passed
    via build_fn (from ctx.sheets_lib.build_formatting_requests).

    Removes any pre-existing banded ranges on the worksheet first so that
    addBanding does not collide with stale banding from a previous sync
    (ws.clear() removes cell values but NOT banding properties).

    For large sheets (500+ players), requests are chunded to stay under
    the Google Sheets API ~10 MB request size limit.

    When partial_update=True, skips structural formatting (fonts, borders, widths,
    column visibility) and only applies banding, percentile shading, and
    filters.  Used for fast mode/timeframe switches.

    Args:
        build_fn: League-specific build_formatting_requests callable.
                  Typically ctx.sheets_lib.build_formatting_requests.
                  Required — must be provided by the orchestrator.
    """
    if build_fn is None:
        raise ValueError(
            "apply_sheet_formatting requires build_fn — pass "
            "ctx.sheets_lib.build_formatting_requests from the orchestrator."
        )
    # --- Remove existing banded ranges, merges, and filters (survive ws.clear())
    meta = worksheet.spreadsheet.fetch_sheet_metadata(
        params={'fields': 'sheets(properties.sheetId,bandedRanges,merges,basicFilter)'}
    )
    delete_requests = []
    for sheet in meta.get('sheets', []):
        if sheet.get('properties', {}).get('sheetId') == worksheet.id:
            for br in sheet.get('bandedRanges', []):
                delete_requests.append({
                    'deleteBanding': {'bandedRangeId': br['bandedRangeId']}
                })
            for merge in sheet.get('merges', []):
                delete_requests.append({
                    'unmergeCells': {'range': merge}
                })
            break

    # Always clear filter — ws.clear() doesn't remove it and metadata
    # detection can be unreliable with gspread versions
    delete_requests.append({
        'clearBasicFilter': {'sheetId': worksheet.id}
    })

    # Execute cleanup as a separate batch so all old state is fully removed
    # before new formatting is applied
    if delete_requests:
        try:
            worksheet.spreadsheet.batch_update({'requests': delete_requests})
        except Exception:
            pass  # clearBasicFilter on a sheet with no filter raises a 400

    requests = build_fn(
        ws_id=worksheet.id,
        columns_list=columns_list,
        header_merges=header_merges,
        n_data_rows=n_data_rows,
        team_name=team_name,
        percentile_cells=percentile_cells,
        n_player_rows=n_player_rows,
        tab_type=tab_type,
        show_advanced=show_advanced,
        partial_update=partial_update,
    )
    if not requests:
        return

    # Grid properties (frozen cols/rows) must be applied BEFORE merges
    # to avoid "can't merge frozen and non-frozen columns" validation errors.
    # Google Sheets validates all requests in a batch against the initial state,
    # so merges would be checked against the sheet's pre-existing frozen layout.
    props = [r for r in requests if 'updateSheetProperties' in r]
    rest = [r for r in requests if 'updateSheetProperties' not in r]
    if props:
        worksheet.spreadsheet.batch_update({'requests': props})

    # Google Sheets API has a ~10 MB request body limit.
    # For large sheets (500+ players), percentile shading alone can
    # generate 50K+ requests.  Chunk to stay well under the limit.
    CHUNK_SIZE = 5000
    for i in range(0, len(rest), CHUNK_SIZE):
        chunk = rest[i:i + CHUNK_SIZE]
        if chunk:
            worksheet.spreadsheet.batch_update({'requests': chunk})


# ============================================================================
# DATA WRITING & WORKSHEET POSITION
# ============================================================================

def write_and_format(worksheet, columns, headers, data_rows,
                      percentile_cells, n_entity_rows,
                      team_name, tab_type, show_advanced,
                      partial_update, build_fn):
    """Resize worksheet, write values, and apply formatting.

    For auto-width columns, header cells are initially blanked so that
    autoResizeDimensions sizes based on data only.  The real headers are
    written back in a second pass after formatting is applied.
    """
    from src.publish.definitions.config import WIDTH_CLASSES

    n_cols = len(columns)
    filter_row = [''] * n_cols
    all_rows = [list(headers['row1']), list(headers['row2']),
                list(headers['row3']), filter_row] + data_rows

    # Pad rows to full width
    all_rows = [r + [''] * (n_cols - len(r)) for r in all_rows]

    # Convert Decimal values to float for JSON serialization
    all_rows = [
        [float(cell) if isinstance(cell, Decimal) else cell for cell in row]
        for row in all_rows
    ]

    # Identify auto-width columns and blank their headers before writing
    auto_col_indices = []
    for idx, entry in enumerate(columns):
        col_def = entry[1]
        wc = col_def.get('width_class')
        is_auto = (wc is None) or (isinstance(wc, str) and WIDTH_CLASSES.get(wc) is None)
        if is_auto and not col_def.get('is_generated_percentile', False):
            auto_col_indices.append(idx)

    saved_headers = {}
    for idx in auto_col_indices:
        saved_headers[idx] = (all_rows[0][idx], all_rows[1][idx], all_rows[2][idx])
        all_rows[0][idx] = ''
        all_rows[1][idx] = ''
        all_rows[2][idx] = ''

    total_rows = len(all_rows)
    worksheet.resize(rows=total_rows, cols=n_cols)
    worksheet.update(range_name='A1', values=all_rows,
                     value_input_option='USER_ENTERED')

    apply_sheet_formatting(
        worksheet, columns,
        header_merges=headers['merges'],
        n_data_rows=len(data_rows),
        team_name=team_name,
        percentile_cells=percentile_cells,
        n_player_rows=n_entity_rows,
        tab_type=tab_type,
        show_advanced=show_advanced,
        partial_update=partial_update,
        build_fn=build_fn,
    )

    # Restore real header values for auto-width columns after auto-resize
    if saved_headers:
        for idx, (h1, h2, h3) in saved_headers.items():
            all_rows[0][idx] = h1
            all_rows[1][idx] = h2
            all_rows[2][idx] = h3
        header_rows = all_rows[:3]
        worksheet.update(range_name='A1', values=header_rows,
                         value_input_option='USER_ENTERED')


def move_sheet_to_position(worksheet, index):
    """Move a worksheet to a specific tab position in the workbook."""
    try:
        worksheet.spreadsheet.batch_update({'requests': [{
            'updateSheetProperties': {
                'properties': {
                    'sheetId': worksheet.id,
                    'index': index,
                },
                'fields': 'index',
            }
        }]})
    except Exception as e:
        logger.warning(f'  Could not move sheet to position {index}: {e}')

