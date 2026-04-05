"""
The Glass - Shared Sheets Sync Utilities

Common Google Sheets operations shared between NBA and NCAA sync pipelines.
Contains gspread client, worksheet management, formatting application,
and data writing helpers.
"""

import logging
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
                           sheet_type: str = 'team',
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
    # --- Remove existing banded ranges (survive ws.clear()) --------------
    meta = worksheet.spreadsheet.fetch_sheet_metadata(
        params={'fields': 'sheets(properties.sheetId,bandedRanges)'}
    )
    delete_requests = []
    for sheet in meta.get('sheets', []):
        if sheet.get('properties', {}).get('sheetId') == worksheet.id:
            for br in sheet.get('bandedRanges', []):
                delete_requests.append({
                    'deleteBanding': {'bandedRangeId': br['bandedRangeId']}
                })
            break

    requests = build_fn(
        ws_id=worksheet.id,
        columns_list=columns_list,
        header_merges=header_merges,
        n_data_rows=n_data_rows,
        team_name=team_name,
        percentile_cells=percentile_cells,
        n_player_rows=n_player_rows,
        sheet_type=sheet_type,
        show_advanced=show_advanced,
        partial_update=partial_update,
    )
    # Prepend deleteBanding so old banding is removed before new is added
    all_requests = delete_requests + requests
    if not all_requests:
        return

    # Google Sheets API has a ~10 MB request body limit.
    # For large sheets (500+ players), percentile shading alone can
    # generate 50K+ requests.  Chunk to stay well under the limit.
    CHUNK_SIZE = 5000
    for i in range(0, len(all_requests), CHUNK_SIZE):
        chunk = all_requests[i:i + CHUNK_SIZE]
        if chunk:
            worksheet.spreadsheet.batch_update({'requests': chunk})


# ============================================================================
# DATA WRITING & WORKSHEET POSITION
# ============================================================================

def write_and_format(worksheet, columns, headers, data_rows,
                      percentile_cells, n_entity_rows,
                      team_name, sheet_type, show_advanced,
                      partial_update, build_fn):
    """Resize worksheet, write values, and apply formatting."""
    n_cols = len(columns)
    filter_row = [''] * n_cols
    all_rows = [headers['row1'], headers['row2'], headers['row3'],
                filter_row] + data_rows

    # Pad rows to full width
    all_rows = [r + [''] * (n_cols - len(r)) for r in all_rows]

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
        sheet_type=sheet_type,
        show_advanced=show_advanced,
        partial_update=partial_update,
        build_fn=build_fn,
    )


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

