"""
The Glass - Shared Sheets Sync Utilities

Common Google Sheets operations shared between NBA and NCAA sync pipelines.
Contains gspread client, worksheet management, formatting application,
and data writing helpers.
"""

import logging

import gspread
from google.oauth2.service_account import Credentials

from lib.sheets_engine import build_formatting_requests

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
                           data_only: bool = False):
    """
    Apply ALL Google Sheets formatting via batch API requests.
    Delegates to sheets_engine.build_formatting_requests (config-driven).

    Removes any pre-existing banded ranges on the worksheet first so that
    addBanding does not collide with stale banding from a previous sync
    (ws.clear() removes cell values but NOT banding properties).

    For large sheets (500+ players), requests are chunked to stay under
    the Google Sheets API ~10 MB request size limit.

    When data_only=True, skips structural formatting (fonts, borders, widths,
    column visibility) and only applies banding, percentile shading, and
    grid resize.  Used for fast mode/timeframe switches.
    """
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

    requests = build_formatting_requests(
        ws_id=worksheet.id,
        columns_list=columns_list,
        header_merges=header_merges,
        n_data_rows=n_data_rows,
        team_name=team_name,
        percentile_cells=percentile_cells,
        n_player_rows=n_player_rows,
        sheet_type=sheet_type,
        show_advanced=show_advanced,
        data_only=data_only,
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
