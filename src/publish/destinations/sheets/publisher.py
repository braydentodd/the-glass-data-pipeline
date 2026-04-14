"""
The Glass - Google Sheets Destination Publisher

Takes pre-calculated, structured Intermediate Representation (IR) dictionaries
from the core Executor and translates them into Google Sheets batch API requests.
"""

import logging
from typing import List, Tuple

from src.publish.destinations.sheets.api_builder import build_formatting_requests
from src.publish.destinations.sheets.client import get_or_create_worksheet, write_and_format

logger = logging.getLogger(__name__)


def publish_tab(
    client,
    spreadsheet,
    tab_name: str,
    ir_payload: dict,
    partial_update: bool = False,
    show_advanced: bool = False,
) -> None:
    """
    Takes an agnostic IR payload and writes it to Google Sheets.
    
    IR format expected:
    {
        "columns_list": [...],
        "headers": {...},
        "data_rows": [...],
        "percentile_cells": [...],
        "n_player_rows": int,
        "tab_type": str,
        "display_name": str
    }
    """
    logger.info(f"    Publishing IR payload to Sheet tab: {tab_name}...")
    
    worksheet = get_or_create_worksheet(spreadsheet, tab_name, clear=not partial_update)
    
    write_and_format(
        worksheet=worksheet,
        columns_list=ir_payload['columns_list'],
        headers=ir_payload['headers'],
        data_rows=ir_payload['data_rows'],
        percentile_cells=ir_payload['percentile_cells'],
        n_player_rows=ir_payload['n_player_rows'],
        team_name=ir_payload['display_name'],
        tab_type=ir_payload['tab_type'],
        show_advanced=show_advanced,
        partial_update=partial_update,
        build_fn=build_formatting_requests,
    )
    
    logger.info(f"    Successfully published {ir_payload['n_player_rows']} main rows.")
