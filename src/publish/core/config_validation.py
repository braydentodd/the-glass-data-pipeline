"""
The Glass - Publish Config Validation

Publish-specific validation: checks TAB_COLUMNS entries against the
declared schema, and cross-references section/subsection consistency.

Uses the generic validation engine from ``src.config_validation``.

Add a new config?  Define a schema dict next to the data in
``src/publish/config.py``, then register it in ``validate_config()`` here.
"""

import logging
from typing import List

from src.core.config_validation import validate_dict_config, validate_flat_config

logger = logging.getLogger(__name__)


# ============================================================================
# CROSS-REFERENCE VALIDATORS
# ============================================================================

def _validate_section_subsection(sheets_columns: dict) -> List[str]:
    """Validate that stats columns have a subsection assigned.

    Stats columns require a subsection for ordering and header display.
    Non-stats columns may optionally have subsections (e.g. profile
    uses 'League' and 'Player' subsections).
    """
    from src.publish.definitions.config import SECTION_CONFIG

    stats_sections = {
        s for s, meta in SECTION_CONFIG.items() if meta.get('is_stats_section')
    }
    errors: List[str] = []

    for col_name, col_def in sheets_columns.items():
        sections = col_def.get('sections', [])
        subsection = col_def.get('subsection')
        is_stats = any(s in stats_sections for s in sections)

        if is_stats and subsection is None:
            errors.append(
                f"TAB_COLUMNS['{col_name}']: stats column missing 'subsection'"
            )

    return errors


def _validate_width_classes(sheets_columns: dict) -> List[str]:
    """Validate that string width_class values are recognized names."""
    from src.publish.definitions.columns import _VALID_WIDTH_CLASSES

    errors: List[str] = []
    for col_name, col_def in sheets_columns.items():
        wc = col_def.get('width_class')
        if isinstance(wc, str) and wc not in _VALID_WIDTH_CLASSES:
            errors.append(
                f"TAB_COLUMNS['{col_name}']: 'width_class' value {wc!r} "
                f"not in {_VALID_WIDTH_CLASSES}"
            )
    return errors


# ============================================================================
# PUBLIC API
# ============================================================================

def validate_config() -> List[str]:
    """Validate all publish configuration at startup.

    Returns:
        Empty list if all valid.

    Raises:
        RuntimeError: If any validation errors are found.
    """
    from src.publish.definitions.columns import TAB_COLUMNS, TAB_COLUMNS_SCHEMA
    from src.publish.definitions.config import (
        GOOGLE_SHEETS_CONFIG, STAT_CONSTANTS, SHEET_FORMATTING, SECTION_CONFIG,
        COLORS, COLOR_THRESHOLDS, MENU_CONFIG,
        GOOGLE_SHEETS_CONFIG_SCHEMA, STAT_CONSTANTS_SCHEMA, SHEET_FORMATTING_SCHEMA,
        SECTION_CONFIG_SCHEMA, COLORS_SCHEMA, COLOR_THRESHOLDS_SCHEMA, MENU_CONFIG_SCHEMA
    )
    from src.core.config_validation import validate_core_constants

    errors: List[str] = []

    errors.extend(validate_core_constants())

    # Schema validations
    errors.extend(validate_dict_config(TAB_COLUMNS, TAB_COLUMNS_SCHEMA, 'TAB_COLUMNS'))
    errors.extend(validate_dict_config(GOOGLE_SHEETS_CONFIG, GOOGLE_SHEETS_CONFIG_SCHEMA, 'GOOGLE_SHEETS_CONFIG'))
    errors.extend(validate_dict_config(SECTION_CONFIG, SECTION_CONFIG_SCHEMA, 'SECTION_CONFIG'))
    errors.extend(validate_dict_config(COLORS, COLORS_SCHEMA, 'COLORS'))
    errors.extend(validate_dict_config(MENU_CONFIG, MENU_CONFIG_SCHEMA, 'MENU_CONFIG'))
    
    errors.extend(validate_flat_config(STAT_CONSTANTS, STAT_CONSTANTS_SCHEMA, 'STAT_CONSTANTS'))
    errors.extend(validate_flat_config(SHEET_FORMATTING, SHEET_FORMATTING_SCHEMA, 'SHEET_FORMATTING'))
    errors.extend(validate_flat_config(COLOR_THRESHOLDS, COLOR_THRESHOLDS_SCHEMA, 'COLOR_THRESHOLDS'))

    # Cross-reference validations
    errors.extend(_validate_section_subsection(TAB_COLUMNS))
    errors.extend(_validate_width_classes(TAB_COLUMNS))

    if errors:
        for err in errors:
            logger.error('Publish config validation: %s', err)
        raise RuntimeError(
            f"Publish config validation failed with {len(errors)} error(s)"
        )

    logger.info('Publish config validation passed (%d columns)', len(TAB_COLUMNS))
    return errors
