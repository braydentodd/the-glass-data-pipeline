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

from src.core.config_validation import validate_dict_config

logger = logging.getLogger(__name__)


# ============================================================================
# CROSS-REFERENCE VALIDATORS
# ============================================================================

def _validate_section_subsection(sheets_columns: dict) -> List[str]:
    """Validate that subsection values are consistent with stats sections.

    Non-stats columns (entities, player_info, analysis, identity) should
    not have a subsection.  Stats columns should have one.
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
        elif not is_stats and subsection is not None:
            errors.append(
                f"TAB_COLUMNS['{col_name}']: non-stats column has unexpected "
                f"subsection '{subsection}'"
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
    errors: List[str] = []

    errors.extend(validate_dict_config(TAB_COLUMNS, TAB_COLUMNS_SCHEMA, 'TAB_COLUMNS'))
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
