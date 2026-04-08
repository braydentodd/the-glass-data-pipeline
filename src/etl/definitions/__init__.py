"""
The Glass - ETL Configuration Package

Re-exports all config symbols so ``from src.etl.config import X`` keeps working.
"""

from src.etl.definitions.config import (                           # noqa: F401
    DB_COLUMNS_SCHEMA,
    ETL_CONFIG,
    ETL_CONFIG_SCHEMA,
    ETL_TABLES,
    TABLES,
    TABLES_SCHEMA,
    TYPE_TRANSFORMS,
    VALID_ENTITY_TYPES,
    VALID_PG_TYPES,
    VALID_SCOPES,
    VALID_UPDATE_FREQUENCIES,
)
from src.etl.definitions.columns import DB_COLUMNS                   # noqa: F401
