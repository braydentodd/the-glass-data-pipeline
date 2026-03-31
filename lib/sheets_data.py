# lib/sheets_data.py
import logging
import psycopg2

logger = logging.getLogger(__name__)

def get_db_table_columns(db_config: dict, db_schema: str, table_name: str, exclude_columns: set = None) -> set:
    """
    Dynamically fetches all column names for a given schema and table 
    using information_schema.columns.
    This replaces hardcoded lists of fields (like ENTITY DB FIELDS).
    
    Args:
        db_config: The database configuration dictionary.
        db_schema: Schema name ('nba' or 'ncaa').
        table_name: Table name (e.g. 'stats_current').
        exclude_columns: Set of columns to exclude from the return array.
        
    Returns:
        Set of column name strings.
    """
    exclude_columns = set(exclude_columns) if exclude_columns else set()
    
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (db_schema, table_name)
            )
            if cur.fetchone() is None:
                logger.warning(f"Table {db_schema}.{table_name} does not exist.")
                return set()

            # Fetch existing columns
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (db_schema, table_name)
            )
            
            columns = set()
            for row in cur.fetchall():
                col = row[0]
                if col not in exclude_columns:
                    columns.add(col)
                    
            return columns
