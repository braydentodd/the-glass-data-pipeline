"""
The Glass - Database Configuration

Single source of truth for database connection settings.
Both NBA and NCAA pipelines share the same PostgreSQL instance
with separate schemas (nba.*, ncaa.*).
"""
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', ''),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', '')
}
