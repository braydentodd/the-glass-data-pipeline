"""
THE GLASS - Flask API Runner

Entry point for the Flask API server.
Consistent with other runners/ entry points.

Usage:
    python -m runners.api [--host HOST] [--port PORT] [--debug]
"""

import argparse
import logging

from dotenv import load_dotenv

from sheets.core.nba_sheets import API_CONFIG
from api.lib import app

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


def main():
    parser = argparse.ArgumentParser(description='Run The Glass Flask API server')
    parser.add_argument('--host', default=API_CONFIG.get('host', '0.0.0.0'),
                        help='Host to bind to')
    parser.add_argument('--port', type=int, default=API_CONFIG.get('port', 5000),
                        help='Port to listen on')
    parser.add_argument('--debug', action='store_true',
                        default=API_CONFIG.get('debug', False),
                        help='Enable debug mode')
    args = parser.parse_args()

    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()
