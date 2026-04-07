"""
Config Export CLI — generate apps-script/config/<LEAGUE>_generated.js.

Usage:
    python -m src.sheets.config_export --league nba

Thin CLI wrapper around src.sheets.core.export.export_config().
"""

import argparse
import logging

from src.sheets.core.export import export_config


def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    parser = argparse.ArgumentParser(description='Export sheets config as JS for clasp push')
    parser.add_argument('--league', choices=['nba', 'ncaa'], required=True)
    args = parser.parse_args()

    path = export_config(args.league)
    print(f'Config exported to {path}')


if __name__ == '__main__':
    main()
