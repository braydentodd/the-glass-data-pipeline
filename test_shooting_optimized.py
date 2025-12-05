#!/usr/bin/env python3
"""Quick test of optimized shooting tracking"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from src.etl import update_player_advanced_stats

print("="*70)
print("🚀 TESTING OPTIMIZED SHOOTING TRACKING - 2025-26")
print("="*70)
print("Now using 6 league-wide calls instead of 439 per-player calls!")
print()

update_player_advanced_stats('2025-26', 2025)
