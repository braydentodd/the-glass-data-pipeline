"""
Expression builder functions for sheets column formulas.

Each function returns a lightweight tuple tree that is evaluated at runtime
by the evaluator in calculations.py. Column configs compose these builders
to express computed values declaratively without executing anything at
import time.

Examples:
    divide('fg2m', 'fg2a')          -> ('divide', 'fg2m', 'fg2a')
    multiply(2, divide('fg3m', ...)) -> ('multiply', 2, ('divide', 'fg3m', ...))
    add('ftm', 'fg2m', 'fg3m')      -> ('add', 'ftm', 'fg2m', 'fg3m')
"""


# Sentinel: resolved at runtime to the number of seasons in the current query
seasons_in_query = ('seasons_in_query',)


def add(*args):
    """Sum of two or more fields/expressions."""
    return ('add', *args)


def subtract(a, b):
    """Difference: a - b."""
    return ('subtract', a, b)


def multiply(a, b):
    """Product: a * b."""
    return ('multiply', a, b)


def divide(a, b):
    """Quotient: a / b.  Returns None on division by zero."""
    return ('divide', a, b)


def lookup(key_field, table, target_field):
    """Cross-table lookup (e.g., team_id -> teams -> abbr)."""
    return ('lookup', key_field, table, target_field)


def team_average(field):
    """Minute-weighted average of a player field across the roster."""
    return ('team_average', field)


def calculate_age(field):
    """Calculate age (years, 1 decimal) from a birthdate field."""
    return ('calculate_age', field)
