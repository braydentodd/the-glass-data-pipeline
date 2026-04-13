import ast

with open('src/publish/core/calculations.py', 'r') as f:
    text = f.read()

text = text.replace(
    '''            if not refs:
                continue

            if is_stats:''',
    '''            if not refs:
                continue

            # team_average implies we extract info from the player entity/stats
            if isinstance(expr, tuple) and expr[0] == 'team_average':
                entity_type = 'player'

            if is_stats:'''
)

with open('src/publish/core/calculations.py', 'w') as f:
    f.write(text)

