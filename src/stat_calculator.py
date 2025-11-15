"""
Stat Calculator - Calculate NBA statistics in different modes
Supports: totals, per-game, per-100 possessions, per-36 minutes, per-X minutes, per-X possessions
"""

class StatCalculator:
    """
    Calculate player statistics in various modes.
    
    Modes:
    - totals: Raw season totals
    - per_game: Stats divided by games played
    - per_100: Stats per 100 possessions
    - per_36: Stats per 36 minutes
    - per_minutes: Stats per X minutes (custom)
    - per_possessions: Stats per X possessions (custom)
    """
    
    def __init__(self, player_stats):
        """
        Initialize calculator with player stats.
        
        Args:
            player_stats (dict): Dictionary containing raw player statistics
                Required keys: games, minutes, fg2m, fg2a, fg3m, fg3a, ftm, fta, etc.
                Should include possessions if available
        """
        self.stats = player_stats
        self.games = player_stats.get('games', 0) or 0
        self.minutes = player_stats.get('minutes', 0) or 0
        self.possessions = player_stats.get('possessions', 0) or 0
        
        # Calculate derived stats
        self.fg2m = player_stats.get('fg2m', 0) or 0
        self.fg2a = player_stats.get('fg2a', 0) or 0
        self.fg3m = player_stats.get('fg3m', 0) or 0
        self.fg3a = player_stats.get('fg3a', 0) or 0
        self.ftm = player_stats.get('ftm', 0) or 0
        self.fta = player_stats.get('fta', 0) or 0
        
        # Calculate points and percentages
        self.points = (self.fg2m * 2) + (self.fg3m * 3) + self.ftm
        self.fg2_pct = (self.fg2m / self.fg2a) if self.fg2a > 0 else 0
        self.fg3_pct = (self.fg3m / self.fg3a) if self.fg3a > 0 else 0
        self.ft_pct = (self.ftm / self.fta) if self.fta > 0 else 0
        
        # Calculate true shooting percentage
        # TS% = PTS / (2 * (FGA + 0.44 * FTA))
        fga = self.fg2a + self.fg3a
        ts_attempts = 2 * (fga + 0.44 * self.fta)
        self.ts_pct = (self.points / ts_attempts) if ts_attempts > 0 else 0
        
        # If possessions not available, estimate from pace and minutes
        if not self.possessions and self.minutes > 0:
            # Average NBA pace is ~100 possessions per 48 minutes
            self.possessions = (self.minutes / 48.0) * 100
    
    def calculate_totals(self):
        """Return raw season totals."""
        return {
            'games': self.games,
            'minutes': self.minutes,
            'points': self.points,
            'ts_pct': self.ts_pct,
            'fg2a': self.fg2a,
            'fg2m': self.fg2m,
            'fg2_pct': self.fg2_pct,
            'fg3a': self.fg3a,
            'fg3m': self.fg3m,
            'fg3_pct': self.fg3_pct,
            'fta': self.fta,
            'ftm': self.ftm,
            'ft_pct': self.ft_pct,
            'assists': self.stats.get('assists', 0),
            'turnovers': self.stats.get('turnovers', 0),
            'oreb_pct': self.stats.get('oreb_pct', 0),
            'dreb_pct': self.stats.get('dreb_pct', 0),
            'steals': self.stats.get('steals', 0),
            'blocks': self.stats.get('blocks', 0),
            'fouls': self.stats.get('fouls', 0),
        }
    
    def calculate_per_game(self):
        """Calculate per-game averages."""
        if self.games == 0:
            return self._empty_stats()
        
        totals = self.calculate_totals()
        per_game = {}
        
        # Divide counting stats by games
        counting_stats = ['minutes', 'points', 'fg2a', 'fg2m', 'fg3a', 'fg3m', 
                         'fta', 'ftm', 'assists', 'turnovers', 
                         'steals', 'blocks', 'fouls']
        
        for stat in counting_stats:
            per_game[stat] = round(totals[stat] / self.games, 1)
        
        # Percentages stay the same
        per_game['games'] = self.games
        per_game['ts_pct'] = totals['ts_pct']
        per_game['fg2_pct'] = totals['fg2_pct']
        per_game['fg3_pct'] = totals['fg3_pct']
        per_game['ft_pct'] = totals['ft_pct']
        per_game['oreb_pct'] = totals['oreb_pct']
        per_game['dreb_pct'] = totals['dreb_pct']
        
        return per_game
    
    def calculate_per_100(self):
        """Calculate per-100 possessions."""
        if self.possessions == 0:
            return self._empty_stats()
        
        totals = self.calculate_totals()
        per_100 = {}
        
        # Scale counting stats to per 100 possessions
        counting_stats = ['points', 'fg2a', 'fg2m', 'fg3a', 'fg3m', 
                         'fta', 'ftm', 'assists', 'turnovers', 
                         'steals', 'blocks', 'fouls']
        
        scale_factor = 100.0 / self.possessions
        
        for stat in counting_stats:
            per_100[stat] = round(totals[stat] * scale_factor, 1)
        
        # Minutes scaled to per 100 possessions
        per_100['minutes'] = round(totals['minutes'] * scale_factor, 1)
        
        # Percentages stay the same
        per_100['games'] = self.games
        per_100['ts_pct'] = totals['ts_pct']
        per_100['fg2_pct'] = totals['fg2_pct']
        per_100['fg3_pct'] = totals['fg3_pct']
        per_100['ft_pct'] = totals['ft_pct']
        per_100['oreb_pct'] = totals['oreb_pct']
        per_100['dreb_pct'] = totals['dreb_pct']
        
        return per_100
    
    def calculate_per_36(self):
        """Calculate per-36 minutes."""
        if self.minutes == 0:
            return self._empty_stats()
        
        totals = self.calculate_totals()
        per_36 = {}
        
        # Scale counting stats to per 36 minutes
        counting_stats = ['points', 'fg2a', 'fg2m', 'fg3a', 'fg3m', 
                         'fta', 'ftm', 'assists', 'turnovers', 
                         'steals', 'blocks', 'fouls']
        
        scale_factor = 36.0 / self.minutes
        
        for stat in counting_stats:
            per_36[stat] = round(totals[stat] * scale_factor, 1)
        
        # Minutes is always 36
        per_36['minutes'] = 36.0
        
        # Percentages stay the same
        per_36['games'] = self.games
        per_36['ts_pct'] = totals['ts_pct']
        per_36['fg2_pct'] = totals['fg2_pct']
        per_36['fg3_pct'] = totals['fg3_pct']
        per_36['ft_pct'] = totals['ft_pct']
        per_36['oreb_pct'] = totals['oreb_pct']
        per_36['dreb_pct'] = totals['dreb_pct']
        
        return per_36
    
    def calculate_per_minutes(self, minutes):
        """
        Calculate per-X minutes.
        
        Args:
            minutes (float): Target minutes to scale to
        """
        if self.minutes == 0 or minutes <= 0:
            return self._empty_stats()
        
        totals = self.calculate_totals()
        per_x = {}
        
        counting_stats = ['points', 'fg2a', 'fg2m', 'fg3a', 'fg3m', 
                         'fta', 'ftm', 'assists', 'turnovers', 
                         'steals', 'blocks', 'fouls']
        
        scale_factor = minutes / self.minutes
        
        for stat in counting_stats:
            per_x[stat] = round(totals[stat] * scale_factor, 1)
        
        # Keep minutes as per-game, not the scaled value
        per_x['minutes'] = round(self.minutes / self.games, 1) if self.games > 0 else 0
        per_x['games'] = self.games
        per_x['ts_pct'] = totals['ts_pct']
        per_x['fg2_pct'] = totals['fg2_pct']
        per_x['fg3_pct'] = totals['fg3_pct']
        per_x['ft_pct'] = totals['ft_pct']
        per_x['oreb_pct'] = totals['oreb_pct']
        per_x['dreb_pct'] = totals['dreb_pct']
        
        return per_x
    
    def calculate_per_possessions(self, possessions):
        """
        Calculate per-X possessions.
        
        Args:
            possessions (float): Target possessions to scale to
        """
        if self.possessions == 0 or possessions <= 0:
            return self._empty_stats()
        
        totals = self.calculate_totals()
        per_x = {}
        
        counting_stats = ['points', 'fg2a', 'fg2m', 'fg3a', 'fg3m', 
                         'fta', 'ftm', 'assists', 'turnovers', 
                         'steals', 'blocks', 'fouls']
        
        scale_factor = possessions / self.possessions
        
        for stat in counting_stats:
            per_x[stat] = round(totals[stat] * scale_factor, 1)
        
        per_x['minutes'] = round(totals['minutes'] * scale_factor, 1)
        per_x['games'] = self.games
        per_x['ts_pct'] = totals['ts_pct']
        per_x['fg2_pct'] = totals['fg2_pct']
        per_x['fg3_pct'] = totals['fg3_pct']
        per_x['ft_pct'] = totals['ft_pct']
        per_x['oreb_pct'] = totals['oreb_pct']
        per_x['dreb_pct'] = totals['dreb_pct']
        
        return per_x
    
    def _empty_stats(self):
        """Return empty stats dict."""
        return {
            'games': 0,
            'minutes': 0,
            'points': 0,
            'ts_pct': 0,
            'fg2a': 0,
            'fg2m': 0,
            'fg2_pct': 0,
            'fg3a': 0,
            'fg3m': 0,
            'fg3_pct': 0,
            'fta': 0,
            'ftm': 0,
            'ft_pct': 0,
            'assists': 0,
            'turnovers': 0,
            'oreb_pct': 0,
            'dreb_pct': 0,
            'steals': 0,
            'blocks': 0,
            'fouls': 0,
        }


def calculate_stats_for_team(players, mode='per_100', custom_value=None):
    """
    Calculate stats for all players on a team in specified mode.
    
    Args:
        players (list): List of player dictionaries with raw stats
        mode (str): One of: 'totals', 'per_game', 'per_100', 'per_36', 
                    'per_minutes', 'per_possessions'
        custom_value (float): Value for per_minutes or per_possessions mode
    
    Returns:
        list: Players with calculated stats in specified mode
    """
    results = []
    
    for player in players:
        calc = StatCalculator(player)
        
        if mode == 'totals':
            calculated = calc.calculate_totals()
        elif mode == 'per_game':
            calculated = calc.calculate_per_game()
        elif mode == 'per_100':
            calculated = calc.calculate_per_100()
        elif mode == 'per_36':
            calculated = calc.calculate_per_36()
        elif mode == 'per_minutes':
            if custom_value is None:
                custom_value = 36  # Default to 36 minutes
            calculated = calc.calculate_per_minutes(custom_value)
        elif mode == 'per_possessions':
            if custom_value is None:
                custom_value = 100  # Default to 100 possessions
            calculated = calc.calculate_per_possessions(custom_value)
        else:
            raise ValueError(f"Invalid mode: {mode}")
        
        # Combine player info with calculated stats
        result = {**player, 'calculated_stats': calculated}
        results.append(result)
    
    return results
