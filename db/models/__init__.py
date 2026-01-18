# Import all models to ensure they are registered with the database
from .season1.daily_stats import DailyStats
from .season1.total_stats import TotalStats
from .season1.freeagents import FreeAgent

from .season2.daily_player_stats import DailyPlayerStats
from .season2.cumulative_player_stats import CumulativePlayerStats
from .season2.daily_matchup_score import DailyMatchupScore

__all__ = [
    'DailyStats', 'TotalStats', 'FreeAgent', 'DailyPlayerStats', 'CumulativePlayerStats', 'DailyMatchupScore'
]
