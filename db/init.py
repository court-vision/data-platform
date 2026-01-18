from db.base import db
# Function to initialize database connection
def init_db():
    """Initialize database connection and create tables if they don't exist."""
    db.connect()
    
    # Import all models to register them
    from .models import DailyPlayerStats, CumulativePlayerStats, DailyMatchupScore

    # Create tables if they don't exist
    db.create_tables([
        DailyPlayerStats, CumulativePlayerStats, DailyMatchupScore
    ], safe=True)

init_db()
