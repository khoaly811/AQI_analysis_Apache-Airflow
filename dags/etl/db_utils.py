from etl.models import Session

def get_db_session():
    """Return a new database session instance."""
    return Session()

def close_session():
    """Close the current session."""
    Session.remove()
