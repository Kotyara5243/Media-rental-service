from datetime import datetime
from ..mariadb import (
    insert_session,
    find_user_by_id,
    find_media_by_id,
    execute_select,
    Session,
)


def calculate_rental_cost(cost_per_day: int, duration_days: int) -> int:
    """Calculate rental cost: cost_per_day × duration_days"""
    return cost_per_day * duration_days


def rent_media(user_id: int, media_id: int, duration_days: int) -> dict:
    """
    Rent media and create session record.
    1. Validate user and media exist
    2. Calculate cost
    3. Create Session in database
    4. Return confirmation
    """
    user = find_user_by_id(user_id)
    if user is None:
        raise ValueError(f"User {user_id} not found")
   
    media = find_media_by_id(media_id)
    if media is None:
        raise ValueError(f"Media {media_id} not found")

    if duration_days <= 0:
        raise ValueError("Duration must be at least 1 day")
  
    total_cost = calculate_rental_cost(media.cost_per_day, duration_days)
 
    session = Session(
        session_id=None,
        user_id=user_id,
        media_id=media_id,
        date_of_rent=datetime.now(),
        cost=total_cost,
        duration=duration_days
    )
    session = insert_session(session)
    
    return {
        'success': True,
        'session_id': session.session_id,
        'user_id': user_id,
        'media_id': media_id,
        'media_name': media.media_name,
        'cost': total_cost,
        'duration': duration_days,
        'date_of_rent': str(session.date_of_rent)
    }


def get_all_media() -> list:
    """Get all media available for rental"""
    query = """
    SELECT m.media_id, m.media_name, m.genre, m.prod_year, m.cost_per_day
    FROM Media m
    ORDER BY m.media_name
    """
    return execute_select(query, ())


def get_user_rentals(user_id: int) -> list:
    """Get all rentals by a user"""
    query = """
    SELECT 
        s.session_id, 
        s.media_id,
        m.media_name, 
        s.cost, 
        s.duration, 
        s.date_of_rent
    FROM Sessions s
    JOIN Media m ON s.media_id = m.media_id
    WHERE s.user_id = %s
    ORDER BY s.date_of_rent DESC
    """
    return execute_select(query, (user_id,))


def get_all_users() -> list:
    """Get all users"""
    query = """
    SELECT u.user_id, u.user_name, u.email
    FROM Users u
    ORDER BY u.user_name
    """
    return execute_select(query, ())
