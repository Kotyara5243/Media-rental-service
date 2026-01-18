from typing import List, Dict

from .mongodb import (
    get_all_users as mongo_get_all_users,
    get_all_media as mongo_get_all_media,
    get_user_rentals as mongo_get_user_rentals,
    insert_rental_session,
)

MAX_DURATION_DAYS = 365


def rent_media(user_id: int, media_id: int, duration_days: int) -> Dict:
    if duration_days < 1:
        raise ValueError("Duration must be at least 1 day")
    if duration_days > MAX_DURATION_DAYS:
        raise ValueError(f"Duration must not exceed {MAX_DURATION_DAYS} days")

    return insert_rental_session(user_id, media_id, duration_days)


def list_users() -> List[Dict]:
    return mongo_get_all_users()


def list_media() -> List[Dict]:
    return mongo_get_all_media()


def list_user_rentals(user_id: int) -> List[Dict]:
    return mongo_get_user_rentals(user_id)
