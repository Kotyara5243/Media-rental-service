
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict
from .mongodb_connection import get_collection
from .mongodb import insert_watch_history, get_all_users

def load_data():
    """
    Gets all users that are in a family and a family member has an active session
    {"user_id": {"user_name" : ..., "available_media" : [ { "family_member": ..., "media_id": ..., "media_name": ...}, ... ], ... }}
    """

    result = defaultdict(lambda: {
        "user_name": None,
        "available_media": []
    })

    for u in get_all_users() :
        user_id = u['user_id']
        media = get_family_shared_media(user_id)
        if not media:
            continue
        result[user_id]['user_name'] = u['user_name']
        result[user_id]["available_media"] = media

    return dict(result)

def get_family_shared_media(user_id: int) -> List[Dict]:
    users = get_collection("users")
    sessions = get_collection("sessions")
    families = get_collection("families")

    user = users.find_one(
        {"user_id": user_id},
        {"family_id": 1}
    )
    if not user or "family_id" not in user:
        return []

    family = families.find_one(
        {"family_id": user["family_id"]},
        {"_id": 0, "users": 1}
    )
    family_members = [member for member in family["users"] if member["user_id"] != user_id]
    print(family_members)
    
    if not family_members:
        return []

    member_ids = [m["user_id"] for m in family_members]
    member_names = {m["user_id"]: m["user_name"] for m in family_members}
    current_time = datetime.now()

    active_sessions = sessions.find(
        {"user.user_id": {"$in": member_ids}},
        {"_id": 0}
    )

    result: List[Dict] = []

    for session in active_sessions:
        rental_end = session["date_of_rent"] + timedelta(
            hours=session["duration"]
        )

        if rental_end > current_time:
            result.append({
                "family_member": member_names.get(session["user"]["user_id"]),
                "media_id": session["media"]["media_id"],
                "media_name": session["media"]["media_name"],
                "type": session["media"]["type"]
            })

    return result

def watch_media(user_id: int, media_id: int) -> list[dict]:
    insert_watch_history(user_id, media_id, 1)
    result = get_family_watches()
    return result

def get_family_watches() -> list[dict]:
    coll = get_collection("watch_history")
    watches = list(coll.find({"family_watch" : 1}, {"_id": 0}))
    return watches