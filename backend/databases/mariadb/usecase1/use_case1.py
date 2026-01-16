from collections import defaultdict
from ..mariadb import *
from ..data_generator import generate_random_data
from datetime import datetime


def generate_test_data() :
    insert_family( Family(None, "Family", datetime.now()) )
    insert_family( Family(None, "Family", datetime.now()) )
    insert_family( Family(None, "Family", datetime.now()) )

    insert_user( User(None, "a", "", datetime.now(), "", "", 1) )
    insert_user( User(None, "b", "", datetime.now(), "", "", 1) )
    insert_user( User(None, "c", "", datetime.now(), "", "", 1) )
    insert_user( User(None, "d", "", datetime.now(), "", "", 2) )
    insert_user( User(None, "e", "", datetime.now(), "", "", 2) )
    insert_user( User(None, "f", "", datetime.now(), "", "", 3) )

    insert_media( Media(None, "x", "", 1, "", "", 1) )
    insert_media( Media(None, "xx", "", 1, "", "", 1) )
    insert_media( Media(None, "y", "", 1, "", "", 1) )
    insert_media( Media(None, "xy", "", 1, "", "", 1) )
    insert_media( Media(None, "yy", "", 1, "", "", 1) )
    insert_media( Media(None, "z", "", 1, "", "", 1) )
    insert_media( Media(None, "zz", "", 1, "", "", 1) )
    insert_media( Media(None, "xyz", "", 1, "", "", 1) )

    insert_session( Session(None, 1, 1, datetime.now(), 1, 1) )
    insert_session( Session(None, 2, 2, datetime.now(), 1, 1) )
    insert_session( Session(None, 4, 1, datetime.now(), 1, 1) )

def load_data() :
    """
    Gets all users that are in a family and a family member has an active session
    {"user_id": [ { "family_member_id": ..., "media_id": ...}, ... ], ... }
    """

    select_result = execute_select(
        """
        SELECT
            u.user_id,
            s.session_id,
            s.media_id,
            s.user_id AS family_member_id
        FROM Users u
        JOIN Users u_ref
            ON u.family_id = u_ref.family_id
        JOIN Sessions s
            ON s.user_id = u_ref.user_id
        WHERE u.user_id <> s.user_id 
            AND DATE_ADD(s.date_of_rent, INTERVAL s.duration HOUR) > NOW();
        """
    )

    print(str(select_result))

    result = defaultdict(list)

    for row in select_result:
        user_id = row["user_id"]
        result[user_id].append({
            "family_member_id": row["family_member_id"],
            "media_id": row["media_id"],
        })

    return dict(result)


    # user_ids = []
    # media_ids = []
    # family_member_ids = []
    
    # for row in select_result :
    #     user_ids.append(row["user_id"])
    #     media_ids.append(row["media_id"])
    #     family_member_ids.append(row["family_member_id"])

    # return result

def watch_media(user_id: int, media_id: int) :
    """
    Creates a session for the given user and media
    """
    insert_watch_history(WatchHistory(None, user_id, media_id, datetime.now(), True))
