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
    {"user_id": {"user_name" : ..., "available_media" : [ { "family_member": ..., "media_id": ..., "media_name": ...}, ... ], ... }}
    """

    select_result = execute_select(
        """
        SELECT
            u.user_id,
            u.user_name,
            s.session_id,
            s.media_id,
            m.media_name,
            u_ref.user_name AS family_member
        FROM Users u
        JOIN Users u_ref
            ON u.family_id = u_ref.family_id
        JOIN Sessions s
            ON s.user_id = u_ref.user_id
        JOIN Media m
            ON m.media_id = s.media_id
        WHERE u.user_id <> s.user_id 
            AND DATE_ADD(s.date_of_rent, INTERVAL s.duration HOUR) > NOW();
        """
    )

    media_type_rows = execute_select(
        """
        SELECT
            m.media_id,
            CASE
                WHEN s.series_id IS NOT NULL THEN 'series'
                WHEN f.film_id IS NOT NULL THEN 'film'
                ELSE 'unknown'
            END AS media_type
        FROM Media m
        LEFT JOIN Series s ON s.media_id = m.media_id
        LEFT JOIN Film f ON f.media_id = m.media_id;
        """
    )

    media_type_map = {
        row["media_id"]: row["media_type"]
        for row in media_type_rows
    }


    print(str(select_result))

    result = defaultdict(lambda: {
        "user_name": None,
        "available_media": []
    })

    for row in select_result:
        user_id = row["user_id"]
        result[user_id]["user_name"] = row["user_name"]
        result[user_id]["available_media"].append({
            "family_member": row["family_member"],
            "media_id": row["media_id"],
            "media_name": row["media_name"],
            "type": media_type_map.get(row["media_id"], "unknown")
        })

    return dict(result)

def watch_media(user_id: int, media_id: int) -> list[dict] :
    insert_watch_history(WatchHistory(None, user_id, media_id, datetime.now(), True))

    select_result = execute_select(
        """
        SELECT 
            u.user_name AS user_name,
            m.media_name AS media_name,
            f.family_type AS family_type,
            wh.date_of_watch AS date_of_watch
        FROM WatchHistory wh
        INNER JOIN Users u ON u.user_id = wh.user_id 
        INNER JOIN Media m ON m.media_id = wh.media_id
        INNER JOIN Family f ON u.family_id = f.family_id
        INNER JOIN Film fi ON fi.media_id = m.media_id 
        WHERE wh.family_watch = TRUE;
        """,
        ()
    )

    return select_result
