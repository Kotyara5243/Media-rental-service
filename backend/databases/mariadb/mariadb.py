from ..models import *
from .mariadb_connection import get_mariadb
from datetime import datetime

TABLE_RESET_ORDER = [
    "WatchHistory",
    "Friendships",
    "Sessions",
    "Device",
    "Film",
    "Series",
    "Media",
    "Users",
    "Family",
]

# --------Common functions----------
def reset_all_tables():
    with get_mariadb() as connection:
        try:
            with connection.cursor() as cursor:
                for table in TABLE_RESET_ORDER:
                    cursor.execute(f"DELETE FROM `{table}`")
                    cursor.execute(f"ALTER TABLE `{table}` AUTO_INCREMENT = 1")
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        
def test_db() :
    try:
        return execute_select_one("SELECT %s", 1)
    except Exception as e:
        raise e
        
def execute_select(sql: str, params: tuple = ()) -> list[dict]:
    with get_mariadb() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
        
def get_table_rows(table_name: str) -> dict:
    rows = execute_select(f"SELECT * FROM `{table_name}`")
    return rows

def list_tables() -> list[str]:
    rows = execute_select("SHOW TABLES")
    return [list(row.values())[0] for row in rows]

def list_all_tables_with_rows() -> dict[str, list[dict]]:
    """
    Returns:
    {
        "Users": [ {row1}, {row2}, ... ],
        "Media": [ {row1}, {row2}, ... ],
        ...
    }
    """
    result: dict[str, list[dict]] = {}

    tables = list_tables()

    for table in tables:
        rows = execute_select(f"SELECT * FROM `{table}`")
        result[table] = rows

    return result


#--------------Inserts--------------

def execute_insert(sql: str, params: tuple) -> int | None:
    """
    Executes an INSERT statement and returns the id of the inserted row.
    """
    with get_mariadb() as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                last_id = cursor.lastrowid
            connection.commit()
            return last_id
        except Exception:
            connection.rollback()
            raise

def insert_user(user: User) -> User:
    user.user_id = execute_insert(
        """
        INSERT INTO Users (user_name, email, birthday, location, bio, family_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            user.user_name,
            user.email,
            user.birthday,
            user.location,
            user.bio,
            user.family_id,
        ),
    )
    return user

def insert_family(family: Family) -> Family:
    family.family_id = execute_insert(
        """
        INSERT INTO Family (family_type, creation_date)
        VALUES (%s, %s)
        """,
        (family.family_type, family.creation_date),
    )
    return family

def insert_friendship(friendship: Friendship) -> None:
    execute_insert(
        """
        INSERT INTO Friendships (user_id, friend_id)
        VALUES (%s, %s)
        """,
        (friendship.user_id, friendship.friend_id),
    )

def insert_media(media: Media) -> Media:
    media.media_id = execute_insert(
        """
        INSERT INTO Media
        (media_name, genre, prod_year, descr, location, cost_per_day)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            media.media_name,
            media.genre,
            media.prod_year,
            media.descr,
            media.location,
            media.cost_per_day,
        ),
    )
    return media


def insert_series(series: Series) -> Series:
    series.series_id = execute_insert(
        """
        INSERT INTO Series (number_of_episodes, is_ongoing, media_id)
        VALUES (%s, %s, %s)
        """,
        (
            series.number_of_episodes,
            series.is_ongoing,
            series.media_id,
        ),
    )
    return series

def insert_film(film: Film) -> Film:
    film.film_id = execute_insert(
        """
        INSERT INTO Film (duration, number_of_parts, media_id)
        VALUES (%s, %s, %s)
        """,
        (
            film.duration,
            film.number_of_parts,
            film.media_id,
        ),
    )
    return film

def insert_session(session: Session) -> Session:
    session.session_id = execute_insert(
        """
        INSERT INTO Sessions
        (user_id, media_id, date_of_rent, cost, duration)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            session.user_id,
            session.media_id,
            session.date_of_rent,
            session.cost,
            session.duration,
        ),
    )
    print(session.session_id)
    return session

def insert_watch_history(history: WatchHistory) -> None:
    execute_insert(
        """
        INSERT INTO WatchHistory
        (user_id, media_id, date_of_watch, family_watch)
        VALUES (%s, %s, %s, %s)
        """,
        (
            history.user_id,
            history.media_id,
            history.date_of_watch,
            history.family_watch,
        ),
    )

def insert_device(device: Device) -> Device:
    device.device_id = execute_insert(
        """
        INSERT INTO Device (device_name, registration_date, user_id)
        VALUES (%s, %s, %s)
        """,
        (
            device.device_name,
            device.registration_date,
            device.user_id,
        ),
    )
    return device

# --------------Removes-----------------

def execute_delete(sql: str, params: tuple) -> int | None:
    """
    Executes a DELETE statement and returns number of affected rows.
    """
    with get_mariadb() as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                affected_rows = cursor.rowcount
            connection.commit()
            return affected_rows
        except Exception:
            connection.rollback()
            raise

def remove_family(family_id: int) -> bool:
    rows = execute_delete(
        "DELETE FROM Family WHERE family_id = %s",
        (family_id,),
    )
    return rows > 0

def remove_friendship(user_id: int, friend_id: int) -> bool:
    rows = execute_delete(
        """
        DELETE FROM Friendships
        WHERE user_id = %s AND friend_id = %s
        """,
        (user_id, friend_id),
    )
    return rows > 0

def remove_media(media_id: int) -> bool:
    rows = execute_delete(
        "DELETE FROM Media WHERE media_id = %s",
        (media_id,),
    )
    return rows > 0

def remove_series(series_id: int) -> bool:
    rows = execute_delete(
        "DELETE FROM Series WHERE series_id = %s",
        (series_id,),
    )
    return rows > 0

def remove_film(film_id: int) -> bool:
    rows = execute_delete(
        "DELETE FROM Film WHERE film_id = %s",
        (film_id,),
    )
    return rows > 0

def remove_session(session_id: int) -> bool:
    rows = execute_delete(
        "DELETE FROM Sessions WHERE session_id = %s",
        (session_id,),
    )
    return rows > 0

def remove_watch_history(user_id: int, media_id: int) -> bool:
    rows = execute_delete(
        """
        DELETE FROM WatchHistory
        WHERE user_id = %s AND media_id = %s
        """,
        (user_id, media_id),
    )
    return rows > 0

def remove_device(device_id: int, user_id: int) -> bool:
    rows = execute_delete(
        """
        DELETE FROM Device
        WHERE device_id = %s AND user_id = %s
        """,
        (device_id, user_id),
    )
    return rows > 0

# ---------------Updates-----------

def execute_update(sql: str, params: tuple) -> int:
    """
    Executes an UPDATE statement and returns number of affected rows.
    """
    with get_mariadb() as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                affected_rows = cursor.rowcount
            connection.commit()
            return affected_rows
        except Exception:
            connection.rollback()
            raise

def update_family(family: Family) -> bool:
    if family.family_id is None:
        raise ValueError("family_id is required for update")

    rows = execute_update(
        """
        UPDATE Family
        SET family_type = %s,
            creation_date = %s
        WHERE family_id = %s
        """,
        (
            family.family_type,
            family.creation_date,
            family.family_id,
        ),
    )
    return rows > 0

def update_user(user: User) -> bool:
    if user.user_id is None:
        raise ValueError("user_id is required for update")

    rows = execute_update(
        """
        UPDATE Users
        SET user_name = %s,
            email = %s,
            birthday = %s,
            location = %s,
            bio = %s,
            family_id = %s
        WHERE user_id = %s
        """,
        (
            user.user_name,
            user.email,
            user.birthday,
            user.location,
            user.bio,
            user.family_id,
            user.user_id,
        ),
    )
    return rows > 0

# No remove method for friendship

def update_media(media: Media) -> bool:
    if media.media_id is None:
        raise ValueError("media_id is required for update")

    rows = execute_update(
        """
        UPDATE Media
        SET media_name = %s,
            genre = %s,
            prod_year = %s,
            descr = %s,
            location = %s,
            cost_per_day = %s
        WHERE media_id = %s
        """,
        (
            media.media_name,
            media.genre,
            media.prod_year,
            media.descr,
            media.location,
            media.cost_per_day,
            media.media_id,
        ),
    )
    return rows > 0

def update_series(series: Series) -> bool:
    if series.series_id is None:
        raise ValueError("series_id is required for update")

    rows = execute_update(
        """
        UPDATE Series
        SET number_of_episodes = %s,
            is_ongoing = %s,
            media_id = %s
        WHERE series_id = %s
        """,
        (
            series.number_of_episodes,
            series.is_ongoing,
            series.media_id,
            series.series_id,
        ),
    )
    return rows > 0

def update_film(film: Film) -> bool:
    if film.film_id is None:
        raise ValueError("film_id is required for update")

    rows = execute_update(
        """
        UPDATE Film
        SET duration = %s,
            number_of_parts = %s,
            media_id = %s
        WHERE film_id = %s
        """,
        (
            film.duration,
            film.number_of_parts,
            film.media_id,
            film.film_id,
        ),
    )
    return rows > 0

def update_session(session: Session) -> bool:
    if session.session_id is None:
        raise ValueError("session_id is required for update")

    rows = execute_update(
        """
        UPDATE Sessions
        SET user_id = %s,
            media_id = %s,
            date_of_rent = %s,
            cost = %s,
            duration = %s
        WHERE session_id = %s
        """,
        (
            session.user_id,
            session.media_id,
            session.date_of_rent,
            session.cost,
            session.duration,
            session.session_id,
        ),
    )
    return rows > 0

def update_watch_history(history: WatchHistory) -> bool:
    if history.watch_history_id is None:
        raise ValueError("watch_history_id is required for update")
    rows = execute_update(
        """
        UPDATE WatchHistory
        SET date_of_watch = %s,
            family_watch = %s
        WHERE watch_history_id = %s AND user_id = %s AND media_id = %s
        """,
        (
            history.date_of_watch,
            history.family_watch,
            history.watch_history_id,
            history.user_id,
            history.media_id,
        ),
    )
    return rows > 0

def update_device(device: Device) -> bool:
    rows = execute_update(
        """
        UPDATE Device
        SET device_name = %s,
            registration_date = %s
        WHERE device_id = %s AND user_id = %s
        """,
        (
            device.device_name,
            device.registration_date,
            device.device_id,
            device.user_id,
        ),
    )
    return rows > 0

# --------------Find by id------------------

def execute_select_one(sql: str, params: tuple) -> dict | None:
    with get_mariadb() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()
        
def find_by_id(table: str, id_column: str, row_id: int) -> dict | None:
    sql = f"SELECT * FROM `{table}` WHERE `{id_column}` = %s"
    return execute_select_one(sql, (row_id,))


def find_user_by_id(user_id: int) -> User | None:
    row = find_by_id("Users", "user_id", user_id)
    return User.from_row(row) if row else None

def find_family_by_id(family_id: int) -> Family | None:
    row = find_by_id("Family", "family_id", family_id)
    return Family.from_row(row) if row else None

def find_media_by_id(media_id: int) -> Media | None:
    row = find_by_id("Media", "media_id", media_id)
    return Media.from_row(row) if row else None

def find_series_by_id(series_id: int) -> Series | None:
    row = find_by_id("Series", "series_id", series_id)
    return Series.from_row(row) if row else None

def find_film_by_id(film_id: int) -> Film | None:
    row = find_by_id("Film", "film_id", film_id)
    return Film.from_row(row) if row else None

def find_session_by_id(session_id: int) -> Session | None:
    row = find_by_id("Sessions", "session_id", session_id)
    return Session.from_row(row) if row else None

def find_device(device_id: int, user_id: int) -> Device | None:
    row = execute_select_one(
        """
        SELECT * FROM Device
        WHERE device_id = %s AND user_id = %s
        """,
        (device_id, user_id),
    )
    return Device.from_row(row) if row else None

def find_friendship(user_id: int, friend_id: int) -> Friendship | None:
    row = execute_select_one(
        """
        SELECT * FROM Friendships
        WHERE user_id = %s AND friend_id = %s
        """,
        (user_id, friend_id),
    )
    return Friendship.from_row(row) if row else None

def find_watch_history(watch_history_id: int, user_id: int, media_id: int) -> WatchHistory | None:
    row = execute_select_one(
        """
        SELECT * FROM WatchHistory
        WHERE watch_history_id = %s AND user_id = %s AND media_id = %s
        """,
        (watch_history_id, user_id, media_id),
    )
    return WatchHistory.from_row(row) if row else None
