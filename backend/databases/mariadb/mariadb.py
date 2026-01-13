from ..models import *
from .mariadb_connection import get_mariadb
from datetime import datetime

# --------Common functions----------
def delete_all_tables() :
    with get_mariadb() as connection :
        try :
            with connection.cursor() as cursor :
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute("DELETE FROM WatchHistory")
                cursor.execute("DELETE FROM Sessions")
                cursor.execute("DELETE FROM Device")
                cursor.execute("DELETE FROM Friendships")
                cursor.execute("DELETE FROM Film")
                cursor.execute("DELETE FROM Series")
                cursor.execute("DELETE FROM Media")
                cursor.execute("DELETE FROM Users")
                cursor.execute("DELETE FROM Family")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            connection.commit()
        except Exception as e:
            print(f"Error: {e}")
            connection.rollback()
            return False
        
def test_db() :
    with get_mariadb() as connection:
        try:
            with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result
        except Exception as e:
            connection.rollback()
            raise e
        
def __execute_select(sql: str, params: tuple = ()) -> list[dict]:
    with get_mariadb() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

def __list_tables() -> list[str]:
    rows = __execute_select("SHOW TABLES")
    # MariaDB returns column name like: Tables_in_<database>
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

    tables = __list_tables()

    for table in tables:
        rows = __execute_select(f"SELECT * FROM `{table}`")
        result[table] = rows

    return result


# def list_tables() :
#     with get_mariadb() as connection:
#         try:
#             with connection.cursor() as cursor:
#                 cursor.execute("SHOW TABLES")
#                 tables = [row[0] for row in cursor.fetchall()]
#                 return tables
#         except Exception:
#             connection.rollback()
#             raise

def add_sample_data() :
    with get_mariadb() as connection:
        try:
            print("Clearing existing data...")
            delete_all_tables()


            # 1. Add one family
            family = Family(None, 'Movie Fans', datetime.now().date())
            insert_family(family)
            print(f"Added 1 family (ID: {family.family_id})")
            
            # 2. Add two users 
            # User 1: Zhami
            zhami = User(None, 'Zhami', 'zhami@example.com', '2000-01-01', 'Vienna', 'Student', family.family_id)
            insert_user(zhami)
            
            # User 2: Grisha
            grisha = User(None, 'Grisha', 'grisha@example.com', '1995-05-05', 'Moscow', 'Movie lover', None)
            insert_user(grisha)

            print(f"Added 2 users (IDs: {zhami.user_id}, {grisha.user_id})")
            
            # 3. Add one device for Zhami 
            device = Device(None, 'iPhone', datetime.now().date(), zhami.user_id)
            insert_device(device)
            print("Added 1 device for Zhami")
            
            # 4. Add one film
            media = Media(None, 'Inception', 'Sci-Fi', 2010, 'A film about dreams', 'USA', 5)
            insert_media(media)
            
            # Add to Film table
            film = Film(None, 148, 1, media.media_id)
            insert_film(film)
            print(f"Added 1 film: Inception (Media ID: {media.media_id})")
            
            # 5. Add one rental session 
            session = Session(None, zhami.user_id, media.media_id, datetime.now().date(), 10, '02:00:00')
            insert_session(session)
            print("Added 1 rental session (Zhami rents Inception)")

            __list_tables()
        except Exception:
            connection.rollback()
            raise

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
    session.session_id = (
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
    device.device_id = (
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
    rows = execute_update(
        """
        UPDATE WatchHistory
        SET date_of_watch = %s,
            family_watch = %s
        WHERE user_id = %s AND media_id = %s
        """,
        (
            history.date_of_watch,
            history.family_watch,
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
