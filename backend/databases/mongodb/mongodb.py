"""
MongoDB implementation for Media Rental Service
Using RAW PyMongo queries

NOSQL DESIGN DECISIONS:
1. DENORMALIZATION: Embed related data to eliminate JOINs
2. COLLECTIONS: users, media, sessions, watch_history
3. EMBEDDED DOCUMENTS: family data in users, film/series details in media
4. N-SIDE REFERENCING: Store array of IDs for friends/devices
5. NO FOREIGN KEYS: Use embedded documents instead
"""

from collections import defaultdict
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from .mongodb_connection import get_collection, list_all_collections
from bson.objectid import ObjectId
from ..mariadb import mariadb


COLLECTIONS = ['users', 'media', 'sessions', 'watch_history', 'counters']

def convert_dates_to_datetime(obj: Any) -> Any:
    """
    Recursively convert all datetime.date objects to datetime.datetime objects.
    PyMongo cannot serialize date objects - only datetime objects.
    """
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return datetime.combine(obj, datetime.min.time())
    elif isinstance(obj, dict):
        return {key: convert_dates_to_datetime(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates_to_datetime(item) for item in obj]
    return obj

def get_next_sequence(sequence_name: str) -> int:
    """
    Get next sequential ID for a collection.
    Simulates SQL AUTO_INCREMENT in MongoDB.
    """
    counters = get_collection('counters')
    result = counters.find_one_and_update(
        {'_id': sequence_name},
        {'$inc': {'seq': 1}},
        upsert=True,
        return_document=True
    )
    return result['seq']


def reset_all_collections():
    for coll_name in COLLECTIONS:
        collection = get_collection(coll_name)
        collection.drop()
  
    get_collection('users').create_index('user_id', unique=True)
    get_collection('media').create_index('media_id', unique=True)
    get_collection('sessions').create_index('session_id', unique=True)
    get_collection('sessions').create_index('user.user_id')
    get_collection('watch_history').create_index('user.user_id')
    # get_collection('family').create_index('user.user_id')
    
    print("All MongoDB collections reset")

def get_all_collections() :
    return list_all_collections()

def insert_user(user_name: str, email: str, birthday: datetime, location: str, 
                bio: str, family_id: int, family_type: str, family_creation_date: datetime,
                devices: List[Dict] = None, friends: List[int] = None) -> int:
    """
    Insert user with EMBEDDED family data and devices.
    NO foreign keys - denormalized design.
    
    Returns: user_id
    """
    users = get_collection('users')
    user_id = get_next_sequence('user_id')
    
    user_doc = {
        'user_id': user_id,
        'user_name': user_name,
        'email': email,
        'birthday': birthday,
        'location': location,
        'bio': bio,
        'family': { 
            'family_id': family_id,
            'family_type': family_type,
            'creation_date': family_creation_date
        },
        'devices': devices or [],  
        'friends': friends or [],  
        'created_at': datetime.now()
    }
    
    users.insert_one(user_doc)
    return user_id


def get_user_by_id(user_id: int) -> Optional[Dict]:
    users = get_collection('users')
    return users.find_one({'user_id': user_id}, {'_id': 0})


def get_all_users() -> List[Dict]:
    users = get_collection('users')
    return list(users.find({}, {'_id': 0}).sort('user_name', 1))


def insert_media(media_name: str, genre: str, prod_year: int, descr: str,
                 location: str, cost_per_day: int, media_type: str,
                 type_details: Dict) -> int:
    """
    Insert media with EMBEDDED film/series details.
    Returns: media_id
    """
    media = get_collection('media')
    media_id = get_next_sequence('media_id')
    
    media_doc = {
        'media_id': media_id,
        'media_name': media_name,
        'genre': genre,
        'prod_year': prod_year,
        'description': descr,
        'location': location,
        'cost_per_day': cost_per_day,
        'type': media_type,  
        'type_details': type_details,  
        'created_at': datetime.now()
    }
    
    media.insert_one(media_doc)
    return media_id


def get_media_by_id(media_id: int) -> Optional[Dict]:
    media = get_collection('media')
    return media.find_one({'media_id': media_id}, {'_id': 0})


def get_all_media() -> List[Dict]:
    media = get_collection('media')
    return list(media.find({}, {'_id': 0}).sort('media_name', 1))


# USE CASE 2: RENT MEDIA (SESSIONS)


def insert_rental_session(user_id: int, media_id: int, duration: int) -> Dict:
    """
    Create rental session with DENORMALIZED user and media data.
    Returns: Complete session document
    """
    sessions = get_collection('sessions')
    
    user = get_user_by_id(user_id)
    if not user:
        raise ValueError(f"User {user_id} not found")
    
    media = get_media_by_id(media_id)
    if not media:
        raise ValueError(f"Media {media_id} not found")
    
    cost = media['cost_per_day'] * duration
    
    session_id = get_next_sequence('session_id')
    
    session_doc = {
        'session_id': session_id,
        'user': {  
            'user_id': user['user_id'],
            'user_name': user['user_name'],
            'email': user['email']
        },
        'media': {  
            'media_id': media['media_id'],
            'media_name': media['media_name'],
            'genre': media['genre'],
            'type': media['type'],
            'cost_per_day': media['cost_per_day']
        },
        'date_of_rent': datetime.now(),
        'cost': cost,
        'duration': duration,
        'created_at': datetime.now()
    }
    
    sessions.insert_one(session_doc)
    
    session_doc.pop('_id', None)
    return session_doc


def get_user_rentals(user_id: int) -> List[Dict]:

    sessions = get_collection('sessions')
    
    # Single query - all data already embedded
    rentals = list(sessions.find(
        {'user.user_id': user_id},
        {'_id': 0}
    ).sort('date_of_rent', -1))
    
    return rentals


def get_all_sessions() -> List[Dict]:
    sessions = get_collection('sessions')
    return list(sessions.find({}, {'_id': 0}).sort('date_of_rent', -1))

# USE CASE 1: WATCH MEDIA (WATCH HISTORY)


def insert_watch_history(user_id: int, media_id: int, family_watch: bool) -> int:
    """
    Record media watch event with DENORMALIZED user and media data.
    """
    watch_history = get_collection('watch_history')
    
    user = get_user_by_id(user_id)
    if not user:
        raise ValueError(f"User {user_id} not found")
  
    media = get_media_by_id(media_id)
    if not media:
        raise ValueError(f"Media {media_id} not found")
    
    watch_id = get_next_sequence('watch_history_id')
  
    watch_doc = {
        'watch_history_id': watch_id,
        'user': {  
            'user_id': user['user_id'],
            'user_name': user['user_name'],
            'family': user['family']  
        },
        'media': {  
            'media_id': media['media_id'],
            'media_name': media['media_name'],
            'type': media['type']
        },
        'date_of_watch': datetime.now(),
        'family_watch': family_watch,
        'created_at': datetime.now()
    }
    
    watch_history.insert_one(watch_doc)
    return watch_id


def get_family_shared_media(user_id: int) -> List[Dict]:
    
    users = get_collection('users')
    sessions = get_collection('sessions')
    
    user = users.find_one({'user_id': user_id}, {'family.family_id': 1})
    if not user:
        return []
    
    family_id = user['family']['family_id']
    
    users = get_collection('users')
    sessions = get_collection('sessions')
    
    user = users.find_one({'user_id': user_id}, {'family.family_id': 1})
    if not user:
        return []
    
    family_id = user['family']['family_id']
    
    family_members = list(users.find(
        {'family.family_id': family_id, 'user_id': {'$ne': user_id}},
        {'user_id': 1, 'user_name': 1}
    ))
    
    family_member_ids = [m['user_id'] for m in family_members]
    member_names = {m['user_id']: m['user_name'] for m in family_members}

    current_time = datetime.now()
    
    result = []
    active_sessions = sessions.find({
        'user.user_id': {'$in': family_member_ids}
    }, {'_id': 0})
    
    for session in active_sessions:
        rental_end = session['date_of_rent']
        from datetime import timedelta
        rental_end = rental_end + timedelta(hours=session['duration'])
        
        if rental_end > current_time:
            result.append({
                'family_member': member_names.get(session['user']['user_id']),
                'media_id': session['media']['media_id'],
                'media_name': session['media']['media_name'],
                'type': session['media']['type']
            })
    
    return result


# DATA GENERATION FOR TESTING


def generate_sample_data():
    reset_all_collections()
    
    # Insert users with embedded family data
    user1 = insert_user(
        user_name="Zhami",
        email="zhami@example.com",
        birthday=datetime(2002, 11, 22),
        location="Almaty",
        bio="Film enthusiast",
        family_id=1,
        family_type="Movie Lovers",
        family_creation_date=datetime(2025, 11, 7),
        devices=[
            {'device_id': 1, 'device_name': 'iPhone 15', 'registration_date': datetime(2025, 1, 1)},
            {'device_id': 2, 'device_name': 'MacBook Pro', 'registration_date': datetime(2025, 2, 1)}
        ],
        friends=[2, 3]
    )
    
    user2 = insert_user(
        user_name="Grisha",
        email="grisha@example.com",
        birthday=datetime(1990, 5, 10),
        location="Moscow",
        bio="Loves Sci-Fi movies",
        family_id=1,
        family_type="Movie Lovers",
        family_creation_date=datetime(2025, 11, 7),
        devices=[{'device_id': 3, 'device_name': 'Samsung Galaxy', 'registration_date': datetime(2025, 3, 1)}],
        friends=[1]
    )
    
    user3 = insert_user(
        user_name="Alice",
        email="alice@example.com",
        birthday=datetime(1995, 8, 15),
        location="Berlin",
        bio="Drama fan",
        family_id=2,
        family_type="Couple",
        family_creation_date=datetime(2025, 10, 1),
        devices=[],
        friends=[1, 2]
    )
    
    # Insert media with embedded film/series details
    media1 = insert_media(
        media_name="Inception",
        genre="Sci-Fi",
        prod_year=2010,
        descr="Dream-sharing technology",
        location="USA",
        cost_per_day=5,
        media_type="film",
        type_details={'duration': 148, 'number_of_parts': 1}
    )
    
    media2 = insert_media(
        media_name="Breaking Bad",
        genre="Crime Drama",
        prod_year=2008,
        descr="Chemistry teacher makes drugs",
        location="USA",
        cost_per_day=3,
        media_type="series",
        type_details={'number_of_episodes': 62, 'is_ongoing': False}
    )
    
    media3 = insert_media(
        media_name="The Matrix",
        genre="Sci-Fi",
        prod_year=1999,
        descr="Reality is not what it seems",
        location="USA",
        cost_per_day=4,
        media_type="film",
        type_details={'duration': 136, 'number_of_parts': 1}
    )
    
    insert_rental_session(user1, media1, duration=3)
    insert_rental_session(user2, media2, duration=5)
    
    insert_watch_history(user1, media1, family_watch=True)
    insert_watch_history(user2, media2, family_watch=False)
    
    print("✓ Sample data generated in MongoDB")
    print(f"  - Users: 3 (with embedded family, devices)")
    print(f"  - Media: 3 (with embedded film/series details)")
    print(f"  - Sessions: 2 (with embedded user/media data)")
    print(f"  - Watch History: 2 (with embedded user/media data)")


def get_database_stats() -> Dict:
    """Get statistics about MongoDB collections"""
    collections = ['users', 'media', 'sessions', 'watch_history']
    stats = {}
    
    for coll_name in collections:
        collection = get_collection(coll_name)
        stats[coll_name] = collection.count_documents({})
    
    return stats


# DATA MIGRATION FROM SQL TO MONGODB

def migrate_from_sql() -> Dict[str, Any]:
    """Migrate all data from SQL to MongoDB with denormalization."""
    try:
        reset_all_collections()
        print("\n" + "="*60)
        print("MIGRATING DATA FROM SQL TO MONGODB")
        print("="*60)
        
        users = _migrate_users()
        print(f" Users migrated: {users}")
        
        media = _migrate_media()
        print(f" Media migrated: {media}")
        
        sessions = _migrate_sessions()
        print(f" Sessions migrated: {sessions}")
        
        watch = _migrate_watch_history()
        print(f" WatchHistory migrated: {watch}")
        
        print("="*60 + "\n")
        
        return {
            "users": users,
            "media": media,
            "sessions": sessions,
            "watch_history": watch
        }
    except Exception as e:
        print(f"\n MIGRATION ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def _migrate_users() -> int:
    try:
        users_coll = get_collection('users')
        
        families_data = mariadb.execute_select("SELECT * FROM Family", ())
        print(f"  [Users] Found {len(families_data)} families")
        families = {f['family_id']: convert_dates_to_datetime(f) for f in families_data}
        
        devices_data = mariadb.execute_select("SELECT * FROM Device", ())
        print(f"  [Users] Found {len(devices_data)} devices")
        devices_by_user = {}
        for d in devices_data:
            d = convert_dates_to_datetime(d)
            uid = d['user_id']
            if uid not in devices_by_user:
                devices_by_user[uid] = []
            devices_by_user[uid].append({'device_id': d['device_id'], 'device_name': d['device_name']})
        
        friends_data = mariadb.execute_select("SELECT * FROM Friendships", ())
        print(f"  [Users] Found {len(friends_data)} friendships")
        friends_by_user = {}
        for f in friends_data:
            uid = f['user_id']
            if uid not in friends_by_user:
                friends_by_user[uid] = []
            friends_by_user[uid].append(f['friend_id'])
        
        users_data = mariadb.execute_select("SELECT * FROM Users", ())
        print(f"  [Users] Found {len(users_data)} users to migrate")
        
        count = 0
        for u in users_data:
            u = convert_dates_to_datetime(u)
            fam = families.get(u['family_id'], {})
            users_coll.insert_one({
                'user_id': u['user_id'],
                'user_name': u['user_name'],
                'email': u['email'],
                'birthday': u['birthday'],
                'location': u['location'],
                'bio': u['bio'],
                'family': {'family_id': fam.get('family_id'), 'family_type': fam.get('family_type')},
                'devices': devices_by_user.get(u['user_id'], []),
                'friends': friends_by_user.get(u['user_id'], [])
            })
            count += 1
        
        get_collection('counters').update_one({'_id': 'user_id'}, {'$set': {'seq': count}}, upsert=True)
        return count
    except Exception as e:
        print(f"   [Users] Migration error: {str(e)}")
        raise


def _migrate_media() -> int:
    try:
        media_coll = get_collection('media')
        count = 0
        migrated_ids = set()
        
        # Migrate films
        films_data = mariadb.execute_select(
            "SELECT m.*, f.film_id, f.duration, f.number_of_parts FROM Media m "
            "LEFT JOIN Film f ON m.media_id = f.media_id WHERE f.film_id IS NOT NULL", ())
        print(f"  [Media] Found {len(films_data)} films")
        
        for m in films_data:
            migrated_ids.add(m['media_id'])
            media_coll.insert_one({
                'media_id': m['media_id'],
                'media_name': m['media_name'],
                'genre': m['genre'],
                'prod_year': m['prod_year'],
                'description': m['descr'],
                'location': m['location'],
                'cost_per_day': m['cost_per_day'],
                'type': 'film',
                'type_details': {'duration': m.get('duration'), 'number_of_parts': m.get('number_of_parts')}
            })
            count += 1
        
        # Migrate series
        series_data = mariadb.execute_select(
            "SELECT m.*, s.series_id, s.number_of_episodes, s.is_ongoing FROM Media m "
            "LEFT JOIN Series s ON m.media_id = s.media_id WHERE s.series_id IS NOT NULL", ())
        print(f"  [Media] Found {len(series_data)} series")
        
        for m in series_data:
            if m['media_id'] not in migrated_ids:
                media_coll.insert_one({
                    'media_id': m['media_id'],
                    'media_name': m['media_name'],
                    'genre': m['genre'],
                    'prod_year': m['prod_year'],
                    'description': m['descr'],
                    'location': m['location'],
                    'cost_per_day': m['cost_per_day'],
                    'type': 'series',
                    'type_details': {'number_of_episodes': m.get('number_of_episodes'), 'is_ongoing': m.get('is_ongoing')}
                })
                count += 1
        
        get_collection('counters').update_one({'_id': 'media_id'}, {'$set': {'seq': count}}, upsert=True)
        return count
    except Exception as e:
        print(f"   [Media] Migration error: {str(e)}")
        raise


def _migrate_sessions() -> int:
    try:
        sessions_coll = get_collection('sessions')
        count = 0
        
        sessions_data = mariadb.execute_select(
            "SELECT s.*, u.user_name, u.email, m.media_name, m.genre, m.cost_per_day, "
            "CASE WHEN f.film_id IS NOT NULL THEN 'film' WHEN se.series_id IS NOT NULL THEN 'series' ELSE 'unknown' END as media_type "
            "FROM Sessions s "
            "JOIN Users u ON s.user_id = u.user_id "
            "JOIN Media m ON s.media_id = m.media_id "
            "LEFT JOIN Film f ON m.media_id = f.media_id "
            "LEFT JOIN Series se ON m.media_id = se.media_id", ())
        print(f"  [Sessions] Found {len(sessions_data)} sessions")
        
        for s in sessions_data:
            s = convert_dates_to_datetime(s)
            sessions_coll.insert_one({
                'session_id': s['session_id'],
                'user': {'user_id': s['user_id'], 'user_name': s['user_name'], 'email': s['email']},
                'media': {
                    'media_id': s['media_id'], 
                    'media_name': s['media_name'], 
                    'genre': s['genre'], 
                    'type': s['media_type'],
                    'cost_per_day': s['cost_per_day']
                },
                'date_of_rent': s['date_of_rent'],
                'cost': s['cost'],
                'duration': s['duration']
            })
            count += 1
        
        get_collection('counters').update_one({'_id': 'session_id'}, {'$set': {'seq': count}}, upsert=True)
        return count
    except Exception as e:
        print(f"   [Sessions] Migration error: {str(e)}")
        raise


def _migrate_watch_history() -> int:
    try:
        watch_coll = get_collection('watch_history')
        count = 0
        
        watch_data = mariadb.execute_select(
            "SELECT w.*, u.user_name, u.family_id, m.media_name, "
            "CASE WHEN f.film_id IS NOT NULL THEN 'film' WHEN s.series_id IS NOT NULL THEN 'series' ELSE 'unknown' END as media_type "
            "FROM WatchHistory w "
            "JOIN Users u ON w.user_id = u.user_id "
            "JOIN Media m ON w.media_id = m.media_id "
            "LEFT JOIN Film f ON m.media_id = f.media_id "
            "LEFT JOIN Series s ON m.media_id = s.media_id", ())
        print(f"  [WatchHistory] Found {len(watch_data)} watch history records")
        
        for w in watch_data:
            w = convert_dates_to_datetime(w)
            watch_coll.insert_one({
                'watch_history_id': w['watch_history_id'],
                'user': {'user_id': w['user_id'], 'user_name': w['user_name'], 'family_id': w['family_id']},
                'media': {'media_id': w['media_id'], 'media_name': w['media_name'], 'type': w['media_type']},
                'date_of_watch': w['date_of_watch'],
                'family_watch': w['family_watch']
            })
            count += 1
        
        get_collection('counters').update_one({'_id': 'watch_history_id'}, {'$set': {'seq': count}}, upsert=True)
        return count
    except Exception as e:
        print(f"   [WatchHistory] Migration error: {str(e)}")
        raise
