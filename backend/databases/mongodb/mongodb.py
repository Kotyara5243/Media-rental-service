"""
MongoDB implementation for Media Rental Service
Using RAW PyMongo queries

NOSQL DESIGN DECISIONS:
1. DENORMALIZATION: Embed related data to eliminate JOINs
2. COLLECTIONS: users, media, sessions, watch_history
3. EMBEDDED DOCUMENTS: film/series details in media
4. N-SIDE REFERENCING: Store array of IDs for friends/devices
5. NO FOREIGN KEYS: Use embedded documents instead
"""

from datetime import datetime, date
from typing import List, Dict, Any, Optional
from .mongodb_connection import get_collection, list_all_collections
from bson.objectid import ObjectId
from ..mariadb import mariadb


COLLECTIONS = ['users', 'media', 'sessions', 'watch_history', 'families', 'counters']

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
    get_collection('families').create_index('families.family_id')
    
    print("All MongoDB collections reset")

def get_all_collections() :
    return list_all_collections()

def insert_user(user_name: str, email: str, birthday: datetime, location: str, 
                bio: str, family_id: int,
                devices: List[Dict] = None, friends: List[int] = None) -> int:
    """
    Insert user with EMBEDDED and devices.
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
        'family_id': family_id,
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

    for rental in rentals:
        rental['media_name'] = rental['media']['media_name']  # helper for the frontend
    
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
            'family_id': user['family_id']  
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

def insert_family(family_type: str, creation_date: datetime, users: List[Dict] = None, ) -> int:
    """
    Family with denormalized user data.
    """
    families = get_collection('families')
    
    family_id = get_next_sequence('watch_history_id')
  
    family_doc = {
        'family_id': family_id,
        'family_type': family_type,
        'users': users or [],
        'creation_date': creation_date,
        'created_at': datetime.now()
    }
    
    families.insert_one(family_doc)
    return family_id

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
        # family_type="Movie Lovers",
        # family_creation_date=datetime(2025, 11, 7),
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
        # family_type="Movie Lovers",
        # family_creation_date=datetime(2025, 11, 7),
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
        # family_type="Couple",
        # family_creation_date=datetime(2025, 10, 1),
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
    print(f"  - Users: 3 (with embedded devices)")
    print(f"  - Media: 3 (with embedded film/series details)")
    print(f"  - Sessions: 2 (with embedded user/media data)")
    print(f"  - Watch History: 2 (with embedded user/media data)")


def get_database_stats() -> Dict:
    """Get statistics about MongoDB collections"""
    collections = ['users', 'media', 'sessions', 'watch_history', 'families']
    stats = {}
    
    for coll_name in collections:
        collection = get_collection(coll_name)
        stats[coll_name] = collection.count_documents({})
    
    return stats


def get_sample_documents() -> Dict:

    samples = {}
    
    # Sample User (shows embedded devices and friends)
    user_sample = get_collection('users').find_one({}, {'_id': 0})
    if user_sample:
        samples['user'] = user_sample
    
    # Sample Media (shows polymorphic type with embedded type_details)
    media_sample = get_collection('media').find_one({}, {'_id': 0})
    if media_sample:
        samples['media'] = media_sample
    
    # Sample Session (shows denormalized user and media data)
    session_sample = get_collection('sessions').find_one({}, {'_id': 0})
    if session_sample:
        samples['session'] = session_sample
    
    # Sample Watch History (shows denormalized user and media data)
    watch_sample = get_collection('watch_history').find_one({}, {'_id': 0})
    if watch_sample:
        samples['watch_history'] = watch_sample
    
    # Sample Family (shows embedded users array)
    family_sample = get_collection('families').find_one({}, {'_id': 0})
    if family_sample:
        samples['family'] = family_sample
    
    return samples
