from typing import Dict, Any
from .mongodb_connection import get_collection
from ..mariadb import mariadb
from .mongodb import convert_dates_to_datetime, reset_all_collections

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

        families = _migrate_family()
        print(f" Families migrated: {families}")
        
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
            print(d)
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
            users_coll.insert_one({
                'user_id': u['user_id'],
                'user_name': u['user_name'],
                'email': u['email'],
                'birthday': u['birthday'],
                'location': u['location'],
                'bio': u['bio'],
                'family_id': u['family_id'],
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

def _migrate_family() -> int:
    try:
        families_coll = get_collection('families')
        count = 0
        
        families_data = mariadb.execute_select("SELECT * FROM Family", ())
        print(f"  [Families] Found {len(families_data)} families")

        user_data = mariadb.execute_select("SELECT * FROM Users", ())
        print(f"  [Families] Found {len(user_data)} users")
        users_by_family = {}
        for u in user_data:
            uid = u['family_id']
            if uid not in users_by_family:
                users_by_family[uid] = []
            users_by_family[uid].append({'user_id': u['user_id'], 'user_name': u['user_name'], 'email': u['email']})
        
        for f in families_data:
            f = convert_dates_to_datetime(f)
            families_coll.insert_one({
                'family_id': f['family_id'],
                'family_type': f['family_type'],
                'users': users_by_family.get(f['family_id'], []),
                'creation_date': f['creation_date']
            })
            count += 1
        
        get_collection('counters').update_one({'_id': 'family_id'}, {'$set': {'seq': count}}, upsert=True)
        return count
    except Exception as e:
        print(f"   [Families] Migration error: {str(e)}")
        raise