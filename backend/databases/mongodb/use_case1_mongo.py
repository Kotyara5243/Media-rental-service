
from datetime import datetime
from typing import List, Dict
from .mongodb_connection import get_collection

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
