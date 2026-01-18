import os
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from typing import Dict, List

MONGODB_HOST = os.getenv('MONGODB_HOST', 'localhost')
MONGODB_PORT = int(os.getenv('MONGODB_PORT', '27017'))
MONGODB_USER = os.getenv('MONGODB_USER', 'root')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD', 'rootpassword')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'media_rental_db')

_client = None
_db = None

def get_mongodb_connection():

    global _client, _db
    
    if _client is None:
        try:
            connection_string = f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/"
            
            _client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        
            _client.admin.command('ping')
            
            _db = _client[MONGODB_DATABASE]
            
            print(f"Connected to MongoDB: {MONGODB_DATABASE}")
            
        except ConnectionFailure as e:
            print(f"Failed to connect to MongoDB: {e}")
            raise
    
    return _db

def close_mongodb_connection():
    global _client, _db
    if _client is not None:
        _client.close()
        _client = None
        _db = None
        print("MongoDB connection closed")

def get_collection(collection_name: str):
    db = get_mongodb_connection()
    return db[collection_name]

def list_all_collections() -> Dict[str, List[dict]]:
    db = get_mongodb_connection()
    result: Dict[str, List[dict]] = {}

    for name in db.list_collection_names():
        collection = db[name]
        result[name] = [
            {**doc, "_id": str(doc["_id"])}
            for doc in collection.find()
        ]

    return result
