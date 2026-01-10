import os
import pymysql
from contextlib import contextmanager
from typing import Generator

def get_db_connection():
    connection = pymysql.connect(
        host=os.getenv('MARIADB_HOST', 'mariadb'),
        user=os.getenv('MARIADB_USER', 'app_user'),
        password=os.getenv('MARIADB_PASSWORD', 'app_password'),
        database=os.getenv('MARIADB_DATABASE', 'media_rental_db'),
        port=3306,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )
    return connection

@contextmanager
def get_db():
    """Context manager for database operations (similar to SQLAlchemy Session)"""
    connection = get_db_connection()
    try:
        yield connection
    finally:
        connection.close()
