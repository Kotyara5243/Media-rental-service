import os
import pymysql
from contextlib import contextmanager
from typing import Generator

def get_mariadb_connection():
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
def get_mariadb():
    """Context manager for database operations"""
    connection = get_mariadb_connection()
    try:
        yield connection
    finally:
        connection.close()
