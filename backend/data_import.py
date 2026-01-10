import pymysql
import os
from datetime import datetime

def add_sample_data():
    print("Adding sample data...")
    
    connection = pymysql.connect(
        host=os.getenv('MARIADB_HOST', 'mariadb'),
        user=os.getenv('MARIADB_USER', 'app_user'),
        password=os.getenv('MARIADB_PASSWORD', 'app_password'),
        database=os.getenv('MARIADB_DATABASE', 'media_rental_db'),
        port=3306,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            print("Clearing existing data...")
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

            # 1. Add one family 
            cursor.execute(
                "INSERT INTO Family (family_type, creation_date) VALUES (%s, %s)",
                ('Movie Fans', datetime.now().date())
            )
            family_id = cursor.lastrowid
            print(f"Added 1 family (ID: {family_id})")
            
            # 2. Add two users 
            # User 1: Zhami
            cursor.execute(
                """INSERT INTO Users (user_name, email, birthday, location, bio, family_id) 
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                ('Zhami', 'zhami@example.com', '2000-01-01', 'Vienna', 'Student', family_id)
            )
            user1_id = cursor.lastrowid
            
            # User 2: Grisha
            cursor.execute(
                """INSERT INTO Users (user_name, email, birthday, location, bio, family_id) 
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                ('Grisha', 'grisha@example.com', '1995-05-05', 'Moscow', 'Movie lover', None)
            )
            user2_id = cursor.lastrowid
            print(f"Added 2 users (IDs: {user1_id}, {user2_id})")
            
            # 3. Add one device for Zhami 
            cursor.execute(
                "INSERT INTO Device (device_name, registration_date, user_id) VALUES (%s, %s, %s)",
                ('iPhone', datetime.now().date(), user1_id)
            )
            print("Added 1 device for Zhami")
            
            # 4. Add one film (Inception - same as your ORM data)
            cursor.execute(
                """INSERT INTO Media (media_name, genre, prod_year, descr, location, cost_per_day) 
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                ('Inception', 'Sci-Fi', 2010, 'A film about dreams', 'USA', 5)
            )
            media_id = cursor.lastrowid
            
            # Add to Film table
            cursor.execute(
                "INSERT INTO Film (duration, number_of_parts, media_id) VALUES (%s, %s, %s)",
                (148, 1, media_id)
            )
            print(f"Added 1 film: Inception (Media ID: {media_id})")
            
            # 5. Add one rental session 
            cursor.execute(
                """INSERT INTO Sessions (user_id, media_id, date_of_rent, cost, duration) 
                   VALUES (%s, %s, %s, %s, %s)""",
                (user1_id, media_id, datetime.now().date(), 10, '02:00:00')
            )
            print("Added 1 rental session (Zhami rents Inception)")
            
            connection.commit()
            print("Done! Data added successfully.")
            return True
            
    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
        return False
    finally:
        connection.close()


if __name__ == "__main__":
    add_sample_data()
