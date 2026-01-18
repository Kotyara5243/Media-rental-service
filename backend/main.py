from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from pydantic import BaseModel

from .databases.mariadb import mariadb
from .databases.mariadb.data_generator import generate_random_data
from .databases.mariadb.usecase1 import use_case1 as uc1_data_gen
from .databases.mariadb.usecase2 import use_case2 as uc2_logic
from .databases.mongodb import mongodb as mongo

app = FastAPI(title="Media Rental Service", version="1.0.0") 

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/")
async def read_root():
    return FileResponse("frontend/index.html")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "media-rental-api"}

@app.get("/api/test")
async def test_endpoint():
    return {"message": "Media Rental API is working!"}

@app.get("/api/test-db")
async def test_database():
    try:
        result = mariadb.test_db()
        
        return {
            "db_status": "Connected to MariaDB",
            "test_query": "SELECT 1",
            "result": result.get("1") if result else None
        }
    except Exception as e:
        print(f"Error in test_database: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "DB_TEST_FAILED",
                "message": "Failed to execute test query on MariaDB"
            }
        )
    
@app.get("/api/test-error")
async def test_error():
    raise HTTPException(
            status_code=500,
            detail={
                "code": "TEST_ERROR",
                "message": "This is a test error"
            }
        )

@app.post("/api/tables/clear")
async def clear_tables():
    try:
        mariadb.reset_all_tables()
        
        return {
            "Tables deleted successfully."
        }
    except Exception as e:
        print("Error in clear_tables: "+str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "code": "CLEAR_DATABASE_ERROR",
                "message": "Failed to clear database"
            }
        )

@app.get("/api/tables")
async def list_tables():
    try:
        tables = mariadb.list_all_tables_with_rows()
        
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        print(f"Error in list_tables: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "LIST_TABLES_FAILED",
                "message": "Failed to list tables"
            }
        )

@app.post("/api/add-data")
async def add_data():
    try:
        mariadb.add_sample_data()
        return {"message": "Sample data added successfully"}
    except Exception as e:
        print(f"Error in add_data: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "ADD_DATA_FAILED",
                "message": "Failed to add sample data"
            }
        )

        
@app.post("/api/generate-data")
async def generate_data():
    try:
        mariadb.reset_all_tables()
        generate_random_data()
        return {"message": "Sample data added successfully"}
    except Exception as e:
        print(f"Error in generate_data: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "GENERATE_DATA_FAILED",
                "message": "Failed to generate random data"
            }
        )

@app.get("/api/check-data")
async def check_data():
    """Lightweight data sanity check: returns table counts."""
    try:
        import pymysql
        connection = pymysql.connect(
            host=os.getenv('MARIADB_HOST', 'mariadb'),
            user=os.getenv('MARIADB_USER', 'app_user'),
            password=os.getenv('MARIADB_PASSWORD', 'app_password'),
            database=os.getenv('MARIADB_DATABASE', 'media_rental_db'),
            port=3306,
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM Users")
            total_users = cursor.fetchone()['count']

            cursor.execute("SELECT COUNT(*) as count FROM Media")
            total_media = cursor.fetchone()['count']

            cursor.execute("SELECT COUNT(*) as count FROM Sessions")
            total_rentals = cursor.fetchone()['count']

            cursor.execute("SELECT COUNT(*) as count FROM WatchHistory")
            total_watch_history = cursor.fetchone()['count']

        connection.close()

        return {
            "total_users": total_users,
            "total_media": total_media,
            "total_rentals": total_rentals,
            "total_watch_history": total_watch_history
        }

    except Exception as e:
        print(f"Error in check_data: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "CHECK_DATA_FAILED",
                "message": "Failed to check data"
            }
        )


 # Use Case 1
@app.get("/api/usecase1/load-data")
async def uc1_load_data() :
    data = uc1_data_gen.load_data()
    return data

class WatchRequest(BaseModel):
    user_id: int
    media_id: int

@app.post("/api/usecase1/watch")
async def uc1_watch_media(request: WatchRequest):
    try:
        uc1_data_gen.watch_media(request.user_id, request.media_id)
        return {"message": "Media watched successfully"}
    except Exception as e:
        print("Error in generate_data: "+str(e))
        raise HTTPException(status_code=500, detail="Error generating data")
    


# Use Case 2: Rent Media

@app.post("/api/usecase2/rent")
async def uc2_rent_media(user_id: int, media_id: int, duration_days: int):
    try:
        return uc2_logic.rent_media(user_id, media_id, duration_days)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error in uc2_rent_media: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "UC2_RENT_FAILED",
                "message": "Failed to rent media"
            }
        )


@app.get("/api/usecase2/media")
async def uc2_get_media():
    try:
        media = uc2_logic.get_all_media()
        return {"media": media, "count": len(media)}
    except Exception as e:
        print(f"Error in uc2_get_media: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "UC2_MEDIA_FAILED",
                "message": "Failed to fetch media"
            }
        )


@app.get("/api/usecase2/user/{user_id}/rentals")
async def uc2_get_user_rentals(user_id: int):
    try:
        rentals = uc2_logic.get_user_rentals(user_id)
        return {"rentals": rentals, "count": len(rentals)}
    except Exception as e:
        print(f"Error in uc2_get_user_rentals: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "UC2_RENTALS_FAILED",
                "message": "Failed to fetch rentals"
            }
        )


@app.get("/api/usecase2/users")
async def uc2_get_users():
    try:
        users = uc2_logic.get_all_users()
        return {"users": users, "count": len(users)}
    except Exception as e:
        print(f"Error in uc2_get_users: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "UC2_USERS_FAILED",
                "message": "Failed to fetch users"
            }
        )

# Migration Endpoints
@app.post("/api/migrate-to-nosql")
async def migrate_to_nosql():
    try:
        mongo.migrate_from_sql()
        return {"message": "Migration to NoSQL completed successfully"}
    except Exception as e:
        print(f"Error in migrate_to_nosql: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MIGRATION_FAILED",
                "message": "Failed to migrate data to NoSQL"
            }
        )
    
@app.post("/api/switch-to-sql")
async def switch_to_sql():
    try:
        mongo.reset_all_collections()
        return {"message": "Switched back to SQL database successfully"}
    except Exception as e:
        print(f"Error in switch_to_sql: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "SWITCH_SQL_FAILED",
                "message": "Failed to switch back to SQL database"
            }
        )


# MONGODB ENDPOINTS (NoSQL Implementation)

@app.get("/api/mongodb/test")
async def test_mongodb():
    try:
        from .databases.mongodb.mongodb_connection import get_mongodb_connection
        db = get_mongodb_connection()
        db.command('ping')
        return {
            "status": "Connected to MongoDB",
            "database": db.name
        }
    except Exception as e:
        print(f"Error in test_mongodb: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_CONNECTION_FAILED",
                "message": str(e)
            }
        )


@app.post("/api/mongodb/generate-data")
async def mongodb_generate_data():
    try:
        mongo.generate_sample_data()
        stats = mongo.get_database_stats()
        return {
            "message": "MongoDB sample data generated successfully",
            "stats": stats
        }
    except Exception as e:
        print(f"Error in mongodb_generate_data: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_GENERATE_FAILED",
                "message": str(e)
            }
        )


@app.get("/api/mongodb/stats")
async def mongodb_stats():
    try:
        stats = mongo.get_database_stats()
        return {"stats": stats}
    except Exception as e:
        print(f"Error in mongodb_stats: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_STATS_FAILED",
                "message": str(e)
            }
        )
    
@app.get("/api/mongodb/collections")
async def mongodb_list_collections():
    try:
        collections = mongo.get_all_collections()
        return {"collections": collections, "count": len(collections)}
    except Exception as e:
        print(f"Error in mongodb_list_collections: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_LIST_FAILED",
                "message": str(e)
            }
        )


@app.post("/api/mongodb/clear")
async def mongodb_clear():
    try:
        mongo.reset_all_collections()
        return {"message": "MongoDB collections cleared successfully"}
    except Exception as e:
        print(f"Error in mongodb_clear: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_CLEAR_FAILED",
                "message": str(e)
            }
        )


# MongoDB Use Case 2: Rent Media
@app.post("/api/mongodb/usecase2/rent")
async def mongodb_uc2_rent(user_id: int, media_id: int, duration_days: int):
    try:
        session = mongo.insert_rental_session(user_id, media_id, duration_days)
        return session
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error in mongodb_uc2_rent: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_RENT_FAILED",
                "message": str(e)
            }
        )


@app.get("/api/mongodb/usecase2/media")
async def mongodb_uc2_get_media():
    try:
        media = mongo.get_all_media()
        return {"media": media, "count": len(media)}
    except Exception as e:
        print(f"Error in mongodb_uc2_get_media: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_MEDIA_FAILED",
                "message": str(e)
            }
        )


@app.get("/api/mongodb/usecase2/users")
async def mongodb_uc2_get_users():
    try:
        users = mongo.get_all_users()
        return {"users": users, "count": len(users)}
    except Exception as e:
        print(f"Error in mongodb_uc2_get_users: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_USERS_FAILED",
                "message": str(e)
            }
        )


@app.get("/api/mongodb/usecase2/user/{user_id}/rentals")
async def mongodb_uc2_get_rentals(user_id: int):
    try:
        rentals = mongo.get_user_rentals(user_id)
        return {"rentals": rentals, "count": len(rentals)}
    except Exception as e:
        print(f"Error in mongodb_uc2_get_rentals: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_RENTALS_FAILED",
                "message": str(e)
            }
        )


# MongoDB Use Case 1: Watch Media
@app.post("/api/mongodb/usecase1/watch")
async def mongodb_uc1_watch(user_id: int, media_id: int, family_watch: bool = True):
    try:
        watch_id = mongo.insert_watch_history(user_id, media_id, family_watch)
        return {"message": "Watch history recorded", "watch_id": watch_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error in mongodb_uc1_watch: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_WATCH_FAILED",
                "message": str(e)
            }
        )


@app.get("/api/mongodb/usecase1/family-media/{user_id}")
async def mongodb_uc1_family_media(user_id: int):
    try:
        media = mongo.get_family_shared_media(user_id)
        return {"media": media, "count": len(media)}
    except Exception as e:
        print(f"Error in mongodb_uc1_family_media: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_FAMILY_MEDIA_FAILED",
                "message": str(e)
            }
        )