from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from pydantic import BaseModel

from .databases.mariadb import mariadb
from .databases.mariadb.data_generator import generate_random_data
from .databases.mariadb.usecase1 import use_case1 as uc1_mariadb
from .databases.mariadb.usecase2 import use_case2 as uc2_logic
from .databases.mongodb import mongodb as mongo
from .databases.mongodb import mongo_migration as mongo_migration
from .databases.mongodb import use_case1_mongo as uc1_mongodb 
from .databases.mongodb import use_case2_mongo as uc2_mongo


app = FastAPI(title="Media Rental Service", version="1.0.0") 

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.on_event("startup")
async def startup_event():
    """Clear MongoDB collections to ensure clean state."""
    try:
        mongo.reset_all_collections()
        print("MongoDB collections cleared on startup")
    except Exception as e:
        print(f"Warning: Could not clear MongoDB on startup: {e}")

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
    """Data sanity check: returns table counts."""
    try:
        total_users = len(mariadb.get_table_rows('Users'))
        total_media = len(mariadb.get_table_rows('Media'))
        total_rentals = len(mariadb.get_table_rows('Sessions'))
        total_watch_history = len(mariadb.get_table_rows('WatchHistory'))
        total_families = len(mariadb.get_table_rows('Family'))
        total_devices = len(mariadb.get_table_rows('Device'))
        total_films = len(mariadb.get_table_rows('Film'))
        total_series = len(mariadb.get_table_rows('Series'))
        total_friendships = len(mariadb.get_table_rows('Friendships'))

        return {
            "total_users": total_users,
            "total_media": total_media,
            "total_rentals": total_rentals,
            "total_watch_history": total_watch_history,
            "total_families": total_families,
            "total_devices": total_devices,
            "total_films": total_films,
            "total_series": total_series,
            "total_friendships": total_friendships
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
    try :
        data = uc1_mariadb.load_data()
        return data
    except Exception as e:
        print("Error in uc1_load_data: "+str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "code": "UC1_LOAD_DATA_FAILED",
                "message": "Failed to load data"
            }
        )

class WatchRequest(BaseModel):
    user_id: int
    media_id: int

@app.post("/api/usecase1/watch")
async def uc1_watch_media(request: WatchRequest):
    try:
        output = uc1_mariadb.watch_media(request.user_id, request.media_id)
        return {"family_watches": output}
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
        mongo_migration.migrate_from_sql()
        mariadb.reset_all_tables()
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


@app.get("/api/mongodb/samples")
async def mongodb_sample_documents():
    try:
        samples = mongo.get_sample_documents()
        return {
            "message": "Sample documents demonstrating NoSQL schema design",
            "collections": samples,
            "design_notes": {
                "user": "Embedded devices array and friends array (N-side referencing)",
                "media": "Polymorphic type field with embedded type_details",
                "session": "Denormalized user and media data (eliminates JOINs)",
                "watch_history": "Denormalized user and media data with family context",
                "family": "Embedded users array showing family members"
            }
        }
    except Exception as e:
        print(f"Error in mongodb_sample_documents: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_SAMPLES_FAILED",
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
        session = uc2_mongo.rent_media(user_id, media_id, duration_days)
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
        media = uc2_mongo.list_media()
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
        users = uc2_mongo.list_users()
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
        rentals = uc2_mongo.list_user_rentals(user_id)
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
async def mongodb_uc1_watch(request: WatchRequest):
    try:
        family_watches = uc1_mongodb.watch_media(request.user_id, request.media_id)
        return {"family_watches": family_watches}
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


@app.get("/api/mongodb/usecase1/load-data")
async def mongodb_uc1_load_data():
    try:
        media = uc1_mongodb.load_data()
        return media
    except Exception as e:
        print(f"Error in mongodb_uc1_load_data: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "code": "MONGODB_LOAD_DATA_FAILED",
                "message": str(e)
            }
        )