from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

from .databases.mariadb import mariadb
from .databases.mariadb.data_generator import generate_random_data

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
        print("Error in test_database: "+e)
        raise HTTPException(status_code=500, detail="Test database error: "+e)
    
@app.get("/api/tables/clear")
async def clear_tables():
    try:
        mariadb.reset_all_tables()
        
        return {
            "Tables deleted successfully."
        }
    except Exception as e:
        print("Error in clear_tables: "+str(e))
        raise HTTPException(status_code=500, detail="Clear database error: "+str(e))

@app.get("/api/tables")
async def list_tables():
    try:
        tables = mariadb.list_all_tables_with_rows()
        
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/add-data")
async def add_data():
    try:
        mariadb.add_sample_data()
        return {"message": "Sample data added successfully"}
    except Exception as e:
        print("Error in add_data: "+str(e))
        raise HTTPException(status_code=500, detail="Error adding sample data")

        
@app.post("/api/generate-data")
async def generate_data():
    try:
        mariadb.reset_all_tables()
        generate_random_data()
        return {"message": "Sample data added successfully"}
    except Exception as e:
        print("Error in generate_data: "+str(e))
        raise HTTPException(status_code=500, detail="Error generating data")

@app.get("/api/check-data")
async def check_data():
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
            # Get total counts
            cursor.execute("SELECT COUNT(*) as count FROM Users")
            total_users = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM Media")
            total_media = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM Sessions")
            total_rentals = cursor.fetchone()['count']
            
            cursor.execute("SELECT * FROM Users WHERE user_name = 'Zhami'")
            zhami = cursor.fetchone()
            
            # Get Zhami's rental if exists
            rental = None
            if zhami:
                cursor.execute("""
                    SELECT m.media_name, s.date_of_rent, s.cost 
                    FROM Sessions s 
                    JOIN Media m ON s.media_id = m.media_id 
                    WHERE s.user_id = %s
                """, (zhami['user_id'],))
                rental = cursor.fetchone()
        
        connection.close()
        
        response = {
            "total_users": total_users,
            "total_media": total_media,
            "total_rentals": total_rentals
        }
        
        # Add Zhami info if found
        if zhami:
            response["zhami"] = {
                "user_id": zhami['user_id'],
                "name": zhami['user_name'],
                "email": zhami['email'],
                "city": zhami['location']
            }
        
        # Add rental info if found
        if rental:
            response["zhami_rental"] = {
                "movie": rental['media_name'],
                "rented_on": rental['date_of_rent'].strftime("%Y-%m-%d"),
                "price": rental['cost']
            }
        
        return response
        
    except Exception as e:
        return {"error": str(e)}
