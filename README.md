## Infrastructure Setup (Milestone 2.1)

### Requirements (Minimal Setup)
- **Docker** (version 20.10 or higher)
- **Docker Compose** (version 2.0 or higher)  
- **Unzip** 


### Start
```bash
# 1. Extract the project archive
unzip media_rental_project.zip
cd media_rental

# 2. Start all services 
docker-compose up -d

# 3. Verify the system is operational
curl http://localhost:5000/health
curl http://localhost:5000/api/test-db

# 4. Access the application
#    Web Interface: http://localhost:5000
#    MariaDB (SQL): localhost:3307
#    MongoDB (NoSQL): localhost:27018