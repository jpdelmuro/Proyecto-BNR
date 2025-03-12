#!/usr/bin/env python3
import logging
import os
from cassandra.cluster import Cluster
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('database_connections.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(handler)

# Database connection parameters from environment variables or defaults
# Cassandra
CASSANDRA_HOSTS = os.getenv('CASSANDRA_HOSTS', 'localhost').split(',')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'elearning')
CASSANDRA_REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')
# MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'elearning')

def connect_cassandra():
    """
    Establishes connection to Cassandra cluster and creates keyspace if needed
    Returns the session object
    """
    logger.info(f"Connecting to Cassandra cluster at {CASSANDRA_HOSTS}")
    try:
        # Connect to cluster
        cluster = Cluster(CASSANDRA_HOSTS)
        session = cluster.connect()
        
        # Create keyspace if it doesn't exist
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {CASSANDRA_REPLICATION_FACTOR}}}
        """)
        
        # Set keyspace for session
        session.set_keyspace(CASSANDRA_KEYSPACE)
        logger.info(f"Connected to Cassandra keyspace: {CASSANDRA_KEYSPACE}")
        
        return session, cluster
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def connect_mongodb():
    """
    Establishes connection to MongoDB
    Returns the database object
    """
    logger.info(f"Connecting to MongoDB at {MONGO_URI}")
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        logger.info(f"Connected to MongoDB database: {MONGO_DB_NAME}")
        
        return db, client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

def setup_cassandra_schema(session):
    """
    Creates all required tables for the E-Learning platform in Cassandra
    """
    logger.info("Creating Cassandra schema")
    
    # Student Activity Table
    session.execute("""
        CREATE TABLE IF NOT EXISTS student_activity (
            activity_id uuid,
            student_id uuid,
            course_id uuid,
            activity_type text,
            timestamp timestamp,
            details text,
            PRIMARY KEY ((student_id, course_id), timestamp, activity_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """)
    
    # Progress Tracking Table
    session.execute("""
        CREATE TABLE IF NOT EXISTS progress_tracking (
            progress_id uuid,
            student_id uuid,
            course_id uuid,
            completion_percentage float,
            grade float,
            last_updated timestamp,
            PRIMARY KEY ((student_id, course_id), last_updated)
        ) WITH CLUSTERING ORDER BY (last_updated DESC)
    """)
    
    # System Notifications Table
    session.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            notification_id uuid,
            user_id uuid,
            course_id uuid,
            notification_type text,
            message text,
            timestamp timestamp,
            is_read boolean,
            PRIMARY KEY ((user_id), timestamp, notification_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """)
    
    # User Sessions Table
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            session_id uuid,
            user_id uuid,
            start_time timestamp,
            last_activity timestamp,
            device_info text,
            active_status boolean,
            PRIMARY KEY (user_id, session_id)
        )
    """)
    
    # Certificates Table
    session.execute("""
        CREATE TABLE IF NOT EXISTS certificates (
            certificate_id uuid,
            student_id uuid,
            course_id uuid,
            student_name text,
            course_title text,
            completion_date timestamp,
            certificate_url text,
            PRIMARY KEY (student_id, course_id, certificate_id)
        )
    """)
    
    logger.info("Cassandra schema created successfully")

def setup_mongodb_collections(db):
    """
    Creates required collections with validation for the E-Learning platform in MongoDB
    """
    logger.info("Setting up MongoDB collections")
    
    # Users Collection (for both students and instructors)
    try:
        db.create_collection("users", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["user_id", "name", "email", "password", "role", "created_at"],
                "properties": {
                    "user_id": {"bsonType": "string"},
                    "name": {"bsonType": "string"},
                    "email": {"bsonType": "string"},
                    "password": {"bsonType": "string"},
                    "role": {"enum": ["student", "instructor"]},
                    "courses_enrolled": {"bsonType": "array"},
                    "courses_completed": {"bsonType": "array"},
                    "courses_list": {"bsonType": "array"},
                    "course_rating": {"bsonType": "double"},
                    "created_at": {"bsonType": "date"}
                }
            }
        })
    except Exception as e:
        logger.warning(f"Users collection setup notice: {str(e)}")
    
    # Courses Collection
    try:
        db.create_collection("courses", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["course_id", "title", "description", "teacher_id", "created_at"],
                "properties": {
                    "course_id": {"bsonType": "string"},
                    "title": {"bsonType": "string"},
                    "description": {"bsonType": "string"},
                    "teacher_id": {"bsonType": "string"},
                    "lessons": {"bsonType": "array"},
                    "created_at": {"bsonType": "date"}
                }
            }
        })
    except Exception as e:
        logger.warning(f"Courses collection setup notice: {str(e)}")
    
    # Lessons Collection
    try:
        db.create_collection("lessons", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["lesson_id", "course_id", "title", "content", "duration"],
                "properties": {
                    "lesson_id": {"bsonType": "string"},
                    "course_id": {"bsonType": "string"},
                    "title": {"bsonType": "string"},
                    "content": {"bsonType": "string"},
                    "duration": {"bsonType": "int"},
                    "resources": {"bsonType": "array"}
                }
            }
        })
    except Exception as e:
        logger.warning(f"Lessons collection setup notice: {str(e)}")
    
    # indexes
    db.users.create_index("email", unique=True)
    db.users.create_index("user_id", unique=True)
    db.courses.create_index("course_id", unique=True)
    db.lessons.create_index("lesson_id", unique=True)
    db.lessons.create_index([("course_id", 1), ("lesson_id", 1)])
    
    logger.info("MongoDB collections and indexes created successfully")

def close_connections(cassandra_cluster, mongo_client):
    """
    Closes all database connections
    """
    logger.info("Closing database connections")
    
    if cassandra_cluster:
        cassandra_cluster.shutdown()
        logger.info("Cassandra connection closed")
    
    if mongo_client:
        mongo_client.close()
        logger.info("MongoDB connection closed")

# Example usage
if __name__ == "__main__":
    # Connect to databases
    cassandra_session, cassandra_cluster = connect_cassandra()
    mongodb, mongo_client = connect_mongodb()
    
    # Create schemas/collections
    setup_cassandra_schema(cassandra_session)
    setup_mongodb_collections(mongodb)
    
    print("Database connections established and schemas created successfully.")
    print(f"Connected to Cassandra keyspace: {CASSANDRA_KEYSPACE}")
    print(f"Connected to MongoDB database: {MONGO_DB_NAME}")
    
  
    # Close connections
    close_connections(cassandra_cluster, mongo_client)