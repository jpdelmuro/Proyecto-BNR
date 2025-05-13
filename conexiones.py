#!/usr/bin/env python3
import logging
import os
from cassandra.cluster import Cluster
from pymongo import MongoClient


# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('database_connections.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(handler)

# Cassandra
CASSANDRA_HOSTS = os.getenv('CASSANDRA_HOSTS', '127.0.0.1').split(',')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'elearning')
CASSANDRA_REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

# MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'elearning')

def connect_cassandra():
    """
    connection to Cassandra cluster, creates keyspace if needed
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
    connection to MongoDB
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

def close_connections(cassandra_cluster, mongo_client):
    """
    Closes connections to Cassandra and MongoDB.
    """
    try:
        if cassandra_cluster:
            cassandra_cluster.shutdown()
            logger.info("Cassandra connection closed.")
    except Exception as e:
        logger.error(f"Error closing Cassandra connection: {str(e)}")

    try:
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed.")
    except Exception as e:
        logger.error(f"Error closing MongoDB connection: {str(e)}")


if __name__ == "__main__":
    cassandra_session = None
    cassandra_cluster = None
    mongodb = None
    mongo_client = None
    
    try:
        # Connect to databases
        cassandra_session, cassandra_cluster = connect_cassandra()
        mongodb, mongo_client = connect_mongodb()
        
        print(f"Connected to Cassandra keyspace: {CASSANDRA_KEYSPACE}")
        print(f"Connected to MongoDB database: {MONGO_DB_NAME}")
        
        # Your database operations would go here
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close connections properly
        close_connections(cassandra_cluster, mongo_client)