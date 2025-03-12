import os
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging

# logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseConnections:
    
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.cassandra_session = None
        self.cassandra_cluster = None
    
    def connect_mongodb(self, host='localhost', port=27017, db_name='learning_platform'):
        """Conectarse al contenedor MongoDB"""
        try:
            connection_url = f"mongodb://{host}:{port}/"
            self.mongo_client = MongoClient(connection_url)
            self.mongo_db = self.mongo_client[db_name]
            logger.info(f"Se conectó exitosamente a MongoDB en {connection_url}")
            # Testeo
            db_list = self.mongo_client.list_database_names()
            logger.info(f" MongoDB databases disponibles: {db_list}")
            return True
        except Exception as e:
            logger.error(f"Falló la conexión a MongoDB: {str(e)}")
            return False
    
    def connect_cassandra(self, hosts=['localhost'], port=9042, keyspace='learning_platform_ks'):
        """Conectarse al contenedor de Cassandra"""
        try:
            # Conectando a Cassandra cluster
            self.cassandra_cluster = Cluster(contact_points=hosts, port=port)
            self.cassandra_session = self.cassandra_cluster.connect()
            
            # Crer el keyspace si no existe
            self.cassandra_session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS %s 
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
                """ % keyspace
            )
            
            # Usar el keyspace
            self.cassandra_session.set_keyspace(keyspace)
            logger.info(f"Se conectó exitosamente a Cassandra cluster en {hosts}:{port} usando el keyspace {keyspace}")
            return True
        except Exception as e:
            logger.error(f"Falló la conexión a Cassandra: {str(e)}")
            return False
    
    def disconnect_mongodb(self):
        """Desconectarse de MongoDB."""
        if self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None
            self.mongo_db = None
            logger.info("Desconectado de MongoDB")
    
    def disconnect_cassandra(self):
        """Disconnect from Cassandra."""
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()
            self.cassandra_cluster = None
            self.cassandra_session = None
            logger.info("Desconectado de Cassandra")
    
    def disconnect_all(self):
        """Desconectado de todas las databases"""
        self.disconnect_mongodb()
        self.disconnect_cassandra()