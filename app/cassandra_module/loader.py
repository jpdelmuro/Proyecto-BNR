import logging
import uuid
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime
import csv
import os

KEYSPACE = "plataforma_online"

CREATE_KEYSPACE = f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
"""

CREATE_STUDENT_ACTIVITY = """
    CREATE TABLE IF NOT EXISTS student_activity (
        activity_id uuid,
        user_email text,
        course_id text,
        tipo_actividad text,
        timestamp timestamp,
        detalles text,
        PRIMARY KEY ((user_email), timestamp, activity_id)
    ) WITH CLUSTERING ORDER BY (timestamp DESC, activity_id DESC);
"""

INSERT_STUDENT_ACTIVITY = """
    INSERT INTO student_activity (user_email, course_id, tipo_actividad, timestamp, activity_id, detalles)
    VALUES (%s, %s, %s, %s, %s, %s)
"""


def connect_to_cassandra():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute(CREATE_KEYSPACE)
    session.set_keyspace(KEYSPACE)
    return session


def create_tables(session):
    session.execute(SimpleStatement(CREATE_STUDENT_ACTIVITY))


def load_cassandra_data(csv_path):
    session = connect_to_cassandra()
    create_tables(session)

    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                user_email = row['user_email']
                course_id = row['course_id']
                tipo = row['tipo_actividad']
                timestamp = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                activity_id = uuid.UUID(row['activity_id'])
                detalles = row['detalles']

                session.execute(INSERT_STUDENT_ACTIVITY, (user_email, course_id, tipo, timestamp, activity_id, detalles))
                logging.info(f"Insertado {user_email} - {course_id} en student_activity")
            except Exception as e:
                logging.error(f"Error insertando fila: {e}")

    session.shutdown()
    print("✓ Datos de Cassandra cargados correctamente.")
