import logging
import uuid
import csv
import random
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.policies import RoundRobinPolicy

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KEYSPACE = "plataforma_online"

CREATE_KEYSPACE = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
"""

TABLES = [
    # (misma definición de tablas que ya tenías)
    """
    CREATE TABLE IF NOT EXISTS student_activity (
        activity_id uuid,
        user_email text,
        course_id text,
        tipo_actividad text,
        timestamp timestamp,
        detalles text,
        PRIMARY KEY ((user_email), timestamp, activity_id)
    ) WITH CLUSTERING ORDER BY (timestamp DESC, activity_id DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS course_progress (
        user_email text,
        course_id text,
        progress_percent int,
        grade float,
        PRIMARY KEY ((user_email), course_id)
    ) WITH CLUSTERING ORDER BY (course_id ASC);
    """,
    """
    CREATE TABLE IF NOT EXISTS system_notifications (
        user_email text,
        notification_id uuid,
        course_id text,
        tipo text,
        notificacion text,
        timestamp timestamp,
        PRIMARY KEY ((user_email), timestamp, notification_id)
    ) WITH CLUSTERING ORDER BY (timestamp DESC, notification_id DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS user_sessions (
        user_email text,
        session_id uuid,
        device_info text,
        last_activity timestamp,
        PRIMARY KEY ((user_email), session_id)
    ) WITH CLUSTERING ORDER BY (session_id DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS certificates (
        certificate_id uuid,
        user_email text,
        course_id text,
        student_name text,
        course_title text,
        completion_date date,
        certificate_url text,
        PRIMARY KEY ((user_email), completion_date, certificate_id)
    ) WITH CLUSTERING ORDER BY (completion_date DESC, certificate_id DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS course_performance (
        course_id text,
        user_email text,
        progress_percent int,
        grade float,
        PRIMARY KEY ((course_id), user_email)
    ) WITH CLUSTERING ORDER BY (user_email ASC);
    """,
    """
    CREATE TABLE IF NOT EXISTS login_logs (
        user_email text,
        session_id uuid,
        start_time timestamp,
        last_activity timestamp,
        device_info text,
        active_status boolean,
        PRIMARY KEY ((user_email), last_activity, session_id)
    ) WITH CLUSTERING ORDER BY (last_activity DESC, session_id DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS task_reminders (
        user_email text,
        task_id uuid,
        task_description text,
        due_date date,
        is_completed boolean,
        PRIMARY KEY ((user_email), task_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS course_views (
        course_id text,
        view_date timestamp,
        views int,
        PRIMARY KEY ((course_id), view_date)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS top_instructors (
        ranking int,
        instructor_email text,
        instructor_name text,
        avg_rating float,
        total_courses int,
        PRIMARY KEY (ranking)
    );
    """
]

def connect_to_cassandra():
    cluster = Cluster(
        contact_points=["127.0.0.1"],
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=5
    )
    session = cluster.connect()
    session.execute(CREATE_KEYSPACE)
    session.set_keyspace(KEYSPACE)
    return session

def create_schema(session):
    for statement in TABLES:
        session.execute(SimpleStatement(statement))
    log.info("Esquema de Cassandra creado correctamente.")

def load_cassandra_data(csv_path):
    session = connect_to_cassandra()
    create_schema(session)

    log.info("Insertando datos desde CSV...")
    instructores_vistos = set()
    ranking = 1

    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Saltar encabezado

        for row in reader:
            try:
                user_email, course_id, tipo_actividad, timestamp_str, activity_id_str, detalles, progress_str, grade_str, device_info, teacher_name, teacher_email, avg_rating_str = row

                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                activity_id = uuid.UUID(activity_id_str)
                progress = int(progress_str)
                grade = float(grade_str)
                avg_rating = float(avg_rating_str)
                session_id = uuid.uuid4()
                certificate_id = uuid.uuid4()
                notification_id = uuid.uuid4()
                task_id = uuid.uuid4()
                completion_date = datetime.now().date()
                view_date = datetime.now()
                certificate_url = f"https://certificados.com/{user_email.split('@')[0]}_curso"
                is_completed = random.choice([True, False])
                active_status = random.choice([True, False])
                total_courses = random.randint(5, 20)

                # Inserciones
                session.execute("""
                    INSERT INTO student_activity (user_email, course_id, tipo_actividad, timestamp, activity_id, detalles)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (user_email, course_id, tipo_actividad, timestamp, activity_id, detalles))

                session.execute("""
                    INSERT INTO course_progress (user_email, course_id, progress_percent, grade)
                    VALUES (%s, %s, %s, %s)
                """, (user_email, course_id, progress, grade))

                session.execute("""
                    INSERT INTO system_notifications (user_email, timestamp, notification_id, course_id, tipo, notificacion)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (user_email, timestamp, notification_id, course_id, "Notificación", f"Notificación para {user_email}"))

                session.execute("""
                    INSERT INTO user_sessions (user_email, session_id, device_info, last_activity)
                    VALUES (%s, %s, %s, %s)
                """, (user_email, session_id, device_info, timestamp))

                session.execute("""
                    INSERT INTO certificates (user_email, completion_date, certificate_id, course_id, student_name, course_title, certificate_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (user_email, completion_date, certificate_id, course_id, user_email.split('@')[0], course_id, certificate_url))

                session.execute("""
                    INSERT INTO course_performance (course_id, user_email, progress_percent, grade)
                    VALUES (%s, %s, %s, %s)
                """, (course_id, user_email, progress, grade))

                session.execute("""
                    INSERT INTO login_logs (user_email, last_activity, session_id, start_time, device_info, active_status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (user_email, timestamp, session_id, timestamp, device_info, active_status))

                session.execute("""
                    INSERT INTO task_reminders (user_email, task_id, task_description, due_date, is_completed)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_email, task_id, "Completar módulo 3", completion_date, is_completed))

                session.execute("""
                    INSERT INTO course_views (course_id, view_date, views)
                    VALUES (%s, %s, %s)
                """, (course_id, view_date, random.randint(10, 100)))

                if teacher_email not in instructores_vistos:
                    session.execute("""
                        INSERT INTO top_instructors (ranking, instructor_email, instructor_name, avg_rating, total_courses)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (ranking, teacher_email, teacher_name, avg_rating, total_courses))
                    instructores_vistos.add(teacher_email)
                    ranking += 1

            except Exception as e:
                log.error(f"Error insertando fila: {e}")

    session.shutdown()
    print("✓ Datos de Cassandra cargados correctamente.")
