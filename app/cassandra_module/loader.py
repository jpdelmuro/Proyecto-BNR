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

    session.execute("""
        TRUNCATE student_activity;
    """)
    session.execute("""
        TRUNCATE course_progress;
    """)
    session.execute("""
        TRUNCATE system_notifications;
    """)
    session.execute("""
        TRUNCATE user_sessions;
    """)
    session.execute("""
        TRUNCATE certificates;
    """)
    session.execute("""
        TRUNCATE course_performance;
    """)
    session.execute("""
        TRUNCATE login_logs;
    """)
    session.execute("""
        TRUNCATE task_reminders;
    """)
    session.execute("""
        TRUNCATE course_views;
    """)
    session.execute("""
        TRUNCATE top_instructors;
    """)

    log.info("Insertando datos desde CSV...")
    instructores_vistos = set()
    ranking = 1

    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Saltar encabezado

        for row in reader:
            try:
                user_email, course_id, tipo_actividad, timestamp_str, activity_id, detalles, progress_percent, grade, device_info, teacher_name, teacher_email,teacher_avg = row
                    
                # Convertir el timestamp a formato datetime de Python
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))  # Asumimos que el formato es ISO 8601
                progress_percent = int(progress_percent)
                grade = float(grade)
                activity_id = uuid.UUID(activity_id)  # Convertir a UUID
                
                INSERT_student_activity = """
                    INSERT INTO student_activity (user_email, course_id, tipo_actividad, timestamp, activity_id, detalles)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                prepared = session.prepare(INSERT_student_activity)
                try:
                    session.execute(prepared, (user_email, course_id, tipo_actividad, timestamp, activity_id, detalles))
                except Exception as e:
                    print(f"Error al insertar datos de actividad del estudiante: {e}")

                INSERT_course_progress = """
                    INSERT INTO course_progress (user_email, course_id, progress_percent, grade)
                    VALUES (?, ?, ?, ?)
                """
                prepared = session.prepare(INSERT_course_progress)
                try:
                    session.execute(prepared, (user_email, course_id, progress_percent, grade))
                except Exception as e:
                    print(f"Error al insertar datos de progreso del curso: {e}")

                notification_id = uuid.uuid4()
                tipo_notificacion = random.choice(['Anuncio', 'Recordatorio', 'Calificación'])
                mensaje = f"{tipo_notificacion} para {user_email}"

                INSERT_system_notifications = """
                    INSERT INTO system_notifications (user_email, timestamp, notification_id, course_id, tipo, notificacion)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                prepared = session.prepare(INSERT_system_notifications)
                try:
                    session.execute(prepared, (user_email, timestamp, notification_id, course_id, tipo_notificacion, mensaje))
                except Exception as e:
                    print(f"Error al insertar datos de notificaciones: {e}")

                session_id = uuid.uuid4()
                last_activity = datetime.now()

                INSERT_user_sessions = """
                    INSERT INTO user_sessions (user_email, session_id, device_info, last_activity)
                    VALUES (?, ?, ?, ?)
                """
                prepared = session.prepare(INSERT_user_sessions)
                try:
                    session.execute(prepared, (user_email, session_id, device_info, last_activity))
                except Exception as e:
                    print(f"Error al insertar datos de sesiones de usuario: {e}")

                student_name = user_email.split('@')[0]  # Obtener el nombre del estudiante a partir del email
                certificate_id = uuid.uuid4()
                completion_date = datetime.now().date()
                certificate_url = f"https://certificados.com/{student_name}_curso"

                INSERT_certificates = """
                    INSERT INTO certificates (user_email, completion_date, certificate_id, course_id, student_name, course_title, certificate_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                prepared = session.prepare(INSERT_certificates)
                try:
                    session.execute(prepared, (user_email, completion_date, certificate_id, course_id, student_name, course_id, certificate_url))
                except Exception as e:
                    print(f"Error al insertar datos de certificados: {e}")


                INSERT_course_performance = """
                    INSERT INTO course_performance (user_email, course_id, progress_percent, grade)
                    VALUES (?, ?, ?, ?)
                """
                prepared = session.prepare(INSERT_course_performance)
                try:
                    session.execute(prepared, (user_email, course_id, progress_percent, grade))
                except Exception as e:
                    print(f"Error al insertar datos de desempeño en curso: {e}")

                            # Insertar login logs
                start_time = datetime.now()
                last_activity = datetime.now()
                device_info = random.choice(['Windows 10 - Chrome', 'MacBook - Safari', 'Android - Firefox'])
                active_status = random.choice([True, False])

                INSERT_login_logs = """
                    INSERT INTO login_logs (user_email, last_activity, session_id, start_time, device_info, active_status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                prepared = session.prepare(INSERT_login_logs)
                try:
                    session.execute(prepared, (user_email, last_activity, session_id, start_time, device_info, active_status))
                except Exception as e:
                    print(f"Error al insertar datos de login logs: {e}")

                # Insertar recordatorios de tareas
                task_id = uuid.uuid4()
                task_description = "Completar módulo 3"
                due_date = datetime.now().date()
                is_completed = random.choice([True, False])

                INSERT_tasks = """
                    INSERT INTO task_reminders (user_email, task_id, task_description, due_date, is_completed)
                    VALUES (?, ?, ?, ?, ?)
                """
                prepared = session.prepare(INSERT_tasks)
                try:
                    session.execute(prepared, (user_email, task_id, task_description, due_date, is_completed))
                except Exception as e:
                    print(f"Error al insertar datos de recordatorios de tareas: {e}")

                view_date = datetime.now()
                views = random.randint(10, 100)

                # Insertar vistas del curso
                INSERT_views = """
                    INSERT INTO course_views (course_id, view_date, views)
                    VALUES (?, ?, ?)
                """
                prepared = session.prepare(INSERT_views)
                try:
                    session.execute(prepared, (course_id, view_date, views))
                except Exception as e:
                    print(f"Error al insertar datos de vistas del curso: {e}")

                instructor_email = teacher_email
                instructor_name = teacher_name
                ranking += 1
                avg_rating = float(teacher_avg)
                total_courses = random.randint(5, 20)
                #print(f"Datos del instructor: {instructor_email}, {instructor_name}, {ranking},{avg_rating}, {total_courses}")
                if instructor_email not in instructores_vistos:
                # Insertar datos de los instructores    
                    INSERT_Instructors = """
                        INSERT INTO top_instructors (ranking, instructor_email, instructor_name, avg_rating, total_courses)
                        VALUES (?, ?, ?, ?, ?);
                    """
                    prepared = session.prepare(INSERT_Instructors)
                    try:
                        session.execute(prepared, (ranking, instructor_email, instructor_name, avg_rating, total_courses))
                        instructores_vistos.add(instructor_email)
                    except Exception as e:
                        print(f"Error al insertar datos del instructor: {e}")

            except Exception as e:
                log.error(f"Error insertando fila: {e}")

    session.shutdown()
    print("✓ Datos de Cassandra cargados correctamente.")
