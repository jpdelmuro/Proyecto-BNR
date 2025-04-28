import logging
import random
import uuid
from datetime import datetime
from cassandra.cluster import Cluster

# Configurar logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Configuración de Keyspace
KEYSPACE = "plataforma_online"
REPLICATION_FACTOR = 1

CREATE_KEYSPACE = f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {REPLICATION_FACTOR} }};
"""

# Creación de Tablas
CREATE_STUDENTS_ACTIVITY = """
    CREATE TABLE IF NOT EXISTS student_activity (
    activity_id uuid,
    user_email text,
    course_id text,
    tipo_actividad text,
    timestamp timestamp,
    detalles text,
    PRIMARY KEY ((user_email), timestamp, activity_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, activity_id Desc);
"""

STUDENT_PROGRESS = """
    CREATE TABLE IF NOT EXISTS course_progress (
    user_email text,
    course_id text,
    progress_percent int,
    grade float,
    PRIMARY KEY (user_email, course_id)
) WITH CLUSTERING ORDER BY (progress_percent);
"""

SYSTEM_NOTIFICATIONS = """
    CREATE TABLE IF NOT EXISTS system_notifications ( 
    user_email text, 
    notification_id uuid, 
    course_id text,  
    tipo text,  
    notificacion text, 
    timestamp timestamp, 
    PRIMARY KEY ((user_email), timestamp, notification_id)) WITH CLUSTERING ORDER BY (timestamp DESC, notification_id DESC);

"""

USER_SESSIONS = """
    CREATE TABLE IF NOT EXISTS user_sessions ( 
    user_email text, 
    session_id uuid,  
    device_info text,  
    last_activity timestamp,  
    PRIMARY KEY ((user_email), session_id) 
) WITH CLUSTERING ORDER BY (last_activity);  
"""

STUDENT_CERTIFICATES = """
    CREATE TABLE IF NOT EXISTS certificates ( 
    certificate_id uuid, 
    user_email text, 
    course_id text, 
    student_name text, 
    course_title text, 
    completion_date date,  
    certificate_url text, PRIMARY KEY (user_email, completion_date, certificate_id) ) WITH CLUSTERING ORDER BY (certificate_id DESC,completion_date DESC);
"""

COURSE_PERFORMANCE = """
    CREATE TABLE IF NOT EXISTS course_performance (  
    progress_id uuid,  
    user_email text, 
    course_id text, 
    progress_percent int, 
    calificaciones float, 
    PRIMARY KEY ((user_email), course_id) 
); 
"""

LOGIN_LOGS = """
CREATE TABLE IF NOT EXISTS login_logs (  
    user_email text, 
    session_id uuid,  
    start_time timestamp,  
    last_activity timestamp,  
    device_info text, 
    active_status boolean, 
    PRIMARY KEY (user_email, last_activity, session_id) ) WITH CLUSTERING ORDER BY (session_id DESC,last_activity DESC); 
"""

TASK_REMINDERS = """
CREATE TABLE IF NOT EXISTS task_reminders (  
    user_email text,  
    task_id uuid,  
    task_description text,  
    due_date date,  
    is_completed boolean,  
    PRIMARY KEY (user_email, task_id)); 
"""

COURSE_VIEWS = """
CREATE TABLE IF NOT EXISTS course_viewss (
    course_id text,
    view_date timestamp,
    views int,
    PRIMARY KEY (course_id, view_date)
);
"""

TOP_INSTRUCTORS = """
CREATE TABLE IF NOT EXISTS top_instructors ( 
    ranking int, 
    instructor_email text, 
    instructor_name text, 
    avg_rating float, 
    total_courses int, 
    PRIMARY KEY (ranking)); """

# Datos de prueba
STUDENTS = [
    ('mike', 'Michael Jones'),
    ('stacy', 'Stacy Malibu'),
    ('john', 'John Doe'),
    ('marie', 'Marie Condo'),
    ('tom', 'Tomas Train')
]

ACTIVITY_TYPE = ['QUIZ', 'FORO', 'TAREA', 'VIDEO', 'EXAMEN']

# Función para poblar las tablas con datos aleatorios
def populate_data(session):
    log.info("Insertando datos de prueba...")

    for student_username, student_name in STUDENTS:
        user_email = f"{student_username}@example.com"
        course_id = f"course_{random.randint(1, 5)}"

        # Insertar actividad aleatoria
        for _ in range(3):  # Cada estudiante hace 3 actividades
            activity_id = uuid.uuid4()
            tipo_actividad = random.choice(ACTIVITY_TYPE)
            timestamp = datetime.utcnow()
            detalles = f"Realizó la actividad {tipo_actividad}"

            session.execute("""
                INSERT INTO student_activity (user_email, course_id, tipo_actividad, timestamp, activity_id, detalles)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user_email, course_id, tipo_actividad, timestamp, activity_id, detalles))

        # Insertar progreso
        progress_percent = random.randint(20, 100)
        grade = round(random.uniform(6.0, 10.0), 2)

        session.execute("""
            INSERT INTO course_progress (user_email, course_id, progress_percent, grade)
            VALUES (%s, %s, %s, %s)
        """, (user_email, course_id, progress_percent, grade))

        # Insertar notificaciones
        notification_id = uuid.uuid4()
        tipo_notificacion = random.choice(['Anuncio', 'Recordatorio', 'Calificación'])
        mensaje = f"{tipo_notificacion} para {student_name}"
        notification_timestamp = datetime.utcnow()

        session.execute("""
            INSERT INTO system_notifications (user_email, timestamp, notification_id, course_id, tipo, notificacion)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (user_email, notification_timestamp, notification_id, course_id, tipo_notificacion, mensaje))

        # Insertar sesiones activas
        session_id = uuid.uuid4()
        device_info = random.choice(['Windows 10 - Chrome', 'MacBook - Safari', 'Android - Firefox'])
        last_activity = datetime.utcnow()

        session.execute("""
            INSERT INTO user_sessions (user_email, session_id, device_info, last_activity)
            VALUES (%s, %s, %s, %s)
        """, (user_email, session_id, device_info, last_activity))

        # Insertar certificados
        certificate_id = uuid.uuid4()
        completion_date = datetime.utcnow().date()
        certificate_url = f"https://certificados.com/{student_username}_curso"

        session.execute("""
            INSERT INTO certificates (user_email, completion_date, certificate_id, course_id, student_name, course_title, certificate_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user_email, completion_date, certificate_id, course_id, student_name, "Curso de Cassandra", certificate_url))

        # Insertar desempeño en curso
        progress_id = uuid.uuid4()
        progress_percent = random.randint(20, 100)
        calificaciones = round(random.uniform(6.0, 10.0), 2)

        session.execute("""
            INSERT INTO course_performance (user_email, course_id, progress_id, progress_percent, calificaciones)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_email, course_id, progress_id, progress_percent, calificaciones))

        # Insertar login logs
        start_time = datetime.utcnow()
        last_activity = datetime.utcnow()
        device_info = random.choice(['Windows 10 - Chrome', 'MacBook - Safari', 'Android - Firefox'])
        active_status = random.choice([True, False])

        session.execute("""
            INSERT INTO login_logs (user_email, last_activity, session_id, start_time, device_info, active_status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (user_email, last_activity, session_id, start_time, device_info, active_status))

        # Insertar recordatorios de tareas
        task_id = uuid.uuid4()
        task_description = "Completar módulo 3"
        due_date = datetime.utcnow().date()
        is_completed = random.choice([True, False])

        session.execute("""
            INSERT INTO task_reminders (user_email, task_id, task_description, due_date, is_completed)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_email, task_id, task_description, due_date, is_completed))

    # Insertar vistas de cursos
    for course_number in range(1, 6):
        course_id = f"course_{course_number}"
        for _ in range(3):  # 3 registros por curso
            view_date = datetime.utcnow()
            views = random.randint(10, 100)

            session.execute("""
                INSERT INTO course_viewss (course_id, view_date, views)
                VALUES (%s, %s, %s)
            """, (course_id, view_date, views))

    # Insertar instructores top
    for ranking in range(1, 6):
        instructor_email = f"instructor{ranking}@example.com"
        instructor_name = f"Instructor {ranking}"
        avg_rating = round(random.uniform(3.0, 5.0), 2)
        total_courses = random.randint(5, 20)

        session.execute("""
            INSERT INTO top_instructors (ranking, instructor_email, instructor_name, avg_rating, total_courses)
            VALUES (%s, %s, %s, %s, %s)
        """, (ranking, instructor_email, instructor_name, avg_rating, total_courses))

    log.info("Datos de prueba insertados correctamente.")


# Crear esquema de base de datos
def create_schema(session):
    log.info("Creando Keyspace...")
    session.execute(CREATE_KEYSPACE)

    # Usar el Keyspace antes de crear las tablas
    session.set_keyspace(KEYSPACE)
    
    log.info("Creando tablas...")
    session.execute(CREATE_STUDENTS_ACTIVITY)
    session.execute(STUDENT_PROGRESS)
    session.execute(SYSTEM_NOTIFICATIONS)
    session.execute(USER_SESSIONS)
    session.execute(STUDENT_CERTIFICATES)
    session.execute(COURSE_PERFORMANCE)
    session.execute(LOGIN_LOGS)
    session.execute(TASK_REMINDERS)
    session.execute(COURSE_VIEWS)
    session.execute(TOP_INSTRUCTORS)

    log.info("Esquema creado exitosamente.")

def main():
    log.info("Conectando a Cassandra...")
    cluster = Cluster(['127.0.0.1'], protocol_version=5)
    session = cluster.connect()

    # Crear esquema de base de datos
    create_schema(session)

    # Poblar datos de prueba
    populate_data(session)

    log.info("Datos insertados correctamente.")

if __name__ == "__main__":
    main()
