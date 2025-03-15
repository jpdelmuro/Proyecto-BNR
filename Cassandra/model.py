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
        student_id UUID,
        course_id UUID,
        activity_id UUID,
        tipo_actividad TEXT,
        activity_timestamp TIMESTAMP,
        detalles TEXT,
        PRIMARY KEY ((student_id, course_id), activity_timestamp, activity_id)
    ) WITH CLUSTERING ORDER BY (activity_timestamp DESC);
"""

STUDENT_PROGRESS = """
    CREATE TABLE IF NOT EXISTS student_progress (
        student_id UUID,
        course_id UUID,
        progress_id UUID,
        porcentaje_completado FLOAT,
        calificaciones INT,
        PRIMARY KEY ((student_id, course_id), progress_id)
    ) WITH CLUSTERING ORDER BY (progress_id DESC);
"""

SYSTEM_NOTIFICATIONS = """
    CREATE TABLE IF NOT EXISTS system_notifications (
        user_id UUID,
        notification_id UUID,
        course_id UUID,
        tipo TEXT,
        notificacion TEXT,
        notification_timestamp TIMESTAMP,
        PRIMARY KEY ((user_id, course_id), notification_timestamp, notification_id)
    ) WITH CLUSTERING ORDER BY (notification_timestamp DESC);
"""

USER_SESSIONS = """
    CREATE TABLE IF NOT EXISTS user_sessions (
        user_id UUID,
        session_id UUID,
        start_time TIMESTAMP,
        last_activity TIMESTAMP,
        device_info TEXT,
        active_status BOOLEAN,
        PRIMARY KEY ((user_id), last_activity, session_id)
    ) WITH CLUSTERING ORDER BY (last_activity DESC);
"""

STUDENT_CERTIFICATES = """
    CREATE TABLE IF NOT EXISTS student_certificates (
        student_id UUID,
        course_id UUID,
        certificate_id UUID,
        student_name TEXT,
        course_title TEXT,
        completion_date TIMESTAMP,
        certificate_url TEXT,
        PRIMARY KEY ((student_id, course_id), certificate_id)
    );
"""

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
        student_id = uuid.uuid4()
        course_id = uuid.uuid4()

        # Insertar actividad aleatoria
        for _ in range(3):  # Cada estudiante hace 3 actividades
            activity_id = uuid.uuid4()
            tipo_actividad = random.choice(ACTIVITY_TYPE)
            timestamp = datetime.utcnow()
            detalles = f"Realizó la actividad {tipo_actividad}"

            session.execute("""
                INSERT INTO student_activity (student_id, course_id, activity_id, tipo_actividad, activity_timestamp, detalles)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (student_id, course_id, activity_id, tipo_actividad, timestamp, detalles))

        # Insertar progreso
        progress_id = uuid.uuid4()
        porcentaje_completado = random.uniform(20, 100)  # Progreso entre 20% y 100%
        calificaciones = random.randint(50, 100)  # Calificación entre 50 y 100

        session.execute("""
            INSERT INTO student_progress (student_id, course_id, progress_id, porcentaje_completado, calificaciones)
            VALUES (%s, %s, %s, %s, %s)
        """, (student_id, course_id, progress_id, porcentaje_completado, calificaciones))

        # Insertar notificaciones
        notification_id = uuid.uuid4()
        tipo_notificacion = random.choice(['Anuncio', 'Recordatorio', 'Calificación'])
        mensaje = f"{tipo_notificacion} para {student_name}"
        notification_timestamp = datetime.utcnow()

        session.execute("""
            INSERT INTO system_notifications (user_id, notification_id, course_id, tipo, notificacion, notification_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (student_id, notification_id, course_id, tipo_notificacion, mensaje, notification_timestamp))

        # Insertar sesiones activas
        session_id = uuid.uuid4()
        start_time = datetime.utcnow()
        last_activity = datetime.utcnow()
        device_info = random.choice(['Windows 10 - Chrome', 'MacBook - Safari', 'Android - Firefox'])
        active_status = random.choice([True, False])

        session.execute("""
            INSERT INTO user_sessions (user_id, session_id, start_time, last_activity, device_info, active_status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (student_id, session_id, start_time, last_activity, device_info, active_status))

        # Insertar certificados para cursos completados
        certificate_id = uuid.uuid4()
        completion_date = datetime.utcnow()
        certificate_url = f"https://certificados.com/{student_username}_curso"

        session.execute("""
            INSERT INTO student_certificates (student_id, course_id, certificate_id, student_name, course_title, completion_date, certificate_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (student_id, course_id, certificate_id, student_name, "Curso de Cassandra", completion_date, certificate_url))

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
