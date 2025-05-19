#!/usr/bin/env python3
import logging
import os
import uuid
import random
from datetime import datetime
from cassandra.cluster import Cluster
from datetime import datetime
from cassandra.policies import RoundRobinPolicy

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('plataforma_estudiantes.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'plataforma_online')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

def print_menu():
    mm_options = {
        0: "Poblar datos de prueba",
        1: "Ver actividades de estudiante",
        2: "ver progreso de estudiante",
        3: "Ver notificaciones del sistema",
        4: "Ver sesiones de usuario",
        5: "Ver certificados",
        6: "Ver progreso del curso",
        7: "Ver logs de inicio de sesión",
        8: "Ver recordatorios de tareas",
        9: "Ver vistas del curso",
        10: "Ver instructores destacados",
        11: "Cambiar email de usuario",
        12: "Eliminar todos los registros del usuario",
        13: "Salir",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def set_user_email():
    user_email = input('**** Email de usuario: ')
    log.info(f"Email de usuario configurado como {user_email}")
    return user_email

# Poblar los datos de prueba o desde CSV
def populate_data(session,user_email):
    log.info("Eliminando datos anteriores...")
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
    # Importar el modelo y ejecutar la función
    import importlib.util
    import os
    script_dir = os.path.dirname(__file__)
    model_path = os.path.join(script_dir, "model.py")
    spec = importlib.util.spec_from_file_location("model", model_path)
    model = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(model)
    # Llamar a la función para poblar datos
    model.populate_data_from_csv(session, '../data/cassandra/test.csv')
    print("Datos de prueba insertados correctamente")

# Selects para cada tabla
def view_activities(session, user_email,option,fecha_inicio=None, fecha_fin=None):
    if option == 1:
        try:
            fecha_inicio = input("Ingrese la fecha de inicio (YYYY-MM-DD): ")
            fecha_fin = input("Ingrese la fecha de fin (YYYY-MM-DD): ") 
            rows = session.execute("""
                SELECT tipo_actividad, timestamp, activity_id, detalles
                    FROM student_activity
                    WHERE user_email = %s AND timestamp >= %s AND timestamp <= %s
            """, (user_email,fecha_inicio, fecha_fin))
        except Exception as e:
            log.error(f"Error al obtener actividades por rango de fechas: {e}")
            print("Error al obtener actividades por rango de fechas")
            return
    elif option == 2:
        rows = session.execute("""
            SELECT tipo_actividad, timestamp, activity_id, detalles
            FROM student_activity
            WHERE user_email = %s
        """, (user_email,))
    else:
        print("Opción inválida, por favor intente de nuevo")
        return
    
    print("Actividades del estudiante:")
    for row in rows:
        print(f"ID: {row.activity_id}, Tipo: {row.tipo_actividad}, Fecha: {row.timestamp}, Detalles: {row.detalles}")

def view_progress(session, user_email,option):
    if option == 1:
        rows = session.execute("""
            SELECT course_id, progress_percent, grade
            FROM course_progress
            WHERE user_email = %s
        """, (user_email,))
    elif option == 2:
        course = input("Ingrese el ID del curso: ")
        rows = session.execute("""
            SELECT course_id, progress_percent, grade
            FROM course_progress
            WHERE user_email = %s AND course_id = %s
        """, (user_email,course))
    else:
        print("Opción inválida, por favor intente de nuevo")
        return
    
    print(f"Progreso del estudiante {user_email}:")
    for row in rows:
        print(f"Curso: {row.course_id}, Progreso: {row.progress_percent}%, Calificación: {row.grade}")

def view_notifications(session, user_email,option,fecha_inicio=None,fecha_fin=None):
    try:
        if option == 1:
            rows = session.execute("""
                SELECT timestamp, notification_id, course_id, tipo, notificacion
                FROM system_notifications
                WHERE user_email = %s
            """, (user_email,))
        elif option == 2:
            fecha_inicio = input("Ingrese la fecha de inicio (YYYY-MM-DD): ")
            fecha_fin = input("Ingrese la fecha de fin (YYYY-MM-DD): ") 
            rows = session.execute("""
                SELECT timestamp, notification_id, course_id, tipo, notificacion
                FROM system_notifications
                WHERE user_email = %s AND timestamp >= %s AND timestamp <= %s
            """, (user_email,fecha_inicio, fecha_fin))
        else:
            print("Opción inválida, por favor intente de nuevo")
            return
    except Exception as e:
        log.error(f"Error al obtener notificaciones: {e}")
        print("Error al obtener notificaciones")
        return

    
    print(f"Notificaciones del sistema para {user_email}:")
    for row in rows:
        print(f"ID: {row.notification_id}, Fecha: {row.timestamp}, Curso: {row.course_id}, Tipo: {row.tipo}, Mensaje: {row.notificacion}")

def view_user_sessions(session, user_email,option):
    try:
        if option == 1:
            rows = session.execute("""
                SELECT session_id, device_info, last_activity
                FROM user_sessions
                WHERE user_email = %s
            """, (user_email,))
        elif option == 2:
            session_id = input("Ingrese el ID de la sesión: ")
            session_id = uuid.UUID(session_id)
            rows = session.execute("""
                SELECT session_id, device_info, last_activity
                FROM user_sessions
                WHERE user_email = %s AND session_id = %s
            """, (user_email,session_id))
        else:
            print("Opción inválida, por favor intente de nuevo")
            return
    except Exception as e:
        log.error(f"Error al obtener sesiones de usuario: {e}")
        print("Error al obtener sesiones de usuario")
        return
    
    print(f"Sesiones de usuario para {user_email}:")
    for row in rows:
        print(f"ID: {row.session_id}, Dispositivo: {row.device_info}, Última actividad: {row.last_activity}")

def view_certificates(session, user_email,option):
    if option == 1:
        rows = session.execute("""
            SELECT completion_date, certificate_id, course_id, student_name, course_title, certificate_url
            FROM certificates
            WHERE user_email = %s
        """, (user_email,))
    elif option == 2:
        fecha_str = input("Ingrese la fecha del certificado: ")
        try:
            fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
            rows = session.execute("""
                SELECT completion_date, certificate_id, course_id, student_name, course_title, certificate_url
                FROM certificates
                WHERE user_email = %s AND completion_date = %s
            """, (user_email,fecha))
        except Exception as e:
            log.error(f"Error al obtener certificados por fecha: {e}")
            print("Error al obtener certificados por fecha")
            return
    else:
        print("Opción inválida, por favor intente de nuevo")
        return
    
    print("Certificados del estudiante:")
    for row in rows:
        print(f"ID: {row.certificate_id}, Fecha: {row.completion_date}, Curso: {row.course_id}, Nombre: {row.student_name}, Título: {row.course_title}, URL: {row.certificate_url}")


def view_course_progress(session, course_id):
    try:
        rows = session.execute("""
            SELECT user_email, progress_percent, grade
            FROM course_performance
            WHERE course_id = %s
        """, (course_id,))
        
        print("Progreso del curso del estudiante:")
        for row in rows:
            print(f"Email: {row.user_email}, Progreso: {row.progress_percent}%, Calificación: {row.grade}")
    except Exception as e:
        log.error(f"Error al obtener el progreso del curso: {e}")
        print("Error al obtener el progreso del curso")
def view_login_logs(session, user_email,option ,fecha_inicio=None,fecha_fin=None):
    try:
        if option == 1:
            rows = session.execute("""
                SELECT last_activity, session_id, start_time, device_info, active_status
                FROM login_logs
                WHERE user_email = %s
            """, (user_email,))
        elif option == 2:
            fecha_inicio = input("Ingrese la fecha de inicio (YYYY-MM-DD): ")
            fecha_fin = input("Ingrese la fecha de fin (YYYY-MM-DD): ")
            rows = session.execute("""
                SELECT last_activity, session_id, start_time, device_info, active_status
                FROM login_logs
                WHERE user_email = %s AND last_activity >= %s AND last_activity <= %s
            """, (user_email,fecha_inicio, fecha_fin))
        else:
            print("Opción inválida, por favor intente de nuevo")
            return
    except Exception as e:
        log.error(f"Error al obtener logs de inicio de sesión: {e}")
        print("Error al obtener logs de inicio de sesión")
        return
    
    print("Logs de inicio de sesión del estudiante:")
    for row in rows:
        print(f"ID: {row.session_id}, Última actividad: {row.last_activity}, Hora de inicio: {row.start_time}, Dispositivo: {row.device_info}, Estado activo: {row.active_status}")

def view_tasks(session, user_email,option):
    try:
        if option == 1:
            rows = session.execute("""
                SELECT task_id, task_description, due_date, is_completed
                FROM task_reminders
                WHERE user_email = %s
            """, (user_email,))
        elif option == 2:
            task_id = input("Ingrese el ID de la tarea: ")
            task_id = uuid.UUID(task_id)
            rows = session.execute("""
                SELECT task_id, task_description, due_date, is_completed
                FROM task_reminders
                WHERE user_email = %s AND task_id = %s
            """, (user_email,task_id))
        else:
            print("Opción inválida, por favor intente de nuevo")
            return
    except Exception as e:
        log.error(f"Error al obtener recordatorios de tareas: {e}")
        print("Error al obtener recordatorios de tareas")
        return
    
    print("Recordatorios de tareas del estudiante:")
    for row in rows:
        print(f"ID: {row.task_id}, Descripción: {row.task_description}, Fecha de vencimiento: {row.due_date}, Completado: {row.is_completed}")

def view_course_views(session, course):
    rows = session.execute("""
        SELECT view_date, views
        FROM course_views
        WHERE course_id = %s
    """, (course,))
    
    print("Vistas del curso:")
    for row in rows:
        print(f"Fecha: {row.view_date}, Vistas: {row.views}")

def view_Teachers(session):
    rows = session.execute("""
        SELECT instructor_email, instructor_name, avg_rating, total_courses
        FROM top_instructors
    """)

    sorted_rows = sorted(rows, key=lambda row: row.avg_rating, reverse=True)
    
    print("Instructores destacados:")
    if not sorted_rows:
        print("No hay instructores destacados disponibles.")
    else:
        for row in sorted_rows:
            print(f"Email: {row.instructor_email}, Nombre: {row.instructor_name}, Calificación promedio: {row.avg_rating}, Cursos totales: {row.total_courses}")

def delete_user_data(session, user_email):
    log.info(f"Eliminando todos los registros del usuario: {user_email}")
    
    tablas_con_user_email = [
        "student_activity",
        "course_progress",
        "system_notifications",
        "user_sessions",
        "certificates",
        "login_logs",
        "task_reminders"
    ]
    
    for tabla in tablas_con_user_email:
        try:
            query = f"DELETE FROM {tabla} WHERE user_email = %s"
            session.execute(query, (user_email,))
            log.info(f"Eliminados registros de la tabla '{tabla}' para {user_email}")
        except Exception as e:
            log.error(f"Error al eliminar de {tabla}: {e}")
    
    print(f"Todos los registros de {user_email} han sido eliminados.")

def main():
    log.info("Conectando a Cassandra")
    cluster = Cluster(CLUSTER_IPS.split(','), load_balancing_policy=RoundRobinPolicy(), protocol_version=5)
    session = cluster.connect()

    # Crear keyspace si no existe
    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {REPLICATION_FACTOR} }};
    """)
    session.set_keyspace(KEYSPACE)

    # Importar el modelo desde la misma carpeta que este archivo
    import os
    import importlib.util

    script_dir = os.path.dirname(__file__)  # carpeta actual de app.py
    model_path = os.path.join(script_dir, "model.py")

    spec = importlib.util.spec_from_file_location("model", model_path)
    model = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(model)

    # Ya no se ejecuta model.create_schema(session)
    # Ya no se borra la tabla course_performance

    user_email = set_user_email()

    while True:
        print_menu()
        option = int(input('Ingrese su opción: '))

        if option == 0:
            populate_data(session, user_email)
        elif option == 1:
            print("Seleccione una opción:")
            print("1. Ver actividades de estudiante por rango de fechas")
            print("2. Ver todas las actividades de estudiante")
            option = int(input('Ingrese su opción: '))
            view_activities(session, user_email, option)
        elif option == 2:
            print("Seleccione una opción:")
            print("1. Ver progreso de estudiante de todos los cursos")
            print("2. Ver progreso de estudiante por curso especifico")
            option = int(input('Ingrese su opción: '))
            view_progress(session, user_email, option)
        elif option == 3:
            print("Seleccione una opción:")
            print("1. Ver notificaciones del sistema")
            print("2. Ver notificaciones por rango de fecha")
            option = int(input('Ingrese su opción: '))
            view_notifications(session, user_email, option)
        elif option == 4:
            print("Seleccione una opción:")
            print("1. Ver sesiones de usuario")
            print("2. Ver sesiones de usuario por id")
            option = int(input('Ingrese su opción: '))
            view_user_sessions(session, user_email, option)
        elif option == 5:
            print("Seleccione una opción:")
            print("1. Ver certificados de estudiante")
            print("2. Ver certificados por fecha")
            option = int(input('Ingrese su opción: '))
            view_certificates(session, user_email, option)
        elif option == 6:
            course = input("Ingrese el ID del curso: ")
            view_course_progress(session, course)
        elif option == 7:
            print("Seleccione una opción:")
            print("1. Ver logs de inicio de sesión")
            print("2. Ver logs de inicio de sesión por fecha")
            option = int(input('Ingrese su opción: '))
            view_login_logs(session, user_email, option)
        elif option == 8:
            print("Seleccione una opción:")
            print("1. Ver recordatorios de tareas")
            print("2. Ver recordatorios de tareas por id")
            option = int(input('Ingrese su opción: '))
            view_tasks(session, user_email, option)
        elif option == 9:
            course = input("Ingrese el ID del curso: ")
            view_course_views(session, course)
        elif option == 10:
            view_Teachers(session)
        elif option == 11:
            user_email = set_user_email()
        elif option == 12:
            delete_user_data(session, user_email)
        elif option == 13:
            print("¡Hasta luego!")
            return
        else:
            print("Opción inválida, por favor intente de nuevo")

if __name__ == '__main__':
    main()

