# cassandra_module/queries.py

from cassandra.cluster import Cluster
from datetime import datetime
import uuid

KEYSPACE = "plataforma_online"

def conectar():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect(KEYSPACE)
    return session

def print_menu():
    opciones = [
        "Ver actividades de estudiante",
        "Ver progreso de estudiante",
        "Ver notificaciones del sistema",
        "Ver sesiones de usuario",
        "Ver certificados",
        "Ver progreso del curso",
        "Ver logs de inicio de sesión",
        "Ver recordatorios de tareas",
        "Ver vistas del curso",
        "Ver instructores destacados",
        "Cambiar email de usuario",
        "Eliminar todos los registros del usuario",
        "Volver al menú principal"
    ]
    print("\n=== CONSULTAS CASSANDRA ===")
    for i, opcion in enumerate(opciones, 1):
        print(f"{i}. {opcion}")

def run_queries():
    session = conectar()
    while True:
        print_menu()
        opcion = input("Seleccione una opción: ").strip()

        if opcion == "1":
            email = input("Email del usuario: ")
            rows = session.execute("SELECT tipo_actividad, timestamp, detalles FROM student_activity WHERE user_email=%s", [email])
            for row in rows:
                print(row)

        elif opcion == "2":
            email = input("Email del usuario: ")
            rows = session.execute("SELECT course_id, progress_percent, grade FROM course_progress WHERE user_email=%s", [email])
            for row in rows:
                print(row)

        elif opcion == "3":
            email = input("Email del usuario: ")
            rows = session.execute("SELECT timestamp, tipo, notificacion FROM system_notifications WHERE user_email=%s", [email])
            for row in rows:
                print(row)

        elif opcion == "4":
            email = input("Email del usuario: ")
            rows = session.execute("SELECT session_id, device_info, last_activity FROM user_sessions WHERE user_email=%s", [email])
            for row in rows:
                print(row)

        elif opcion == "5":
            email = input("Email del usuario: ")
            rows = session.execute("SELECT certificate_id, course_id, course_title FROM certificates WHERE user_email=%s", [email])
            for row in rows:
                print(row)

        elif opcion == "6":
            course = input("ID del curso: ")
            rows = session.execute("SELECT user_email, progress_percent, grade FROM course_performance WHERE course_id=%s", [course])
            for row in rows:
                print(row)

        elif opcion == "7":
            email = input("Email del usuario: ")
            rows = session.execute("SELECT session_id, start_time, active_status FROM login_logs WHERE user_email=%s", [email])
            for row in rows:
                print(row)

        elif opcion == "8":
            email = input("Email del usuario: ")
            rows = session.execute("SELECT task_id, task_description, due_date, is_completed FROM task_reminders WHERE user_email=%s", [email])
            for row in rows:
                print(row)

        elif opcion == "9":
            course = input("ID del curso: ")
            rows = session.execute("SELECT view_date, views FROM course_views WHERE course_id=%s", [course])
            for row in rows:
                print(row)

        elif opcion == "10":
            rows = session.execute("SELECT instructor_email, instructor_name, avg_rating, total_courses FROM top_instructors")
            for row in rows:
                print(row)

        elif opcion == "11":
            old_email = input("Email actual del usuario: ")
            new_email = input("Nuevo email del usuario: ")
            tablas = [
                "student_activity", "course_progress", "system_notifications",
                "user_sessions", "certificates", "course_performance",
                "login_logs", "task_reminders"
            ]
            for tabla in tablas:
                session.execute(f"UPDATE {tabla} SET user_email=%s WHERE user_email=%s", [new_email, old_email])
            print("✓ Email actualizado.")

        elif opcion == "12":
            email = input("Email del usuario a eliminar: ")
            tablas = [
                "student_activity", "course_progress", "system_notifications",
                "user_sessions", "certificates", "course_performance",
                "login_logs", "task_reminders"
            ]
            for tabla in tablas:
                session.execute(f"DELETE FROM {tabla} WHERE user_email=%s", [email])
            print("✓ Datos del usuario eliminados.")

        elif opcion == "13":
            break

        else:
            print("Opción no válida.")
