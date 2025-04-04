import argparse
import logging
import os
import requests
import json

# Configurar el logger
log = logging.getLogger()
log.setLevel(logging.INFO)
handler = logging.FileHandler('students.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Leer variable de entorno o usar localhost por defecto
STUDENTS_API_URL = os.getenv("STUDENTS_API_URL", "http://localhost:8000/students")

# Función: imprimir estudiante bonito
def prints(student_id):
    student = get_data(student_id)
    if student:
        print(json.dumps(student, indent=4))
    else:
        print("Estudiante no encontrado.")

# Función: listar todos los estudiantes
def list_(_):
    try:
        res = requests.get(STUDENTS_API_URL)
        res.raise_for_status()
        students = res.json()
        print(json.dumps(students, indent=2))
    except Exception as e:
        log.error(f"Error al listar estudiantes: {e}")
        print("No se pudieron obtener los estudiantes.")

# Función: obtener info de un estudiante
def get_data(student_id):
    try:
        res = requests.get(f"{STUDENTS_API_URL}/{student_id}")
        if res.status_code == 200:
            return res.json()
        else:
            log.warning(f"Estudiante {student_id} no encontrado")
            return None
    except Exception as e:
        log.error(f"Error al obtener estudiante {student_id}: {e}")
        return None

# Función: actualizar un estudiante
def update(student_id):
    current = get_data(student_id)
    if not current:
        print("Estudiante no encontrado.")
        return

    print("Información actual:")
    print(json.dumps(current, indent=2))

    print("\nIntroduce nuevos datos (deja en blanco para mantener el actual):")
    name = input(f"Nombre [{current['name']}]: ") or current['name']
    email = input(f"Email [{current['email']}]: ") or current['email']

    data = {
        "name": name,
        "email": email
        # Agrega más campos si los usas
    }

    try:
        res = requests.put(f"{STUDENTS_API_URL}/{student_id}", json=data)
        if res.status_code == 200:
            print("Estudiante actualizado.")
        else:
            print("No se pudo actualizar el estudiante.")
    except Exception as e:
        log.error(f"Error al actualizar estudiante {student_id}: {e}")

# Función: eliminar un estudiante
def delete(student_id):
    try:
        res = requests.delete(f"{STUDENTS_API_URL}/{student_id}")
        if res.status_code == 200:
            print("Estudiante eliminado.")
        else:
            print("No se pudo eliminar el estudiante.")
    except Exception as e:
        log.error(f"Error al eliminar estudiante {student_id}: {e}")

# Función principal para parsear los comandos
def main():
    parser = argparse.ArgumentParser(description="Cliente CLI para Students API")
    parser.add_argument("action", choices=["list", "get", "update", "delete", "print"], help="Acción a realizar")
    parser.add_argument("--id", help="ID del estudiante")

    args = parser.parse_args()

    actions = {
        "list": list_,
        "get": lambda _: print(get_data(args.id)),
        "update": update,
        "delete": delete,
        "print": prints
    }

    if args.action != "list" and not args.id:
        print("Se requiere --id para esta acción.")
        return

    actions[args.action](args.id)

if __name__ == "__main__":
    main()
