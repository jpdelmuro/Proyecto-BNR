import csv
from datetime import datetime, timedelta
import uuid

# Datos base
first_names = ["Mike", "Anna", "Li", "Carlos", "Maria", "Luis", "Fernanda", "Diego", "Sofia", "Juan", 
               "Isabel", "Andres", "Valeria", "Pablo", "Camila", "Jorge", "Diana", "Pedro", "Lucia", 
               "Ricardo", "Elena", "Hector", "Alejandra", "Raul", "Julieta", "Emilio", "Angela", 
               "Ruben", "Paola", "Martin", "Teresa", "Francisco", "Laura", "Manuel", "Viviana", 
               "Tomas", "Daniela", "Oscar", "Marina", "Ivan", "Rocio", "Alfredo", "Beatriz"]

students = [(f"{name.lower()}@iteso.mx", name) for name in first_names]

courses = [
    "Sistemas_Operativos", "Bases_de_Datos", "Redes_de_Computadoras", "Programacion_Avanzada",
    "Inteligencia_Artificial", "Ingenieria_de_Software", "Seguridad_Informatica", "Arquitectura_de_Computadoras",
    "Matematicas_Discretas", "Desarrollo_Web"
]

instructor_names = [
    "Jose", "Miguel", "Paty", "Luis", "Ana", "Carlos", "Lucia", "Eduardo", "Raquel", "Hugo",
    "Beatriz", "Mario", "Sandra", "Tomas", "Veronica", "Daniel", "Cecilia", "Javier", "Nora", "Felipe"
]
instructors = [(name, f"{name.lower()}@iteso.mx") for name in instructor_names]

activity_types = ["QUIZ", "TAREA", "FORO", "LECTURA", "VIDEO", "PROYECTO", "EVALUACION"]
devices = ["Macbook", "iPhone", "Windows Laptop", "Android Tablet", "iPad", "Linux PC", "Chromebook"]
descriptions = {
    "QUIZ": "Completo el quiz",
    "TAREA": "Entrego tarea",
    "FORO": "Participó en foro",
    "LECTURA": "Leyo el material",
    "VIDEO": "Vio el video completo",
    "PROYECTO": "Subio proyecto final",
    "EVALUACION": "Presento evaluación final"
}

# --- Definir dos grupos de alumnos ---
group_a_students = students[:10]   # primeros 10 alumnos para primeros 5 cursos
group_b_students = students[10:20] # siguientes 10 alumnos para últimos 5 cursos

# --- Asignar alumnos según grupos ---
course_students = {}
for i, course in enumerate(courses):
    if i < 5:
        course_students[course] = group_a_students
    else:
        course_students[course] = group_b_students

# --- Asignar 2 instructores por curso sin repetir ---
course_instructors = {}
start_idx = 0
for course in courses:
    course_instructors[course] = instructors[start_idx:start_idx+2]
    start_idx += 2

fixed_activities = activity_types[:5]

rows = []
base_time = datetime(2024, 10, 10, 8, 0, 0)

for course in courses:
    for student_idx, student in enumerate(course_students[course]):
        num_activities = 2
        for i in range(num_activities):
            activity = activity_types[i % len(activity_types)]
            email, name = student
            timestamp = base_time + timedelta(days=student_idx*2 + i, hours=8, minutes=0)
            activity_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{email}-{course}-{activity}-{i}"))
            detalles = f"{descriptions[activity]} del curso {course}"
            progress = 50 + (student_idx % 10) * 5
            grade = 6.0 + i * 0.8
            device = devices[(student_idx + i) % len(devices)]

            teacher_name, teacher_email = course_instructors[course][student_idx % 2]
            teacher_avg = 7.0 + (student_idx * 1.5)

            rows.append([
                email, course, activity, timestamp.isoformat() + "Z", activity_id, detalles,
                progress, grade, device, teacher_name, teacher_email, round(teacher_avg, 1)
            ])

# Ruta del csv 
output_path = r"C:\Users\sebas\OneDrive\Documentos\estructurada\NoSQL\proyecto\ultimo.csv"
with open(output_path, "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["user_email", "course_id", "tipo_actividad", "timestamp", "activity_id", "detalles",
                     "progress_percent", "grade", "device_info", "teacher_name", "teacher_email", "teacher_avg"])
    writer.writerows(rows)
