import csv
import random
from datetime import datetime, timedelta
import uuid

# Datos base
first_names = ["Mike", "Anna", "Li", "Carlos", "Maria", "Luis", "Fernanda", "Diego", "Sofia", "Juan", 
               "Isabel", "Andres", "Valeria", "Pablo", "Camila", "Jorge", "Diana", "Pedro", "Lucia", 
               "Ricardo", "Elena", "Hector", "Alejandra", "Raul", "Julieta", "Emilio", "Angela", 
               "Ruben", "Paola", "Martin", "Teresa", "Francisco", "Laura", "Manuel", "Viviana", 
               "Tomas", "Daniela", "Oscar", "Marina", "Ivan", "Rocio", "Alfredo", "Beatriz"]

# Crear lista de estudiantes
random.shuffle(first_names)
students = [(f"{name.lower()}@iteso.mx", name) for name in first_names]

# 10 cursos
courses = [
    "math_101", "ethics_201", "environment_301", "physics_102", "history_202",
    "biology_303", "literature_101", "philosophy_204", "art_105", "programming_110"
]

# 20 instructores
instructor_names = [
    "Jose", "Miguel", "Paty", "Luis", "Ana", "Carlos", "Lucia", "Eduardo", "Raquel", "Hugo",
    "Beatriz", "Mario", "Sandra", "Tomas", "Veronica", "Daniel", "Cecilia", "Javier", "Nora", "Felipe"
]
instructors = [(name, f"{name.lower()}@iteso.mx") for name in instructor_names]

activity_types = ["QUIZ", "TAREA", "FORO", "LECTURA", "VIDEO", "PROYECTO", "EVALUACION"]
devices = ["Macbook", "iPhone", "Windows Laptop", "Android Tablet", "iPad", "Linux PC", "Chromebook"]
descriptions = {
    "QUIZ": "Completó el quiz",
    "TAREA": "Entregó tarea",
    "FORO": "Participó en foro",
    "LECTURA": "Leyó el material",
    "VIDEO": "Vio el video completo",
    "PROYECTO": "Subió proyecto final",
    "EVALUACION": "Presentó evaluación final"
}

# Asignar 4 estudiantes a cada curso
course_students = {}
student_idx = 0
for course in courses:
    course_students[course] = []
    # Asignar 4 estudiantes a este curso
    for _ in range(4):
        if student_idx < len(students):
            course_students[course].append(students[student_idx])
            student_idx += 1
        else:
            # Si no hay suficientes estudiantes, volver a usar algunos
            course_students[course].append(random.choice(students))

# Asignar 2 maestros a cada curso
course_instructors = {}
for course in courses:
    course_instructors[course] = random.sample(instructors, 2)

# Generar 5 actividades por estudiante en cada curso
rows = []
base_time = datetime(2024, 10, 10, 8, 0, 0)

for course in courses:
    for student in course_students[course]:
        # Obtener actividades aleatorias para este estudiante (5 actividades)
        activities = random.sample(activity_types, 5) if len(activity_types) >= 5 else random.choices(activity_types, k=5)
        
        for activity in activities:
            email, name = student
            timestamp = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))
            activity_id = str(uuid.uuid4())
            detalles = f"{descriptions[activity]} del curso {course}"
            progress = random.randint(50, 100)
            grade = round(random.uniform(6.0, 10.0), 1)
            device = random.choice(devices)
            
            # Seleccionar uno de los dos maestros asignados al curso
            teacher_name, teacher_email = random.choice(course_instructors[course])
            teacher_avg = round(random.uniform(7.0, 10.0), 1)
            
            rows.append([
                email, course, activity, timestamp.isoformat() + "Z", activity_id, detalles,
                progress, grade, device, teacher_name, teacher_email, teacher_avg
            ])

# Guardar en CSV
output_path = r"C:\Users\sebas\OneDrive\Documentos\estructurada\NoSQL\proyecto\output_structured.csv"
with open(output_path, "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["user_email", "course_id", "tipo_actividad", "timestamp", "activity_id", "detalles",
                     "progress_percent", "grade", "device_info", "teacher_name", "teacher_email", "teacher_avg"])
    writer.writerows(rows)
