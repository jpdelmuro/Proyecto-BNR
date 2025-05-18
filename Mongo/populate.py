import csv
from pymongo import MongoClient
from datetime import datetime

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["ProyectoMongoPython"]

# Limpiar colecciones si ya existen
db.usuarios.drop()
db.cursos.drop()
db.lecciones.drop()
db.instructores.drop()

# 1. Insertar USUARIOS
with open("csv/usuarios.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    usuarios = []
    for row in reader:
        doc = {
            "user_id": row["user_id"],
            "name": row["name"],
            "email": row["email"],
            "password": row["password"],
            "courses_enrolled": [row["courses_enrolled"]] if row["courses_enrolled"] else [],
            "courses_completed": [],
            "created_at": datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
        }
        if row["completed_course_id"]:
            doc["courses_completed"].append({
                "course_id": row["completed_course_id"],
                "score": float(row["score"])
            })
        usuarios.append(doc)
    db.usuarios.insert_many(usuarios)

# 2. Insertar CURSOS
with open("csv/cursos.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    cursos = []
    for row in reader:
        doc = {
            "course_id": row["course_id"],
            "title": row["title"],
            "description": row["description"],
            "teacher_id": row["teacher_id"],
            "lessons": row["lessons"].split(";") if row["lessons"] else [],
            "created_at": datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")),
            "rating": float(row["rating"]),
            "category": row["category"]
        }
        cursos.append(doc)
    db.cursos.insert_many(cursos)

# 3. Insertar LECCIONES
with open("csv/lecciones.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    lecciones = []
    for row in reader:
        doc = {
            "lesson_id": row["lesson_id"],
            "course_id": row["course_id"],
            "title": row["title"],
            "content": row["content"],
            "duration": int(row["duration"]),
            "resources": [row["resource"]] if row["resource"] else [],
            "comments": []
        }
        if row["comment_user_id"]:
            doc["comments"].append({
                "user_id": row["comment_user_id"],
                "comment": row["comment"],
                "timestamp": datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
            })
        lecciones.append(doc)
    db.lecciones.insert_many(lecciones)

# 4. Insertar INSTRUCTORES
with open("csv/instructores.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    instructores = []
    for row in reader:
        doc = {
            "user_id": row["user_id"],
            "name": row["name"],
            "email": row["email"],
            "password": row["password"],
            "courses_list": row["courses_list"].split(";") if row["courses_list"] else [],
            "created_at": datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
        }
        instructores.append(doc)
    db.instructores.insert_many(instructores)

# 5. Crear Índices
# USUARIOS
db.usuarios.create_index({"email": 1})
db.usuarios.create_index({"name": 1})

# CURSOS
db.cursos.create_index({"title": "text"})
db.cursos.create_index({"teacher_id": 1})
db.cursos.create_index({"category": 1, "rating": -1})

# LECCIONES
db.lecciones.create_index({"course_id": 1})
db.lecciones.create_index({"comments.user_id": 1})

# INSTRUCTORES
db.instructores.create_index({"user_id": 1})

print("Datos cargados exitosamente en la base 'ProyectoMongoPython'")
