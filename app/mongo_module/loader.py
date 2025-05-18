import os
import csv
from pymongo import MongoClient
from datetime import datetime


def connect_to_mongo():
    client = MongoClient("mongodb://localhost:27017")
    return client["ProyectoMongoPython"]


def load_csv_data(filepath):
    with open(filepath, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def load_mongo_data(csv_dir):
    db = connect_to_mongo()

    # Limpiar colecciones existentes
    db.usuarios.drop()
    db.cursos.drop()
    db.lecciones.drop()
    db.instructores.drop()

    # === USUARIOS ===
    usuarios_data = load_csv_data(os.path.join(csv_dir, "usuariosM.csv"))
    usuarios = []
    for row in usuarios_data:
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

    # === CURSOS ===
    cursos_data = load_csv_data(os.path.join(csv_dir, "cursosM.csv"))
    cursos = []
    for row in cursos_data:
        cursos.append({
            "course_id": row["course_id"],
            "title": row["title"],
            "description": row["description"],
            "teacher_id": row["teacher_id"],
            "lessons": row["lessons"].split(";") if row["lessons"] else [],
            "created_at": datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")),
            "rating": float(row["rating"]),
            "category": row["category"]
        })
    db.cursos.insert_many(cursos)

    # === LECCIONES ===
    lecciones_data = load_csv_data(os.path.join(csv_dir, "leccionesM.csv"))
    lecciones = []
    for row in lecciones_data:
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

    # === INSTRUCTORES ===
    instructores_data = load_csv_data(os.path.join(csv_dir, "instructoresM.csv"))
    instructores = []
    for row in instructores_data:
        instructores.append({
            "user_id": row["user_id"],
            "name": row["name"],
            "email": row["email"],
            "password": row["password"],
            "courses_list": row["courses_list"].split(";") if row["courses_list"] else [],
            "created_at": datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
        })
    db.instructores.insert_many(instructores)

    print("Datos de MongoDB cargados correctamente.")
