from pymongo.database import Database
from datetime import datetime

# Crear un usuario
def crear_usuario(db: Database, user_data: dict):
    return db.usuarios.insert_one(user_data)

# Cambiar contraseña por email
def cambiar_contrasena(db: Database, email: str, nueva_contrasena: str):
    return db.usuarios.update_one(
        {"email": email},
        {"$set": {"password": nueva_contrasena}}
    )

# Consultar perfil de usuario (sin contraseña)
def consultar_perfil(db: Database, email: str):
    return db.usuarios.find_one(
        {"email": email},
        {"password": 0}
    )

# Buscar cursos por título 
def buscar_cursos_por_titulo(db, texto):
    stop_words = {"de", "la", "el", "y", "en", "los", "las", "no", "por", "a", "del", "al"}
    texto_limpio = " ".join([palabra for palabra in texto.split() if palabra.lower() not in stop_words])
    
    resultados = db.cursos.find({ "$text": { "$search": texto_limpio } })
    return list(resultados)


# Buscar cursos por profesor
def cursos_por_profesor_nombre(db, nombre_profesor):
    profe = db.instructores.find_one({ "name": { "$regex": nombre_profesor, "$options": "i" } })
    if not profe:
        return None, []

    cursos = list(db.cursos.find({ "teacher_id": profe["user_id"] }))
    return profe["name"], cursos


# Obtener cursos en los que está inscrito un usuario
def cursos_inscritos_por_usuario(db, nombre_usuario):
    user = db.usuarios.find_one({ "name": { "$regex": nombre_usuario, "$options": "i" } })
    if not user:
        return None, []
    
    pipeline = [
        { "$match": { "user_id": user["user_id"] } },
        { "$unwind": "$courses_enrolled" },
        {
            "$lookup": {
                "from": "cursos",
                "localField": "courses_enrolled",
                "foreignField": "course_id",
                "as": "curso_info"
            }
        },
        { "$unwind": "$curso_info" },
        {
            "$project": {
                "_id": 0,
                "course_id": "$courses_enrolled",
                "title": "$curso_info.title",
                "instructor": "$curso_info.teacher_id",
                "created_at": "$curso_info.created_at"
            }
        }
    ]
    cursos = list(db.usuarios.aggregate(pipeline))
    return user["name"], cursos


# Obtener lecciones de un curso
def obtener_lecciones_de_curso(db, titulo):
    curso = db.cursos.find_one({ "title": { "$regex": titulo, "$options": "i" } })
    if not curso:
        return None, []

    lecciones = list(db.lecciones.find(
        { "course_id": curso["course_id"] },
        { "_id": 0, "title": 1, "duration": 1 }
    ))
    return curso["title"], lecciones


# Crear curso nuevo
def crear_curso(db, titulo, descripcion, categoria, nombre_profesor):
    profesor = db.instructores.find_one({ "name": { "$regex": nombre_profesor, "$options": "i" } })
    if not profesor:
        return None, "Profesor no encontrado."

    # Generar nuevo ID basado en el más alto
    cursos = list(db.cursos.find({}, { "course_id": 1 }))
    ids = [int(c["course_id"][1:]) for c in cursos if c["course_id"].startswith("c")]
    nuevo_id = f"c{max(ids)+1:03}" if ids else "c001"

    nuevo_curso = {
        "course_id": nuevo_id,
        "title": titulo,
        "description": descripcion,
        "teacher_id": profesor["user_id"],
        "lessons": [],
        "created_at": datetime.utcnow(),
        "rating": 0,
        "category": categoria
    }

    db.cursos.insert_one(nuevo_curso)

    # También actualiza al profesor
    db.instructores.update_one(
        { "user_id": profesor["user_id"] },
        { "$push": { "courses_list": nuevo_id } }
    )

    return nuevo_curso, None


# Consultar perfil de profesor y cursos que imparte
def perfil_profesor_con_cursos(db, nombre_profesor):
    profesor = db.instructores.find_one({ "name": { "$regex": nombre_profesor, "$options": "i" } })
    if not profesor:
        return None, []

    pipeline = [
        { "$match": { "user_id": profesor["user_id"] } },
        {
            "$lookup": {
                "from": "cursos",
                "localField": "user_id",
                "foreignField": "teacher_id",
                "as": "cursos_impartidos"
            }
        },
        {
            "$project": {
                "_id": 0,
                "nombre": "$name",
                "email": 1,
                "course_rating": 1,
                "cursos_impartidos": {
                    "$map": {
                        "input": "$cursos_impartidos",
                        "as": "curso",
                        "in": {
                            "title": "$$curso.title",
                            "category": "$$curso.category",
                            "rating": "$$curso.rating"
                        }
                    }
                }
            }
        }
    ]

    resultado = list(db.instructores.aggregate(pipeline))
    return resultado[0] if resultado else None


# Obtener todos los comentarios de un alumno
def comentarios_por_usuario(db, nombre_usuario):
    user = db.usuarios.find_one({ "name": { "$regex": nombre_usuario, "$options": "i" } })
    if not user:
        return None, []

    pipeline = [
        { "$unwind": "$comments" },
        { "$match": { "comments.user_id": user["user_id"] } },
        {
            "$project": {
                "_id": 0,
                "leccion": "$title",
                "comentario": "$comments.comment",
                "fecha": "$comments.timestamp"
            }
        },
        { "$sort": { "fecha": -1 } }
    ]

    resultados = list(db.lecciones.aggregate(pipeline))
    return user["name"], resultados


# Total de cursos completados por usuario
def total_cursos_completados(db, nombre_usuario):
    user = db.usuarios.find_one({ "name": { "$regex": nombre_usuario, "$options": "i" } })
    if not user:
        return None, 0

    pipeline = [
        { "$match": { "user_id": user["user_id"] } },
        {
            "$project": {
                "_id": 0,
                "total": { "$size": "$courses_completed" }
            }
        }
    ]

    resultado = list(db.usuarios.aggregate(pipeline))
    total = resultado[0]["total"] if resultado else 0
    return user["name"], total


