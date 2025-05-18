from mongo_module.resources import (
    crear_usuario,
    cambiar_contrasena,
    consultar_perfil,
    buscar_cursos_por_titulo,
    cursos_por_profesor_nombre,
    cursos_inscritos_por_usuario,
    obtener_lecciones_de_curso,
    crear_curso,
    perfil_profesor_con_cursos,
    comentarios_por_usuario,
    total_cursos_completados,
)
from datetime import datetime


def menu_consultas_mongo(db):
    while True:
        print("\n=== CONSULTAS MONGODB ===")
        print("1. Crear usuario")
        print("2. Cambiar contraseña")
        print("3. Consultar perfil")
        print("4. Buscar curso por título (usa $text)")
        print("5. Buscar cursos por profesor")
        print("6. Cursos en los que está inscrito un usuario")
        print("7. Ver lecciones de un curso")
        print("8. Crear nuevo curso")
        print("9. Ver perfil de profesor")
        print("10. Comentarios de un usuario")
        print("11. Total de cursos completados por usuario")
        print("0. Volver al menú principal")
        opcion = input("Seleccione una opción: ")

        from shared.sync import sync_users_across_dbs  # ← asegúrate de importar esto

# ...

        if opcion == "1":
            email = input("Email: ")
            usuarios = list(db.usuarios.find({}, {"user_id": 1}))
            ids = [int(u["user_id"][1:]) for u in usuarios if u["user_id"].startswith("u")]
            nuevo_id = f"u{max(ids)+1:03}" if ids else "u001"

            user = {
                "user_id": nuevo_id,
                "name": input("Nombre: "),
                "email": email,
                "password": input("Contraseña: "),
                "courses_enrolled": [],
                "courses_completed": [],
                "created_at": datetime.utcnow()
            }
            crear_usuario(db, user)
            print(f"Usuario creado con ID automático: {nuevo_id}")

            # Sincronización automática tras creación
            print("→ Sincronizando usuario con Cassandra y Dgraph...")
            sync_users_across_dbs()
            print("✓ Usuario sincronizado.")


        elif opcion == "2":
            email = input("Email: ")
            nueva = input("Nueva contraseña: ")
            cambiar_contrasena(db, email, nueva)
            print("Contraseña actualizada.")

            print("→ Sincronizando usuario con Cassandra y Dgraph...")
            sync_users_across_dbs()
            print("✓ Usuario sincronizado.")


        elif opcion == "3":
            email = input("Email del usuario: ")
            perfil = consultar_perfil(db, email)
            if perfil:
                print("→ Perfil:")
                for clave, valor in perfil.items():
                    print(f"{clave}: {valor}")
            else:
                print("Usuario no encontrado.")

        elif opcion == "4":
            texto = input("Texto a buscar en título: ")
            resultados = buscar_cursos_por_titulo(db, texto)
            if not resultados:
                print("No se encontraron cursos.")
            for curso in resultados:
                print("\n--- Curso ---")
                print(f"ID: {curso.get('course_id')}")
                print(f"Título: {curso.get('title')}")
                print(f"Descripción: {curso.get('description')}")
                print(f"Profesor ID: {curso.get('teacher_id')}")
                print(f"Lecciones: {', '.join(curso.get('lessons', []))}")
                print(f"Fecha de creación: {curso.get('created_at').strftime('%d/%m/%Y %H:%M')}")
                print(f"Calificación: {curso.get('rating')}")
                print(f"Categoría: {curso.get('category')}")

        elif opcion == "5":
            nombre = input("Nombre del profesor: ")
            nombre_profesor, cursos = cursos_por_profesor_nombre(db, nombre)
            if nombre_profesor is None:
                print("No se encontró ningún profesor con ese nombre.")
            elif not cursos:
                print(f"{nombre_profesor} no tiene cursos asignados.")
            else:
                print(f"Cursos del profesor {nombre_profesor}:")
                for curso in cursos:
                    print("\n--- Curso ---")
                    print(f"ID: {curso.get('course_id')}")
                    print(f"Título: {curso.get('title')}")
                    print(f"Descripción: {curso.get('description')}")
                    print(f"Fecha: {curso.get('created_at').strftime('%d/%m/%Y %H:%M')}")
                    print(f"Rating: {curso.get('rating')}")
                    print(f"Categoría: {curso.get('category')}")

        elif opcion == "6":
            nombre = input("Nombre del usuario: ")
            nombre_usuario, cursos = cursos_inscritos_por_usuario(db, nombre)
            if nombre_usuario is None:
                print("No se encontró ningún usuario con ese nombre.")
            elif not cursos:
                print(f"{nombre_usuario} no está inscrito en ningún curso.")
            else:
                print(f"Cursos inscritos por {nombre_usuario}:")
                for curso in cursos:
                    print("\n--- Curso inscrito ---")
                    print(f"ID: {curso['course_id']}")
                    print(f"Título: {curso['title']}")
                    print(f"Instructor ID: {curso['instructor']}")
                    print(f"Fecha: {curso['created_at'].strftime('%d/%m/%Y %H:%M')}")

        elif opcion == "7":
            titulo = input("Nombre del curso: ")
            titulo_curso, lecciones = obtener_lecciones_de_curso(db, titulo)
            if titulo_curso is None:
                print("No se encontró el curso.")
            elif not lecciones:
                print(f"El curso '{titulo_curso}' no tiene lecciones.")
            else:
                print(f"Lecciones del curso '{titulo_curso}':")
                for lec in lecciones:
                    print(f"- {lec['title']} ({lec['duration']} minutos)")

        elif opcion == "8":
            nombre_profesor = input("Nombre del profesor: ")
            titulo = input("Título del curso: ")
            descripcion = input("Descripción: ")
            categoria = input("Categoría: ")
            nuevo_curso, error = crear_curso(db, titulo, descripcion, categoria, nombre_profesor)
            if error:
                print(f"Error: {error}")
            else:
                print("\nCurso creado exitosamente:")
                print(f"ID: {nuevo_curso['course_id']}")
                print(f"Título: {nuevo_curso['title']}")
                print(f"Profesor ID: {nuevo_curso['teacher_id']}")
                print(f"Categoría: {nuevo_curso['category']}")
                print(f"Fecha: {nuevo_curso['created_at'].strftime('%d/%m/%Y %H:%M')}")

        elif opcion == "9":
            nombre = input("Nombre del profesor: ")
            perfil = perfil_profesor_con_cursos(db, nombre)
            if not perfil:
                print("Profesor no encontrado.")
            else:
                print(f"Nombre: {perfil['nombre']}")
                print(f"Email: {perfil['email']}")
                print("Cursos impartidos:")
                for curso in perfil.get("cursos_impartidos", []):
                    print(f"- {curso['title']} ({curso['category']}) - Rating: {curso['rating']}")

        elif opcion == "10":
            nombre = input("Nombre del usuario: ")
            nombre_usuario, comentarios = comentarios_por_usuario(db, nombre)
            if nombre_usuario is None:
                print("Usuario no encontrado.")
            elif not comentarios:
                print(f"{nombre_usuario} no ha hecho comentarios aún.")
            else:
                print(f"\nComentarios de {nombre_usuario}:")
                for c in comentarios:
                    fecha_str = c['fecha'].strftime('%d/%m/%Y %H:%M') if c.get('fecha') else "Sin fecha"
                    print(f"- [{fecha_str}] En '{c['leccion']}': {c['comentario']}")

        elif opcion == "11":
            nombre = input("Nombre del usuario: ")
            nombre_usuario, total = total_cursos_completados(db, nombre)
            if nombre_usuario is None:
                print("Usuario no encontrado.")
            else:
                print(f"{nombre_usuario} ha completado {total} curso(s).")

        elif opcion == "0":
            break

        else:
            print("Opción no válida.")
