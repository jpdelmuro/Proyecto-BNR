import csv
import pydgraph
import json

# === ESTABLISH DGRAPH CLIENT ===
client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)

# === SET SCHEMA ===
def set_schema(client):
    schema = """
    type Usuario {
        nombre
        amigos
        inscritoEn
        completado
    }

    type Instructor {
        nombre
        cursos
    }

    type Curso {
        titulo
        categoria
        lecciones
        creadoPor
    }

    type Leccion {
        titulo
        curso
        interacciones
    }

    type Interaccion {
        tipo
        contenido
        fecha
        leccion
        usuario
        instructor
    }

    nombre: string @index(exact) .
    titulo: string @index(term, fulltext) .
    categoria: string @index(trigram, hash) .
    tipo: string @index(hash) .
    contenido: string .
    fecha: datetime .

    amigos: [uid] @reverse .
    inscritoEn: [uid] @reverse .
    completado: [uid] @reverse .
    cursos: [uid] @reverse .
    lecciones: [uid] @reverse .
    creadoPor: uid @reverse .
    curso: uid @reverse .
    interacciones: [uid] @reverse .
    leccion: uid @reverse .
    usuario: uid @reverse .
    instructor: uid @reverse .
    """
    return client.alter(pydgraph.Operation(schema=schema))

# === DROP ALL ===
def drop_all(client):
    client.alter(pydgraph.Operation(drop_all=True))
    print("Successfully dropped all data and schema")

# === LOAD FUNCTIONS ===
def load_usuarios(file_path):
    print("Cargando usuarios desde:", file_path)
    txn = client.txn()
    try:
        usuarios = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                usuarios.append({
                    'uid': '_:' + row['nombre'],
                    'dgraph.type': 'Usuario',
                    'nombre': row['nombre']
                })
            resp = txn.mutate(set_obj=usuarios)
        txn.commit()
        print("✓ Usuarios cargados")
        return resp.uids
    finally:
        txn.discard()

def load_instructores(file_path):
    print("Cargando instructores desde:", file_path)
    txn = client.txn()
    try:
        instructores = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                instructores.append({
                    'uid': '_:' + row['nombre'],
                    'dgraph.type': 'Instructor',
                    'nombre': row['nombre']
                })
            resp = txn.mutate(set_obj=instructores)
        txn.commit()
        print("✓ Instructores cargados")
        return resp.uids
    finally:
        txn.discard()

def load_cursos(file_path):
    print("Cargando cursos desde:", file_path)
    txn = client.txn()
    try:
        cursos = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                cursos.append({
                    'uid': '_:' + row['titulo'],
                    'dgraph.type': 'Curso',
                    'titulo': row['titulo'],
                    'categoria': row['categoria']
                })
            resp = txn.mutate(set_obj=cursos)
        txn.commit()
        print("✓ Cursos cargados")
        return resp.uids
    finally:
        txn.discard()

def load_lecciones(file_path):
    print("Cargando lecciones desde:", file_path)
    txn = client.txn()
    try:
        lecciones = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                lecciones.append({
                    'uid': '_:' + row['titulo'],
                    'dgraph.type': 'Leccion',
                    'titulo': row['titulo']
                })
            resp = txn.mutate(set_obj=lecciones)
        txn.commit()
        print("✓ Lecciones cargadas")
        return resp.uids
    finally:
        txn.discard()

def create_user_relations(file_path, user_uids, curso_uids):
    print("Creando relaciones de usuarios desde:", file_path)
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                uid = user_uids[row['nombre']]
                data = {'uid': uid}
                if row['amigos']:
                    data['amigos'] = [{'uid': user_uids[a.strip()]} for a in row['amigos'].split(',') if a.strip() in user_uids]
                if row['inscritoEn']:
                    data['inscritoEn'] = [{'uid': curso_uids[c.strip()]} for c in row['inscritoEn'].split(',') if c.strip() in curso_uids]
                if row['completado']:
                    data['completado'] = [{'uid': curso_uids[c.strip()]} for c in row['completado'].split(',') if c.strip() in curso_uids]
                txn.mutate(set_obj=data)
        txn.commit()
        print("✓ Relaciones de usuarios creadas")
    finally:
        txn.discard()

def create_instructor_relations(file_path, instructor_uids, curso_uids):
    print("Creando relaciones de instructores con cursos...")
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                instructor_uid = instructor_uids.get(row['nombre'])
                if instructor_uid:
                    data = { 'uid': instructor_uid }

                    if row.get('cursos'):
                        cursos_ids = [
                            {'uid': curso_uids[c.strip()]}
                            for c in row['cursos'].split(',') if c.strip() in curso_uids
                        ]
                        data['cursos'] = cursos_ids

                    txn.mutate(set_obj=data)
        txn.commit()
        print("✓ Relaciones de cursos con instructores creadas")
    finally:
        txn.discard()


def create_interactions(file_path, user_uids, leccion_uids, instructor_uids):
    print("Creando interacciones desde:", file_path)
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                interaction = {
                    'uid': '_:interaccion_' + row['usuario'] + '_' + (row.get('leccion') or row.get('instructor', '')), 
                    'dgraph.type': 'Interaccion',
                    'tipo': row['tipo'],
                    'contenido': row.get('contenido', ''),
                    'fecha': row['fecha'],
                    'usuario': { 'uid': user_uids[row['usuario']] }
                }
                if row.get('leccion') and row['leccion'] in leccion_uids:
                    interaction['leccion'] = { 'uid': leccion_uids[row['leccion']] }
                if row.get('instructor') and row['instructor'] in instructor_uids:
                    interaction['instructor'] = { 'uid': instructor_uids[row['instructor']] }
                txn.mutate(set_obj=interaction)
        txn.commit()
        print("✓ Interacciones creadas")
    finally:
        txn.discard()


# === CONSULTAS (QUERIES) ===
def query_cursos_completados(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        nombre
        completado {
          titulo
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    data = json.loads(res.json)

    usuario = data.get("usuario", [])
    if not usuario:
        print(f"No se encontró al usuario '{nombre}'.")
        return

    completados = usuario[0].get("completado", [])
    if not completados:
        print(f"El usuario '{nombre}' no tiene cursos completados aún.")
    else:
        print(f"Cursos completados por '{nombre}':")
        print(json.dumps(completados, indent=2))

def query_interaccion_usuario(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        nombre
        ~usuario {
          tipo
          contenido
          fecha
          leccion {
            titulo
          }
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))


def query_interacciones_por_leccion(titulo):
    query = """
    query q($titulo: string) {
      leccion(func: eq(titulo, $titulo)) {
        titulo
        interacciones: ~leccion {
          tipo
          contenido
          fecha
          usuario {
            nombre
          }
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$titulo": titulo})
    print(json.dumps(json.loads(res.json), indent=2))



def query_amigos(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        nombre
        amigos {
          nombre
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))

def query_recomendaciones_por_likes(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        liked: ~usuario @filter(eq(tipo, "like")) {
          instructor {
            nombre
            cursos {
              titulo
              categoria
            }
          }
        }
      }
    }
    """
    print("Recomendaciones basadas en likes a instructores.")
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    data = json.loads(res.json)

    usuario = data.get("usuario", [])
    if not usuario or not usuario[0].get("liked"):
        print(f"No se encontraron instructores likeados por '{nombre}'.")
        return

    resultado = []
    for like in usuario[0]["liked"]:
        if "instructor" in like:
            instructor = like["instructor"]
            for curso in instructor.get("cursos", []):
                resultado.append({
                    "instructor": instructor["nombre"],
                    "titulo": curso["titulo"],
                    "categoria": curso["categoria"]
                })

    if not resultado:
        print(f"No se encontraron cursos en instructores likeados por '{nombre}'.")
    else:
        print(json.dumps(resultado, indent=2))


def query_cursos_amigos(nombre):
    query = """
    query q($name: string) {
      var(func: eq(nombre, $name)) {
        completado {
          u as uid
        }
        amigos {
          cursos_amigos as completado
        }
      }

      recomendaciones(func: uid(cursos_amigos)) @filter(NOT uid(u)) {
        titulo
        categoria
      }
    }
    """
    print("Recomendaciones basadas en cursos tomados por amigos (que el usuario aún no ha tomado):")
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))


def query_ruta_aprendizaje(nombre):
    query = """
    query q($name: string) {
      var(func: eq(nombre, $name)) {
        completado {
          cat as categoria
        }
      }

      recomendaciones(func: type(Curso)) @filter(eq(categoria, val(cat))) {
        titulo
        categoria
      }
    }
    """
    print("Ruta sugerida basada en la categoría de cursos completados del usuario.")
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))

def query_recomendacion_instructores_amigos(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        amigos {
          ~usuario @filter(eq(tipo, "like")) {
            instructor {
              nombre
              cursos {
                titulo
              }
            }
          }
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))

def query_sugerencia_amigos_por_cursos(nombre):
    query = """
    query q($name: string) {
      var(func: eq(nombre, $name)) {
        completado {
          cursos_comunes as uid
        }
      }

      recomendacion(func: type(Usuario)) @filter(uid(cursos_comunes) AND NOT eq(nombre, $name)) {
        nombre
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))

def query_usuarios_inscritos_a_curso(titulo):
    query = """
    query q($titulo: string) {
      curso(func: eq(titulo, $titulo)) {
        titulo
        ~inscritoEn {
          nombre
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$titulo": titulo})
    print(json.dumps(json.loads(res.json), indent=2))

def query_usuarios_completaron_curso(titulo):
    query = """
    query q($titulo: string) {
      curso(func: eq(titulo, $titulo)) {
        titulo
        ~completado {
          nombre
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$titulo": titulo})
    print(json.dumps(json.loads(res.json), indent=2))

def query_cursos_por_instructor(nombre):
    query = """
    query q($name: string) {
      instructor(func: eq(nombre, $name)) {
        nombre
        cursos {
          titulo
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))

def query_cursos_inscritos_por_usuario(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        nombre
        inscritoEn {
          titulo
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))




# === MENÚ INTERACTIVO === #

if __name__ == "__main__":
    drop_all(client)
    set_schema(client)

    usuarios = load_usuarios("usuarios.csv")
    instructores = load_instructores("instructores.csv")
    cursos = load_cursos("cursos.csv")
    lecciones = load_lecciones("lecciones.csv")

    create_user_relations("usuarios.csv", usuarios, cursos)
    create_instructor_relations("instructores.csv", instructores, cursos)
    create_interactions("interacciones.csv", usuarios, lecciones, instructores)


    while True:
        print("\n=== MENÚ DE CONSULTAS DGRAPH ===")
        print("1. Cursos completados por un usuario")
        print("2. Interacciones de un usuario")
        print("3. Interacciones en una lección")
        print("4. Interacciones de un usuario")
        print("5. Amigos de un usuario")
        print("6. Recomendaciones por profesor (likes a instructores)")
        print("7. Recomendaciones por amigos (cursos que ellos completaron)")
        print("8. Ruta de aprendizaje (por categoría de cursos completados)")
        print("9. Recomendación de instructores que amigos han likeado")
        print("10. Sugerencia de nuevos amigos por cursos completados similares")
        print("11. Estudiantes inscritos a un curso")
        print("12. Estudiantes que han completado un curso")
        print("13. Cursos creados por un instructor")
        print("14. Cursos en los que está inscrito un usuario")
        print("15. Salir")

        choice = input("Selecciona una opción: ")

        if choice == '1':
            nombre = input("Nombre del usuario: ")
            query_cursos_completados(nombre)
        elif choice == '2':
            nombre = input("Nombre del usuario: ")
            query_interaccion_usuario(nombre)
        elif choice == '3':
            titulo = input("Título de la lección: ")
            query_interacciones_por_leccion(titulo)
        elif choice == '4':
            nombre = input("Nombre del usuario: ")
            query_interacciones_usuario(nombre)
        elif choice == '5':
            nombre = input("Nombre del usuario: ")
            query_amigos(nombre)
        elif choice == '6':
            nombre = input("Nombre del usuario: ")
            query_recomendaciones_por_likes(nombre)
        elif choice == '7':
            nombre = input("Nombre del usuario: ")
            query_cursos_amigos(nombre)
        elif choice == '8':
            nombre = input("Nombre del usuario: ")
            query_ruta_aprendizaje(nombre)
        elif choice == '9':
            nombre = input("Nombre del usuario: ")
            query_recomendacion_instructores_amigos(nombre)
        elif choice == '10':
            nombre = input("Nombre del usuario: ")
            query_sugerencia_amigos_por_cursos(nombre)
        elif choice == '11':
            titulo = input("Título del curso: ")
            query_usuarios_inscritos_a_curso(titulo)
        elif choice == '12':
            titulo = input("Título del curso: ")
            query_usuarios_completaron_curso(titulo)
        elif choice == '13':
            nombre = input("Nombre del instructor: ")
            query_cursos_por_instructor(nombre)
        elif choice == '14':
            nombre = input("Nombre del usuario: ")
            query_cursos_inscritos_por_usuario(nombre)
        elif choice == '15':
            print("Cerrando conexión... ¡Hasta luego!")
            client_stub.close()
            break
        else:
            print("Opción inválida. Intenta nuevamente.")
