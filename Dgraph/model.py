import csv
import pydgraph
import json

# === ESTABLECER CLIENTE ===
client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)

# === DEFINIR ESQUEMA REDUCIDO ===
def set_schema(client):
    schema = """
    type Usuario {
        nombre
        amigos
        completado
    }

    type Instructor {
        nombre
        cursos
    }

    type Curso {
        titulo
        categoria
    }

    type Interaccion {
        tipo
        fecha
        usuario
        instructor
    }

    nombre: string @index(exact) .
    titulo: string @index(term, fulltext) .
    categoria: string @index(trigram) .
    tipo: string @index(hash) .
    fecha: datetime .

    amigos: [uid] @reverse .
    completado: [uid] @reverse .
    cursos: [uid] .
    usuario: uid @reverse .
    instructor: uid @reverse .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def load_usuarios(file_path):
    txn = client.txn()
    try:
        data = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append({
                    'uid': '_:' + row['nombre'],
                    'dgraph.type': 'Usuario',
                    'nombre': row['nombre']
                })
        response = txn.mutate(set_obj=data)
        txn.commit()
        return response.uids
    finally:
        txn.discard()

def load_instructores(file_path):
    txn = client.txn()
    try:
        data = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append({
                    'uid': '_:' + row['nombre'],
                    'dgraph.type': 'Instructor',
                    'nombre': row['nombre']
                })
        response = txn.mutate(set_obj=data)
        txn.commit()
        return response.uids
    finally:
        txn.discard()

def load_cursos(file_path):
    txn = client.txn()
    try:
        data = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append({
                    'uid': '_:' + row['titulo'],
                    'dgraph.type': 'Curso',
                    'titulo': row['titulo'],
                    'categoria': row['categoria']
                })
        response = txn.mutate(set_obj=data)
        txn.commit()
        return response.uids
    finally:
        txn.discard()

def load_interacciones(file_path, usuarios, instructores):
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                interaccion = {
                    'uid': '_:interaccion_' + row['usuario'] + '_' + row['instructor'],
                    'dgraph.type': 'Interaccion',
                    'tipo': row['tipo'],
                    'fecha': row['fecha'],
                    'usuario': {'uid': usuarios[row['usuario']]},
                    'instructor': {'uid': instructores[row['instructor']]}
                }
                txn.mutate(set_obj=interaccion)
        txn.commit()
    finally:
        txn.discard()

# === CONSULTAS ===
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
    res = client.txn(read_only=True).query(query, variables={"$name": nombre})
    print(json.dumps(json.loads(res.json), indent=2))

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

# === MAIN MENU ===
if __name__ == "__main__":
    set_schema(client)
    print("Esquema cargado. Ahora puedes usar las consultas del menú interactivo.")
    while True:
        print("\n=== MENÚ DE CONSULTAS DGRAPH ===")
        print("1. Recomendaciones por profesor (likes a instructores)")
        print("2. Recomendaciones por amigos (cursos completados)")
        print("3. Ruta de aprendizaje (categoría de cursos completados)")
        print("4. Recomendación de instructores que amigos han likeado")
        print("5. Sugerencia de nuevos amigos por cursos similares")
        print("6. Salir")

        choice = input("Selecciona una opción: ")

        if choice == '1':
            nombre = input("Nombre del usuario: ")
            query_recomendaciones_por_likes(nombre)
        elif choice == '2':
            nombre = input("Nombre del usuario: ")
            query_cursos_amigos(nombre)
        elif choice == '3':
            nombre = input("Nombre del usuario: ")
            query_ruta_aprendizaje(nombre)
        elif choice == '4':
            nombre = input("Nombre del usuario: ")
            query_recomendacion_instructores_amigos(nombre)
        elif choice == '5':
            nombre = input("Nombre del usuario: ")
            query_sugerencia_amigos_por_cursos(nombre)
        elif choice == '6':
            print("Cerrando conexión...")
            client_stub.close()
            break
        else:
            print("Opción inválida. Intenta nuevamente.")
