import os
import csv
import pydgraph

def connect_to_dgraph():
    stub = pydgraph.DgraphClientStub('localhost:9080')
    return pydgraph.DgraphClient(stub)

def set_schema(client):
    schema = """
    type Usuario {
        nombre
        amigos
        completado
    }

    type Instructor {
        nombre
        cursos            # Añadimos la propiedad cursos al tipo Instructor
    }

    type Curso {
        titulo
        categoria
        creadoPor
    }

    type Interaccion {
        tipo
        fecha
        usuario
        instructor
    }

    nombre: string @index(exact) .
    titulo: string @index(term, fulltext) .
    categoria: string @index(hash) .
    tipo: string @index(hash) .
    fecha: datetime .

    amigos: [uid] @reverse .
    completado: [uid] @reverse .
    creadoPor: uid @reverse .
    usuario: uid @reverse .
    instructor: uid @reverse .
    cursos: [uid] @reverse .   # Añadimos la definición del predicado cursos
    """
    client.alter(pydgraph.Operation(schema=schema))

def drop_all(client):
    client.alter(pydgraph.Operation(drop_all=True))

def load_usuarios(client, file_path):
    txn = client.txn()
    try:
        data = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append({
                    'uid': row['uid'].strip(),
                    'dgraph.type': 'Usuario',
                    'nombre': row['nombre']
                })
        response = txn.mutate(set_obj=data)
        txn.commit()
    finally:
        txn.discard()

def add_usuario_relaciones(client, file_path):
    txn = client.txn()
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data = {'uid': row['uid'].strip()}
                if row.get('amigos'):
                    data['amigos'] = [{'uid': uid.strip()} for uid in row['amigos'].split(',') if uid.strip()]
                if row.get('completado'):
                    data['completado'] = [{'uid': uid.strip()} for uid in row['completado'].split(',') if uid.strip()]
                txn.mutate(set_obj=data)
        txn.commit()
    finally:
        txn.discard()

def load_instructores(client, file_path):
    txn = client.txn()
    try:
        data = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append({
                    'uid': row['uid'].strip(),
                    'dgraph.type': 'Instructor',
                    'nombre': row['nombre']
                })
        response = txn.mutate(set_obj=data)
        txn.commit()
    finally:
        txn.discard()

def load_cursos(client, file_path):
    txn = client.txn()
    try:
        data = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                curso = {
                    'uid': row['uid'].strip(),
                    'dgraph.type': 'Curso',
                    'titulo': row['titulo'],
                    'categoria': row['categoria'],
                    'creadoPor': { 'uid': row['creadoPor'].strip() }
                }
                data.append(curso)
        response = txn.mutate(set_obj=data)
        txn.commit()
    finally:
        txn.discard()

# Añadimos esta nueva función para establecer relaciones bidireccionales
def add_instructor_curso_relaciones(client, file_path):
    txn = client.txn()
    try:
        # Primero, creamos un diccionario para mapear instructores a sus cursos
        instructor_to_cursos = {}
        
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                instructor_uid = row['creadoPor'].strip()
                curso_uid = row['uid'].strip()
                
                if instructor_uid not in instructor_to_cursos:
                    instructor_to_cursos[instructor_uid] = []
                instructor_to_cursos[instructor_uid].append(curso_uid)
        
        # Luego, actualizamos cada instructor con sus cursos
        for instructor_uid, cursos_uids in instructor_to_cursos.items():
            instructor_data = {
                'uid': instructor_uid,
                'cursos': [{'uid': curso_uid} for curso_uid in cursos_uids]
            }
            txn.mutate(set_obj=instructor_data)
            
        txn.commit()
    finally:
        txn.discard()

def load_interacciones(client, file_path):
    txn = client.txn()
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for idx, row in enumerate(reader):
                interaccion = {
                    'uid': f'_:interaccion_{idx}',
                    'dgraph.type': 'Interaccion',
                    'tipo': row['tipo'],
                    'fecha': row['fecha'],
                    'usuario': {'uid': row['usuario'].strip()},
                    'instructor': {'uid': row['instructor'].strip()}
                }
                txn.mutate(set_obj=interaccion)
        txn.commit()
    finally:
        txn.discard()

def load_dgraph_data(folder):
    client = connect_to_dgraph()
    drop_all(client)
    set_schema(client)

    load_usuarios(client, os.path.join(folder, "usuariosD.csv"))
    load_instructores(client, os.path.join(folder, "instructoresD.csv"))
    load_cursos(client, os.path.join(folder, "cursosD.csv"))
    add_usuario_relaciones(client, os.path.join(folder, "usuariosD.csv"))
    # Añadimos la nueva llamada a función
    add_instructor_curso_relaciones(client, os.path.join(folder, "cursosD.csv"))
    load_interacciones(client, os.path.join(folder, "interaccionesD.csv"))

    print("✓ Datos de Dgraph cargados correctamente.")