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
    categoria: string @index(trigram) .
    tipo: string @index(hash) .
    fecha: datetime .

    amigos: [uid] @reverse .
    completado: [uid] @reverse .
    creadoPor: uid @reverse .
    usuario: uid @reverse .
    instructor: uid @reverse .
    """
    client.alter(pydgraph.Operation(schema=schema))

def drop_all(client):
    client.alter(pydgraph.Operation(drop_all=True))

def load_usuarios(client, file_path):
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

def add_usuario_relaciones(client, file_path, usuarios_uid, cursos_uid):
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                uid = usuarios_uid[row['nombre']]
                data = {'uid': uid}
                if row.get('amigos'):
                    data['amigos'] = [{'uid': usuarios_uid[am.strip()]} for am in row['amigos'].split(',') if am.strip() in usuarios_uid]
                if row.get('completado'):
                    data['completado'] = [{'uid': cursos_uid[c.strip()]} for c in row['completado'].split(',') if c.strip() in cursos_uid]
                txn.mutate(set_obj=data)
        txn.commit()
    finally:
        txn.discard()

def load_instructores(client, file_path):
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

def load_cursos(client, file_path, instructores_uid):
    txn = client.txn()
    try:
        data = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                curso = {
                    'uid': '_:' + row['titulo'],
                    'dgraph.type': 'Curso',
                    'titulo': row['titulo'],
                    'categoria': row['categoria']
                }
                if row.get('creadoPor') and row['creadoPor'] in instructores_uid:
                    curso['creadoPor'] = { 'uid': instructores_uid[row['creadoPor']] }
                data.append(curso)
        response = txn.mutate(set_obj=data)
        txn.commit()
        return response.uids
    finally:
        txn.discard()

def load_interacciones(client, file_path, usuarios, instructores):
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['instructor'] not in instructores:
                    continue
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

def load_dgraph_data(folder):
    client = connect_to_dgraph()
    drop_all(client)
    set_schema(client)

    usuarios_uid = load_usuarios(client, os.path.join(folder, "usuariosD.csv"))
    instructores_uid = load_instructores(client, os.path.join(folder, "instructoresD.csv"))
    cursos_uid = load_cursos(client, os.path.join(folder, "cursosD.csv"), instructores_uid)

    add_usuario_relaciones(client, os.path.join(folder, "usuariosD.csv"), usuarios_uid, cursos_uid)
    load_interacciones(client, os.path.join(folder, "interaccionesD.csv"), usuarios_uid, instructores_uid)

    print("✓ Datos de Dgraph cargados correctamente.")
