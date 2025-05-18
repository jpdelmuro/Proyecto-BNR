import os
import csv
import pydgraph


def connect_to_dgraph():
    stub = pydgraph.DgraphClientStub('localhost:9080')
    return pydgraph.DgraphClient(stub)


def set_schema(client):
    schema = """
    type Usuario {
        nombre: string
        amigos: [uid]
        inscritoEn: [uid]
        completado: [uid]
    }

    type Instructor {
        nombre: string
        cursos: [uid]
    }

    type Curso {
        titulo: string
        categoria: string
        lecciones: [uid]
        creadoPor: uid
    }

    type Leccion {
        titulo: string
        curso: uid
        interacciones: [uid]
    }

    type Interaccion {
        tipo: string
        contenido: string
        fecha: datetime
        leccion: uid
        usuario: uid
        instructor: uid
    }

    nombre: string @index(exact) .
    user_id: string @index(exact) .
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
    client.alter(pydgraph.Operation(schema=schema))

# ... resto del archivo sigue igual ...

def drop_all(client):
    client.alter(pydgraph.Operation(drop_all=True))


def load_entities(client, filename, tipo, uid_key, extra_fields=None):
    txn = client.txn()
    try:
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            data = []
            for row in reader:
                entry = {
                    'uid': '_:' + row[uid_key],
                    'dgraph.type': tipo,
                    uid_key: row[uid_key]
                }
                if extra_fields:
                    for field, conv in extra_fields.items():
                        if row.get(field):
                            entry[field] = conv(row[field])
                data.append(entry)
            resp = txn.mutate(set_obj=data)
        txn.commit()
        return resp.uids
    finally:
        txn.discard()


def load_dgraph_data(folder):
    client = connect_to_dgraph()
    drop_all(client)
    set_schema(client)

    usuarios_uid = load_entities(client, os.path.join(folder, "usuariosD.csv"), "Usuario", "nombre")
    instructores_uid = load_entities(client, os.path.join(folder, "instructoresD.csv"), "Instructor", "nombre")
    cursos_uid = load_entities(client, os.path.join(folder, "cursosD.csv"), "Curso", "titulo", {"categoria": str})
    lecciones_uid = load_entities(client, os.path.join(folder, "leccionesD.csv"), "Leccion", "titulo")

    # Interacciones
    txn = client.txn()
    try:
        with open(os.path.join(folder, "interaccionesD.csv"), 'r') as file:
            reader = csv.DictReader(file)
            data = []
            for row in reader:
                entry = {
                    'uid': '_:interaccion_' + row['usuario'] + '_' + (row.get('leccion') or row.get('instructor', '')),
                    'dgraph.type': 'Interaccion',
                    'tipo': row['tipo'],
                    'contenido': row.get('contenido', ''),
                    'fecha': row['fecha'],
                    'usuario': { 'uid': usuarios_uid[row['usuario']] }
                }
                if row.get('leccion') and row['leccion'] in lecciones_uid:
                    entry['leccion'] = { 'uid': lecciones_uid[row['leccion']] }
                if row.get('instructor') and row['instructor'] in instructores_uid:
                    entry['instructor'] = { 'uid': instructores_uid[row['instructor']] }
                data.append(entry)
            txn.mutate(set_obj=data)
        txn.commit()
    finally:
        txn.discard()

    print("✓ Datos de Dgraph cargados correctamente.")
