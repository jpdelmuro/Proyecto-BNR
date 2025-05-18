from pymongo import MongoClient
from cassandra.cluster import Cluster
import pydgraph
from datetime import datetime


# === MONGODB ===
def get_mongo_users():
    client = MongoClient("mongodb://localhost:27017")
    db = client["ProyectoMongoPython"]
    usuarios = db.usuarios.find({}, {"user_id": 1, "name": 1, "email": 1, "_id": 0})
    return list(usuarios)


# === CASSANDRA ===
def insert_users_to_cassandra(users):
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("plataforma_online")

    session.execute("""
        CREATE TABLE IF NOT EXISTS user_basic (
            user_id text PRIMARY KEY,
            email text,
            name text,
            inserted_at timestamp
        );
    """)

    for user in users:
        try:
            session.execute(
                "INSERT INTO user_basic (user_id, email, name, inserted_at) VALUES (%s, %s, %s, %s)",
                (user["user_id"], user["email"], user["name"], datetime.utcnow())
            )
        except Exception as e:
            print(f"Error insertando en Cassandra: {e}")


# === DGRAPH ===
def insert_users_to_dgraph(users):
    stub = pydgraph.DgraphClientStub('localhost:9080')
    client = pydgraph.DgraphClient(stub)
    txn = client.txn()
    try:
        data = []
        for user in users:
            data.append({
                "uid": f"_:{user['user_id']}",
                "dgraph.type": "Usuario",
                "nombre": user["name"],
                "user_id": user["user_id"]
            })
        txn.mutate(set_obj=data, commit_now=True)
    except Exception as e:
        print(f"Error insertando en Dgraph: {e}")
    finally:
        txn.discard()
        stub.close()


# === SYNC MASTER ===
def sync_users_across_dbs():
    print("→ Obteniendo usuarios desde MongoDB...")
    users = get_mongo_users()
    print(f"  Usuarios encontrados: {len(users)}")

    print("→ Insertando usuarios en Cassandra...")
    insert_users_to_cassandra(users)

    print("→ Insertando usuarios en Dgraph...")
    insert_users_to_dgraph(users)

    print("✓ Sincronización de usuarios completada.")
