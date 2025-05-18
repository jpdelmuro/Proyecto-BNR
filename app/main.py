import sys
import json
import pydgraph
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra_module.loader import load_cassandra_data
from mongo_module.loader import load_mongo_data
from dgraph_module.loader import load_dgraph_data
from shared.sync import sync_users_across_dbs
from mongo_module.queries import menu_consultas_mongo
from cassandra_module.queries import run_queries as consultas_cassandra


def mostrar_menu():
    print("\n=== MENÚ PRINCIPAL ===")
    print("1. Cargar datos en todas las bases")
    print("2. Consultar usuario")
    print("3. Sincronizar usuarios entre bases")
    print("4. Consultas MongoDB")
    print("5. Consultas Cassandra")
    print("6. Consultas Dgraph")
    print("7. Salir")


def cargar_datos():
    print("\nCargando datos en Cassandra...")
    load_cassandra_data("data/cassandra/test.csv")

    print("Cargando datos en MongoDB...")
    load_mongo_data("data/mongo/")

    print("Cargando datos en Dgraph...")
    load_dgraph_data("data/dgraph/")

    print("\n✓ Datos cargados en todas las bases correctamente.")


def consultar_usuario():
    user_id = input("\nIngrese el user_id del usuario (ej: u001): ").strip()
    nombre_mongo = nombre_cassandra = nombre_dgraph = None

    # MongoDB
    try:
        mongo_client = MongoClient("mongodb://localhost:27017")
        db = mongo_client["ProyectoMongoPython"]
        user = db.usuarios.find_one({"user_id": user_id})
        if user:
            nombre_mongo = user.get("name")
    except Exception as e:
        print(f"Error consultando en MongoDB: {e}")

    # Cassandra
    try:
        cluster = Cluster(["127.0.0.1"])
        session = cluster.connect("plataforma_online")
        row = session.execute("SELECT name FROM user_basic WHERE user_id = %s", [user_id]).one()
        if row:
            nombre_cassandra = row.name
    except Exception as e:
        print(f"Error consultando en Cassandra: {e}")

    # Dgraph
    try:
        stub = pydgraph.DgraphClientStub("localhost:9080")
        client = pydgraph.DgraphClient(stub)
        query = """
        query q($id: string) {
          usuario(func: eq(user_id, $id)) {
            nombre
          }
        }
        """
        res = client.txn(read_only=True).query(query, variables={"$id": user_id})
        data = json.loads(res.json)
        if data.get("usuario"):
            nombre_dgraph = data["usuario"][0].get("nombre")
        stub.close()
    except Exception as e:
        print(f"Error consultando en Dgraph: {e}")

    print(f"\n→ Resultados para el usuario '{user_id}':")
    print(f"MongoDB:     {nombre_mongo if nombre_mongo else 'No encontrado'}")
    print(f"Cassandra:   {nombre_cassandra if nombre_cassandra else 'No encontrado'}")
    print(f"Dgraph:      {nombre_dgraph if nombre_dgraph else 'No encontrado'}")


def sincronizar_usuarios():
    print("\nSincronizando usuarios entre las tres bases de datos...")
    sync_users_across_dbs()
    print("✓ Sincronización completada.")


def main():
    while True:
        mostrar_menu()
        opcion = input("Seleccione una opción: ").strip()

        if opcion == "1":
            cargar_datos()
        elif opcion == "2":
            consultar_usuario()
        elif opcion == "3":
            sincronizar_usuarios()
        elif opcion == "4":
            mongo_client = MongoClient("mongodb://localhost:27017")
            db = mongo_client["ProyectoMongoPython"]
            menu_consultas_mongo(db)
        elif opcion == "5":
            consultas_cassandra()
        elif opcion == "6":
            from dgraph_module.queries import menu_consultas_dgraph
            menu_consultas_dgraph()
        elif opcion == "7":
            print("\nSaliendo del programa. ¡Hasta luego!")
            sys.exit(0)
        else:
            print("\nOpción no válida. Intente nuevamente.")


if __name__ == "__main__":
    main()
