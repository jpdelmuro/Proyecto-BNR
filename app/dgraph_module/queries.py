import pydgraph
import json

def get_dgraph_client():
    stub = pydgraph.DgraphClientStub("localhost:9080")
    return pydgraph.DgraphClient(stub), stub

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

def query_runner(query, variables):
    client, stub = get_dgraph_client()
    try:
        res = client.txn(read_only=True).query(query, variables=variables)
        print(json.dumps(json.loads(res.json), indent=2))
    except Exception as e:
        print(f"Error en consulta Dgraph: {e}")
    finally:
        stub.close()

def menu_consultas_dgraph():
    while True:
        print("""
=== CONSULTAS DGRAPH ===
5. Recomendaciones por profesor (likes a instructores)
6. Recomendaciones por amigos (cursos que ellos completaron)
7. Ruta de aprendizaje (categoría de cursos completados)
8. Recomendación de instructores que amigos han likeado
9. Sugerencia de nuevos amigos por cursos similares
0. Volver al menú principal
        """)
        opcion = input("Seleccione una opción: ").strip()

        if opcion == "1":
            query_recomendaciones_por_likes(input("Nombre del usuario: "))
        elif opcion == "2":
            query_cursos_amigos(input("Nombre del usuario: "))
        elif opcion == "3":
            query_ruta_aprendizaje(input("Nombre del usuario: "))
        elif opcion == "4":
            query_recomendacion_instructores_amigos(input("Nombre del usuario: "))
        elif opcion == "5":
            query_sugerencia_amigos_por_cursos(input("Nombre del usuario: "))
        elif opcion == "0":
            break
        else:
            print("Opción no válida. Intente de nuevo.")