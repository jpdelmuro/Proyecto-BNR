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
    res = query_runner_raw(query, {"$name": nombre})
    print("\nInstructores likeados y sus cursos:")
    for entry in res.get("usuario", []):
        for like in entry.get("liked", []):
            inst = like.get("instructor")
            if inst:
                print(f"\n👨‍🏫 {inst['nombre']}")
                for curso in inst.get("cursos", []):
                    print(f"   ➤ {curso['titulo']} ({curso['categoria']})")

def query_cursos_amigos(nombre):
    query = """
    query q($name: string) {
      var(func: eq(nombre, $name)) {
        completado {
          u as uid
        }
      }

      usuario(func: eq(nombre, $name)) {
        amigos {
          nombre
          completado @filter(NOT uid(u)) {
            titulo
            categoria
          }
        }
      }
    }
    """
    res = query_runner_raw(query, {"$name": nombre})
    print("\n👥 Cursos completados por tus amigos (que tú aún no has completado):")

    for usuario in res.get("usuario", []):
        for amigo in usuario.get("amigos", []):
            cursos = amigo.get("completado", [])
            if cursos:
                print(f"- {amigo['nombre']} completó:")
                for curso in cursos:
                    print(f"   ➤ {curso['titulo']} ({curso['categoria']})")


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
    res = query_runner_raw(query, {"$name": nombre})
    print("\nRuta de aprendizaje sugerida:")
    categorias = {}
    for curso in res.get("recomendaciones", []):
        cat = curso["categoria"]
        categorias.setdefault(cat, []).append(curso["titulo"])
    for cat, cursos in categorias.items():
        print(f"\n📂 {cat}")
        for c in cursos:
            print(f"   ↳ {c}")

def query_recomendacion_instructores_amigos(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        amigos {
          nombre
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
    res = query_runner_raw(query, {"$name": nombre})
    print("\nInstructores likeados por tus amigos:")
    for u in res.get("usuario", []):
        for amigo in u.get("amigos", []):
            for like in amigo.get("~usuario", []):
                inst = like.get("instructor")
                if inst:
                    print(f"- {amigo['nombre']} likeó al instructor {inst['nombre']}:")
                    for curso in inst.get("cursos", []):
                        print(f"   ➤ {curso['titulo']}")

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
        completado {
          titulo
        }
      }
    }
    """
    res = query_runner_raw(query, {"$name": nombre})
    print("\nSugerencias de nuevos amigos (por cursos en común):")
    for usuario in res.get("recomendacion", []):
        for curso in usuario.get("completado", []):
            print(f"- Curso en común: {curso['titulo']} ➤ posible amigo: {usuario['nombre']}")

def query_runner_raw(query, variables):
    client, stub = get_dgraph_client()
    try:
        res = client.txn(read_only=True).query(query, variables=variables)
        return json.loads(res.json)
    except Exception as e:
        print(f"Error en consulta Dgraph: {e}")
        return {}
    finally:
        stub.close()

def menu_consultas_dgraph():
    while True:
        print("""
=== CONSULTAS DGRAPH ===

1. Recomendaciones por profesor (likes a instructores)
2. Recomendaciones por amigos (cursos que ellos completaron)
3. Ruta de aprendizaje (categoría de cursos completados)
4. Recomendación de instructores que amigos han likeado
5. Sugerencia de nuevos amigos por cursos similares
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
