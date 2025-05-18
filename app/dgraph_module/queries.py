import pydgraph
import json

def get_dgraph_client():
    stub = pydgraph.DgraphClientStub("localhost:9080")
    return pydgraph.DgraphClient(stub), stub

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
    return query_runner(query, {"$name": nombre})

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
    return query_runner(query, {"$name": nombre})

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
    return query_runner(query, {"$titulo": titulo})

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
    return query_runner(query, {"$name": nombre})

def query_recomendaciones_por_likes(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        liked: ~usuario @filter(eq(tipo, \"like\")) {
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
    return query_runner(query, {"$name": nombre})

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
    return query_runner(query, {"$name": nombre})

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
    return query_runner(query, {"$name": nombre})

def query_recomendacion_instructores_amigos(nombre):
    query = """
    query q($name: string) {
      usuario(func: eq(nombre, $name)) {
        amigos {
          ~usuario @filter(eq(tipo, \"like\")) {
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
    return query_runner(query, {"$name": nombre})

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
    return query_runner(query, {"$name": nombre})

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
    return query_runner(query, {"$titulo": titulo})

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
    return query_runner(query, {"$titulo": titulo})

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
    return query_runner(query, {"$name": nombre})

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
    return query_runner(query, {"$name": nombre})

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
1. Cursos completados por un usuario
2. Interacciones de un usuario
3. Interacciones en una lección
4. Amigos de un usuario
5. Recomendaciones por profesor (likes a instructores)
6. Recomendaciones por amigos (cursos que ellos completaron)
7. Ruta de aprendizaje (categoría de cursos completados)
8. Recomendación de instructores que amigos han likeado
9. Sugerencia de nuevos amigos por cursos similares
10. Estudiantes inscritos a un curso
11. Estudiantes que han completado un curso
12. Cursos creados por un instructor
13. Cursos en los que está inscrito un usuario
0. Volver al menú principal
        """)
        opcion = input("Seleccione una opción: ").strip()

        if opcion == "1":
            query_cursos_completados(input("Nombre del usuario: "))
        elif opcion == "2":
            query_interaccion_usuario(input("Nombre del usuario: "))
        elif opcion == "3":
            query_interacciones_por_leccion(input("Título de la lección: "))
        elif opcion == "4":
            query_amigos(input("Nombre del usuario: "))
        elif opcion == "5":
            query_recomendaciones_por_likes(input("Nombre del usuario: "))
        elif opcion == "6":
            query_cursos_amigos(input("Nombre del usuario: "))
        elif opcion == "7":
            query_ruta_aprendizaje(input("Nombre del usuario: "))
        elif opcion == "8":
            query_recomendacion_instructores_amigos(input("Nombre del usuario: "))
        elif opcion == "9":
            query_sugerencia_amigos_por_cursos(input("Nombre del usuario: "))
        elif opcion == "10":
            query_usuarios_inscritos_a_curso(input("Título del curso: "))
        elif opcion == "11":
            query_usuarios_completaron_curso(input("Título del curso: "))
        elif opcion == "12":
            query_cursos_por_instructor(input("Nombre del instructor: "))
        elif opcion == "13":
            query_cursos_inscritos_por_usuario(input("Nombre del usuario: "))
        elif opcion == "0":
            break
        else:
            print("Opción no válida. Intente de nuevo.")