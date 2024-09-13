import duckdb
import pandas as pd
import json
import os

def conexionBBDD(bbdd, tabla):
    if not os.path.exists(bbdd):
        raise FileNotFoundError(f"El archivo {bbdd} no existe")
    
    if not os.access(bbdd, os.R_OK):
        raise PermissionError(f"No se tiene permiso de lectura en el archivo {bbdd}")
    
    try:
        conn = duckdb.connect(database=bbdd)
        print(f"Conexión establecida con {bbdd}")
        return conn.execute(f"SELECT * FROM {tabla}").fetchdf()
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
    
def ejecutar_query(bbdd, query):
    """
    Ejecuta una consulta SQL en una base de datos DuckDB y devuelve el resultado como un DataFrame.

    Args:
        db_path (str): Ruta al archivo de la base de datos DuckDB.
        query (str): Consulta SQL a ejecutar.

    Returns:
        pandas.DataFrame: Resultado de la consulta en un DataFrame.
    """
    # Conectar a la base de datos
    conn = duckdb.connect(database=bbdd, read_only=True)
    
    try:
        # Ejecutar la consulta y obtener el resultado
        result_df = conn.execute(query).fetchdf()
        return result_df
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None
    finally:
        # Cerrar la conexión
        conn.close()

def database_to_dataframe(database_path, query):
    """
    Ejecuta una consulta SQL en una base de datos DuckDB y devuelve los resultados como un DataFrame de pandas.

    Args:
        database_path (str): La ruta al archivo de la base de datos DuckDB. Usa ':memory:' para una base de datos en memoria.
        query (str): La consulta SQL que se ejecutará en la base de datos.

    Returns:
        pd.DataFrame: Un DataFrame de pandas con los resultados de la consulta.
    """
    # Conectar a la base de datos
    conn = duckdb.connect(database=database_path)
    
    try:
        # Ejecutar la consulta SQL y obtener los resultados en un DataFrame
        df = conn.execute(query).df()
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        df = pd.DataFrame()  # Devolver un DataFrame vacío en caso de error
    finally:
        # Cerrar la conexión
        conn.close()
    
    return df

def read_json(file_path):
    """
    Lee un archivo JSON y lo convierte en un diccionario de Python.

    Args:
        file_path (str): La ruta al archivo JSON que se desea leer.

    Returns:
        dict: El contenido del archivo JSON como un diccionario de Python.
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Error: El archivo '{file_path}' no se encontró.")
        return None
    except json.JSONDecodeError:
        print(f"Error: El archivo '{file_path}' no es un JSON válido.")
        return None
    except Exception as e:
        print(f"Error inesperado: {e}")
        return None
def crear_bbdd_desde_csv(carpeta_csv, ruta_bbdd):
    """
    Crea una base de datos DuckDB a partir de múltiples archivos CSV en una carpeta.

    Parámetros:
    carpeta_csv (str): Ruta a la carpeta que contiene los archivos CSV.
    ruta_bbdd (str): Ruta donde se guardará la base de datos DuckDB (archivo .duckdb).

    Descripción:
    - Recorre todos los archivos CSV en la carpeta especificada.
    - Crea una tabla en la base de datos DuckDB para cada archivo CSV, utilizando el nombre del archivo como el nombre de la tabla.
    - Lee el contenido de cada archivo CSV usando la función `read_csv_auto` de DuckDB, que detecta automáticamente los tipos de datos.
    - Guarda la base de datos en la ubicación especificada.

    Ejemplo de uso:
    crear_bbdd_desde_csv('ruta/a/carpeta_csv', 'ruta/a/base_de_datos.duckdb')
    """
    # Crear una conexión a DuckDB y almacenar la base de datos en un archivo
    conn = duckdb.connect(database=ruta_bbdd)
    
    # Recorrer todos los archivos CSV en la carpeta especificada
    for archivo in os.listdir(carpeta_csv):
        if archivo.endswith(".csv"):
            ruta_csv = os.path.join(carpeta_csv, archivo)
            nombre_tabla = os.path.splitext(archivo)[0]  # Usar el nombre del archivo (sin .csv) como nombre de la tabla
            
            try:
                # Cargar el CSV en DuckDB y crear una tabla
                query = f"CREATE OR REPLACE TABLE {nombre_tabla} AS SELECT * FROM read_csv_auto('{ruta_csv}')"
                conn.execute(query)
                print(f"Tabla {nombre_tabla} creada a partir de {archivo}")
            except Exception as e:
                print(f"Error al crear la tabla para {archivo}: {e}")
    
    # Cerrar la conexión a la base de datos
    conn.close()
    print(f"Base de datos creada en {ruta_bbdd}")


def eliminar_tabla(db_path, table_name):
    """
    Elimina una tabla de la base de datos DuckDB.

    Args:
        db_path (str): Ruta al archivo de la base de datos DuckDB.
        table_name (str): Nombre de la tabla que se desea eliminar.

    Returns:
        None
    """
    # Conectar a la base de datos DuckDB
    conn = duckdb.connect(database=db_path, read_only=False)

    try:
        # Verificar si la tabla existe
        table_exists_query = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}';
        """
        table_exists = conn.execute(table_exists_query).fetchone()[0] > 0

        if table_exists:
            # Eliminar la tabla
            drop_table_query = f"""
            DROP TABLE {table_name};
            """
            conn.execute(drop_table_query)
            print(f"Tabla '{table_name}' eliminada con éxito.")
        else:
            print(f"La tabla '{table_name}' no existe en la base de datos.")

    except Exception as e:
        print(f"Error al eliminar la tabla: {e}")

    finally:
        # Cerrar la conexión
        conn.close()
