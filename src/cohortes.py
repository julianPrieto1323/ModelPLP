import duckdb
import usoBBDD

def create_cohort(db_path, tables, where_conditions, cohort_name):
    """
    Crea una cohorte en una base de datos DuckDB a partir de tablas y condiciones proporcionadas.

    :param db_path: Ruta al archivo de base de datos DuckDB.
    :param tables: Lista de nombres de tablas a combinar.
    :param where_conditions: Lista de condiciones SQL para filtrar los datos.
    :param cohort_name: Nombre de la tabla de cohorte resultante (opcional).
    :return: DataFrame con la cohorte resultante.
    """
    # Conectar a la base de datos
    con = duckdb.connect(database=db_path, read_only=False)
    
    # Generar las condiciones de JOIN basadas en los atributos comunes
    joins_clause, _ = usoBBDD.generar_joins(db_path, tables)
    
    # Crear la cláusula de combinación de tablas
    if len(tables) > 1:
        tables_clause = f'"{tables[0]}"'
        for i in range(1, len(tables)):
            tables_clause += f' LEFT JOIN "{tables[i]}" ON {joins_clause}'
    else:
        tables_clause = f'"{tables[0]}"'
    
    # Crear la cláusula de condiciones
    where_clause = ' AND '.join(where_conditions)
    
    # Asegúrate de que las condiciones WHERE sean válidas para evitar errores de conversión
    if not where_clause:
        where_clause = '1=1'  # No aplicar filtro si no se proporcionan condiciones
    
    # Construir la consulta SQL para crear la cohorte
    query = f"""
    CREATE OR REPLACE TABLE {cohort_name} AS
    SELECT *
    FROM {tables_clause}
    WHERE {where_clause}
    """
    
    print("Executing query:")
    print(query)
    
    try:
        # Ejecutar la consulta
        con.execute(query)
        
        # Leer la cohorte resultante en un DataFrame
        result = con.execute(f"SELECT * FROM {cohort_name}").df()
    except Exception as e:
        print(f"Error executing query: {e}")
        result = None
    
    # Cerrar la conexión
    con.close()
    
    return result


def create_target_outcome_cohorts(db_path, target_tables, target_conditions, outcome_tables, outcome_conditions, target_cohort_name, outcome_cohort_name):
    """
    Crea las cohortes de objetivo y de resultado en una base de datos DuckDB a partir de tablas y condiciones proporcionadas.

    :param db_path: Ruta al archivo de base de datos DuckDB.
    :param target_join_columns: Lista de pares de columnas para unir las tablas en la cohorte de objetivo.
    :param target_conditions: Lista de condiciones SQL para filtrar los datos en la cohorte de objetivo.
    :param outcome_conditions: Lista de condiciones SQL para filtrar los datos en la cohorte de resultado.
    :param target_cohort_name: Nombre de la tabla de cohorte de objetivo (opcional).
    :param outcome_cohort_name: Nombre de la tabla de cohorte de resultado (opcional).
    :return: Tupla de DataFrames con las cohortes de objetivo y de resultado.
    """
    # Crear la cohorte de objetivo
    target_cohort_df = create_cohort(db_path, target_tables, target_conditions, target_cohort_name)
    
    # Crear la cohorte de resultado
    outcome_cohort_df = create_cohort(db_path, outcome_tables, outcome_conditions, outcome_cohort_name)
    
    return target_cohort_df, outcome_cohort_df

def extract_predictive_features_with_target(db_path, tables, feature_columns, target_table, target_condition, cohort_name, index_date_column):
    """
    Extrae las variables predictorias basadas en la definición de PLP de OMOP y añade la columna 'target' a partir de una tabla de outcomes.
    
    :param db_path: Ruta al archivo de base de datos DuckDB.
    :param tables: Lista de nombres de tablas a combinar.
    :param feature_columns: Diccionario con tablas como claves y columnas de características como valores.
    :param target_table: Nombre de la tabla que contiene los outcomes.
    :param target_condition: Condición que define el valor de 'target' (ejemplo: "outcome.event = 1").
    :param cohort_name: Nombre de la tabla de características predictoras resultante.
    :param index_date_column: Nombre de la columna que define la fecha índice (ej. fecha del evento diagnóstico).
    :return: DataFrame con las variables predictorias y la columna 'target'.
    """
    
    # Conectar a la base de datos
    con = duckdb.connect(database=db_path, read_only=False)
    
    # Generar las cláusulas de JOIN a partir de las tablas
    join_conditions = usoBBDD.generar_joins(db_path, tables)

    # Construir la cláusula de combinación de tablas (features)
    tables_clause = ' LEFT JOIN '.join([f'"{table}"' for table in tables])
    
    # Unir la tabla de target
    tables_clause += f' LEFT JOIN "{target_table}" ON {join_conditions}'
    
    # Generar la cláusula de selección de columnas de características
    feature_columns_clause = ', '.join([f"{table}.{column}" for table, columns in feature_columns.items() for column in columns])
    
    # Agregar la fecha índice para calcular el periodo de tiempo en relación al outcome
    time_window_clause = f"{index_date_column} BETWEEN '2019-01-01' AND '2023-01-01'"
    
    # Construir la consulta SQL para extraer las variables predictorias y la columna target
    query = f"""
    CREATE OR REPLACE TABLE {cohort_name} AS
    SELECT {feature_columns_clause},
           CASE WHEN {target_condition} THEN 1 ELSE 0 END AS target,
           {time_window_clause} AS within_time_window
    FROM {tables_clause}
    """
    
    print("Executing query:")
    print(query)
    
    # Ejecutar la consulta
    con.execute(query)
    
    # Leer las variables predictorias resultantes en un DataFrame
    result = con.execute(f"SELECT * FROM {cohort_name}").df()
    
    # Cerrar la conexión
    con.close()
    
    return result

