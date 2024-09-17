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
    
    # Generar las condiciones de JOIN basadas en los atributos comunes (como "person_id")
    joins_clause = usoBBDD.generar_joins(db_path, tables)

    # Crear la cláusula de combinación de tablas
    if len(tables) > 1:
        tables_clause = f'"{tables[0]}"'
        for i in range(1, len(tables)):
            tables_clause += f' LEFT JOIN "{tables[i]}" ON {joins_clause}'
    else:
        tables_clause = tables[0]
    
    # Crear la cláusula de condiciones
    where_clause = ' AND '.join(where_conditions)
    
    # Construir la consulta SQL para crear la cohorte
    query = f"""
    CREATE OR REPLACE TABLE {cohort_name} AS
    SELECT *
    FROM {tables_clause}
    WHERE {where_clause}
    """
    
    print("Executing query:")
    print(query)
    
    # Ejecutar la consulta
    con.execute(query)
    
    # Leer la cohorte resultante en un DataFrame
    result = con.execute(f"SELECT * FROM {cohort_name}").df()
    
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


def extract_predictive_features_with_target(db_path, tables, feature_columns, target_table, target_condition, cohort_name):
    """
    Extrae las variables predictorias de una base de datos DuckDB y añade la columna 'target' a partir de una tabla de outcomes.

    :param db_path: Ruta al archivo de base de datos DuckDB.
    :param tables: Lista de nombres de tablas a combinar.
    :param join_columns: Lista de pares de columnas para unir las tablas. Cada par debe ser una tupla (tabla1_columna, tabla2_columna).
    :param where_conditions: Lista de condiciones SQL para filtrar los datos. Cada condición debe ser una cadena SQL.
    :param feature_columns: Lista de columnas a seleccionar como variables predictorias.
    :param target_table: Nombre de la tabla que contiene los outcomes.
    :param target_join_column: Condición de unión entre las tablas de características y la tabla de outcomes.
    :param target_condition: Condición que define el valor de 'target' (ejemplo: "outcome.event = 1").
    :param cohort_name: Nombre de la tabla de características predictoras resultante (opcional).
    :return: DataFrame con las variables predictorias resultantes y la columna 'target'.
    """
    
    # Conectar a la base de datos
    con = duckdb.connect(database=db_path, read_only=False)
    
    if len(tables) > 0:
        join_columns = []
        for i in range(0, len(tables) - 1):
           join_columns.append(tables[i] + '.person_id')

    # Crear la cláusula de combinación de tablas (features)
    if len(tables) > 1:
        tables_clause = f'"{tables[0]}"'
        for i in range(1, len(tables)):
            join_condition = join_columns[i-1]
            tables_clause += f' LEFT JOIN "{tables[i]}" ON {join_condition}'
    else:
        tables_clause = tables[0]
    
    # Unir con la tabla de outcome para obtener la columna target
    tables_clause += f' LEFT JOIN "{target_table}" ON {target_join_column}'
    
    # Crear la cláusula de condiciones
    where_clause = ' AND '.join(where_conditions)
    
    # Crear la cláusula de selección de columnas, incluyendo el target
    feature_columns_clause = ', '.join(feature_columns)
    
    # Construir la consulta SQL para extraer las variables predictorias y la columna target
    query = f"""
    CREATE OR REPLACE TABLE {cohort_name} AS
    SELECT {feature_columns_clause},
           CASE WHEN {target_condition} THEN 1 ELSE 0 END AS target
    FROM {tables_clause}
    WHERE {where_clause}
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


