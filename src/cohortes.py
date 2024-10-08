import duckdb
from . import usoBBDD

column_to_table_map = {
    # Tabla PERSON
    'person_id': 'person',
    'year_of_birth': 'person',
    'month_of_birth': 'person',
    'day_of_birth': 'person',
    'gender_concept_id': 'person',
    'race_concept_id': 'person',
    'ethnicity_concept_id': 'person',
    'location_id': 'person',
    'provider_id': 'person',
    'care_site_id': 'person',
    
    # Tabla OBSERVATION_PERIOD
    'observation_period_id': 'OBSERVATION_PERIOD',
    'person_id': 'OBSERVATION_PERIOD',
    'observation_period_start_date': 'OBSERVATION_PERIOD',
    'observation_period_end_date': 'OBSERVATION_PERIOD',
    
    # Tabla visit_occurrence
    'visit_occurrence_id': 'visit_occurrence',
    'person_id': 'visit_occurrence',
    'visit_concept_id': 'visit_occurrence',
    'visit_start_date': 'visit_occurrence',
    'visit_end_date': 'visit_occurrence',
    'visit_type_concept_id': 'visit_occurrence',
    'provider_id': 'visit_occurrence',
    'care_site_id': 'visit_occurrence',
    
    # Tabla CONDITION_OCCURRENCE
    'condition_occurrence_id': 'CONDITION_OCCURRENCE',
    'person_id': 'CONDITION_OCCURRENCE',
    'condition_concept_id': 'CONDITION_OCCURRENCE',
    'condition_start_date': 'CONDITION_OCCURRENCE',
    'condition_end_date': 'CONDITION_OCCURRENCE',
    'condition_type_concept_id': 'CONDITION_OCCURRENCE',
    'visit_occurrence_id': 'CONDITION_OCCURRENCE',
    
    # Tabla DRUG_EXPOSURE
    'drug_exposure_id': 'DRUG_EXPOSURE',
    'person_id': 'DRUG_EXPOSURE',
    'drug_concept_id': 'DRUG_EXPOSURE',
    'drug_exposure_start_date': 'DRUG_EXPOSURE',
    'drug_exposure_end_date': 'DRUG_EXPOSURE',
    'drug_type_concept_id': 'DRUG_EXPOSURE',
    'visit_occurrence_id': 'DRUG_EXPOSURE',
    'route_concept_id': 'DRUG_EXPOSURE',
    'dose_unit_concept_id': 'DRUG_EXPOSURE',
    
    # Tabla PROCEDURE_OCCURRENCE
    'procedure_occurrence_id': 'PROCEDURE_OCCURRENCE',
    'person_id': 'PROCEDURE_OCCURRENCE',
    'procedure_concept_id': 'PROCEDURE_OCCURRENCE',
    'procedure_date': 'PROCEDURE_OCCURRENCE',
    'procedure_type_concept_id': 'PROCEDURE_OCCURRENCE',
    'visit_occurrence_id': 'PROCEDURE_OCCURRENCE',
    
    # Tabla MEASUREMENT
    'measurement_id': 'MEASUREMENT',
    'person_id': 'MEASUREMENT',
    'measurement_concept_id': 'MEASUREMENT',
    'measurement_date': 'MEASUREMENT',
    'measurement_type_concept_id': 'MEASUREMENT',
    'value_as_number': 'MEASUREMENT',
    'value_as_concept_id': 'MEASUREMENT',
    'unit_concept_id': 'MEASUREMENT',
    'visit_occurrence_id': 'MEASUREMENT',
    
    # Tabla OBSERVATION
    'observation_id': 'OBSERVATION',
    'person_id': 'OBSERVATION',
    'observation_concept_id': 'OBSERVATION',
    'observation_date': 'OBSERVATION',
    'observation_type_concept_id': 'OBSERVATION',
    'value_as_string': 'OBSERVATION',
    'value_as_concept_id': 'OBSERVATION',
    'unit_concept_id': 'OBSERVATION',
    'visit_occurrence_id': 'OBSERVATION',
    
    # Tabla DEVICE_EXPOSURE
    'device_exposure_id': 'DEVICE_EXPOSURE',
    'person_id': 'DEVICE_EXPOSURE',
    'device_concept_id': 'DEVICE_EXPOSURE',
    'device_exposure_start_date': 'DEVICE_EXPOSURE',
    'device_exposure_end_date': 'DEVICE_EXPOSURE',
    'device_type_concept_id': 'DEVICE_EXPOSURE',
    'visit_occurrence_id': 'DEVICE_EXPOSURE',
    
    # Tabla DEATH
    'person_id': 'DEATH',
    'death_date': 'DEATH',
    'death_type_concept_id': 'DEATH',
    
    # Tabla NOTE
    'note_id': 'NOTE',
    'person_id': 'NOTE',
    'note_date': 'NOTE',
    'note_text': 'NOTE',
    
    # Tabla NOTE_NLP
    'note_nlp_id': 'NOTE_NLP',
    'note_id': 'NOTE_NLP',
    'note_date': 'NOTE_NLP',
    'section_concept_id': 'NOTE_NLP',
    'note_nlp_concept_id': 'NOTE_NLP',
    
    # Tabla COST
    'cost_id': 'COST',
    'person_id': 'COST',
    'total_charge': 'COST',
    'total_cost': 'COST',
    
    # Tabla PAYER_PLAN_PERIOD
    'payer_plan_period_id': 'PAYER_PLAN_PERIOD',
    'person_id': 'PAYER_PLAN_PERIOD',
    'payer_concept_id': 'PAYER_PLAN_PERIOD',
    'plan_concept_id': 'PAYER_PLAN_PERIOD',
    'payer_plan_period_start_date': 'PAYER_PLAN_PERIOD',
    'payer_plan_period_end_date': 'PAYER_PLAN_PERIOD',
    
    # Tabla CARE_SITE
    'care_site_id': 'CARE_SITE',
    'location_id': 'CARE_SITE',
    'care_site_name': 'CARE_SITE',
    
    # Tabla PROVIDER
    'provider_id': 'PROVIDER',
    'provider_name': 'PROVIDER',
    'provider_specialty_concept_id': 'PROVIDER',
    
    # Tabla LOCATION
    'location_id': 'LOCATION',
    'address_1': 'LOCATION',
    'address_2': 'LOCATION',
    'city': 'LOCATION',
    'state': 'LOCATION',
    'zip': 'LOCATION',
    
    # Tabla CONCEPT
    'concept_id': 'CONCEPT',
    'concept_name': 'CONCEPT',
    'domain_id': 'CONCEPT',
    'vocabulary_id': 'CONCEPT',
    'concept_class_id': 'CONCEPT',
    'standard_concept': 'CONCEPT',
    'concept_code': 'CONCEPT',
    
    # Tabla CONCEPT_ANCESTOR
    'ancestor_concept_id': 'CONCEPT_ANCESTOR',
    'descendant_concept_id': 'CONCEPT_ANCESTOR',
    
    # Tabla CONDITION_ERA
    'condition_era_id': 'CONDITION_ERA',
    'person_id': 'CONDITION_ERA',
    'condition_concept_id': 'CONDITION_ERA',
    'condition_era_start_date': 'CONDITION_ERA',
    'condition_era_end_date': 'CONDITION_ERA',
    
    # Tabla DRUG_ERA
    'drug_era_id': 'DRUG_ERA',
    'person_id': 'DRUG_ERA',
    'drug_concept_id': 'DRUG_ERA',
    'drug_era_start_date': 'DRUG_ERA',
    'drug_era_end_date': 'DRUG_ERA'
}

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

def extract_predictive_features(db_path, tables, feature_columns, target_table, target_condition, cohort_name, 
                                index_date_column, start_date='2019-01-01', end_date='2023-01-01'):
    """
    Extrae las variables predictorias de una base de datos DuckDB y añade la columna 'target' a partir de una tabla de outcomes.
    
    :param db_path: Ruta al archivo de base de datos DuckDB.
    :param tables: Lista de nombres de tablas a combinar.
    :param feature_columns: Diccionario con las columnas a seleccionar como variables predictorias por tabla.
    :param target_table: Nombre de la tabla que contiene los outcomes.
    :param target_condition: Condición que define el valor de 'target'.
    :param cohort_name: Nombre de la tabla de características predictoras resultante.
    :param index_date_column: Columna que define la fecha índice.
    :param start_date: Fecha de inicio para la ventana de tiempo (por defecto '2019-01-01').
    :param end_date: Fecha de fin para la ventana de tiempo (por defecto '2023-01-01').
    :return: DataFrame con las variables predictorias resultantes y la columna 'target'.
    """
    
    # Conectar a la base de datos
    con = duckdb.connect(database=db_path, read_only=False)
    join_conditions = {
    "visit_occurrence": "person.person_id = visit_occurrence.person_id",
    "drug_exposure": "person.person_id = drug_exposure.person_id",
    "condition_occurrence": "person.person_id = condition_occurrence.person_id"
    }
    # Construir la cláusula de combinación de tablas de manera dinámica usando join_conditions
    join_clauses = []
    base_table = tables[0]  # La primera tabla es la base
    tables_clause = f'"{base_table}"'
    
    for table in tables[1:]:
        if table in join_conditions:
            join_condition = join_conditions[table]
            tables_clause += f' LEFT JOIN "{table}" ON {join_condition}'
        else:
            raise ValueError(f"No join condition provided for table: {table}")
    
    # Crear la cláusula de selección de columnas, incluyendo el target
    feature_columns_clause = ', '.join([f'{table}.{col}' for table, cols in feature_columns.items() for col in cols])
    
    # Construir la consulta SQL para extraer las variables predictorias y la columna target
    query = f"""
    CREATE OR REPLACE TABLE {cohort_name} AS
    SELECT {feature_columns_clause},
           CASE WHEN {target_condition} THEN 1 ELSE 0 END AS target,
           {index_date_column} BETWEEN '{start_date}' AND '{end_date}' AS within_time_window
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

def extract_tables_from_conditions(conditions):
    """
    Extrae nombres de tablas de las condiciones SQL proporcionadas.
    Asume que los nombres de tabla preceden a las columnas, como en 'person.year_of_birth'.
    """
    tables = set()
    for condition in conditions:
        table = condition.split('.')[0]  # Extraer la parte antes del '.'
        tables.add(table)
    return list(tables)

def create_cohort_conditions(db_path, where_conditions, cohort_name):
    """
    Crea una cohorte en una base de datos DuckDB a partir de condiciones proporcionadas, deduciendo las tablas necesarias.

    :param db_path: Ruta al archivo de base de datos DuckDB.
    :param where_conditions: Lista de condiciones SQL que incluyen solo las características (sin tablas).
    :param cohort_name: Nombre de la tabla de cohorte resultante.
    :return: DataFrame con la cohorte resultante.
    """
    # Identificar las tablas a partir de las condiciones
    tables = set()
    for condition in where_conditions:
        for column in column_to_table_map:
            if column in condition:
                tables.add(column_to_table_map[column])
    
    # Generar las condiciones de JOIN basadas en las tablas
    joins_clause, _ = usoBBDD.generar_joins(db_path, list(tables))
    
    # Crear la cláusula de combinación de tablas
    tables_clause = f'"{list(tables)[0]}"'
    for table in list(tables)[1:]:
        tables_clause += f' LEFT JOIN "{table}" ON {joins_clause}'
    
    # Crear la cláusula de condiciones
    where_clause = ' AND '.join(where_conditions)
    
    # Construir la consulta SQL
    query = f"""
    CREATE OR REPLACE TABLE {cohort_name} AS
    SELECT *
    FROM {tables_clause}
    WHERE {where_clause}
    """
    
    print("Executing query:")
    print(query)
    
    # Ejecutar la consulta y obtener los resultados
    con = duckdb.connect(database=db_path, read_only=False)
    con.execute(query)
    result = con.execute(f"SELECT * FROM {cohort_name}").df()
    con.close()
    
    return result


def create_target_outcome_cohorts_conditions(db_path, target_conditions, outcome_conditions, target_cohort_name, outcome_cohort_name):
    """
    Crea las cohortes de objetivo y de resultado deduciendo las tablas necesarias a partir de las condiciones.

    :param db_path: Ruta al archivo de base de datos DuckDB.
    :param target_conditions: Lista de condiciones para la cohorte de objetivo (sin tablas).
    :param outcome_conditions: Lista de condiciones para la cohorte de resultado (sin tablas).
    :param target_cohort_name: Nombre de la cohorte de objetivo.
    :param outcome_cohort_name: Nombre de la cohorte de resultado.
    :return: Tupla de DataFrames con las cohortes de objetivo y de resultado.
    """
    # Identificar tablas para target y outcome
    target_tables = set()
    outcome_tables = set()
    
    for condition in target_conditions:
        for column in column_to_table_map:
            if column in condition:
                target_tables.add(column_to_table_map[column])
    
    for condition in outcome_conditions:
        for column in column_to_table_map:
            if column in condition:
                outcome_tables.add(column_to_table_map[column])

    # Crear las cohortes
    target_cohort_df = create_cohort_conditions(db_path, where_conditions=target_conditions, cohort_name=target_cohort_name)
    outcome_cohort_df = create_cohort_conditions(db_path, where_conditions=outcome_conditions, cohort_name=outcome_cohort_name)
    
    return target_cohort_df, outcome_cohort_df


def extract_predictive_features_conditions(db_path, feature_conditions, target_condition, cohort_name, 
                                index_date_column, start_date, end_date):
    """
    Extrae variables predictorias deduciendo las tablas a partir de las condiciones proporcionadas.
    
    :param db_path: Ruta al archivo de base de datos DuckDB.
    :param feature_conditions: Lista de condiciones para las características predictivas (sin tablas).
    :param target_condition: Condición que define el valor de 'target'.
    :param cohort_name: Nombre de la cohorte de características predictoras resultante.
    :param index_date_column: Columna que define la fecha índice.
    :param start_date: Fecha de inicio para la ventana de tiempo.
    :param end_date: Fecha de fin para la ventana de tiempo.
    :return: DataFrame con las variables predictorias resultantes.
    """
    # Identificar las tablas a partir de las condiciones
    tables = set()
    for condition in feature_conditions:
        for column in column_to_table_map:
            if column in condition:
                tables.add(column_to_table_map[column])
    
    # Asegúrate de que la tabla "person" esté presente solo si no existe ya en el conjunto de tablas
    if 'person' not in tables:
        tables.add('person')
    
    # Asegúrate de incluir la tabla que contiene `condition_concept_id`
    tables.add('CONDITION_ERA')  # Agrega la tabla que contiene la columna que necesitas
    
    # Generar las condiciones de JOIN basadas en atributos comunes
    tables_clause = f'"{list(tables)[0]}"'  # Comienza con la primera tabla
    for table in list(tables)[1:]:
        join_condition = f'person.person_id = {table}.person_id'  # Asegúrate de que sea correcto para todas las tablas
        tables_clause += f' LEFT JOIN "{table}" ON {join_condition}'
    
    # Crear la cláusula de selección de columnas
    feature_columns_clause = ', '.join(feature_conditions)
    
    # Construir la consulta SQL
    query = f"""
    CREATE OR REPLACE TABLE {cohort_name} AS
    SELECT {feature_columns_clause},
           CASE WHEN CONDITION_ERA.condition_concept_id = 201826 THEN 1 ELSE 0 END AS target,
           {index_date_column} BETWEEN '{start_date}' AND '{end_date}' AS within_time_window
    FROM {tables_clause}
    """
    
    print("Executing query:")
    print(query)
    
    # Ejecutar la consulta y obtener los resultados
    con = duckdb.connect(database=db_path, read_only=False)
    con.execute(query)
    result = con.execute(f"SELECT * FROM {cohort_name}").df()
    con.close()
    
    return result


