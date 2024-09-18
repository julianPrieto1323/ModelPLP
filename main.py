import duckdb
from src import usoBBDD
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

# Configuración para el ejemplo simplificado
db_path = 'data/synthea_bbdd'
target_tables = ['person']  # Simplificado para pruebas
target_conditions = ['person.year_of_birth < 1990']  # Ejemplo: personas nacidas antes de 1990
cohort_name = 'cohorte_1'

# Ejecutar la creación de la cohorte
cohorte_1 = create_cohort(
    db_path=db_path,
    tables=target_tables,
    where_conditions=target_conditions,
    cohort_name=cohort_name
)

print('==============================')
print('          COHORTE 1')
print(cohorte_1.head())
