from src import plp, miscelania

# Parámetros de ejemplo
db_path = 'data/Eunomia/synthea27nj_5.4_bbdd'

#peticiones.crear_bbdd_desde_csv('data/Eunomia/Synthea27Nj_5.4', 'data/Eunomia/Synthea27Nj_5.4_BBDD')

# Parámetros para la cohorte target
target_tables = ['person', 'VISIT_DETAIL']
target_join_columns = ['person.person_id = VISIT_DETAIL.person_id']
target_conditions = ['person.year_of_birth < 1990']  # Ejemplo: personas mayores de 30 años

# Parámetros para la cohorte outcome
outcome_tables = ['person', 'CONDITION_ERA', 'CONCEPT']
outcome_join_columns = ['person.person_id = CONDITION_ERA.person_id', 'CONDITION_ERA.condition_concept_id = CONCEPT.concept_id']
outcome_conditions = ['CONCEPT.concept_name = \'Acute viral pharyngitis\'']  # Ejemplo: pacientes con Acute viral pharyngitis

# Parámetros para las características predictorias
feature_tables = ['person', 'VISIT_DETAIL', 'DRUG_EXPOSURE']
feature_join_columns = [
    'person.person_id = VISIT_DETAIL.person_id',
    'person.person_id = DRUG_EXPOSURE.person_id'
]
feature_conditions = ['VISIT_DETAIL.visit_detail_start_date BETWEEN \'2019-01-01\' AND \'2023-01-01\'']
feature_columns = ['person.person_id', 'person.year_of_birth', 'VISIT_DETAIL.visit_detail_start_date', 'DRUG_EXPOSURE.drug_concept_id']

# Definir tabla de outcome (target) y condiciones
target_table = 'CONDITION_ERA'
target_join_column = 'person.person_id = CONDITION_ERA.person_id'
target_condition = 'CONDITION_ERA.condition_concept_id = 201826'  # Ejemplo: Diabetes Milletius de tipo 2

# Ejecutar el PLP
results = plp.run_plp_with_algorithms(db_path, target_tables, target_join_columns, target_conditions, outcome_tables, outcome_join_columns, outcome_conditions, 
                                  feature_tables, feature_join_columns, feature_conditions, feature_columns, target_table, target_join_column, target_condition)

miscelania.print_study_summary(feature_columns, target_condition, results)