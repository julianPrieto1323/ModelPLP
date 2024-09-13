import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from src import peticiones, plp, cohortes
#peticiones.crear_bbdd_desde_csv('data/Eunomia/Synthea27Nj_5.4', 'data/Eunomia/Synthea27Nj_5.4_BBDD')
'''
####################################################
####################################################
            CREACIÓN DE COHORTES
####################################################
####################################################

'''
# Parámetros de ejemplo
db_path = 'data/Eunomia/synthea27nj_5.4_bbdd'

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
target_condition = 'CONDITION_ERA.condition_concept_id = 201826'  # Ejemplo: Diabetes

# Ejecutar el PLP
import json
from achilles import Achilles

# Cargar la configuración de ACHILLES
config = {
    "data_source": "data/Eunomia/synthea27nj_5.4_bbdd",  # Reemplaza con el nombre de tu fuente de datos
    "data_format": "omop_cdm"  # Reemplaza con el formato correcto de tus datos (OMOP CDM)
}

# Crear un objeto ACHILLES
achilles = Achilles(config)

# Evaluar la calidad de los datos
quality_report = achilles.run()

# Mostrar el informe de calidad
print(quality_report)

# Ejemplo de uso: Calificar la calidad de los datos según diferentes métricas
metrics = {
    "data_distribution": quality_report["data_distribution"],
    "missing_values": quality_report["missing_values"]
}

# Calcular un score de calidad general
score = achilles.calculate_score(metrics)

print(f"Score de calidad: {score:.2f}")