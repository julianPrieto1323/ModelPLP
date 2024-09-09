import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score
import sys
import os

# Agregar el directorio raíz al sys.path
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(root_dir)

from miscelania import consultas as consultas 
from miscelania import graficas as graficas
from miscelania import herramientas as herramientas
# Cargar los datos
# Asegúrate de reemplazar 'your_data.csv' con la ruta correcta a tu archivo de datos
# Consulta SQL para extraer datos
herramientas.clear_console()
query = """
SELECT
    "desynpuf_id", "bene_birth_dt", "bene_death_dt", "bene_sex_ident_cd", "bene_race_cd",
    "bene_esrd_ind", "sp_state_code", "bene_county_cd", "bene_hi_cvrage_tot_mons", "bene_smi_cvrage_tot_mons",
    "bene_hmo_cvrage_tot_mons", "plan_cvrg_mos_num", "sp_alzhdmta", "sp_chf", "sp_chrnkidn",
    "sp_cncr", "sp_copd", "sp_depressn", "sp_diabetes", "sp_ischmcht", "sp_osteoprs", "sp_ra_oa",
    "sp_strketia", "medreimb_ip", "benres_ip", "pppymt_ip", "medreimb_op", "benres_op", "pppymt_op",
    "medreimb_car", "benres_car", "pppymt_car", "clm_id", "clm_from_dt", "clm_thru_dt", "icd9_dgns_cd_1",
    "prf_physn_npi_1", "hcpcs_cd_1", "line_nch_pmt_amt_1", "line_bene_ptb_ddctbl_amt_1", "line_coinsrnc_amt_1",
    "line_prcsg_ind_cd_1", "line_icd9_dgns_cd_1"
FROM cohorte_1
WHERE "sp_diabetes" IS NOT NULL
ORDER BY RANDOM()
LIMIT 100
"""
#Realizamos la consulta SQL

#Guardamos el resultado de la consulta en un dataframe de Pandas
df = consultas.execute_query(query)
print("El DF cargado es: ")
print(df.head())

# Preparar los datos
# Seleccionar características y objetivo
# Lista de características ajustada
features = [
    'bene_birth_dt', 'bene_sex_ident_cd', 'bene_race_cd', 'sp_state_code', 'bene_county_cd',
    'bene_hi_cvrage_tot_mons', 'bene_smi_cvrage_tot_mons', 'bene_hmo_cvrage_tot_mons',
    'plan_cvrg_mos_num', 'sp_alzhdmta', 'sp_chf', 'sp_chrnkidn', 'sp_cncr', 'sp_copd',
    'sp_depressn', 'sp_ischmcht', 'sp_osteoprs', 'sp_ra_oa', 'sp_strketia',
    'medreimb_ip', 'benres_ip', 'pppymt_ip', 'medreimb_op', 'benres_op', 'pppymt_op',
    'medreimb_car', 'benres_car', 'pppymt_car'
]
target = 'sp_diabetes'
# Verificar la distribución de la variable objetivo
print("Distribución de la variable objetivo (sp_diabetes):")
print(df[target].value_counts())

# Verificar si los nombres de columnas son correctos
missing_features = [f for f in features if f not in df.columns]
if missing_features:
    print(f"Las siguientes columnas están faltando: {missing_features}")

# Preparar los datos si todas las columnas están presentes
if not missing_features:
    X = df[features]
    y = df[target]

    # Manejar valores nulos
    X = X.fillna(0)  # O usa una estrategia más adecuada para tu caso
    y = y.fillna(0)

    # Dividir los datos en conjuntos de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Crear el modelo de regresión logística
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)

    # Realizar predicciones
    y_pred = model.predict(X_test)

    # Evaluar el modelo
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    print(f"Accuracy: {accuracy:.2f}")
    print("Classification Report:")
    print(report)
    graficas.plot_target_distribution(df, 'sp_diabetes')
    graficas.plot_feature_histograms(df, features)
    graficas.plot_correlation_matrix(df, features)
    graficas.plot_confusion_matrix(y_test, y_pred)
    

else:
    print("No se puede continuar con el modelo debido a columnas faltantes.")
