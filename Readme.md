
# Proyecto de Predicción a Nivel de Paciente (PLP)

Este proyecto implementa un estudio de Predicción a Nivel de Paciente (PLP) utilizando el modelo de datos OMOP (Observational Medical Outcomes Partnership). El objetivo del estudio es predecir un desenlace clínico a partir de variables predictorias derivadas de datos de pacientes.

## Estructura del Proyecto
El código fuente del proyecto está organizado en la carpeta **src**, que contiene tres scripts principales:

**1. peticiones.py**:

Este script contiene las funciones para realizar solicitudes a la base de datos. Permite la extracción de datos necesarios para la creación de cohortes y la obtención de variables predictorias.

**2. cohortes.py**

Define las funciones para la creación de cohortes de pacientes (target cohort y outcome cohort) y la extracción de las variables predictorias (predictive features). Estos datos son fundamentales para ejecutar el estudio PLP.

**Funciones principales:**
- create_cohort: Crea una cohorte de pacientes a partir de las tablas, columnas y condiciones proporcionadas.
- extract_predictive_features: Extrae las variables predictorias, seleccionando las características relevantes para la predicción.

**3. plp.py**

Este script ejecuta el estudio PLP y entrena un modelo de clasificación, evaluando su rendimiento.

**Funciones principales:**

- **run_plp**: Realiza todo el flujo del estudio PLP. Incluye la creación de cohortes, la extracción de las características predictorias, el entrenamiento del modelo de predicción y la evaluación de su precisión.

## Flujo del Proyecto

**1. Extracción de Datos:** Se extraen las cohortes de pacientes de la base de datos mediante las funciones en `cohortes.py`, utilizando las tablas y condiciones especificadas.

**2. Creación de Cohortes:**

**- Target Cohort:** Define la población de pacientes objetivo para el estudio.

**- Outcome Cohort:** Define el desenlace clínico que se desea predecir (por ejemplo, diagnóstico de una enfermedad).

**3. Extracción de Variables Predictorias:**
Se extraen características de los pacientes, como visitas médicas, prescripciones de medicamentos y condiciones previas, que se utilizarán como variables de entrada en el modelo.

**4. Entrenamiento del Modelo:** El script `plp.py` entrena un modelo de clasificación basado en los datos de entrada, utilizando un algoritmo como el árbol de decisión de `scikit-learn`.

**5. Evaluación del Modelo:** Se evalúa el rendimiento del modelo utilizando métricas de `precisión`, `recall` y `F1-score`.

## Instrucciones para Ejecutar el Proyecto

**1. Requisitos Previos:** 

- Instalar `duckdb` para la gestión de la base de datos.
- Instalar `scikit-learn` para el entrenamiento y evaluación del modelo.
- Instalar `pandas` para el manejo de los DataFrames.

Para instalar, estos paquetes puedes ejecutar:

`pip install duckdb scikit-learn pandas
`

**2.- Ejecutar el Estudio:**

- Configura las tablas y condiciones para las cohortes y variables predictorias en los scripts correspondientes.

- Ejecuta el notebook principal para entrenar el modelo y obtener los resultados:

`main.ipynb`

**3. Evaluación de Resultados:**
- El script `plp.py` imprimirá las métricas del modelo, **precisión**, **recall** y **F1-score**.

## Personalización

Este proyecto está diseñado para ser flexible y adaptable. Puedes cambiar las tablas, las columnas y las condiciones para ajustar el análisis a tus necesidades. Además, puedes experimentar con diferentes algoritmos de clasificación en `plp.py` para mejorar la precisión del modelo.
## Dataset
El dataset utilizado en este proyecto se encuentra en https://www.kaggle.com/datasets/drscarlat/cmssynpuf55m se ha usado la carpeta Synthea27Nj_5.4. En esta carpeta se encuentran los archivos CSV que forman la BBDD.
