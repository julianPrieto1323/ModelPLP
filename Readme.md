# Patient Level Prediction (PLP) Project

This project implements a Patient Level Prediction (PLP) study using the Observational Medical Outcomes Partnership (OMOP) data model. The objective of the study is to predict a clinical outcome from predictor variables derived from patient data.

## Project Structure
The project source code is organized in the **src** folder, which contains three main scripts:

**1. requests.py**:

This script contains the functions for making requests to the database. It allows the extraction of data needed to create cohorts and obtain predictor variables.

**2. cohortes.py**

Defines the functions for the creation of patient cohorts (target cohort and outcome cohort) and the extraction of predictive features. These data are essential for running the PLP study.

Main functions:** ** Create_cohort: Creates a cohort
- create_cohort: Creates a cohort of patients from the tables, columns and conditions provided.
- extract_predictive_features: Extract the predictive variables, selecting the relevant features for prediction.

**3. plp.py**

This script runs the PLP study and trains a classification model, evaluating its performance.

**4. miscelania.py**

This script shows metrics from the algorithims ran on the plp study.

**Main functions:**

- **run_plp_with_algorithms**: Performs the entire PLP study flow. It includes creating cohorts, extracting the predictive features, training the prediction model and evaluating its accuracy. The algorithims are: Decision Tree, Random Forest, Logistic Regression, SVM, MLP Classifier.

## Project Flow

**1. Data Extraction:** 

Patient cohorts are extracted from the database using the functions in `cohortes.py`, using the specified tables and conditions.

**2. Cohort Creation:** 

Target Cohort

**- Target Cohort:** 
Defines the target patient population for the study.

**- Outcome Cohort:** 

Defines the clinical outcome to be predicted (e.g. diagnosis of a disease).

**3. Extraction of Predictor Variables:** 

Patient characteristics, such as medical visits, medication prescriptions and previous conditions, are extracted and will be used as input variables in the model.

**4. Model Training:** 

The `plp.py` script trains a classification model based on the input data, using an algorithm such as the `scikit-learn` decision tree.

**5. Model Evaluation:** 

Model performance is evaluated using `accuracy`, `recall` and `F1-score` metrics.


## Instructions for Executing the Project

**1. Prerequisites:** 

- Install `duckdb` for database management.
- Install `scikit-learn` for model training and evaluation.
- Install `pandas` for DataFrames management.

To install these packages you can run

`pip install duckdb scikit-learn pandas`

**Execute the Study:**

- Set up the tables and conditions for the cohorts and predictor variables in the corresponding scripts.

- Run the main notebook to train the model and obtain the results:

`main.ipynb`

**3. Evaluation of Results:**
- The `miscelania.py` script will print the model metrics, **precision**, **recall** and **F1-score**.

## Customization

This project is designed to be flexible and customizable. You can change the tables, columns and conditions to adjust the analysis to your needs. In addition, you can experiment with different sorting algorithms in `plp.py` to improve the accuracy of the model.

## Dataset 

The dataset used is located at [datasets](https://github.com/OHDSI/EunomiaDatasets/tree/3efd533eb95a41a56d5b0758b0d7c8fa57e1303e/datasets) and the CSV set in the Synthea folder has been used.


## Usage/Examples
This section describes the principal funtions of the repository

### Downloading dataset
The function loadSynthea lets you download the dataset Synthea to try the API with a CDM Dataset
```python
import peticiones from src
peticiones.loadSynthea(path) 
```
### Creating a cohort
This function lets you create a cohort from the full Dataset with specific characteristics

```python
import cohortes from src
cohortes.create_cohort(db_path, conditions, cohort_name) 
```
### Extract predictive features
This function lets you extract the predictives variables from a Database specifying the target feature.

```python
import cohortes from src
cohortes.extract_predictive_features_with_target(db_path, cohort_name, target) 
```

### Cohort to pandas
This function lets you convert the cohorts created to a pandas dataframe to use it for PLP study.

```python
import cohortes from src
cohortes.cohort_to_pd(cohort_name) 
```