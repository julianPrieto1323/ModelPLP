import duckdb
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, classification_report
from . import cohortes
import pandas as pd

# Función principal para ejecutar el PLP
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier

def run_plp_with_algorithms(db_path, target_tables, target_join_columns, target_conditions, outcome_tables, outcome_join_columns, outcome_conditions, feature_tables, feature_join_columns, feature_conditions, feature_columns, target_table, target_join_column, target_condition):
    # Crear cohorte target
    print("Creating target cohort...")
    target_cohort_df = cohortes.create_cohort(db_path, target_tables, target_join_columns, target_conditions, 'target_cohort')
    print('Target Cohort')
    print(target_cohort_df.head())
    # Crear cohorte outcome
    print("Creating outcome cohort...")
    outcome_cohort_df = cohortes.create_cohort(db_path, outcome_tables, outcome_join_columns, outcome_conditions, 'outcome_cohort')
    print('Outcome Cohort')
    print(outcome_cohort_df.head())

    # Extraer variables predictorias y añadir la columna target
    print("Extracting predictive features with target...")
    predictive_features_df = cohortes.extract_predictive_features_with_target(
        db_path, feature_tables, feature_join_columns, feature_conditions, feature_columns, target_table, target_join_column, target_condition, 'predictive_features'
    )
    print('Predictive features: ')
    print(predictive_features_df.head())
    # Conversión de columnas datetime a formato adecuado para el modelo
    if 'visit_detail_start_date' in predictive_features_df.columns:
        reference_date = pd.to_datetime('2020-01-01')  # Ejemplo de referencia
        predictive_features_df['visit_detail_start_date'] = (pd.to_datetime(predictive_features_df['visit_detail_start_date']) - reference_date).dt.days

    # Separar características (X) y el target (y)
    X = predictive_features_df.drop('target', axis=1)
    y = predictive_features_df['target']
    
    # Asegurarse de que no haya columnas de tipo datetime
    X = X.apply(pd.to_numeric, errors='coerce').fillna(0)
    
    # Dividir el conjunto de datos en entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Lista de modelos a evaluar
    algorithms = {
        "Decision Tree": DecisionTreeClassifier(class_weight='balanced'),
        "Random Forest": RandomForestClassifier(class_weight='balanced'),
        "Logistic Regression": LogisticRegression(class_weight='balanced', max_iter=1000),
        "SVM": SVC(class_weight='balanced'),
        "MLP Classifier": MLPClassifier(max_iter=1000)
    }

    results = {}

    # Entrenar y evaluar cada algoritmo
    for name, model in algorithms.items():
        print(f"Training {name}...")
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        
        # Calcular precisión y reporte de clasificación
        accuracy = accuracy_score(y_test, y_pred)
        report = classification_report(y_test, y_pred, zero_division=0)
        
        results[name] = {
            "model": model,
            "accuracy": accuracy,
            "report": report
        }
        
        print(f"{name} Accuracy: {accuracy}")
        print(f"{name} Classification Report:\n{report}\n")

    return results

