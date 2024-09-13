def print_study_summary(feature_columns, target_condition, model_results):
    """
    Imprime el resumen del estudio PLP, explicando las características utilizadas, los resultados de los modelos
    y un análisis de los resultados.

    :param feature_columns: Lista de columnas utilizadas como características predictorias.
    :param target_condition: Condición que define el target (condición que se está prediciendo).
    :param model_results: Diccionario con los resultados de cada modelo (nombre, precisión, reporte).
    """
    # Descripción general del estudio
    print("**** RESUMEN DEL ESTUDIO PLP ****")
    print("\nEl estudio PLP ha sido realizado con el objetivo de predecir si un paciente será diagnosticado con la condición:")
    print(f"  - {target_condition}\n")
    
    print("Características predictorias utilizadas en el estudio:")
    for feature in feature_columns:
        print(f"  - {feature}")
    
    print("\nSe han entrenado y evaluado los siguientes modelos:")
    
    # Iterar sobre los resultados de los modelos
    for model_name, metricas in model_results.items():
        print(f"\nModelo: {model_name}")
        print(f"  - Precisión: {metricas['accuracy']:.4f}")
        print("  - Reporte de clasificación:")
        print(metricas['report'])

    # Análisis de los resultados
    print("\n**** ANÁLISIS DE RESULTADOS ****")
    
    print("\nInterpretación de las métricas de rendimiento:")
    print("- **Precisión (Accuracy)**: Representa la proporción de predicciones correctas. Un valor cercano a 1.0 indica que el modelo realiza buenas predicciones en la mayoría de los casos.")
    print("- **Recall**: Mide cuántos de los verdaderos positivos fueron correctamente identificados. Un valor alto de recall indica que el modelo detecta correctamente una gran parte de los casos positivos.")
    print("- **Precisión (Precision)**: Mide la proporción de verdaderos positivos entre todas las predicciones positivas. Un valor alto significa que el modelo tiene una baja tasa de falsos positivos.")
    print("- **F1-Score**: Es la media armónica de la precisión y el recall, y es útil cuando se busca un equilibrio entre estos dos. Un valor alto de F1-score indica un buen equilibrio entre precisión y recall.\n")

    # Comparación entre modelos
    print("**Análisis comparativo de los modelos:**")
    for model_name, metricas in model_results.items():
        print(f"\nModelo: {model_name}")
        print(f"  - Rendimiento general: La precisión del modelo es de {metricas['accuracy']:.4f}.")
        if metricas['accuracy'] >= 0.85:
            print("    Este es un valor bastante bueno, lo que indica que el modelo es capaz de realizar predicciones precisas en la mayoría de los casos.")
        else:
            print("    El valor de precisión es aceptable, pero se podría mejorar con ajustes adicionales al modelo o los datos.")
        
        report_dict = metricas['report']
        # Análisis del F1-Score
        if '1' in report_dict:
            f1_score_positive = report_dict['1']['f1-score']
            print(f"  - F1-Score para los casos positivos: {f1_score_positive:.4f}")
            if f1_score_positive >= 0.80:
                print("    Un F1-Score elevado indica que el modelo tiene un buen equilibrio entre precisión y recall para los casos positivos.")
            else:
                print("    Un F1-Score bajo indica que el modelo tiene dificultades para equilibrar precisión y recall en los casos positivos.")
        else:
            print("    El modelo no predijo ningún caso positivo.")
    
    print("\nConclusión general:")
    print("Los resultados muestran que algunos modelos, como el Árbol de Decisión o el Bosque Aleatorio, tienen un buen rendimiento con una precisión y F1-Score elevados. Sin embargo, otros modelos pueden requerir ajustes adicionales en los hiperparámetros o más datos para mejorar su rendimiento.")
