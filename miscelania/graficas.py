import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import roc_curve, auc, confusion_matrix, ConfusionMatrixDisplay
import pandas as pd

def plot_target_distribution(df, target):
    """Plot the distribution of the target variable."""
    plt.figure(figsize=(8, 6))
    sns.countplot(x=target, data=df)
    plt.title(f'Distribución de la variable objetivo ({target})')
    plt.xlabel(target)
    plt.ylabel('Count')
    plt.savefig("imagenes/Distribución de datos de los datos de diabetes.png")
    plt.show()

def plot_feature_histograms(df, features, num_features=10):
    """Plot histograms for the first `num_features` of the provided features."""
    plt.figure(figsize=(15, 10))
    features_to_plot = features[:num_features]  # Limitar al número especificado de características
    for i, feature in enumerate(features_to_plot):
        plt.subplot(2, 5, i+1)
        sns.histplot(df[feature], kde=True, bins=30)
        plt.title(feature)
        plt.xlabel('')
        plt.ylabel('')
    plt.tight_layout()
    plt.savefig("imagenes/Histograma de las características del DF.png")
    plt.show()

def plot_correlation_matrix(df, features):
    """Plot the correlation matrix of the provided features."""
    corr_matrix = df[features].corr()
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f', vmin=-1, vmax=1)
    plt.title('Matriz de Correlación de Características')
    plt.savefig("imagenes/Matriz de correlación de las características del DF.png")
    plt.show()


def plot_confusion_matrix(y_true, y_pred):
    # Convertir las etiquetas de 1 y 2 a 0 y 1
    y_true = (y_true - 1).astype(int)
    y_pred = (y_pred - 1).astype(int)
    
    # Calcular la matriz de confusión
    cm = confusion_matrix(y_true, y_pred)
    
    # Crear una instancia de ConfusionMatrixDisplay
    disp = ConfusionMatrixDisplay(confusion_matrix=cm)
    
    # Plotear la matriz de confusión
    disp.plot(cmap=plt.cm.Blues)
    plt.savefig("imagenes/Matriz de confusión del modelo de regresión.png")
    plt.show()
