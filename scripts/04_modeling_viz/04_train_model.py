import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import StandardScaler # <--- Mejora técnica
import joblib
import os

# === CONFIGURACIÓN ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
INPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_final.csv")
MODEL_DIR = os.path.join(ROOT_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

def train_optimized():
    print("🛠️ Ejecutando ajuste rápido y optimización del modelo...")
    df = pd.read_csv(INPUT_PATH)
    
    # Definimos las variables incluyendo la del TSV
    features = [
        'score_genetic_association', 
        'score_literature', 
        'score_rna_expression', 
        'score_animal_model', 
        'score_affected_pathway',
        'geneticConstraint' # <--- Aquí está nuestra variable clave
    ]
    
    X = df[features]
    y = df['target_success']

    # Escalado de datos (Estandarización)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=y
    )

    # Entrenamos el Random Forest con pesos balanceados
    model = RandomForestClassifier(
        n_estimators=200, 
        max_depth=10,
        class_weight='balanced', # <--- Ayuda con el desbalanceo
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    # Evaluación
    y_prob = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_prob)
    
    print(f"\n🚀 NUEVO ROC-AUC SCORE: {auc:.4f}")
    print("-" * 30)

    # Importancia de Variables
    importances = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
    print("💎 NUEVA IMPORTANCIA DE VARIABLES:")
    print(importances)

    # Guardar modelo y scaler (necesitarás el scaler en el dashboard también)
    joblib.dump(model, os.path.join(MODEL_DIR, "alzheimer_drug_predictor.pkl"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, "scaler.pkl"))
    
    # Gráfica
    plt.figure(figsize=(10,6))
    sns.barplot(x=importances.values, y=importances.index, palette="magma")
    plt.title("Importancia de Variables (Modelo Optimizado)")
    plt.savefig(os.path.join(ROOT_DIR, "data", "processed", "feature_importance_optimized.png"))
    
    print("\n✅ Ajuste completado. Modelo y gráfica actualizados.")

if __name__ == "__main__":
    train_optimized()