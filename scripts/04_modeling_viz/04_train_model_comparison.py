import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler
import joblib
import os

# === CONFIGURACIÓN ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
INPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_final.csv")
MODEL_DIR = os.path.join(ROOT_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

def run_tournament():
    print("🏆 Iniciando Torneo de Modelos (Versión Final)...")
    df = pd.read_csv(INPUT_PATH)
    
    features = [
        'score_genetic_association', 'score_literature', 'score_rna_expression', 
        'score_animal_model', 'score_affected_pathway', 'geneticConstraint'
    ]
    X = df[features]
    y = df['target_success']

    # 1. Escalado (Vital para que la Regresión Logística compita justamente)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=y
    )

    # Cálculo del peso para XGBoost
    ratio = (len(y) - sum(y)) / sum(y)

    # Definición de los 3 contendientes
    models = {
        "Regresión Logística": LogisticRegression(class_weight='balanced', random_state=42),
        "Random Forest": RandomForestClassifier(n_estimators=200, class_weight='balanced', random_state=42),
        "XGBoost": XGBClassifier(scale_pos_weight=ratio, eval_metric='logloss', random_state=42)
    }

    print("\n📊 RESULTADOS DE LA COMPARATIVA:")
    print("-" * 40)
    
    best_auc = 0
    best_model_name = ""

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_prob = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_prob)
        print(f"🔹 {name}: AUC = {auc:.4f}")
        
        if auc > best_auc:
            best_auc = auc
            best_model_name = name

    # 2. Análisis detallado del ganador
    print(f"\n🏆 EL GANADOR ES: {best_model_name}")
    winner_model = models[best_model_name]
    
    # Guardar modelo y scaler
    joblib.dump(winner_model, os.path.join(MODEL_DIR, "alzheimer_drug_predictor.pkl"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, "scaler.pkl"))

    # 3. Importancia de Variables del ganador (si es basado en árboles)
    if hasattr(winner_model, 'feature_importances_'):
        importances = pd.Series(winner_model.feature_importances_, index=features).sort_values(ascending=False)
        print("\n💎 IMPORTANCIA DE VARIABLES DEL GANADOR:")
        print(importances)
        
        # Guardar gráfica
        plt.figure(figsize=(10,6))
        sns.barplot(x=importances.values, y=importances.index, hue=importances.index, palette="viridis", legend=False)
        plt.title(f"Importancia de Variables - {best_model_name}")
        plt.savefig(os.path.join(ROOT_DIR, "data", "processed", "final_feature_importance.png"))

    print("\n✅ Proceso finalizado. Todo listo para el TFM.")

if __name__ == "__main__":
    run_tournament()
    