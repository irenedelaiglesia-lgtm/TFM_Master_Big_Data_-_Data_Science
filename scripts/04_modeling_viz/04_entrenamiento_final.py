import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler
import joblib
import os

# === CONFIGURACIÓN ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
INPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_enriquecido_v2.csv")
MODEL_DIR = os.path.join(ROOT_DIR, "models")
REPORTS_DIR = os.path.join(ROOT_DIR, "reports")

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# 1. Función de "Verdad Terrestre" (Ground Truth)
def define_tier(row):
    # Tier 1: Líderes (Fase avanzada o Química muy potente)
    if row['max_clinical_phase'] >= 3 or row['max_pchembl'] >= 7.0:
        return 1 
    # Tier 2: Validados (Fase temprana o Química media)
    elif row['max_clinical_phase'] >= 1 or row['max_pchembl'] >= 5.0:
        return 2 
    # Tier 3: Emergentes (Sin fármacos ni clínica)
    else:
        return 3 

def train_final_model():
    print("🚀 Entrenando Modelo Final (Predictivo/Honesto)...")
    
    # Carga
    df = pd.read_csv(INPUT_PATH)
    
    # Definir Target (Y)
    df['tier'] = df.apply(define_tier, axis=1)
    
    # Definir Features (X) - SOLO BIOLOGÍA Y SEGURIDAD
    # Excluimos explícitamente la química (max_pchembl, n_drugs) para que el modelo "piense"
    features_biologicas = [
        'score_genetic_association', 
        'score_literature', 
        'score_rna_expression', 
        'score_animal_model', 
        'score_affected_pathway', 
        'geneticConstraint'  # Variable crítica de gnomAD
    ]
    
    print(f"🧬 Variables utilizadas para predecir: {features_biologicas}")
    
    X = df[features_biologicas].fillna(0)
    y = df['tier']

    # Escalado
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Split
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42, stratify=y)

    # Entrenamiento (Random Forest con pesos balanceados para compensar clases)
    rf = RandomForestClassifier(
        n_estimators=300,        # Más árboles para estabilidad
        max_depth=10,            # Profundidad controlada para evitar overfitting
        class_weight='balanced', # Clave para que detecte bien los Tier 1 aunque sean pocos
        random_state=42
    )
    rf.fit(X_train, y_train)

    # Evaluación
    y_pred = rf.predict(X_test)
    
    print("\n📊 REPORTE DE RENDIMIENTO REAL (BIOLÓGICO):")
    # Imprimimos el reporte del 55-60% (que es el bueno científicamente)
    print(classification_report(y_test, y_pred, target_names=['Tier 1', 'Tier 2', 'Tier 3']))

    # Guardar Matriz de Confusión
    plt.figure(figsize=(8, 6))
    sns.heatmap(confusion_matrix(y_test, y_pred), annot=True, fmt='d', cmap='Greens')
    plt.title('Matriz de Confusión: Modelo Predictivo Biológico')
    plt.ylabel('Tier Real (Basado en Fármacos)')
    plt.xlabel('Predicción IA (Basada en Biología)')
    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_DIR, "matriz_confusion_final.png"))

    # Guardar Artefactos
    joblib.dump(rf, os.path.join(MODEL_DIR, "alzheimer_tier_predictor.pkl"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, "scaler_tier.pkl"))
    
    print("\n✅ ¡Modelo Final Guardado!")
    print(f"📂 Modelo: {os.path.join(MODEL_DIR, 'alzheimer_tier_predictor.pkl')}")
    print("👉 Este es el modelo que usará tu Dashboard para encontrar 'Joyas Ocultas'.")

if __name__ == "__main__":
    train_final_model()