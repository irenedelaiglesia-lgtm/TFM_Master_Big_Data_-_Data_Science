import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from imblearn.over_sampling import SMOTE
import joblib
import os

# === CONFIGURACIÓN ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
INPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_enriquecido_v2.csv") # Usamos el v2 enriquecido
MODEL_DIR = os.path.join(ROOT_DIR, "models")
REPORTS_DIR = os.path.join(ROOT_DIR, "reports")
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

def define_tier(row):
    """
    Reglas de Negocio para la Clasificación de Tiers (Madurez):
    - Tier 1 (Líder): Fase Clínica 3/4 O Potencia Química muy alta (>7 pChEMBL).
    - Tier 2 (Validado): Fase Clínica 1/2 O Potencia Química media (5-7 pChEMBL).
    - Tier 3 (Emergente): Fase 0 y sin química potente (Investigación básica).
    """
    if row['max_clinical_phase'] >= 3 or row['max_pchembl'] >= 7.0:
        return 1 # Tier 1: Líder
    elif row['max_clinical_phase'] >= 1 or row['max_pchembl'] >= 5.0:
        return 2 # Tier 2: Validado
    else:
        return 3 # Tier 3: Emergente

def train_tier_model():
    print("🚀 Iniciando Entrenamiento del Modelo de Madurez (Tiers)...")
    
    # 1. Carga de Datos
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Error: No se encuentra {INPUT_PATH}")
        return
    
    df = pd.read_csv(INPUT_PATH)
    
    # 2. Ingeniería de la Variable Objetivo (Tier)
    print("🏷️ Generando etiquetas de Tier (1, 2, 3)...")
    df['tier'] = df.apply(define_tier, axis=1)
    
    print("Distribución de Clases (Tiers):")
    print(df['tier'].value_counts().sort_index())

    # 3. Selección de Variables (Features)
    # Incluimos Biología, Genética, Seguridad (gnomAD) y Química (ChEMBL)
    features = [
        'score_genetic_association', 
        'score_literature', 
        'score_rna_expression', 
        'score_animal_model', 
        'score_affected_pathway', 
        'geneticConstraint',      # Seguridad
        'max_pchembl',            # Química (Potencia)
        'drug_count',             # Química (Madurez)
        'n_drugs'                 # Clínica
    ]
    
    X = df[features]
    y = df['tier']

    # Rellenar nulos por seguridad (aunque el reporte dijo que había 0)
    X = X.fillna(0)

    # 4. Escalado de Datos
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 5. Split Train/Test
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42, stratify=y)

    # 6. Balanceo con SMOTE (Crucial porque habrá muchos Tier 3 y pocos Tier 1)
    print("⚖️ Aplicando SMOTE para balancear las clases...")
    smote = SMOTE(random_state=42, k_neighbors=3) # k_neighbors bajo por si hay pocas muestras en Tier 1
    X_train_bal, y_train_bal = smote.fit_resample(X_train, y_train)

    # 7. Entrenamiento (Random Forest)
    print("🧠 Entrenando Random Forest Multiclase...")
    rf = RandomForestClassifier(
        n_estimators=200,
        max_depth=12,
        random_state=42,
        class_weight='balanced'
    )
    rf.fit(X_train_bal, y_train_bal)

    # 8. Evaluación
    y_pred = rf.predict(X_test)
    
    print("\n📊 REPORTE DE CLASIFICACIÓN:")
    print(classification_report(y_test, y_pred, target_names=['Tier 1', 'Tier 2', 'Tier 3']))
    
    # Matriz de Confusión Visual
    plt.figure(figsize=(8, 6))
    sns.heatmap(confusion_matrix(y_test, y_pred), annot=True, fmt='d', cmap='Blues',
                xticklabels=['Tier 1', 'Tier 2', 'Tier 3'],
                yticklabels=['Tier 1', 'Tier 2', 'Tier 3'])
    plt.title('Matriz de Confusión: Predicción de Tiers')
    plt.ylabel('Realidad')
    plt.xlabel('Predicción del Modelo')
    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_DIR, "confusion_matrix_tiers.png"))
    print("📸 Matriz de confusión guardada en reports/")

    # 9. Importancia de Variables
    importances = pd.Series(rf.feature_importances_, index=features).sort_values(ascending=False)
    print("\n💎 IMPORTANCIA DE VARIABLES:")
    print(importances)
    
    # 10. Guardar Modelos
    joblib.dump(rf, os.path.join(MODEL_DIR, "tier_predictor_model.pkl"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, "tier_scaler.pkl"))
    print("\n✅ ¡Modelo de Tiers entrenado y guardado con éxito!")

if __name__ == "__main__":
    train_tier_model()