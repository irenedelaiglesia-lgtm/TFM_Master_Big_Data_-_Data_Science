import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
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

# === ESTRATEGIA BINARIA ===
# 0: INVESTIGACIÓN (Emergente) - Sin evidencia clínica ni química fuerte.
# 1: POTENCIAL (Prometedora + Consolidada) - Tiene ALGO (Fármacos, Fase clínica o Química).
NOMBRES_CLASES = ['Investigación (Clase 0)', 'Alto Potencial (Clase 1)']

def define_class_binary(row):
    # Si tiene fase clínica, O tiene fármacos, O tiene buena química... ES CLASE 1
    if (row['max_clinical_phase'] >= 1) or (row['max_pchembl'] >= 4.0) or (row['n_drugs'] > 0):
        return 1 
    # Si no tiene nada de lo anterior... ES CLASE 0
    else:
        return 0

def train_binary_model():
    print("🚀 Iniciando Entrenamiento BINARIO (Detector de Potencial)...")
    
    if not os.path.exists(INPUT_PATH):
        print("❌ Error: Falta el dataset.")
        return
    
    df = pd.read_csv(INPUT_PATH)
    
    # 1. TARGET BINARIO
    df['target_class'] = df.apply(define_class_binary, axis=1)
    
    counts = df['target_class'].value_counts().sort_index()
    print("\n📊 Distribución Perfecta:")
    print(f"   - Clase 0 (Investigación): {counts.get(0, 0)}")
    print(f"   - Clase 1 (Potencial):     {counts.get(1, 0)}")
    
    # 2. FEATURES (Añadimos interacciones que funcionaron bien)
    features_base = ['score_genetic_association', 'score_literature', 'score_rna_expression', 'geneticConstraint']
    df[features_base] = df[features_base].fillna(0)

    # Interacciones clave
    df['interaction_gen_lit'] = df['score_genetic_association'] * df['score_literature']
    
    features_final = features_base + ['interaction_gen_lit', 'score_animal_model', 'score_affected_pathway']
    
    X = df[features_final]
    y = df['target_class']

    # 3. SPLIT (No hace falta SMOTE porque ya está balanceado 50/50!!)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # 4. ENTRENAMIENTO (Gradient Boosting)
    print("\n🧠 Entrenando Gradient Boosting Binario...")
    
    param_grid = {
        'n_estimators': [100, 200],
        'learning_rate': [0.05, 0.1],
        'max_depth': [3, 4, 5],
        'subsample': [0.8, 1.0]
    }
    
    gb = GradientBoostingClassifier(random_state=42)
    grid = GridSearchCV(gb, param_grid, cv=5, scoring='f1', n_jobs=-1) # Optimizamos F1 para clase 1
    grid.fit(X_train_scaled, y_train)
    
    best_model = grid.best_estimator_
    print(f"✅ Mejor configuración: {grid.best_params_}")

    # 5. EVALUACIÓN DE ALTO NIVEL
    y_pred = best_model.predict(X_test_scaled)
    y_prob = best_model.predict_proba(X_test_scaled)[:, 1]
    
    print("\n🏆 REPORTE FINAL (BINARIO):")
    print(classification_report(y_test, y_pred, target_names=NOMBRES_CLASES))

    # Matriz de Confusión
    plt.figure(figsize=(6, 5))
    sns.heatmap(confusion_matrix(y_test, y_pred), annot=True, fmt='d', cmap='Blues', 
                xticklabels=['Inv.', 'Potencial'], yticklabels=['Inv.', 'Potencial'])
    plt.title('Matriz de Confusión Binaria')
    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_DIR, "matriz_confusion_binaria.png"))
    
    # Curva ROC (Importante para modelos binarios)
    auc = roc_auc_score(y_test, y_prob)
    fpr, tpr, _ = roc_curve(y_test, y_prob)
    
    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, label=f"AUC = {auc:.2f}", color='darkorange', lw=2)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.xlabel('Falsos Positivos')
    plt.ylabel('Verdaderos Positivos')
    plt.title('Curva ROC - Capacidad de Detección')
    plt.legend()
    plt.savefig(os.path.join(REPORTS_DIR, "curva_roc.png"))
    print(f"🌟 AUC-ROC Score: {auc:.2f} (Excelente si > 0.75)")

    # Guardar
    joblib.dump(best_model, os.path.join(MODEL_DIR, "alzheimer_tier_predictor.pkl"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, "scaler_tier.pkl"))
    print("\n💾 Modelo Binario Guardado.")

if __name__ == "__main__":
    train_binary_model()