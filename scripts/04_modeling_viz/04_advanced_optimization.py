import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE  # <--- Técnica avanzada 1
import joblib
import os

# === CONFIGURACIÓN ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
INPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_final.csv")
MODEL_DIR = os.path.join(ROOT_DIR, "models")

def run_advanced_optimization():
    print("🧪 Iniciando Fase de Optimización Avanzada...")
    df = pd.read_csv(INPUT_PATH)
    
    features = ['score_genetic_association', 'score_literature', 'score_rna_expression', 
                'score_animal_model', 'score_affected_pathway', 'geneticConstraint']
    X = df[features]
    y = df['target_success']

    # 1. Escalado
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 2. Manejo de Desbalanceo con SMOTE (Generación de datos sintéticos)
    print("⚖️ Aplicando SMOTE para equilibrar las clases...")
    smote = SMOTE(random_state=42)
    X_res, y_res = smote.fit_resample(X_scaled, y)

    X_train, X_test, y_train, y_test = train_test_split(X_res, y_res, test_size=0.2, random_state=42)

    # 3. Búsqueda de Hiperparámetros (Tuning)
    print("🔍 Buscando la mejor configuración de Random Forest (Tuning)...")
    param_dist = {
        'n_estimators': [100, 200, 300],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5, 10],
        'bootstrap': [True, False]
    }

    rf = RandomForestClassifier(random_state=42)
    # Probamos 10 combinaciones aleatorias con validación cruzada de 3 carpetas
    random_search = RandomizedSearchCV(rf, param_distributions=param_dist, n_iter=10, cv=3, scoring='roc_auc', n_jobs=-1)
    random_search.fit(X_train, y_train)

    best_rf = random_search.best_estimator_
    
    # 4. Validación Cruzada Final (Estabilidad)
    print("🛡️ Validando estabilidad con Cross-Validation (5-folds)...")
    cv_scores = cross_val_score(best_rf, X_res, y_res, cv=5, scoring='roc_auc')

    # 5. Evaluación final en el set de test
    y_prob = best_rf.predict_proba(X_test)[:, 1]
    final_auc = roc_auc_score(y_test, y_prob)

    print("\n✅ RESULTADOS DE LA OPTIMIZACIÓN AVANZADA:")
    print(f"Mejor configuración encontrada: {random_search.best_params_}")
    print(f"AUC medio en Validación Cruzada: {np.mean(cv_scores):.4f} (+/- {np.std(cv_scores):.4f})")
    print(f"AUC final en set de Test: {final_auc:.4f}")

    # Guardar el "Súper Modelo"
    joblib.dump(best_rf, os.path.join(MODEL_DIR, "alzheimer_drug_predictor_optimized.pkl"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, "scaler_optimized.pkl"))
    print("\n📦 Modelo optimizado guardado.")

if __name__ == "__main__":
    run_advanced_optimization()
    