import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler
import os

# === CONFIGURACIÓN ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
INPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_enriquecido_v2.csv")
REPORTS_DIR = os.path.join(ROOT_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

# 1. DEFINICIÓN DE TIERS (Balanceada)
def define_tier(row):
    # Tier 1: Líderes
    if row['max_clinical_phase'] >= 3 or row['max_pchembl'] >= 7.0:
        return 1 
    # Tier 2: Prometedores (Más inclusivo)
    elif (row['max_clinical_phase'] >= 1) or (row['max_pchembl'] >= 4.5) or (row['n_drugs'] > 0):
        return 2 
    # Tier 3: Emergentes
    else:
        return 3 

def run_validation_complete():
    print("🕵️ INICIANDO AUDITORÍA Y BÚSQUEDA DE JOYAS (Ajustado)...")
    
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Error: No se encuentra {INPUT_PATH}")
        return

    df = pd.read_csv(INPUT_PATH)
    df['tier_real'] = df.apply(define_tier, axis=1)
    
    print("\n📊 Distribución de Clases:")
    print(df['tier_real'].value_counts().sort_index())

    # --- MODELO HONESTO ---
    features_honest = [
        'score_genetic_association', 'score_literature', 'score_rna_expression', 
        'score_animal_model', 'score_affected_pathway', 'geneticConstraint'
    ]
    
    X = df[features_honest].fillna(0)
    y = df['tier_real']
    
    # Split & Scale
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Entrenar
    rf = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42, class_weight='balanced')
    rf.fit(X_train_scaled, y_train)
    
    # Reporte rápido
    print("\n📊 Precisión Global (Accuracy): {:.2%}".format(rf.score(X_test_scaled, y_test)))

    # --- GENERACIÓN DE "JOYAS OCULTAS" ---
    print("\n💎 BUSCANDO DISCREPANCIAS (Tier 3 -> Predicción Tier 1/2)...")
    
    # Predecimos sobre TODO
    X_full = scaler.transform(X)
    probs = rf.predict_proba(X_full)
    
    # Identificar índices de las clases
    idx_t1 = np.where(rf.classes_ == 1)[0][0] # Índice Tier 1
    idx_t2 = np.where(rf.classes_ == 2)[0][0] # Índice Tier 2
    
    # Guardamos probabilidades
    df['prob_Tier1'] = probs[:, idx_t1]
    df['prob_Tier2'] = probs[:, idx_t2]
    df['prob_Potencial'] = df['prob_Tier1'] + df['prob_Tier2'] # Suma de T1+T2
    
    # FILTRO:
    # 1. Realidad: Es Tier 3 (Sin nada)
    # 2. Predicción: Alta probabilidad de ser Tier 1 (> 40%) O Alto Potencial General (> 55%)
    # Usamos un umbral más realista para 3 clases (40% ya es ganar al azar)
    
    joyas = df[
        (df['tier_real'] == 3) & 
        (df['prob_Tier1'] > 0.40) 
    ].sort_values(by='prob_Tier1', ascending=False)
    
    print(f"✨ Candidatos encontrados (Prob Tier 1 > 40%): {len(joyas)}")
    
    if len(joyas) == 0:
        print("⚠️ Aún estricto. Probando por 'Potencial Combinado' (T1+T2 > 50%)...")
        joyas = df[
            (df['tier_real'] == 3) & 
            (df['prob_Potencial'] > 0.50)
        ].sort_values(by='prob_Potencial', ascending=False)
        print(f"✨ Candidatos por Potencial Combinado: {len(joyas)}")

    # Mostrar y Guardar Top 10
    if len(joyas) > 0:
        top_joyas = joyas.head(10)
        print("\n🏆 TOP 5 DIANAS PROPUESTAS:")
        print(top_joyas[['gene_symbol', 'prob_Tier1', 'prob_Potencial', 'score_genetic_association']].to_string(index=False))
        
        output_csv = os.path.join(REPORTS_DIR, "dianas_ocultas.csv")
        joyas.to_csv(output_csv, index=False)
        print(f"\n📄 Reporte guardado: {output_csv}")
    else:
        print("❌ No se encontraron candidatos. Revisa si las features tienen valores distintos de 0.")

if __name__ == "__main__":
    run_validation_complete()