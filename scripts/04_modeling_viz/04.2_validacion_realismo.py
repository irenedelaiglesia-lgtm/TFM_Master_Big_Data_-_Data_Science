import pandas as pd
import joblib
import os
import matplotlib.pyplot as plt
import seaborn as sns

# === CONFIGURACIÓN ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

INPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_enriquecido_v2.csv")
MODEL_PATH = os.path.join(ROOT_DIR, "models", "alzheimer_tier_predictor.pkl")
SCALER_PATH = os.path.join(ROOT_DIR, "models", "scaler_tier.pkl")
REPORTS_DIR = os.path.join(ROOT_DIR, "reports")

os.makedirs(REPORTS_DIR, exist_ok=True)

# Lógica Binaria (La misma que usaste en entrenamiento)
def define_class_binary(row):
    if (row['max_clinical_phase'] >= 1) or (row['max_pchembl'] >= 4.0) or (row['n_drugs'] > 0):
        return 1 # Potencial
    else:
        return 0 # Investigación (Sin nada)

def run_audit_inference():
    print("🕵️ INICIANDO MINERÍA DE 'JOYAS OCULTAS' (MODELO BINARIO)...")
    
    # 1. Cargar Datos y Artefactos
    if not os.path.exists(INPUT_PATH): return
    
    df = pd.read_csv(INPUT_PATH)
    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    
    # 2. Preparar Features (IGUAL QUE EN EL ENTRENAMIENTO)
    features_base = ['score_genetic_association', 'score_literature', 'score_rna_expression', 'geneticConstraint']
    df[features_base] = df[features_base].fillna(0)
    
    # IMPORTANTE: Recrear las interacciones
    df['interaction_gen_lit'] = df['score_genetic_association'] * df['score_literature']
    
    features_final = features_base + ['interaction_gen_lit', 'score_animal_model', 'score_affected_pathway']
    
    X = df[features_final]
    X_scaled = scaler.transform(X) # Usamos el scaler entrenado

    # 3. Generar Predicciones
    # Probabilidad de ser Clase 1 (Potencial)
    probs = model.predict_proba(X_scaled)[:, 1]
    
    df['prob_exito_IA'] = probs
    df['clase_real'] = df.apply(define_class_binary, axis=1)

    # 4. BUSCAR JOYAS OCULTAS
    # Condición: En la realidad es Clase 0 (Nadie la investiga clínicamente)
    # PERO la IA dice que tiene > 65% de probabilidad de ser Potencial
    
    umbral = 0.65
    joyas = df[
        (df['clase_real'] == 0) & 
        (df['prob_exito_IA'] > umbral)
    ].sort_values(by='prob_exito_IA', ascending=False)
    
    print(f"\n💎 RESULTADOS DE LA MINERÍA:")
    print(f"   - Total Dianas en 'Investigación' (Clase 0): {len(df[df['clase_real']==0])}")
    print(f"   - Joyas Identificadas (Prob > {umbral:.0%}): {len(joyas)}")

    # 5. Guardar Reporte CSV
    output_csv = os.path.join(REPORTS_DIR, "dianas_ocultas.csv")
    cols_reporte = ['gene_symbol', 'gene_name', 'prob_exito_IA', 'score_genetic_association', 'score_literature', 'interaction_gen_lit']
    
    joyas[cols_reporte].to_csv(output_csv, index=False)
    
    print(f"\n🏆 TOP 10 CANDIDATOS PROPUESTOS:")
    print(joyas[cols_reporte].head(10).to_string(index=False))
    
    # 6. Gráfico de "La Oportunidad"
    plt.figure(figsize=(10, 6))
    # Histograma de probabilidades SOLO para la clase 0
    sns.histplot(data=df[df['clase_real']==0], x='prob_exito_IA', bins=30, color='teal', kde=True)
    plt.axvline(umbral, color='red', linestyle='--', label=f'Umbral Corte ({umbral})')
    plt.title('Distribución de Potencial en Dianas No Exploradas (Clase 0)')
    plt.xlabel('Probabilidad de Éxito asignada por la IA')
    plt.ylabel('Número de Dianas')
    plt.legend()
    plt.savefig(os.path.join(REPORTS_DIR, "distribucion_joyas_ocultas.png"))
    print("\n📸 Gráfico guardado: distribucion_joyas_ocultas.png")

if __name__ == "__main__":
    run_audit_inference()