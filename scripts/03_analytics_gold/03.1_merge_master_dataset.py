import pandas as pd
import sqlite3
import os

# === CONFIGURACIÓN DE RUTAS ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

# Bases de datos y archivos
DB_PATH = os.path.join(ROOT_DIR, "tablas", "alzheimer_targets.db")
ADDITIONAL_INFO_CSV = os.path.join(ROOT_DIR, "data", "processed", "alzheimer_additional_info.csv")
OUTPUT_CSV = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_final.csv")

def merge_datasets():
    print("🔗 Iniciando integración de fuentes (API + TSV)...")
    
    # 1. Cargar targets limpios de la API (Desde SQLite o CSV clean)
    conn = sqlite3.connect(DB_PATH)
    # Cargamos la tabla
    df_api = pd.read_sql("SELECT * FROM alzheimer_targets_clean", conn)
    conn.close()
    
    # 2. Cargar información adicional del TSV
    if not os.path.exists(ADDITIONAL_INFO_CSV):
        print(f"❌ Error: No se encuentra {ADDITIONAL_INFO_CSV}. Ejecuta el script 01 de info adicional.")
        return
    
    df_tsv = pd.read_csv(ADDITIONAL_INFO_CSV)

    print(f"📊 Targets de API: {len(df_api)}")
    print(f"📊 Info adicional TSV: {len(df_tsv)}")

    # 3. MERGE (Unión): Cruzamos por 'gene_symbol' e 'symbol'
    # Usamos 'left' para mantener todos los targets que extrajimos de la API
    df_final = pd.merge(
        df_api, 
        df_tsv, 
        left_on='gene_symbol', 
        right_on='symbol', 
        how='left'
    )

    # Limpieza post-merge
    # Eliminamos 'symbol' porque ya tenemos 'gene_symbol'
    if 'symbol' in df_final.columns:
        df_final = df_final.drop(columns=['symbol'])
    
    # Rellenamos nulos que puedan haber surgido del cruce (dianas en API que no están en TSV)
    df_final = df_final.fillna(0)

    # 4. VARIABLE OBJETIVO (Target para el modelo de ML)
    # Definimos éxito como haber llegado al menos a Fase 1
    df_final['target_success'] = (df_final['max_clinical_phase'] > 0).astype(int)

    # 5. Guardar Dataset Maestro (CAPA GOLD)
    df_final.to_csv(OUTPUT_CSV, index=False)
    
    conn_gold = sqlite3.connect(DB_PATH)
    df_final.to_sql("dataset_maestro_final", conn_gold, if_exists="replace", index=False)
    conn_gold.close()

    print(f"✅ Dataset Maestro (GOLD) creado con {len(df_final)} filas.")
    print(f"🎯 Dianas con éxito clínico identificado: {df_final['target_success'].sum()}")
    print(f"📁 Guardado en: {OUTPUT_CSV}")

if __name__ == "__main__":
    merge_datasets()