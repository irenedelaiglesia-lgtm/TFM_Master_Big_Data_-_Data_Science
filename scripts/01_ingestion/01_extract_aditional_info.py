import pandas as pd
import sqlite3
import os

# =========================
# CONFIGURACIÓN
# =========================

TSV_FILE_NAME = "OT-MONDO_0004975-associated-targets-5_2_2026-v25_12.tsv"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

INPUT_PATH = os.path.join(ROOT_DIR, "data", "raw", TSV_FILE_NAME)
OUTPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "alzheimer_additional_info.csv")
DB_PATH = os.path.join(ROOT_DIR, "tablas", "alzheimer_targets.db")

def process_tsv():
    print(f"📂 Leyendo archivo TSV: {TSV_FILE_NAME}...")
    
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Error: No se encuentra el archivo en {INPUT_PATH}")
        print("Asegúrate de haber movido el TSV a la carpeta 'data/raw'.")
        return

    # 1. Cargar TSV
    df = pd.read_csv(INPUT_PATH, sep='\t')

    # 2. Limpieza de "No data"
    # Reemplazamos el texto "No data" por NaN real de pandas para poder operar
    df = df.replace("No data", pd.NA)

    # 3. Selección de columnas estratégicas para el modelo de ML
    # Estas columnas suelen ser complementarias a las de la API
    cols_interes = [
        'symbol', 
        'geneticConstraint', 
        'hasHighQualityChemicalProbes', 
        'target_id' if 'target_id' in df.columns else 'symbol' # Ajuste según columnas del TSV
    ]
    
    # Nota: El TSV de exportación a veces usa 'symbol' como ID principal. 
    # Vamos a quedarnos con las columnas de scores adicionales:
    cols_scores = [c for c in df.columns if 'Score' in c or 'genetic' in c.lower()]
    
    # Para tu TFM, nos interesan especialmente estas:
    df_subset = df[['symbol', 'geneticConstraint', 'hasHighQualityChemicalProbes', 'expressionAtlas']].copy()

    # 4. Convertir a numérico lo que sea necesario
    df_subset['geneticConstraint'] = pd.to_numeric(df_subset['geneticConstraint'], errors='coerce').fillna(0)
    df_subset['expressionAtlas'] = pd.to_numeric(df_subset['expressionAtlas'], errors='coerce').fillna(0)

    # 5. Guardar en SQLite (Capa Silver)
    conn = sqlite3.connect(DB_PATH)
    df_subset.to_sql("additional_target_info", conn, if_exists="replace", index=False)
    conn.close()

    # 6. Guardar en CSV
    df_subset.to_csv(OUTPUT_PATH, index=False)
    
    print(f"✅ Información adicional procesada: {len(df_subset)} filas.")
    print(f"📁 Guardado en: {OUTPUT_PATH}")

if __name__ == "__main__":
    process_tsv()