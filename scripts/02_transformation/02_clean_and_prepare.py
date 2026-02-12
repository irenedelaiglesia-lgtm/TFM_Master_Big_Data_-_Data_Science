import pandas as pd
import sqlite3
import os

# ---- RUTAS RELATIVAS (Portabilidad) ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

INPUT_CSV = os.path.join(ROOT_DIR, "data", "processed", "alzheimer_top_targets.csv")
OUTPUT_CSV = os.path.join(ROOT_DIR, "data", "processed", "alzheimer_top_targets_clean.csv")
DB_PATH = os.path.join(ROOT_DIR, "tablas", "alzheimer_targets.db")

print("🧹 Iniciando limpieza del Dataset de la API...")

if not os.path.exists(INPUT_CSV):
    print(f"❌ Error: No se encuentra {INPUT_CSV}")
else:
    df = pd.read_csv(INPUT_CSV)

    # ---- 1️⃣ Normalizar Identificadores ----
    # Muy importante para el Merge de mañana
    df["gene_symbol"] = df["gene_symbol"].str.upper().str.strip()
    df["target_id"] = df["target_id"].str.strip()

    # ---- 2️⃣ Asegurar que los scores son numéricos (Casting) ----
    # Buscamos todas las columnas que empiecen por 'score_' o sean de fase/fármacos
    cols_to_fix = [c for c in df.columns if 'score_' in c] + ['max_clinical_phase', 'n_drugs']
    
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ---- 3️⃣ Feature Engineering Inicial (Flags) ----
    # Esto aporta valor analítico al TFM
    df["has_clinical_trials"] = df["max_clinical_phase"] > 0
    df["is_high_confidence"] = df["score_overall"] > 0.6

    # ---- 4️⃣ Guardar CSV limpio (Capa Silver) ----
    df.to_csv(OUTPUT_CSV, index=False)

    # ---- 5️⃣ Guardar en SQLite ----
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("alzheimer_targets_clean", conn, if_exists="replace", index=False)
    conn.close()

    print(f"✅ Limpieza completada. {len(df)} filas procesadas.")
    print(f"📂 Archivo limpio guardado en: {OUTPUT_CSV}")