import pandas as pd
import sqlite3
import os

# === CONFIG ===
CSV_PATH = "data/processed/alzheimer_top_targets_clean.csv"
SQLITE_PATH = "tablas/alzheimer_targets.db"
TABLE_NAME = "alzheimer_top_targets"

# === CARGA DATOS ===
df = pd.read_csv(CSV_PATH)

print("\n📊 INFO GENERAL")
print(df.info())

print("\n🧼 VALORES NULOS POR COLUMNA")
print(df.isnull().sum())

print("\n🔢 VALORES ÚNICOS POR COLUMNA")
print(df.nunique())

# === ANÁLISIS DE SCORES ===
score_cols = [
    "score_overall",
    "score_genetic_association",
    "score_known_drug",
    "score_literature",
    "score_rna_expression",
    "score_animal_model",
    "score_affected_pathway"
]

print("\n📈 ESTADÍSTICAS DE SCORES")
print(df[score_cols].describe())

# === TARGETS CON TODOS LOS SCORES A 0 ===
print("\n⚠️ TARGETS CON TODOS LOS SCORES A 0")

zero_score_targets = df[
    (df[score_cols].fillna(0) == 0).all(axis=1)
][["target_id", "gene_symbol", "gene_name"]]

print(zero_score_targets.head(10))
print(f"Total: {len(zero_score_targets)}")

# === TARGETS SIN EVIDENCIA GENÉTICA ===
print("\n🔍 TARGETS SIN EVIDENCIA GENÉTICA (score_genetic_association = 0 o NaN)")

no_genetic_targets = df[
    df["score_genetic_association"].fillna(0) == 0
][["gene_symbol", "score_overall", "score_literature"]]

print(no_genetic_targets.head(10))
print(f"Total: {len(no_genetic_targets)}")

# === COMPROBACIÓN SQLITE ===
if os.path.exists(SQLITE_PATH):
    conn = sqlite3.connect(SQLITE_PATH)
    sql_df = pd.read_sql(
        f"SELECT COUNT(*) as n FROM {TABLE_NAME}",
        conn
    )

    print("\n🗄️ FILAS EN SQLITE")
    print(sql_df)

    conn.close()
else:
    print("\n⚠️ No existe la base de datos SQLite")

print("\n✅ Análisis de calidad completado")

# === EXPORTAR INFORME DE CALIDAD ===
quality_report_path = "data/processed/data_quality_report.txt"
with open(quality_report_path, "w", encoding="utf-8") as f:
    f.write("=== INFORME DE CALIDAD DE DATOS ===\n\n")
    f.write(f"Nulos por columna:\n{df.isnull().sum().to_string()}\n\n")
    f.write(f"Estadísticas de scores:\n{df[score_cols].describe().to_string()}\n\n")
    f.write(f"Targets sin evidencia genética: {len(no_genetic_targets)}\n")

print(f"✅ Informe guardado en: {quality_report_path}")

