import pandas as pd
import sqlite3
import os

# === CONFIGURACIÓN DE RUTAS ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

CSV_PATH = os.path.join(ROOT_DIR, "data", "processed", "alzheimer_top_targets_clean.csv")
REPORT_PATH = os.path.join(ROOT_DIR, "reports", "data_quality_report.txt")
SQLITE_PATH = os.path.join(ROOT_DIR, "tablas", "alzheimer_targets.db")

# === CARGA DATOS ===
if not os.path.exists(CSV_PATH):
    print("❌ Error: Ejecuta primero 02_clean_and_prepare.py")
    exit()

df = pd.read_csv(CSV_PATH)

# === ANÁLISIS DE SCORES ===
score_cols = [c for c in df.columns if 'score_' in c]
no_genetic = df[df['score_genetic_association'] == 0]
zero_scores = df[df[score_cols].sum(axis=1) == 0]

# === GENERACIÓN DEL REPORTE ===
output_text = []
output_text.append("=== INFORME DE CALIDAD DE DATOS (CAPA SILVER) ===\n")
output_text.append(f"Total filas procesadas: {len(df)}")
output_text.append(f"Total columnas: {len(df.columns)}\n")

output_text.append("--- INTEGRIDAD ---")
output_text.append(f"Nulos por columna:\n{df.isnull().sum().to_string()}\n")

output_text.append("--- ESTADÍSTICAS DE EVIDENCIA ---")
output_text.append(f"Estadísticas de Scores:\n{df[score_cols].describe().to_string()}\n")

output_text.append("--- HALLAZGOS CRÍTICOS ---")
output_text.append(f"1. Targets sin evidencia genética (score_genetic = 0): {len(no_genetic)} ({len(no_genetic)/len(df):.1%})")
output_text.append(f"2. Targets sin ninguna evidencia (Todos scores 0): {len(zero_scores)}")
output_text.append(f"3. Targets con fármacos conocidos (score_known_drug > 0): {len(df[df['score_known_drug'] > 0])}")

# Guardar en archivo
with open(REPORT_PATH, "w", encoding="utf-8") as f:
    f.write("\n".join(output_text))

print(f"✅ Informe detallado guardado en: {REPORT_PATH}")
# Imprimimos también en consola un resumen
print("\n".join(output_text[:10])) 
print("...")