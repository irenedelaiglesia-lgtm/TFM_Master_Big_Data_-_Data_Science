import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import sys

# === CONFIGURACIÓN ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
INPUT_FILE = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_enriquecido_v2.csv")
REPORT_DIR = os.path.join(ROOT_DIR, "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

# Archivo de reporte de texto
REPORT_FILE = os.path.join(REPORT_DIR, "reporte_calidad_datos.txt")

def log_report(text, file_obj):
    """Escribe tanto en consola como en el archivo de reporte"""
    print(text)
    file_obj.write(text + "\n")

def run_audit():
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        log_report("="*60, f)
        log_report("📊 REPORTE DE CALIDAD Y ANÁLISIS DEL DATASET MAESTRO", f)
        log_report("="*60, f)

        # 1. CARGA DE DATOS
        if not os.path.exists(INPUT_FILE):
            log_report(f"❌ ERROR CRÍTICO: No se encuentra el archivo {INPUT_FILE}", f)
            return
        
        df = pd.read_csv(INPUT_FILE)
        log_report(f"✅ Dataset cargado correctamente.", f)
        log_report(f"📉 Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas", f)
        
        # 2. INTEGRIDAD DE VARIABLES (Nulos y Ceros)
        log_report("\n" + "-"*40, f)
        log_report("1. ANÁLISIS DE INTEGRIDAD (Nulos y Dispersión)", f)
        log_report("-"*40, f)
        
        audit_df = pd.DataFrame({
            'Tipo Dato': df.dtypes,
            'Nulos (NaN)': df.isnull().sum(),
            '% Nulos': (df.isnull().sum() / len(df) * 100).round(2),
            'Ceros (0)': (df == 0).sum(),
            '% Ceros': ((df == 0).sum() / len(df) * 100).round(2)
        })
        
        log_report(audit_df.to_string(), f)
        
        # Alerta si faltan variables críticas
        required_cols = ['max_pchembl', 'drug_count', 'geneticConstraint', 'target_success']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            log_report(f"\n⚠️ ALERTA: Faltan columnas críticas: {missing}", f)
        else:
            log_report(f"\n✅ Todas las variables críticas están presentes.", f)

        # 3. VERIFICACIÓN DE LÓGICA DE NEGOCIO (Coherencia)
        log_report("\n" + "-"*40, f)
        log_report("2. VERIFICACIÓN DE LÓGICA BIOLÓGICA", f)
        log_report("-"*40, f)
        
        # Check A: Si Fase > 0, Success debe ser 1
        errors_a = df[(df['max_clinical_phase'] > 0) & (df['target_success'] == 0)]
        if len(errors_a) > 0:
            log_report(f"❌ ERROR LÓGICO A: {len(errors_a)} genes tienen Fase Clínica > 0 pero Success = 0.", f)
        else:
            log_report("✅ TEST A PASADO: Coherencia Fase Clínica vs Target Success correcta.", f)
            
        # Check B: Si pChEMBL > 0, drug_count debería ser > 0 (normalmente)
        errors_b = df[(df['max_pchembl'] > 0) & (df['drug_count'] == 0)]
        if len(errors_b) > 0:
            log_report(f"⚠️ ADVERTENCIA B: {len(errors_b)} genes tienen potencia química pero 0 fármacos (Revisar ChEMBL).", f)
        else:
            log_report("✅ TEST B PASADO: Coherencia Química correcta.", f)

        # 4. PESO DE LAS VARIABLES (Correlación)
        log_report("\n" + "-"*40, f)
        log_report("3. ANÁLISIS DE PODER PREDICTIVO (Correlación)", f)
        log_report("-"*40, f)
        
        # Seleccionamos solo numéricas
        numeric_df = df.select_dtypes(include=[np.number])
        corr_matrix = numeric_df.corr()
        
        # Ranking de correlación con el éxito
        target_corr = corr_matrix['target_success'].drop('target_success').sort_values(ascending=False)
        log_report(target_corr.to_string(), f)
        
        # Generar Gráfico de Calor (Heatmap)
        plt.figure(figsize=(12, 10))
        sns.heatmap(corr_matrix, annot=False, cmap='coolwarm', linewidths=0.5)
        plt.title('Matriz de Correlación de Variables del Dataset Maestro')
        plt.tight_layout()
        heatmap_path = os.path.join(REPORT_DIR, "matriz_correlacion.png")
        plt.savefig(heatmap_path)
        plt.close()
        log_report(f"\n📸 Gráfico de correlación guardado en: {heatmap_path}", f)

        # 5. DISTRIBUCIÓN DE TIERS (Simulación preliminar)
        log_report("\n" + "-"*40, f)
        log_report("4. DISTRIBUCIÓN PRELIMINAR DE MADUREZ", f)
        log_report("-"*40, f)
        
        # Definimos Tiers simples para ver cómo se distribuyen
        tier1 = df[(df['max_clinical_phase'] >= 3) | (df['max_pchembl'] >= 7)]
        tier2 = df[(df['max_clinical_phase'].between(1, 2)) | ((df['max_pchembl'] > 5) & (df['max_pchembl'] < 7))]
        tier3 = df[(df['max_clinical_phase'] == 0) & (df['max_pchembl'] <= 5)]
        
        log_report(f"🧬 TIER 1 (Líderes / Alta Potencia): {len(tier1)} genes", f)
        log_report(f"🧪 TIER 2 (En Validación / Potencia Media): {len(tier2)} genes", f)
        log_report(f"🌑 TIER 3 (Emergentes / Sin Química): {len(tier3)} genes", f)
        
        log_report("\n" + "="*60, f)
        log_report("✅ REPORTE GENERADO CON ÉXITO", f)
        log_report("="*60, f)

if __name__ == "__main__":
    run_audit()