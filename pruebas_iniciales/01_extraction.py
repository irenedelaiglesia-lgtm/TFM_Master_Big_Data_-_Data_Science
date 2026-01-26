import pandas as pd
from chembl_webresource_client.new_client import new_client
import os

# --- CONFIGURACIÓN DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(BASE_DIR, '..', 'data', 'raw')
os.makedirs(RAW_DATA_PATH, exist_ok=True)

# --- LISTA DE DIANAS (ALZHEIMER) ---
# Diccionario con Nombre: ID de ChEMBL
DIANAS_OBJETIVO = {
    "ACHE": "CHEMBL220",
    "BACE1": "CHEMBL4822",
    "MAPT": "CHEMBL2363065",
    "GSK3B": "CHEMBL262"
}

def extraer_datos_diana(nombre_comun, target_id):
    print(f"\n>>> Iniciando descarga para {nombre_comun} ({target_id})...")
    
    # 1. Conexión con la API
    activities = new_client.activity
    
    # 2. Filtros de búsqueda 
    res = activities.filter(target_chembl_id=target_id) \
                    .filter(standard_type="IC50") \
                    .filter(standard_units="nM")
    
    # 3. Convertir a DataFrame
    df = pd.DataFrame.from_dict(res)
    
    if not df.empty:
        filename = f"raw_{nombre_comun.lower()}_alzheimer.csv"
        full_path = os.path.join(RAW_DATA_PATH, filename)
        df.to_csv(full_path, index=False)
        print(f"✅ Éxito: Guardadas {len(df)} filas en {filename}")
    else:
        print(f"⚠️ No se encontraron datos para {nombre_comun}")

# --- EJECUCIÓN ---
if __name__ == "__main__":
    for nombre, chembl_id in DIANAS_OBJETIVO.items():
        extraer_datos_diana(nombre, chembl_id)
    print("\n[Día 1] Proceso de extracción masiva completado.")