import pandas as pd
from chembl_webresource_client.new_client import new_client
import os

# --- CONFIGURACIÓN DE RUTAS AUTOMÁTICAS ---
# Detecta la carpeta donde está este script (scripts/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define la ruta de destino subiendo un nivel y entrando en data/raw
raw_data_path = os.path.join(BASE_DIR, '..', 'data', 'raw')
file_name = 'raw_ache_alzheimer.csv'
full_path = os.path.join(raw_data_path, file_name)

print("--- INICIANDO EXTRACCIÓN: DIANA NEURODEGENERATIVA (ACHE) ---")

# Crear la carpeta de destino si no existe
if not os.path.exists(raw_data_path):
    os.makedirs(raw_data_path, exist_ok=True)
    print(f"Carpeta creada en: {raw_data_path}")

# --- CONEXIÓN A CHEMBL ---
target = new_client.target
activity = new_client.activity

try:
    print("Buscando diana: Acetylcholinesterase (ACHE) en humanos...")
    # Buscamos la proteína específica
    res = target.filter(target_gene_name__iexact='ACHE').filter(organism='Homo sapiens')
    
    if not res:
        print("Error: No se encontró la diana en ChEMBL.")
    else:
        target_id = res[0]['target_chembl_id']
        print(f"Diana confirmada: {target_id}")

        # --- DESCARGA DE DATOS ---
        print("Descargando actividades IC50... (Esto puede tardar un poco)")
        activities = activity.filter(target_chembl_id=target_id).filter(standard_type="IC50")
        
        # Convertimos los resultados a un DataFrame de Pandas
        df = pd.DataFrame.from_records(activities)
        
        # --- GUARDADO ---
        if not df.empty:
            df.to_csv(full_path, index=False)
            print("\n" + "="*40)
            print(f"¡ÉXITO! Se han descargado {len(df)} registros.")
            print(f"Archivo guardado en: {full_path}")
            print("="*40)
        else:
            print("Aviso: No se encontraron registros de IC50 para esta búsqueda.")

except Exception as e:
    print(f"\n[ERROR]: Se produjo un fallo durante la ejecución: {e}")