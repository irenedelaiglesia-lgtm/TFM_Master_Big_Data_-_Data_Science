import pandas as pd
import requests
import time
import os
import sqlite3  # <--- Nuevo import

# === CONFIGURACIÓN DE RUTAS ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

INPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_final.csv")
OUTPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_enriquecido_v2.csv")
DB_PATH = os.path.join(ROOT_DIR, "tablas", "alzheimer_targets.db") # <--- Ruta DB

# Identificación para la API (Evita bloqueos)
HEADERS = {'User-Agent': 'TFM-Alzheimer-Project/1.0 (contact: irene@example.com)'}

def get_chembl_data_pro(uniprot_id):
    """Estrategia de búsqueda: Componente -> Target -> Activity"""
    if not uniprot_id or pd.isna(uniprot_id): return 0.0, 0
    
    comp_url = f"https://www.ebi.ac.uk/chembl/api/data/target_component.json?accession={uniprot_id}"
    
    try:
        res = requests.get(comp_url, headers=HEADERS, timeout=10)
        if res.status_code != 200: return 0.0, 0
        
        comp_data = res.json()
        if not comp_data.get('target_components'): return 0.0, 0
        
        targets = comp_data['target_components'][0].get('targets', [])
        if not targets: return 0.0, 0
        
        tid = targets[0]['target_chembl_id']
        
        act_url = f"https://www.ebi.ac.uk/chembl/api/data/activity.json?target_chembl_id={tid}&pchembl_value__isnull=false&order_by=-pchembl_value&limit=1"
        act_res = requests.get(act_url, headers=HEADERS, timeout=10).json()
        
        drug_count = int(act_res['page_meta']['total_count'])
        max_pchembl = float(act_res['activities'][0]['pchembl_value']) if act_res['activities'] else 0.0
        
        return max_pchembl, drug_count

    except Exception:
        return 0.0, 0

# === MAIN ===
if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Error: No existe {INPUT_PATH}. Ejecuta primero '03.1_merge_master_dataset.py'.")
        exit()

    df = pd.read_csv(INPUT_PATH)
    print(f"🚀 Iniciando Enriquecimiento ChEMBL para {len(df)} genes...")
    print("⏳ Esto puede tardar unos minutos...")

    results = []
    for i, row in df.iterrows():
        p, count = get_chembl_data_pro(row['uniprot_id'])
        results.append({'max_pchembl': p, 'drug_count': count})
        
        if (i + 1) % 50 == 0:
            print(f"📊 Procesados {i + 1}/{len(df)} targets...")
        
        time.sleep(0.1) 

    df_new = pd.DataFrame(results)
    df_final = pd.concat([df.reset_index(drop=True), df_new], axis=1)
    
    # 1. Guardar CSV
    df_final.to_csv(OUTPUT_PATH, index=False)
    
    # 2. Guardar SQLITE (Capa Gold)
    conn = sqlite3.connect(DB_PATH)
    df_final.to_sql("dataset_maestro_gold", conn, if_exists="replace", index=False)
    conn.close()
    
    print(f"\n✨ ¡FIN! Dataset guardado en CSV y SQLite ('dataset_maestro_gold')")