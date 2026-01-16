import pandas as pd
from chembl_webresource_client.new_client import new_client
import os

print("--- Iniciando descarga de datos ---")

# Creamos la carpeta data si no existe
if not os.path.exists('data'):
    os.makedirs('data')

# Conectamos con ChEMBL
try:
    target = new_client.target
    # Buscamos HER2 (ERBB2)
    res = target.filter(target_gene_name__iexact='ERBB2').only(['target_chembl_id'])
    target_id = res[0]['target_chembl_id']
    print(f"Diana encontrada: {target_id}")

    # Bajamos solo 10 actividades para probar rápido
    activity = new_client.activity
    activities = activity.filter(target_chembl_id=target_id).filter(standard_type="IC50")[:10]
    
    # Convertimos a tabla y guardamos
    df = pd.DataFrame.from_records(activities)
    df.to_csv('data/prueba_tfm.csv', index=False)
    
    print("--- ¡ÉXITO! El archivo data/prueba_tfm.csv ha sido creado ---")

except Exception as e:
    print(f"Error detectado: {e}")