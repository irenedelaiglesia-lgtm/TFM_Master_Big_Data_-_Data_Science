import pandas as pd
import os

# --- RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(BASE_DIR, '..', 'data', 'processed', 'consolidado_alzheimer.csv')
OUTPUT_JOYAS = os.path.join(BASE_DIR, '..', 'data', 'processed', 'farmacos_multitarget.csv')

print("--- EJECUTANDO MINERÍA DE DATOS ---")

df = pd.read_csv(INPUT_PATH)

# 1. Minería de Selectividad: ¿Qué fármacos aparecen en más de una diana?
# Agrupamos por fármaco y contamos cuántas dianas distintas tiene
multitarget = df.groupby('id_farmaco').agg({
    'diana_objetivo': ['count', lambda x: list(x.unique())],
    'pchembl': 'mean',
    'nombre': 'first'
}).reset_index()

multitarget.columns = ['id_farmaco', 'num_apariciones', 'lista_dianas', 'potencia_media', 'nombre']

# Filtramos los que aparecen en 2 o más dianas
joyas = multitarget[multitarget['num_apariciones'] >= 2].sort_values(by='num_apariciones', ascending=False)

# 2. Guardar resultados
joyas.to_csv(OUTPUT_JOYAS, index=False)

print(f"\n✅ Hallazgo de Minería: Se han detectado {len(joyas)} fármacos con acción dual/múltiple.")
print("Vista previa de los mejores candidatos:")
print(joyas[['nombre', 'lista_dianas', 'num_apariciones']].head(10))