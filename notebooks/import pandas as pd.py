import pandas as pd
import os

ruta_joyas = os.path.join('data', 'processed', 'farmacos_multitarget.csv')
df_joyas = pd.read_csv(ruta_joyas)
print(f"Total de fármacos multiobjetivo: {len(df_joyas)}")