import pandas as pd
import sqlite3
import os

# --- CONFIGURACIÓN DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Ahora leemos el archivo CONSOLIDADO que generó Spark
input_path = os.path.join(BASE_DIR, '..', 'data', 'processed', 'consolidado_alzheimer.csv')
db_path = os.path.join(BASE_DIR, '..', 'tablas', 'tfm_farmacos.db')

# Crear carpeta de tablas si no existe
os.makedirs(os.path.dirname(db_path), exist_ok=True)

print("--- INICIANDO CARGA A SQL (CAPA GOLD) ---")

try:
    # 1. Leer el archivo consolidado
    if not os.path.exists(input_path):
        print(f"Error: No se encuentra el archivo en {input_path}")
    else:
        df = pd.read_csv(input_path)
        print(f"Cargando {len(df)} registros consolidados desde el CSV...")

        # 2. Conectar a SQLite
        conn = sqlite3.connect(db_path)
        
        # 3. Guardar en SQL (Si la tabla existe, la reemplaza con los nuevos datos)
        df.to_sql('moleculas_alzheimer', conn, if_exists='replace', index=False)
        
        print("\n" + "="*40)
        print("¡PIPELINE COMPLETADO CON ÉXITO!")
        print(f"Base de datos: {db_path}")
        print("Tabla: 'moleculas_alzheimer'")
        print("="*40)

        # 4. Comprobación rápida: Ver cuántos fármacos hay por cada diana en la DB
        query = """
            SELECT diana_objetivo, COUNT(*) as total 
            FROM moleculas_alzheimer 
            GROUP BY diana_objetivo
        """
        res = pd.read_sql_query(query, conn)
        print("\nResumen de carga en Base de Datos:")
        print(res)

        conn.close()

except Exception as e:
    print(f"Error al cargar en SQL: {e}")