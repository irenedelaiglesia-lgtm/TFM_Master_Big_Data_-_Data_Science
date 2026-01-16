import pandas as pd
import sqlite3
import os

# --- CONFIGURACIÓN DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

input_path = os.path.join(BASE_DIR, '..', 'data', 'processed', 'datos_limpios.csv')

db_path = os.path.join(BASE_DIR, '..', 'tablas', 'tfm_farmacos.db')

print("--- INICIANDO CARGA A SQL (CAPA GOLD) ---")

# 1. Verificar que el archivo de Spark existe
if not os.path.exists(input_path):
    print(f"Error: No se encuentra el archivo de Spark en {input_path}")
else:
    # 2. Leer los datos procesados
    df = pd.read_csv(input_path)
    print(f"Cargando {len(df)} registros desde el CSV...")

    # 3. Conectar a SQLite (se crea la carpeta si no existiera, pero 'tablas' ya la tienes)
    # Nos aseguramos de que la carpeta existe 
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    
    try:
        # 4. Cargar los datos en la tabla
        df.to_sql('moleculas_alzheimer', conn, if_exists='replace', index=False)
        
        print("\n" + "="*40)
        print("¡PIPELINE COMPLETADO CON ÉXITO!")
        print(f"Base de datos actualizada: {db_path}")
        print(f"Tabla creada: 'moleculas_alzheimer'")
        print("="*40)

        # 5. Pequeña comprobación SQL
        query = "SELECT id_farmaco, nombre, ic50_nm, nivel_potencia FROM moleculas_alzheimer LIMIT 5"
        comprobacion = pd.read_sql_query(query, conn)
        print("\nVista previa de la tabla SQL:")
        print(comprobacion)

    except Exception as e:
        print(f"Error al cargar en SQL: {e}")
    finally:
        conn.close()