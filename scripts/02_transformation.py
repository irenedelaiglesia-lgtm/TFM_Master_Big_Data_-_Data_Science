import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, regexp_replace

# --- CONFIGURACIÓN DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_PATH = os.path.join(BASE_DIR, '..', 'data', 'raw')
OUTPUT_PATH = os.path.join(BASE_DIR, '..', 'data', 'processed', 'consolidado_alzheimer.csv')

DIANAS = ["ache", "bace1", "mapt", "gsk3b"]

print("--- INICIANDO CONSOLIDACIÓN (ESTRATEGIA DE SEGURIDAD TOTAL) ---")

# Forzamos la configuración de Spark para que sea más permisivo con los Casts
spark = SparkSession.builder \
    .appName("TFM_Consolidacion_Silver") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

dfs = []
for diana in DIANAS:
    file_path = os.path.join(RAW_PATH, f"raw_{diana}_alzheimer.csv")
    if os.path.exists(file_path):
        print(f"Leyendo como texto: {diana.upper()}...")
        # TRUCO MAESTRO: inferSchema=False para que no intente convertir números aún
        temp_df = spark.read.csv(file_path, header=True, inferSchema=False) \
            .withColumn("diana_objetivo", lit(diana.upper()))
        dfs.append(temp_df)

if dfs:
    # Unimos todos los datasets
    df_union = dfs[0]
    for next_df in dfs[1:]:
        df_union = df_union.unionByName(next_df, allowMissingColumns=True)

    # LIMPIEZA RADICAL DE CARACTERES
    # Reemplazamos cualquier cosa que no sea un número o un punto decimal por vacío
    df_clean = df_union.withColumn("val_clean", regexp_replace(col("standard_value"), "[^0-9.]", "")) \
                       .withColumn("pchem_clean", regexp_replace(col("pchembl_value"), "[^0-9.]", ""))

    # TRANSFORMACIÓN FINAL CON FILTRO DE SEGURIDAD
    df_silver = df_clean.select(
        col("diana_objetivo"),
        col("molecule_chembl_id").alias("id_farmaco"),
        coalesce(col("molecule_pref_name"), lit("Sin nombre")).alias("nombre"),
        col("val_clean").cast("double").alias("ic50_nm"),
        col("pchem_clean").cast("double").alias("pchembl"),
        coalesce(col("document_year").cast("int"), lit(0)).alias("anio"),
        col("canonical_smiles").alias("smiles")
    ).filter(
        col("id_farmaco").isNotNull() & 
        (col("ic50_nm") > 0) & 
        (col("pchembl") > 0)
    )

    # Clasificación de potencia
    df_silver = df_silver.withColumn("nivel_potencia", 
        when(col("pchembl") >= 7, "Muy Alta")
        .when((col("pchembl") >= 5) & (col("pchembl") < 7), "Media")
        .otherwise("Baja")
    )

    # Acción final: Convertir a Pandas y guardar
    print(f"Procesando y exportando registros...")
    # Usamos collect para forzar la ejecución antes de convertir a Pandas
    final_count = df_silver.count()
    print(f"Total registros limpios detectados: {final_count}")
    
    pandas_df = df_silver.toPandas()
    pandas_df.to_csv(OUTPUT_PATH, index=False)

    print("\n" + "="*40)
    print("ÉXITO: CAPA SILVER CONSOLIDADA")
    df_silver.groupBy("diana_objetivo").count().show()
    print("="*40)

spark.stop()