import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit

# --- CONFIGURACIÓN DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(BASE_DIR, '..', 'data', 'raw', 'raw_ache_alzheimer.csv')
output_path = os.path.join(BASE_DIR, '..', 'data', 'processed', 'datos_limpios.csv')

print("--- TRANSFORMACIÓN AVANZADA CON SPARK ---")

spark = SparkSession.builder.appName("TFM_Neuro_Silver").getOrCreate()

if not os.path.exists(input_path):
    print(f"Error: No existe el archivo en {input_path}")
else:
    # Leer datos
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Transformación con las nuevas columnas interesantes
    df_final = df.select(
        col("molecule_chembl_id").alias("id_farmaco"),
        # Si no hay nombre preferido, ponemos 'Sin nombre comercial'
        coalesce(col("molecule_pref_name"), lit("Sin nombre comercial")).alias("nombre"),
        col("standard_value").cast("double").alias("ic50_nm"),
        # pchembl_value es vital para análisis estadísticos
        col("pchembl_value").cast("double").alias("pchembl"),
        col("document_year").cast("int").alias("año"),
        col("canonical_smiles").alias("smiles"),
        col("assay_description").alias("descripcion_ensayo")
    ).filter(col("id_farmaco").isNotNull())

    # Clasificación de potencia basada en pChEMBL (Escala logarítmica)
    # Generalmente: > 6 es potente, > 8 es muy potente
    df_final = df_final.withColumn("nivel_potencia", 
        when(col("pchembl") >= 7, "Muy Alta")
        .when((col("pchembl") >= 5) & (col("pchembl") < 7), "Media")
        .otherwise("Baja")
    )

    # Guardar usando el "puente" de Pandas para evitar errores de Hadoop en Windows
    print("Exportando Capa Silver optimizada...")
    pandas_df = df_final.toPandas()
    pandas_df.to_csv(output_path, index=False)

    print("\n" + "="*40)
    print(f"REGISTROS PROCESADOS: {len(pandas_df)}")
    print(f"TABLA ACTUALIZADA CON NOMBRES Y AÑOS")
    print("="*40)
    print(pandas_df[['id_farmaco', 'nombre', 'pchembl', 'nivel_potencia']].head(10))

spark.stop()