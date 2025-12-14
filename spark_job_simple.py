from pyspark.sql import SparkSession

# Crear sesión Spark
spark = SparkSession.builder.appName("SimpleSparkTaxiJob").getOrCreate()

# Ruta al dataset exportado
DATA_PATH = "gs://bobucket_maam_01/tlc_yellow_trips_2022/"

# Leer todos los fragmentos CSV
df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

# Mostrar una muestra de los datos
df.show(5)

# Operación simple: contar registros y mostrar columnas disponibles
row_count = df.count()
columns = df.columns

print(f"\n Total de filas procesadas: {row_count:,}")
print(f" Columnas del dataset: {columns}\n")

# Guardar una pequeña copia de los primeros registros (para confirmar escritura)
df.limit(1000).write.mode("overwrite").csv("gs://bobucket_maam_01/output_simple_spark/")

spark.stop()
