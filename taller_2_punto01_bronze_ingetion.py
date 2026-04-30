#Importar librerías
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

#================================================
# CONFIGURACIÓN

CATALOG= "airlin_mantenimiento"
BRONZE_SCHEMA="bronze"
VOLUME_NAME= "landing"

VUELOS_PATH= f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/vuelos_diarios.csv"
AERONAVES_PATH= f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/aeronaves.csv"
AEROPUERTOS_PATH= f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/aeropuertos.csv"
MANTENIMIENTOS_PATH= f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/mantenimientos_rds.csv"
spark= SparkSession.builder.getOrCreate()

#================================================
#Asegurar esquema bronze
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
#Creamos el volumen 
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME_NAME}")

#================================================
#Leer CSV
#LEEMOS VUELOS
vuelos_raw=(spark.read
            .option("header",True)
            .option("inferSchema",True)
            .csv(VUELOS_PATH)
           )

aeronaves_raw= (spark.read
                .option("header",True)
                .option("inferSchema",True)
                .csv(AERONAVES_PATH)
               )

aeropuertos_raw= (spark.read
                  .option("header", True)
                  .option("inferSchema", True)
                  .csv(AEROPUERTOS_PATH)
                 )

mantenimientos_raw= (spark.read
                     .option("header", True)
                     .option("inferSchema", True)
                     .csv(MANTENIMIENTOS_PATH)
                )



#================================================
#Validación
print("filas vuelos:", vuelos_raw.count())
print("filas aeronaves:", aeronaves_raw.count())
print("filas aeropuertos:", aeropuertos_raw.count())
print("filas mantenimientos:", mantenimientos_raw.count())

#================================================
# Normalizar tipos de datos + timestamp

# Normalizar vuelos bronze
vuelos_bronze = (
    vuelos_raw
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("vuelo_id", F.col("vuelo_id").cast("string"))
    .withColumn("origen_id", F.col("origen_id").cast("string"))
    .withColumn("destino_id", F.col("destino_id").cast("string"))
    .withColumn("aeronave_id", F.col("aeronave_id").cast("string"))
    .withColumn("estado", F.col("estado").cast("string"))
    .withColumn("duracion_min", F.col("duracion_min").cast("int"))
    .withColumn("ingestion_time", F.current_timestamp())
)

# Normalizar aeronaves bronze
aeronaves_bronze = (
    aeronaves_raw
    .withColumn("aeronave_id", F.col("aeronave_id").cast("string"))
    .withColumn("modelo", F.col("modelo").cast("string"))
    .withColumn("fabricante", F.col("fabricante").cast("string"))
    .withColumn("anio_fabricacion", F.col("anio_fabricacion").cast("int"))
    .withColumn("ingestion_time", F.current_timestamp())
    )

# Normalizar aeropuertos bronze
aeropuertos_bronze = (
    aeropuertos_raw
    .withColumn("aeropuerto_id",F.col("aeropuerto_id").cast("string"))
    .withColumn("nombre", F.col("nombre").cast("string"))
    .withColumn("ciudad", F.col("ciudad").cast("string"))
    .withColumn("pais", F.col("pais").cast("string"))
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .withColumn("ingestion_time", F.current_timestamp())
    )

# Normalizar mantenimientos bronze
mantenimientos_bronze = (
    mantenimientos_raw
    .withColumn("mantenimiento_id",F.col("mantenimiento_id").cast("string"))
    .withColumn("aeronave_id",F.col("aeronave_id").cast("string"))
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("tipo",F.col("tipo").cast("string"))
    .withColumn("costo_usd",F.col("costo_usd").cast("double"))
    .withColumn("duracion_hr",F.col("duracion_hr").cast("double"))
    .withColumn("ingestion_time", F.current_timestamp())
)
#================================================================
#Guardar tablas Bronze (guardaremos en tabla Delta)

vuelos_bronze.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.vuelos")

aeronaves_bronze.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.aeronaves")

aeropuertos_bronze.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.aeropuertos")

mantenimientos_bronze.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.mantenimientos")

print("Bronze Ingestion Completada")
print(f"Tablas creadas:{CATALOG}.{BRONZE_SCHEMA}.vuelos, aeronaves, aeropuertos, mantenimientos")





























