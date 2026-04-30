
#================================================
#1. Importar librerías
#================================================
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

#================================================
# CONFIGURACIÓN
#================================================

CATALOG= "airlin_mantenimiento"
BRONZE_SCHEMA="bronze"
SILVER_SCHEMA="silver"

spark= SparkSession.builder.getOrCreate()

#================================================
#Asegurar esquema Silver
#================================================
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

#================================================
#Leemos Bronze
#================================================
vuelos_bz=spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.vuelos")
aeronaves_bz=spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeronaves")
aeropuertos_bz=spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeropuertos")
mantenimientos_bz=spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.mantenimientos")

#================================================
#Transformaciones
#================================================

vuelos_slv = (
    vuelos_bz
    .select("vuelo_id",
            "fecha",
            "origen_id",
            "destino_id",
            "aeronave_id",
            "estado",
            "duracion_min"
            )
.withColumn("fecha", F.to_date(F.col("fecha")))
.withColumn("duracion_min", F.col("duracion_min").cast("int"))
.dropDuplicates(["vuelo_id"])
)

aeronaves_slv = (
    aeronaves_bz
    .select("aeronave_id",
            "modelo",
            "fabricante",
            "anio_fabricacion"
            )
.dropDuplicates(["aeronave_id"])
)

aeropuertos_slv = (
    aeropuertos_bz
    .select("aeropuerto_id",
            "nombre",
            "ciudad",
            "pais",
            "lat",
            "lon"
            )
.withColumn("lat", F.col("lat").cast("double"))
.withColumn("lon", F.col("lon").cast("double"))
.dropDuplicates(["aeropuerto_id"])
)

mantenimientos_slv = (
    mantenimientos_bz
    .select("mantenimiento_id",
            "aeronave_id",
            "fecha",
            "tipo",
            "costo_usd",
            "duracion_hr"
    )
.withColumn("fecha", F.to_date(F.col("fecha")))
.withColumn("costo_usd", F.col("costo_usd").cast("double"))
.withColumn("duracion_hr", F.col("duracion_hr").cast("double"))
.dropDuplicates(["mantenimiento_id"])
)

#================================================
#Validación de calidad
#================================================

print("Nulos vuelo_id", vuelos_slv.filter(F.col("vuelo_id").isNull()).count())

print("Duplicados vuelos", vuelos_slv.groupBy("vuelo_id").count().filter(F.col("count")>1).count())


#================================================
#Guardar tablas Silver (guardaremos en tabla Delta)
#================================================

vuelos_slv.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")

aeronaves_slv.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")

aeropuertos_slv.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")

mantenimientos_slv.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")


print("Silver Transformer Completada")
print(f"Tablas creadas:{CATALOG}.{SILVER_SCHEMA}.vuelos, aeronaves, aeropuertos, mantenimientos")






















