#================================================
#1. Importar librerías
#================================================
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

#================================================
# CONFIGURACIÓN
#================================================
CATALOG= "airlin_mantenimiento"
SILVER_SCHEMA="silver"
GOLD_SCHEMA="gold"

spark= SparkSession.builder.getOrCreate()
#================================================
#Asegurar esquema Silver
#================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

#================================================
#Leer silver para puntualidad
#================================================
df_vuelos= spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")
df_aeronaves= spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")
df_aeropuertos= spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")
df_mantenimientos= spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")


#================================================
#Capa Gold - KPI de puntualidad de vuelos
#================================================

#Preparas aeropuertos de origen 
df_aeropuertos_origen = df_aeropuertos.select(
    F.col("aeropuerto_id").alias("origen_id"),
    F.col("nombre").alias("origen_nombre"),
    F.col("ciudad").alias("origen_ciudad"),
    F.col("pais").alias("origen_pais"),
    )

#Validación
df_aeropuertos_origen.show()

#Enriquecer vuelos

df_vuelos_enriquecidos = (
   df_vuelos
   .join(df_aeropuertos_origen, on="origen_id", how="left")
   .join(df_aeronaves, on="aeronave_id", how="left")
   .withColumn("anio", F.year(F.col("fecha")))
   .withColumn("mes", F.month(F.col("fecha")))
)

#Validación
display(df_vuelos_enriquecidos)

#Calcular el KPI  de puntualidad
df_kpi_puntualidad = (
    df_vuelos_enriquecidos
        .groupBy("modelo","fabricante","origen_pais","mes","anio")
        .agg(
            F.count("vuelo_id").alias("total_vuelos"),
            F.count(F.when(F.col("estado") == "a_tiempo", True)).alias("vuelos_a_tiempo"),
            F.count(F.when(F.col("estado") == "retrasado", True)).alias("vuelos_retrasados"),
            F.count(F.when(F.col("estado") == "cancelado", True)).alias("vuelos_cancelados"),
            F.round(F.avg("duracion_min"),2).alias("duracion_promedio_min")
        )
        .withColumn("puntualidad_pct", F.round((F.col("vuelos_a_tiempo") / F.col("total_vuelos")*100), 2))
        .withColumn("retraso_pct", F.round((F.col("vuelos_retrasados") / F.col("total_vuelos")*100), 2))
        .withColumn("cancelacion_pct", F.round((F.col("vuelos_cancelados") / F.col("total_vuelos")*100), 2))
        .withColumn("duracion_promedio_hr", F.round(F.col("duracion_promedio_min") / 60, 2))
)

#Guardar Gold Puntualidad
df_kpi_puntualidad.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos")


#================================================
#Capa Gold = KPI de costos de mantenimiento
#================================================

df_mantenimientos = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

#ENRIQUECER MANTENIMIENTO
df_mantenimientos_enriquecido = (
    df_mantenimientos
    .join(df_aeronaves, on="aeronave_id", how="left")
    .withColumn("anio", F.year(F.col("fecha")))
    .withColumn("mes", F.month(F.col("fecha")))
)
#validacion
display(df_mantenimientos_enriquecido)

#Calcular el KPI  de mantenimiento
df_kpi_mantenimiento = (
    df_mantenimientos_enriquecido
        .groupBy("aeronave_id", "modelo", "fabricante", "tipo", "mes", "anio")
        .agg(
            F.count("mantenimiento_id").alias("total_mantenimientos"),
            F.round(F.avg("costo_usd"),2).alias("costo_promedio_usd"),
            F.round(F.sum("costo_usd"),2).alias("costo_total_usd"),
            F.round(F.avg("duracion_hr"),2).alias("duracion_promedio_hr"),
            F.round(F.sum("duracion_hr"),2).alias("duracion_total_hr"))
)


#Guardar Gold mantenimientos
df_kpi_mantenimiento.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento")


print("Gold Analytics Completada")
print(f"Tablas creadas:{CATALOG}.{GOLD_SCHEMA}.vuelos, aeronaves, aeropuertos, mantenimientos")
























