"""Utilidades compartidas por todos los jobs Spark del pipeline del hospital.

Centraliza aquí la creación de la sesión, las rutas de las tablas y los esquemas
para no repetir el mismo bloque de 30 líneas en cada script.
"""
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, FloatType, TimestampType, LongType)

# --------------------------------------------------------------------------
# Rutas
# --------------------------------------------------------------------------
# Entrada del pipeline batch: CSV semiestructurados (bucket "spark", no "hospital")
ENTRADA_CSV = os.getenv("ENTRADA_CSV", "s3a://spark/spo2temperatura/")

# Capas del pipeline batch
BRONZE = "s3a://hospital/bronze/spo2temperatura"
BRONZE_CKPT = "s3a://hospital/bronze/spo2temperatura_checkpoint"
SILVER = "s3a://hospital/silver/spo2temperatura"
GOLD = "s3a://hospital/gold/spo2temperatura"
SCD1 = "s3a://hospital/gold/spo2temperatura_scd1"
AUDIT = "s3a://hospital/audit/spo2temperatura"

# Capas del pipeline de streaming (Kafka)
ST_BRONZE = "s3a://hospital/bronze/ECG_RPM"
ST_BRONZE_CKPT = "s3a://hospital/bronze/ECG_RPM_checkpoint"
ST_SILVER = "s3a://hospital/silver/ECG_RPM"
ST_SILVER_CKPT = "s3a://hospital/silver/ECG_RPM_checkpoint"
ST_GOLD = "s3a://hospital/gold/ECG_RPM_minute"
ST_GOLD_CKPT = "s3a://hospital/gold/ECG_RPM_minute_checkpoint"
ST_ALERTAS_CKPT = "s3a://hospital/gold/gold_alerts_checkpoint"
ST_ALERTAS = "s3a://hospital/gold/alertas"

# Directorio compartido entre el contenedor de Airflow y los jobs, para pasar
# información de una tarea a otra (está montado en el compose como ./spark-data)
DIR_ESTADO = Path(os.getenv("DIR_ESTADO", "/opt/spark-data/estado"))

# Kafka: desde dentro de Docker siempre el listener interno
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensores-pacientes")

# --------------------------------------------------------------------------
# Esquemas
# --------------------------------------------------------------------------
SCHEMA_CSV = StructType([
    StructField("paciente_id", StringType(), True),
    StructField("temperatura", DoubleType(), True),
    StructField("SpO2", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])

SCHEMA_CKPT = StructType([
    StructField("archivo", StringType(), True),
    StructField("processed_at", TimestampType(), True),
    StructField("status", StringType(), True),
])

# JSON que publica el generador en Kafka
SCHEMA_SENSOR = StructType([
    StructField("paciente_id", StringType(), True),
    StructField("ECG", FloatType(), True),
    StructField("RPM", FloatType(), True),
    StructField("ts", TimestampType(), True),
])

SCHEMA_ST_BRONZE = StructType([
    StructField("raw_value", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("offset", LongType(), True),
    StructField("ingest_time", TimestampType(), True),
])


# --------------------------------------------------------------------------
# Sesión
# --------------------------------------------------------------------------
def crear_sesion(nombre, cores=1, shuffle_partitions=None, extra=None):
    """Devuelve una SparkSession configurada para MinIO y Delta.

    cores: valor de spark.cores.max. En standalone, sin este límite una
    aplicación se queda con todos los cores del cluster y las siguientes
    esperan indefinidamente.
    """
    if shuffle_partitions is None:
        shuffle_partitions = max(2, cores * 2)

    # Si el script se lanza con spark-submit, el master llega por --master.
    # Lo leemos del entorno para que también funcione ejecutándolo a mano.
    master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    builder = (SparkSession.builder
        .appName(nombre)
        .master(master)
        .config("spark.submit.deployMode", "client")

        .config("spark.cores.max", str(cores))
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", os.getenv("EXECUTOR_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))

        # Delta
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # MinIO (S3A)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Los workers ya llevan los jars en su imagen: aquí solo hace falta
        # ponerlos en el classpath del driver, que corre en el contenedor de Airflow.
        .config("spark.driver.extraClassPath", os.getenv("DRIVER_CLASSPATH", "/opt/spark/jars/*"))
    )

    for clave, valor in (extra or {}).items():
        builder = builder.config(clave, valor)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("LOG_LEVEL", "WARN"))
    return spark


# --------------------------------------------------------------------------
# Estado entre tareas
# --------------------------------------------------------------------------
def ruta_estado(lote):
    return DIR_ESTADO / f"{lote}.json"


def escribir_estado(lote, datos):
    DIR_ESTADO.mkdir(parents=True, exist_ok=True)
    destino = ruta_estado(lote)
    destino.write_text(json.dumps(datos, indent=2), encoding="utf-8")
    print(f"Estado escrito en {destino}: {datos}")
    return destino


def leer_estado(lote):
    origen = ruta_estado(lote)
    if not origen.exists():
        raise SystemExit(f"No existe el fichero de estado {origen}. "
                         "¿Se ejecutó la tarea de detección?")
    datos = json.loads(origen.read_text(encoding="utf-8"))
    print(f"Estado leído de {origen}: {datos}")
    return datos


# --------------------------------------------------------------------------
# Varios
# --------------------------------------------------------------------------
def listar_csv(spark, ruta):
    """Lista los .csv de un directorio en S3A usando el FileSystem de Hadoop."""
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI.create(ruta),
        spark._jsc.hadoopConfiguration()
    )
    estados = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(ruta))
    return sorted(e.getPath().toString() for e in estados
                  if e.getPath().toString().lower().endswith(".csv"))


def argumentos(descripcion):
    """Parser común: todos los jobs reciben el identificador de lote."""
    import argparse
    p = argparse.ArgumentParser(description=descripcion)
    p.add_argument("--lote", required=True,
                   help="Identificador del lote (fecha de la ejecución de Airflow, sin separadores)")
    p.add_argument("--cores", type=int, default=1, help="spark.cores.max")
    return p
