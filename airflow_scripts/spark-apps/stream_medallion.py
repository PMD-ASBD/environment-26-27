"""Pipeline de streaming de Kafka, orquestable desde Airflow.

La diferencia con el notebook es el trigger. En el notebook las consultas son
continuas: arrancan y no terminan nunca, por lo que no se pueden representar como
una tarea de Airflow (una tarea que no acaba bloquea el DAG para siempre).

Aquí se usa Trigger.AvailableNow: la consulta procesa todo lo que hay pendiente
en el origen y **termina**. Eso la convierte en un job finito que Airflow puede
programar cada N minutos como cualquier otra tarea, conservando:

  - el checkpoint, y con él los offsets de Kafka y la semántica exactamente-una-vez
  - la misma lógica de transformación que en el notebook

Se invoca una vez por capa:
    stream_medallion.py --capa bronze
    stream_medallion.py --capa silver
    stream_medallion.py --capa gold
    stream_medallion.py --capa alertas

AVISO sobre la capa gold: la agregación por ventana en modo append no emite una
ventana hasta que el watermark la ha superado. Con AvailableNow, el watermark solo
avanza con los datos de esa ejecución, así que la última ventana de cada ejecución
queda pendiente y se emite en la siguiente. Es decir, las alertas van un ciclo por
detrás. Es un comportamiento esperado, no un fallo, y conviene explicarlo.
"""
import argparse

from delta.tables import DeltaTable
from pyspark.sql import functions as F

import comun

UMBRAL_ECG = 90
UMBRAL_TEMPERATURA = 30
VENTANA_ESTADO = "6 hours"


# --------------------------------------------------------------------------
def capa_bronze(spark):
    """Kafka -> bronze. Guarda el evento tal como llega, para auditoría."""
    raw = (spark.readStream
        .format("kafka")
        # Desde dentro de Docker, el listener interno. El 9094 (EXTERNAL)
        # anuncia localhost y solo sirve para clientes del host.
        .option("kafka.bootstrap.servers", comun.KAFKA_BOOTSTRAP)
        .option("subscribe", comun.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

    bronze = raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic"),
        F.col("offset"),
        F.col("timestamp").alias("ingest_time"),
    )

    return (bronze.writeStream
        .queryName("bronze_delta_ecg_rpm")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", comun.ST_BRONZE_CKPT)
        .trigger(availableNow=True)
        .start(comun.ST_BRONZE))


def capa_silver(spark):
    """bronze -> silver. Parsea el JSON, tipifica y valida rangos fisiológicos."""
    bronze = spark.readStream.format("delta").load(comun.ST_BRONZE)

    silver = (bronze
        .withColumn("json", F.from_json(F.col("raw_value"), comun.SCHEMA_SENSOR))
        .select("json.*")
        .filter(F.col("paciente_id").isNotNull())
        .filter(F.col("ECG").between(10, 220))
        .filter(F.col("RPM").isNotNull()))

    return (silver.writeStream
        .queryName("silver_delta_ecg_rpm")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", comun.ST_SILVER_CKPT)
        .trigger(availableNow=True)
        .start(comun.ST_SILVER))


def capa_gold(spark):
    """silver -> gold. Media de ECG y RPM por paciente en ventanas de un minuto."""
    silver = spark.readStream.format("delta").load(comun.ST_SILVER)

    gold = (silver
        .withWatermark("ts", "30 seconds")
        .groupBy(F.window(F.col("ts"), "1 minute"), F.col("paciente_id"))
        .agg(F.avg("RPM").alias("rpm_avg"),
             F.avg("ECG").alias("ecg_avg")))

    return (gold.writeStream
        .queryName("gold_delta_ecg_rpm_minute")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", comun.ST_GOLD_CKPT)
        .trigger(availableNow=True)
        .start(comun.ST_GOLD))


def capa_alertas(spark):
    """gold + SCD1 -> alertas.

    Cruza un dato caliente (la media de ECG del último minuto, que llega por
    Kafka) con un dato lento (el último estado conocido de temperatura, que
    produce el pipeline batch). Esa asimetría es la razón de ser de este bloque:
    ninguna de las dos señales detecta el patrón por separado.
    """
    def procesar_lote(micro_batch_df, batch_id):
        print(f"=== LOTE DE ALERTAS {batch_id} ===")

        # Sin la tabla SCD1 la consulta entera moriría con PATH_NOT_FOUND.
        if not DeltaTable.isDeltaTable(spark, comun.SCD1):
            print("Falta la tabla SCD1: salto este lote. "
                  "Ejecuta antes el DAG batch.")
            return

        ahora = F.current_timestamp()
        estado = (spark.read.format("delta").load(comun.SCD1)
            .filter(F.col("timestamp") >= ahora - F.expr(f"interval {VENTANA_ESTADO}"))
            .filter(F.col("temperatura") < UMBRAL_TEMPERATURA))

        alertas = (micro_batch_df
            .filter(F.col("ecg_avg") > UMBRAL_ECG)
            .join(estado, on="paciente_id", how="inner")
            .withColumn("detectada_en", F.current_timestamp()))

        n = alertas.count()
        if n == 0:
            print("Sin alertas en este lote.")
            return

        print(f"{n} alertas:")
        alertas.show(truncate=False)
        (alertas.write.format("delta").mode("append")
            .option("mergeSchema", "true").save(comun.ST_ALERTAS))

    gold = spark.readStream.format("delta").load(comun.ST_GOLD)

    return (gold.writeStream
        .queryName("alertas_corazon")
        .foreachBatch(procesar_lote)
        .option("checkpointLocation", comun.ST_ALERTAS_CKPT)
        .trigger(availableNow=True)
        .start())


CAPAS = {
    "bronze": capa_bronze,
    "silver": capa_silver,
    "gold": capa_gold,
    "alertas": capa_alertas,
}


def main():
    p = argparse.ArgumentParser(description="Pipeline de streaming por micro-batch")
    p.add_argument("--capa", required=True, choices=sorted(CAPAS))
    p.add_argument("--cores", type=int, default=2, help="spark.cores.max")
    p.add_argument("--timeout", type=int, default=600,
                   help="Segundos máximos de espera antes de abortar la tarea")
    args = p.parse_args()

    spark = comun.crear_sesion(
        f"stream-{args.capa}", cores=args.cores,
        # Varias consultas podrían convivir si se lanzan en paralelo
        extra={"spark.scheduler.mode": "FAIR"})

    try:
        query = CAPAS[args.capa](spark)

        # Con availableNow la consulta termina sola al agotar lo pendiente.
        # El timeout es una red de seguridad para que la tarea de Airflow no
        # se quede colgada indefinidamente si algo va mal.
        terminada = query.awaitTermination(args.timeout)
        if not terminada:
            query.stop()
            raise SystemExit(
                f"La capa {args.capa} no terminó en {args.timeout}s. "
                "Revisa el origen de datos.")

        if query.exception() is not None:
            raise SystemExit(f"La capa {args.capa} falló: {query.exception()}")

        progreso = query.lastProgress
        if progreso:
            print(f"Capa {args.capa}: "
                  f"{progreso.get('numInputRows', 0)} filas de entrada")
        else:
            print(f"Capa {args.capa}: sin datos pendientes.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
