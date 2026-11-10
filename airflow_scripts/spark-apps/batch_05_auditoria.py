"""Tarea 6: auditoría del lote.

Cuenta lo que ha entrado en cada capa en este lote y lo registra. A diferencia
del notebook, los recuentos se obtienen releyendo las tablas Delta filtradas por
`lote`, porque cada tarea es un proceso aparte y no comparte memoria.

run_at es cuándo se ejecutó la ingesta; min_ts_nuevos es el tiempo de evento más
antiguo de los datos del lote. Son cosas distintas y conviene no confundirlas.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

import comun


def main():
    args = comun.argumentos(__doc__).parse_args()
    estado = comun.leer_estado(args.lote)

    spark = comun.crear_sesion(f"auditoria-{args.lote}", cores=args.cores)
    try:
        por_lote = F.col("lote") == args.lote

        bronze = (spark.read.format("delta").load(comun.BRONZE).filter(por_lote)
                  if DeltaTable.isDeltaTable(spark, comun.BRONZE) else None)
        silver = (spark.read.format("delta").load(comun.SILVER).filter(por_lote)
                  if DeltaTable.isDeltaTable(spark, comun.SILVER) else None)
        gold = (spark.read.format("delta").load(comun.GOLD).filter(por_lote)
                if DeltaTable.isDeltaTable(spark, comun.GOLD) else None)

        bronze_count = bronze.count() if bronze is not None else 0
        silver_count = silver.count() if silver is not None else 0
        gold_count = gold.count() if gold is not None else 0
        descartadas = bronze_count - silver_count

        min_ts = None
        if silver_count:
            min_ts = silver.select(F.min("timestamp")).collect()[0][0]

        fila = (args.lote, estado["n_nuevos"], bronze_count, silver_count,
                gold_count, descartadas, min_ts)

        audit = (spark.createDataFrame([fila],
                "lote string, archivos long, bronze_count long, "
                "silver_count long, gold_count long, descartadas long, "
                "min_ts_nuevos timestamp")
            .withColumn("run_at", F.current_timestamp()))

        audit.show(truncate=False)

        modo = "append" if DeltaTable.isDeltaTable(spark, comun.AUDIT) else "overwrite"
        (audit.write.format("delta").mode(modo)
            .option("mergeSchema", "true").save(comun.AUDIT))
        print(f"Auditoría guardada (modo {modo})")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
