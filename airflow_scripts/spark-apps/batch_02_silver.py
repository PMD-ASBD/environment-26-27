"""Tarea 3: ingesta en silver.

Lee de bronze **solo las filas de este lote** (por eso bronze lleva la columna
`lote`: cada tarea es un proceso independiente y no puede heredar DataFrames en
memoria de la tarea anterior, como sí ocurría en el notebook).

Aquí sí corresponde deduplicar y descartar valores fuera de rango. El MERGE usa
la clave de negocio (paciente_id, timestamp), de modo que relanzar la tarea no
duplica filas. El dropDuplicates previo es imprescindible: sin él, dos filas del
origen con la misma clave hacen fallar el MERGE.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

import comun

TEMP_MIN = 20
TEMP_MAX = 50


def main():
    args = comun.argumentos(__doc__).parse_args()
    spark = comun.crear_sesion(f"silver-{args.lote}", cores=args.cores)
    try:
        bronze_lote = (spark.read.format("delta").load(comun.BRONZE)
            .filter(F.col("lote") == args.lote))

        total = bronze_lote.count()
        if total == 0:
            print(f"Bronze no tiene filas del lote {args.lote}: nada que hacer.")
            return

        silver_nuevo = (bronze_lote
            .filter((F.col("temperatura") >= TEMP_MIN) &
                    (F.col("temperatura") <= TEMP_MAX))
            .dropDuplicates(["paciente_id", "timestamp"]))

        validas = silver_nuevo.count()
        print(f"Lote {args.lote}: {total} filas en bronze, "
              f"{validas} válidas, {total - validas} descartadas")

        if DeltaTable.isDeltaTable(spark, comun.SILVER):
            tabla = DeltaTable.forPath(spark, comun.SILVER)
            (tabla.alias("t").merge(
                silver_nuevo.alias("s"),
                "t.paciente_id = s.paciente_id AND t.timestamp = s.timestamp")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            print("Silver actualizada con MERGE")
        else:
            silver_nuevo.write.format("delta").save(comun.SILVER)
            print("Silver creada")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
