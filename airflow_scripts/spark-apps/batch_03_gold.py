"""Tarea 4: agregación en gold (media de temperatura y SpO2 por paciente, día y hora).

Es incremental, pero recalculando **grupos completos**. El error habitual es
filtrar silver por el timestamp de los datos nuevos y agregar solo eso: entonces
la media de un grupo se calcula sin sus filas antiguas, y como el MERGE hace
whenMatchedUpdateAll, la media correcta queda sobrescrita por una parcial.

Ejemplo: el paciente 4 tiene lecturas a las 10:00 y 10:30, y gold guarda la media
de ambas. Llega una lectura a las 10:45. Si solo se agrega esa, gold pasa a
contener únicamente la media de las 10:45.

Aquí se identifican primero los grupos que toca el lote nuevo y después se releen
de silver **todas** las filas de esos grupos.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

import comun


def main():
    args = comun.argumentos(__doc__).parse_args()
    spark = comun.crear_sesion(f"gold-{args.lote}", cores=args.cores)
    try:
        silver = (spark.read.format("delta").load(comun.SILVER)
            .withColumn("fecha", F.to_date("timestamp"))
            .withColumn("hora", F.hour("timestamp")))

        # 1. Grupos afectados por este lote
        claves = (silver
            .filter(F.col("lote") == args.lote)
            .select("paciente_id", "fecha", "hora")
            .distinct())

        n_claves = claves.count()
        if n_claves == 0:
            print(f"Silver no tiene filas del lote {args.lote}: nada que agregar.")
            return
        print(f"Grupos (paciente, fecha, hora) afectados: {n_claves}")

        # 2. Todas las filas de silver, pero solo de esos grupos
        afectado = silver.join(claves, ["paciente_id", "fecha", "hora"], "inner")

        # 3. Recalcular la media completa de cada grupo
        gold_nuevo = (afectado
            .groupBy("paciente_id", "fecha", "hora")
            .agg(F.avg("temperatura").alias("avg_temperatura"),
                 F.avg("SpO2").alias("avg_SpO2"))
            .withColumn("lote", F.lit(args.lote)))

        if DeltaTable.isDeltaTable(spark, comun.GOLD):
            tabla = DeltaTable.forPath(spark, comun.GOLD)
            (tabla.alias("t").merge(
                gold_nuevo.alias("s"),
                "t.paciente_id = s.paciente_id AND t.fecha = s.fecha "
                "AND t.hora = s.hora")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            print("Gold actualizada con MERGE")
        else:
            gold_nuevo.write.format("delta").save(comun.GOLD)
            print("Gold creada")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
