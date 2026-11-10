"""Tarea 2: ingesta en bronze.

Bronze guarda el dato tal y como llega. Es append-only: un MERGE aquí
sobrescribiría el original, que es justo lo que esta capa debe conservar, y
además fallaría si dos filas del origen comparten clave. La idempotencia la
garantiza la tabla de checkpoint: un archivo ya registrado no se vuelve a leer.

Se añaden tres columnas de trazabilidad:
  archivo      de qué CSV viene cada fila
  ingest_time  cuándo se ingestó
  lote         qué ejecución del DAG la trajo (permite que las tareas
               siguientes procesen solo lo de este lote)
"""
from pyspark.sql import functions as F

import comun


def main():
    args = comun.argumentos(__doc__).parse_args()
    estado = comun.leer_estado(args.lote)
    nuevos = estado["nuevos"]

    if not nuevos:
        print("No hay archivos nuevos: nada que hacer.")
        return

    spark = comun.crear_sesion(f"bronze-{args.lote}", cores=args.cores)
    try:
        df = (spark.read.format("csv")
            .option("header", "true")
            .schema(comun.SCHEMA_CSV)
            .load(nuevos)
            .withColumn("archivo", F.input_file_name())
            .withColumn("ingest_time", F.current_timestamp())
            .withColumn("lote", F.lit(args.lote)))

        (df.write.format("delta").mode("append")
            .option("mergeSchema", "true")
            .save(comun.BRONZE))

        filas = df.count()
        print(f"Ingestadas {filas} filas en bronze (lote {args.lote})")

        # Registrar los archivos como procesados, con las tres columnas
        registro = (spark.createDataFrame(
                [(f, None, "OK") for f in nuevos], comun.SCHEMA_CKPT)
            .withColumn("processed_at", F.current_timestamp()))
        registro.write.format("delta").mode("append").save(comun.BRONZE_CKPT)
        print(f"Registrados {len(nuevos)} archivos en el checkpoint")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
