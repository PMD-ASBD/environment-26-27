"""Tarea 1: detectar qué CSV son nuevos.

Compara el listado del directorio de entrada con la tabla Delta de checkpoint y
deja el resultado en un fichero de estado que leen las tareas siguientes.
No escribe en ninguna capa: su único cometido es decidir si hay trabajo.
"""
from delta.tables import DeltaTable

import comun


def main():
    args = comun.argumentos(__doc__).parse_args()
    spark = comun.crear_sesion(f"detectar-{args.lote}", cores=args.cores)

    try:
        todos = comun.listar_csv(spark, comun.ENTRADA_CSV)

        if DeltaTable.isDeltaTable(spark, comun.BRONZE_CKPT):
            procesados = {r["archivo"] for r in
                          spark.read.format("delta").load(comun.BRONZE_CKPT)
                          .select("archivo").collect()}
        else:
            # Primera ejecución: creamos la tabla de checkpoint vacía
            (spark.createDataFrame([], comun.SCHEMA_CKPT)
                .write.format("delta").mode("overwrite").save(comun.BRONZE_CKPT))
            procesados = set()
            print("Tabla de checkpoint creada (vacía)")

        nuevos = [f for f in todos if f not in procesados]

        print(f"Archivos en origen: {len(todos)}")
        print(f"Ya procesados:      {len(procesados)}")
        print(f"Nuevos:             {len(nuevos)}")
        for f in nuevos:
            print("   ", f)

        comun.escribir_estado(args.lote, {
            "lote": args.lote,
            "n_nuevos": len(nuevos),
            "nuevos": nuevos,
        })
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
