"""Tarea 5: tabla de estado actual del paciente (SCD1).

Las capas anteriores responden a preguntas sobre el pasado: qué lecturas hubo y
cuál fue la media por hora. El proceso de alertas en streaming necesita otra
cosa: cómo está cada paciente **ahora**.

SCD1 (Slowly Changing Dimension tipo 1) significa sobrescribir: solo existe el
valor vigente, sin historia. Lo materializa el mode("overwrite") de la escritura.
El tipo 2, en cambio, añadiría una fila nueva marcando la vigente y conservaría
toda la historia.

El estado actual se obtiene con una función de ventana: se particiona por
paciente, se ordenan sus lecturas de más reciente a más antigua y se conserva la
primera.

La opción --demo-ahora reescribe el timestamp con la hora actual. Es un atajo
para que los datos de ejemplo (con fechas antiguas) pasen el filtro de 6 horas
del proceso de alertas. En un entorno real se conserva el timestamp de la lectura.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import comun

UMBRAL_TEMPERATURA = 30


def main():
    parser = comun.argumentos(__doc__)
    parser.add_argument("--demo-ahora", action="store_true",
                        help="Falsea el timestamp con la hora actual (solo demo)")
    args = parser.parse_args()

    spark = comun.crear_sesion(f"scd1-{args.lote}", cores=args.cores)
    try:
        if not DeltaTable.isDeltaTable(spark, comun.SILVER):
            raise SystemExit("No existe silver: ejecuta antes las tareas anteriores")

        silver = spark.read.format("delta").load(comun.SILVER)

        # Última lectura de cada paciente
        w = Window.partitionBy("paciente_id").orderBy(F.col("timestamp").desc())

        scd1 = (silver
            .withColumn("rn", F.row_number().over(w))
            .filter("rn = 1").drop("rn")
            .select("paciente_id",
                    "temperatura",
                    F.col("SpO2").alias("spo2"),
                    "timestamp"))

        if args.demo_ahora:
            scd1 = scd1.withColumn("timestamp", F.current_timestamp())
            print("AVISO: timestamp falseado con la hora actual (modo demo)")

        # El overwrite es lo que lo convierte en SCD1
        scd1.write.format("delta").mode("overwrite").save(comun.SCD1)

        total = scd1.count()
        en_riesgo = scd1.filter(F.col("temperatura") < UMBRAL_TEMPERATURA).count()
        print(f"SCD1 escrita: {total} pacientes, "
              f"{en_riesgo} por debajo de {UMBRAL_TEMPERATURA} grados")
        if en_riesgo == 0:
            print("AVISO: ningún paciente cumple el umbral de temperatura, "
                  "así que el proceso de alertas no emitirá nada.")
        else:
            (scd1.filter(F.col("temperatura") < UMBRAL_TEMPERATURA)
                .orderBy("paciente_id").show(20, truncate=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
