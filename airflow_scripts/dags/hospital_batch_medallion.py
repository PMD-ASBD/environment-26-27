"""DAG del pipeline batch semiestructurado: CSV -> bronze -> silver -> gold.

Cada capa es una tarea independiente. Eso tiene dos consecuencias que conviene
explicar en clase:

  - Ventaja: el grafo refleja la arquitectura, y cada capa se puede relanzar por
    separado desde la interfaz sin repetir las anteriores.
  - Coste: cada tarea abre su propia SparkSession (unos segundos) y no hereda
    DataFrames en memoria. Por eso las tareas se comunican releyendo las tablas
    Delta, filtradas por la columna `lote`, en lugar de pasarse variables.

Condicional: si la tarea de detección no encuentra CSV nuevos, el
ShortCircuitOperator corta la rama y las capas aparecen como "skipped" en el
grafo, sin lanzar ninguna sesión Spark innecesaria.
"""
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

APPS = "/opt/spark-apps"
COMUN = f"{APPS}/comun.py"

# Tiene que ir en spark-submit: con la JVM ya arrancada, en el builder no surte efecto.
JARS_DRIVER = "/opt/spark/jars/*"

# Identifica la ejecución. En Airflow 3, un lanzamiento manual desde la CLI o la API no
# tiene logical_date (ni, por tanto, ts_nodash): en ese caso se usa run_after.
LOTE = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y%m%dT%H%M%S') }}"

# El pipeline batch se queda con 1 core para dejar los otros 3 al streaming.
CORES_BATCH = 1

DEFAULTS = {
    "owner": "arquitectura-big-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def hay_archivos_nuevos(**contexto):
    """Lee el fichero de estado que dejó la tarea de detección.

    Devolver False hace que el ShortCircuitOperator marque como "skipped" todo
    lo que viene después, que es justo lo que queremos cuando no hay CSV nuevos.
    """
    import json
    from pathlib import Path

    ejecucion = contexto["dag_run"]
    lote = (ejecucion.logical_date or ejecucion.run_after).strftime("%Y%m%dT%H%M%S")
    ruta = Path(f"/opt/spark-data/estado/{lote}.json")
    if not ruta.exists():
        raise FileNotFoundError(f"No existe {ruta}")

    estado = json.loads(ruta.read_text(encoding="utf-8"))
    n = estado.get("n_nuevos", 0)
    print(f"Lote {lote}: {n} archivos nuevos")
    return n > 0


def job(task_id, script, cores=CORES_BATCH, extra_args=None):
    """Crea una tarea spark-submit para uno de los scripts del pipeline."""
    args = ["--lote", LOTE, "--cores", str(cores)]
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{APPS}/{script}",
        conn_id="spark_default",
        py_files=COMUN,
        driver_class_path=JARS_DRIVER,
        name=f"{{{{ dag.dag_id }}}}.{task_id}",
        application_args=args + (extra_args or []),
        verbose=False,
    )


with DAG(
    dag_id="hospital_batch_medallion",
    description="Ingesta batch de CSV de temperatura y SpO2 en arquitectura medallion",
    default_args=DEFAULTS,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",      # cada hora; ajústalo a tu caso
    catchup=False,
    max_active_runs=1,          # evita que dos ejecuciones se pisen en las tablas
    tags=["hospital", "batch", "delta", "medallion"],
    doc_md=__doc__,
) as dag:

    detectar = job("detectar_archivos", "batch_00_detectar.py")

    comprobar = ShortCircuitOperator(
        task_id="hay_archivos_nuevos",
        python_callable=hay_archivos_nuevos,
        doc_md="Corta la rama si la detección no encontró CSV nuevos.",
    )

    bronze = job("bronze", "batch_01_bronze.py")
    silver = job("silver", "batch_02_silver.py")
    gold = job("gold", "batch_03_gold.py")

    # --demo-ahora falsea el timestamp para que los CSV antiguos pasen el filtro
    # de 6 horas del proceso de alertas. Quítalo cuando uses datos reales.
    scd1 = job("scd1_estado_actual", "batch_04_scd1.py",
               extra_args=["--demo-ahora"])

    auditoria = job("auditoria", "batch_05_auditoria.py")

    # Tras refrescar el estado de los pacientes, lanza la evaluación de alertas
    # sin esperar a su propio horario.
    lanzar_alertas = TriggerDagRunOperator(
        task_id="lanzar_evaluacion_alertas",
        trigger_dag_id="hospital_streaming_micro_batch",
        wait_for_completion=False,
        doc_md="Dependencia entre DAGs: el estado SCD1 acaba de cambiar.",
    )

    # Cierra el DAG en verde tanto si se ejecutó todo como si se cortó la rama.
    fin = EmptyOperator(
        task_id="fin",
        trigger_rule="none_failed_min_one_success",
    )

    detectar >> comprobar >> bronze >> silver >> gold >> scd1 >> auditoria
    scd1 >> lanzar_alertas
    [auditoria, lanzar_alertas] >> fin
    detectar >> fin
