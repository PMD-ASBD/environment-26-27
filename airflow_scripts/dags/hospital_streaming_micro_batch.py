"""DAG del pipeline de streaming de Kafka, orquestado como micro-batch.

Airflow no es un supervisor de procesos continuos: una tarea que no termina
bloquea el DAG para siempre. Por eso las consultas usan Trigger.AvailableNow,
que procesa lo pendiente y termina, conservando el checkpoint y con él los
offsets de Kafka y la semántica exactamente-una-vez.

Resultado: lo que en el notebook son cuatro consultas continuas, aquí son cuatro
tareas encadenadas que se repiten cada cinco minutos. La latencia pasa de
segundos a minutos, a cambio de obtener reintentos, histórico de ejecuciones,
alertas de fallo y dependencias con el pipeline batch.

Condicional: la capa de alertas necesita la tabla SCD1, que produce el DAG batch.
Si no existe, el ShortCircuitOperator la salta en lugar de dejar morir la tarea
con un PATH_NOT_FOUND.
"""
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG

APPS = "/opt/spark-apps"
COMUN = f"{APPS}/comun.py"

# Tiene que ir en spark-submit: con la JVM ya arrancada, en el builder no surte efecto.
JARS_DRIVER = "/opt/spark/jars/*"

# Reparto de los 4 cores del cluster: 3 para el streaming, 1 para el batch.
CORES_STREAM = 3

DEFAULTS = {
    "owner": "arquitectura-big-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    # Si una ejecución se solapa con la siguiente, el checkpoint lo impediría
    # con un error de bloqueo; mejor no llegar a ese punto.
    "execution_timeout": timedelta(minutes=12),
}


def existe_tabla_scd1():
    """Comprueba en MinIO si la tabla SCD1 ya tiene log de transacciones.

    Se hace con boto3 en lugar de Spark porque es una comprobación de
    existencia: arrancar una sesión Spark para esto costaría más que la
    propia tarea.
    """
    import os

    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_PASSWORD", "minioadmin123"),
        region_name="us-east-1",
    )
    respuesta = s3.list_objects_v2(
        Bucket="hospital",
        Prefix="gold/spo2temperatura_scd1/_delta_log/",
        MaxKeys=1,
    )
    existe = respuesta.get("KeyCount", 0) > 0
    print("Tabla SCD1 disponible" if existe else
          "No existe la tabla SCD1: ejecuta antes el DAG hospital_batch_medallion")
    return existe


def capa(nombre, cores=CORES_STREAM, timeout=600):
    return SparkSubmitOperator(
        task_id=f"stream_{nombre}",
        application=f"{APPS}/stream_medallion.py",
        conn_id="spark_default",
        py_files=COMUN,
        driver_class_path=JARS_DRIVER,
        name=f"{{{{ dag.dag_id }}}}.stream_{nombre}",
        application_args=["--capa", nombre,
                          "--cores", str(cores),
                          "--timeout", str(timeout)],
        verbose=False,
    )


with DAG(
    dag_id="hospital_streaming_micro_batch",
    description="Kafka -> bronze -> silver -> gold -> alertas con Trigger.AvailableNow",
    default_args=DEFAULTS,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,      # imprescindible: dos ejecuciones competirían por el checkpoint
    tags=["hospital", "streaming", "kafka", "delta"],
    doc_md=__doc__,
) as dag:

    bronze = capa("bronze")
    silver = capa("silver")
    gold = capa("gold")

    comprobar_scd1 = ShortCircuitOperator(
        task_id="existe_tabla_scd1",
        python_callable=existe_tabla_scd1,
        doc_md="Salta las alertas si el pipeline batch todavía no generó el estado.",
    )

    alertas = capa("alertas")

    fin = EmptyOperator(
        task_id="fin",
        trigger_rule="none_failed_min_one_success",
    )

    bronze >> silver >> gold >> comprobar_scd1 >> alertas >> fin
    gold >> fin
