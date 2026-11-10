# Notebooks convertidos a aplicaciones orquestadas con Airflow

## Dónde va cada cosa

Esta carpeta guarda los originales. Para instalarlos en el entorno, desde
`docker-compose/`:

```bash
make load-dags
make run
```

`make load-dags` copia cada fichero a su sitio y `make run` arranca el entorno, que ya tiene
creados la conexión de Spark, los buckets y el topic:

```
airflow_scripts/               docker-compose/
├── dags/*.py            →     dags/         montado en /opt/airflow/dags
└── spark-apps/*.py      →     spark-apps/   montado en /opt/spark-apps
```

Los jobs importan `comun.py`, que forma parte del entorno y está en
`docker-compose/spark-apps/`.

Las copias se sobrescriben cada vez que se ejecuta `make load-dags`, así que los
cambios se hacen aquí.

## Los dos DAGs

### hospital_batch_medallion

```
detectar_archivos → hay_archivos_nuevos → bronze → silver → gold → scd1 → auditoria → fin
                                                               └→ lanzar_evaluacion_alertas ┘
```

Una tarea por capa. Si no hay CSV nuevos, el `ShortCircuitOperator` corta la rama
y todo lo posterior sale como *skipped*, sin arrancar ninguna sesión Spark.

### hospital_streaming_micro_batch

```
stream_bronze → stream_silver → stream_gold → existe_tabla_scd1 → stream_alertas → fin
```

Cada cinco minutos. La comprobación de la tabla SCD1 evita que falte el lado
batch del `join` y la tarea muera con `PATH_NOT_FOUND`.

## Qué cambia respecto a los notebooks

**Las tareas no comparten memoria.** En el notebook, silver usaba el
`df_bronze` que seguía vivo en el kernel. Aquí cada tarea es un proceso con su
propia sesión, así que se comunican a través de las tablas Delta, filtradas por
una columna nueva, `lote`, con la fecha de la ejecución del DAG. Es un
cambio pequeño en el código y bastante importante en el concepto: cada ejecución
queda identificada y puedes relanzar una capa sin repetir las anteriores.

**El streaming pasa a micro-batch.** En lugar de cuatro consultas continuas, cada
capa usa `Trigger.AvailableNow`: procesa lo pendiente y termina. Los checkpoints
son los mismos, así que se conservan los offsets de Kafka y la semántica
exactamente-una-vez. Una tarea que no termina bloquearía el DAG para siempre, de
modo que no había alternativa razonable.

Esto es un buen punto de discusión para clase: se pierde latencia (de segundos a
minutos) y se gana todo lo que da un orquestador, es decir, reintentos, histórico
de ejecuciones, notificaciones de fallo y dependencias entre pipelines.

**La sesión está en un solo sitio.** `comun.py` tiene la factoría de
`SparkSession`, las rutas y los esquemas. El bloque de 30 líneas de configuración
ya no se repite nueve veces.

## Reparto de cores

Tienes 4 cores (2 workers × 2). El reparto que viene puesto, que es el que
describes en tu propio notebook:

| DAG | `spark.cores.max` |
|---|---|
| batch | 1 |
| streaming | 3 |

Se cambia en la constante `CORES_BATCH` / `CORES_STREAM` de cada DAG. Sin ese
límite, la primera aplicación se queda con todos los cores y la siguiente espera
indefinidamente, que es el `(0 + 0) / 4` sin mensaje de error.

## Probarlo

```bash
# Comprobar que los DAGs se importan sin errores
docker compose exec airflow-scheduler airflow dags list-import-errors

# Lanzar el batch a mano
docker compose exec airflow-scheduler airflow dags trigger hospital_batch_medallion

# Un script suelto, sin Airflow (útil para depurar)
docker compose exec airflow-scheduler python /opt/spark-apps/batch_00_detectar.py --lote prueba01
docker compose exec airflow-scheduler python /opt/spark-apps/stream_medallion.py --capa bronze
```

El generador de Kafka sigue ejecutándose aparte, desde tu Ubuntu, como hasta
ahora.

## Avisos conocidos

**La capa gold del streaming va un ciclo por detrás.** Una agregación por ventana
en modo `append` no emite una ventana hasta que el watermark la supera, y con
`AvailableNow` el watermark solo avanza con los datos de esa ejecución. La última
ventana de cada ejecución queda pendiente y sale en la siguiente. Es el
comportamiento esperado, no un fallo, y merece explicarse.

**`max_active_runs=1` no es opcional.** Dos ejecuciones simultáneas competirían
por el mismo checkpoint y por las mismas tablas Delta.

**`--demo-ahora` en la tarea `scd1`.** Falsea el `timestamp` con la hora actual
para que los CSV de diciembre de 2025 pasen el filtro de 6 horas de las alertas.
Quítalo del DAG cuando generes datos con fecha reciente.

**Para que salgan alertas** hace falta que la media de ECG supere 90, y la media
de un minuto diluye los picos. Arranca el generador con
`PROB_ANOMALIA=0.8`, y con `PACIENTES` restringido a los ids que la tarea `scd1`
lista como de temperatura baja.
