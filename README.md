# Entorno de prácticas: ingesta de datos de un hospital

Este entorno simula la plataforma de datos de un hospital. Pretende ser el entorno de partida
para la creación de vuestro proyecto Big Data.
Los datos llegan de dos formas, como ficheros CSV y como lecturas continuas de sensores, y pasan por las capas
bronze, silver y gold de un data lake. Todo funciona en contenedores Docker dentro de tu
ordenador.

## Qué necesitas

- **Docker** con Docker Compose v2. Comprueba que `docker compose version` responde.
- **make**, para las órdenes del entorno (`make run`, `make load-dags`…).
- **Python 3** y un editor, por ejemplo Visual Studio Code con la extensión de Python.
- **Al menos 16 GB de RAM.** El entorno ocupa unos 9 GB sin hacer nada.
- En **Windows**, usa WSL2 y ejecuta todas las órdenes desde la terminal de Ubuntu.

Los puertos de la tabla de servicios tienen que estar libres. Si tienes otro proyecto
Docker en marcha, páralo antes.

## Arrancar el entorno

Desde la carpeta `docker-compose/`:

```bash
make run
```

Es lo mismo que `./scripts/run.sh`. La primera vez construye las imágenes y tarda bastantes minutos. Cuando acabe, la última
línea debe ser:

```
RESULTADO: entorno en marcha
```

Si ves `[AVISO]` en alguna línea, consulta [Si algo falla](#si-algo-falla).

## Servicios y puertos

| Servicio | Para qué sirve | Dirección | Acceso |
|---|---|---|---|
| Jupyter | Notebooks de las prácticas | http://localhost:8888 | Sin contraseña |
| Airflow | Lanzar y vigilar los pipelines | http://localhost:8085 | Sin contraseña |
| MinIO (consola) | Ver los ficheros del data lake | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Spark Master | Estado del clúster | http://localhost:8080 | Sin contraseña |
| Spark Worker 1 | Estado del primer worker | http://localhost:8081 | Sin contraseña |
| Spark Worker 2 | Estado del segundo worker | http://localhost:8082 | Sin contraseña |
| Spark UI | Detalle de la sesión de un notebook | http://localhost:4040 (hasta 4050) | Solo con una sesión abierta |
| MinIO (API S3) | Acceso a los ficheros por programa | `localhost:9000` | `minioadmin` / `minioadmin123` |
| Kafka | Mensajes de los sensores | `localhost:9094` | Sin contraseña |
| SQL Server | Base de datos `HOSPITAL` | `localhost:1433` | `sa` / `Password1234%` |

Esas direcciones valen desde tu ordenador. **Dentro de un notebook** cada servicio se
llama por su nombre: `minio:9000`, `kafka:9092`, `sqlserver:1433` y
`spark://spark-master:7077`.

## Trabajar con el entorno

### Notebooks

Instala los notebooks en el entorno, desde `docker-compose/`:

```bash
make load-notebooks
```

Después abre Jupyter y entra en la carpeta `work/`. Los notebooks están numerados en el
orden en que se trabajan. Volver a ejecutar la orden sobrescribe las copias de `work/`, y
con ello los cambios que hayas guardado en ellas.

### Generar datos

Prepara una sola vez los entornos de Python, desde la raíz del repositorio:

```bash
python3 -m venv python4csv/env
python4csv/env/bin/pip install pandas minio

python3 -m venv python4kafka/env
python4kafka/env/bin/pip install confluent-kafka
```

Para subir 4 ficheros CSV a MinIO, con unas pocas temperaturas erróneas a propósito:

```bash
cd python4csv
./env/bin/python producer_csv.py
```

Para enviar lecturas de sensores a Kafka, en otra terminal. Se queda funcionando hasta
que pulses `Ctrl+C`:

```bash
cd python4kafka
./env/bin/python generador_sensores.py
```

### Pipelines en Airflow

El entorno arranca sin DAGs. En `airflow_scripts/` tienes dos pipelines de ejemplo, que
puedes usar como punto de partida para los tuyos: los DAGs van en `airflow_scripts/dags/`
y los jobs de Spark en `airflow_scripts/spark-apps/`. Para instalarlos, desde
`docker-compose/`:

```bash
make load-dags
make run
```

`make load-dags` copia los DAGs y sus jobs al entorno, y `make run` lo arranca y los
registra en Airflow.

Hay dos DAGs. Actívalos con el interruptor que tienen en la lista de Airflow:

| DAG | Qué hace | Cuándo se ejecuta |
|---|---|---|
| `hospital_batch_medallion` | Procesa los CSV nuevos y actualiza el estado de cada paciente | Cada hora |
| `hospital_streaming_micro_batch` | Procesa los sensores y genera alertas | Cada 5 minutos |

Para no esperar, pulsa el botón de lanzar (▶) en cada DAG. Lanza primero el batch: las
alertas necesitan el estado de los pacientes que genera.

El resultado se ve en la consola de MinIO, dentro del bucket `hospital`.

Una alerta aparece cuando un paciente tiene un ECG medio mayor de 90 y su última
temperatura es menor de 30. Como los datos son aleatorios, puede que no salga ninguna en
una ejecución y eso no es un error. Si sale, llega un ciclo más tarde que el dato.

## Parar el entorno

Desde `docker-compose/`:

```bash
docker compose stop
```

Tus datos se conservan. Para volver, ejecuta otra vez `./scripts/run.sh`.

No uses `docker compose down -v` ni `make reset` si no quieres perder lo que has hecho:
borran datos. Para parar sin borrar nada, usa `docker compose stop`.

## Si algo falla

1. Espera un par de minutos y repite `./scripts/run.sh`. No borra nada, así que
   puedes ejecutarlo siempre que quieras.
2. Comprueba que ningún otro programa usa los puertos de la tabla.
3. Mira qué dice el servicio que falla, cambiando `kafka` por su nombre:
   `docker compose logs --tail 50 kafka`
4. Si sigue fallando, avisa al profesorado y copia la salida de `run.sh`.
