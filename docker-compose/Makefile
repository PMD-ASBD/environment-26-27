# Tareas del entorno. Se ejecuta desde docker-compose/.
#
#   make run                      levanta, inicializa y verifica (scripts/run.sh)
#   make load-dags                instala los DAGs y jobs de ../airflow_scripts
#   make load-notebooks           instala los notebooks de ../notebooks en Jupyter
#   make reset                    para el entorno y borra lo generado
#
# run acepta argumentos para run.sh: make run ARGS=--e2e

ARGS ?=

AIRFLOW_DIR := ../airflow_scripts
# Jobs que instala make load-dags en spark-apps/, junto a los ficheros propios del entorno
COPIAS_SPARK := $(notdir $(wildcard $(AIRFLOW_DIR)/spark-apps/*.py))

# Muchos ficheros de minio-data/ son de root, porque los crean los contenedores, así que
# se borran desde un contenedor en lugar de con rm en el host.
IMAGEN_LIMPIEZA := alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

.PHONY: run load-dags load-notebooks reset

run:
	./scripts/run.sh $(ARGS)

# Copia los DAGs y sus jobs a dags/ y spark-apps/, montadas en Airflow y Spark.
# Sobrescribe las copias, así que los cambios se hacen en ../airflow_scripts.
# No arranca el entorno: después hay que hacer make run.
load-dags:
	mkdir -p dags spark-apps
	cp $(AIRFLOW_DIR)/dags/*.py dags/
	cp $(AIRFLOW_DIR)/spark-apps/*.py spark-apps/

# Copia los notebooks originales de ../notebooks a notebooks/, que Jupyter monta como work/.
# Sobrescribe las copias que ya existan, así que los cambios definitivos se hacen en ../notebooks.
load-notebooks:
	mkdir -p notebooks
	cp ../notebooks/*.ipynb notebooks/

# Para el entorno y borra todo lo que se genera al arrancar: tablas y CSV de MinIO, logs,
# estado de las tareas, airflow.cfg, cachés y las copias de make load-dags. config/ y
# plugins/ se borran si quedan vacías: airflow-init las recrea con el propietario correcto.
# dags/ y notebooks/ se mantienen por su .gitkeep. Los notebooks copiados se conservan. Los volúmenes de Docker (Kafka, Postgres y SQL Server) también;
# para borrarlos, usa `docker compose down -v`.
reset:
	docker compose down --remove-orphans
	docker run --rm -v "$(CURDIR):/entorno" -w /entorno $(IMAGEN_LIMPIEZA) sh -c ' \
		rm -rf logs minio-data spark-data config/airflow.cfg notebooks/.ipynb_checkpoints && \
		rm -f dags/*.py $(addprefix spark-apps/,$(COPIAS_SPARK)) && \
		find . -type d -name __pycache__ -prune -exec rm -rf {} + && \
		{ rmdir config plugins 2>/dev/null || true; }'
