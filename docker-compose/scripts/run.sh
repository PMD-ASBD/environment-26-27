#!/usr/bin/env bash
# Arranca el entorno de prácticas y comprueba que todo funciona por dentro.
#
#   ./scripts/run.sh               levanta, inicializa y verifica
#   ./scripts/run.sh --e2e         además hace pasar un paciente de prueba por los DAGs
#
# Si existe scripts/verify.sh, termina ejecutándolo y sale con código 0 solo si todas las
# comprobaciones pasan. Sin él (repositorio de alumnos), sale con 0 si no hubo avisos.
set -euo pipefail
cd "$(dirname "$0")/.."

MAX_ESPERA=300
TOPIC=sensores-pacientes

avisos=0
aviso() {
    echo "[AVISO] $1"
    avisos=$((avisos + 1))
}

echo "== Levantando contenedores (la primera vez construye las imágenes y tarda) =="
docker compose up -d

echo "== Esperando a que los servicios estén sanos (máximo ${MAX_ESPERA}s) =="
limite=$((SECONDS + MAX_ESPERA))
while :; do
    pendientes=$(docker compose ps -a --format '{{.Service}} {{.State}} {{.Health}}' |
        awk '$1 == "airflow-init" { next } $2 != "running" || $3 == "starting" || $3 == "unhealthy"' |
        awk '{print $1}' | paste -sd' ' -)
    [ -z "$pendientes" ] && break
    if [ "$SECONDS" -ge "$limite" ]; then
        aviso "Siguen sin estar listos: $pendientes"
        break
    fi
    sleep 5
done

echo "== Inicializando MinIO y Kafka (idempotente) =="
docker compose exec -T airflow-scheduler python - < scripts/inicializar.py
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 1

echo "== Inicializando SQL Server (idempotente) =="
# La contraseña se toma del propio contenedor para no repetirla aquí.
sqlcmd() {
    docker compose exec -T sqlserver sh -c \
        '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -f 65001 -b -r0 "$@"' \
        sqlcmd "$@"
}
# El contenedor figura como arrancado antes de que SQL Server acepte conexiones.
limite=$((SECONDS + 120))
until sqlcmd -Q 'SELECT 1' >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$limite" ]; then
        aviso "SQL Server no responde"
        break
    fi
    sleep 5
done
if sqlcmd < scripts/init_sqlserver.sql >/dev/null; then
    echo "Aplicado scripts/init_sqlserver.sql"
else
    aviso "No se pudo aplicar scripts/init_sqlserver.sql"
fi

# Sin esto, los DAGs recién copiados por make load-dags tardan en aparecer hasta que el
# dag-processor vuelve a recorrer la carpeta.
if compgen -G 'dags/*.py' >/dev/null; then
    echo "== Registrando los DAGs de dags/ =="
    docker compose exec -T airflow-scheduler airflow dags reserialize >/dev/null 2>&1 ||
        aviso "No se pudieron registrar los DAGs"
fi

if [ -x scripts/verify.sh ]; then
    exec ./scripts/verify.sh "$@"
fi

echo
docker compose ps --format 'table {{.Service}}\t{{.State}}\t{{.Health}}'
echo
if [ "$avisos" -eq 0 ]; then
    echo "RESULTADO: entorno en marcha"
else
    echo "RESULTADO: entorno en marcha con $avisos aviso(s); revisa las líneas [AVISO]"
    exit 1
fi
