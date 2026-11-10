#!/usr/bin/env python3
"""Crea el topic y publica lecturas simuladas de sensores (ECG/RPM) en Kafka.

El JSON publicado encaja con schema_silver del notebook:
    {"paciente_id": "3", "ECG": 72.4, "RPM": 15.1, "ts": "2026-10-27 17:50:12.123"}

Se configura por variables de entorno (todas opcionales):
    KAFKA_BOOTSTRAP   destino del broker            (localhost:9094)
    KAFKA_TOPIC       topic de publicación          (sensores-pacientes)
    KAFKA_PARTICIONES particiones al crear el topic (3)
    PACIENTES         ids separados por comas       (1,2,3,4,5,6,7)
    INTERVALO_SEG     segundos entre lecturas       (1.0)
    PROB_ANOMALIA     probabilidad de ECG alto      (0.15)
    PROB_INVALIDO     probabilidad de dato inválido (0.0)
    MAX_MENSAJES      0 = indefinido                (0)
"""
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Desde el host se usa el listener EXTERNAL; desde dentro de Docker sería kafka:9092
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "sensores-pacientes")
PARTICIONES = int(os.getenv("KAFKA_PARTICIONES", "3"))
PACIENTES = [p.strip() for p in os.getenv("PACIENTES", "1,2,3,4,5,6,7").split(",") if p.strip()]
INTERVALO = float(os.getenv("INTERVALO_SEG", "1.0"))
PROB_ANOMALIA = float(os.getenv("PROB_ANOMALIA", "0.15"))
PROB_INVALIDO = float(os.getenv("PROB_INVALIDO", "0.0"))
MAX_MENSAJES = int(os.getenv("MAX_MENSAJES", "0"))

seguir = True


def parar(signum, frame):
    global seguir
    print("\nParando el generador...", flush=True)
    seguir = False


signal.signal(signal.SIGINT, parar)
signal.signal(signal.SIGTERM, parar)


def esperar_broker(admin, intentos=30):
    """El broker tarda unos segundos en aceptar conexiones tras arrancar."""
    for i in range(1, intentos + 1):
        try:
            admin.list_topics(timeout=5)
            return
        except Exception as e:
            print(f"Kafka no responde todavía ({i}/{intentos}): {e}", flush=True)
            time.sleep(2)
    sys.exit("No se pudo conectar con Kafka")


def crear_topic(admin):
    existentes = admin.list_topics(timeout=10).topics
    if TOPIC in existentes:
        n = len(existentes[TOPIC].partitions)
        print(f"El topic '{TOPIC}' ya existe ({n} particiones)", flush=True)
        return
    futuros = admin.create_topics(
        [NewTopic(TOPIC, num_partitions=PARTICIONES, replication_factor=1)]
    )
    for nombre, futuro in futuros.items():
        try:
            futuro.result()
            print(f"Topic creado: {nombre} ({PARTICIONES} particiones)", flush=True)
        except Exception as e:
            # Otra instancia pudo crearlo entre el list y el create
            print(f"No se pudo crear '{nombre}': {e}", flush=True)


def lectura():
    """Devuelve (dict, etiqueta) con una lectura simulada."""
    paciente = random.choice(PACIENTES)
    # ts naive en UTC con el formato que Spark parsea por defecto
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    if random.random() < PROB_INVALIDO:
        # Dato que la capa silver debe descartar (ECG fuera de rango fisiológico)
        return {"paciente_id": paciente, "ECG": 999.0, "RPM": 15.0, "ts": ts}, "INVÁLIDO"

    if random.random() < PROB_ANOMALIA:
        # ECG alto: dispara la alerta de gold (ecg_avg > 90)
        datos = {
            "paciente_id": paciente,
            "ECG": round(random.uniform(95, 140), 1),
            "RPM": round(random.uniform(20, 28), 1),
            "ts": ts,
        }
        return datos, "ANOMALÍA"

    datos = {
        "paciente_id": paciente,
        "ECG": round(random.uniform(60, 85), 1),
        "RPM": round(random.uniform(12, 18), 1),
        "ts": ts,
    }
    return datos, "normal"


def entregado(err, msg):
    if err is not None:
        print(f"Error al publicar: {err}", flush=True)


def main():
    conf = {"bootstrap.servers": BOOTSTRAP}
    admin = AdminClient(conf)
    esperar_broker(admin)
    crear_topic(admin)

    productor = Producer(conf)
    print(
        f"Publicando en '{TOPIC}' cada {INTERVALO}s "
        f"(pacientes: {', '.join(PACIENTES)}). Ctrl+C para parar.",
        flush=True,
    )

    enviados = 0
    try:
        while seguir and (MAX_MENSAJES == 0 or enviados < MAX_MENSAJES):
            datos, etiqueta = lectura()
            productor.produce(
                TOPIC,
                # la clave reparte cada paciente siempre a la misma partición
                key=datos["paciente_id"].encode(),
                value=json.dumps(datos).encode(),
                callback=entregado,
            )
            productor.poll(0)
            enviados += 1
            if etiqueta != "normal" or enviados % 10 == 0:
                print(f"[{enviados:>5}] {etiqueta:<9} {datos}", flush=True)
            time.sleep(INTERVALO)
    finally:
        pendientes = productor.flush(10)
        print(f"Terminado. Mensajes enviados: {enviados}, sin confirmar: {pendientes}", flush=True)


if __name__ == "__main__":
    main()
