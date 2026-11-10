"""Deja MinIO listo en una instalación nueva. Es idempotente.

Crea los buckets y, si no hay ningún CSV de entrada, sube uno de ejemplo para que el
DAG batch tenga algo que procesar. Si ya hay CSV, no toca nada.

Se ejecuta dentro del contenedor de Airflow, que ya trae boto3 (lo hace run.sh).
"""
import csv
import io
import os
import random
from datetime import datetime, timedelta, timezone

import boto3

BUCKETS = ["hospital", "spark"]
PREFIJO_CSV = "spo2temperatura/"
PACIENTES = 10
LECTURAS = 40

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("MINIO_USER", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_PASSWORD", "minioadmin123"),
    region_name="us-east-1",
)

existentes = {b["Name"] for b in s3.list_buckets()["Buckets"]}
for bucket in BUCKETS:
    if bucket in existentes:
        print(f"Bucket ya existe: {bucket}")
    else:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket creado: {bucket}")

if s3.list_objects_v2(Bucket="spark", Prefix=PREFIJO_CSV, MaxKeys=1).get("KeyCount", 0):
    print("Ya hay CSV de entrada: no se sube el de ejemplo")
else:
    aleatorio = random.Random(2026)
    ahora = datetime.now(timezone.utc)
    salida = io.StringIO()
    escritor = csv.writer(salida)
    escritor.writerow(["paciente_id", "temperatura", "SpO2", "timestamp"])
    for i in range(LECTURAS):
        escritor.writerow([
            aleatorio.randint(1, PACIENTES),
            round(aleatorio.uniform(20, 50), 2),
            round(aleatorio.uniform(60, 100), 2),
            (ahora - timedelta(seconds=i)).isoformat(),
        ])
    clave = f"{PREFIJO_CSV}datos_ejemplo.csv"
    s3.put_object(Bucket="spark", Key=clave, Body=salida.getvalue().encode())
    print(f"CSV de ejemplo subido: s3a://spark/{clave}")
