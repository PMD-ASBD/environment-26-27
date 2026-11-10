import pandas as pd
import random
from datetime import datetime, timezone
from io import BytesIO
import time
import math
from minio import Minio                  # pip install Minio

folder_name="spo2temperatura"
subfolder_name= None            # None: se guarda en la carpeta principal
rows=10                          # filas por archivo
latency=1000                    # ms entre envíos
duration=4                      # segundos en total
max_id_paciente=100


blob_service_client = Minio(
    "localhost:9000",
    access_key='minioadmin',
    secret_key='minioadmin123',
    secure=False
)


num_files = math.trunc(duration / (latency /1000))

for i in range(num_files):
    data = []
    for j in range(rows):
        id_paciente = math.trunc(random.uniform(1, max_id_paciente))

        # generamos aleatoriamente errores de temperatura para comprobar la validacion de datos
        generar_error_temperatura=random.uniform(0, 100)
        if generar_error_temperatura > 90:
            temperatura = round(random.uniform(-100, 100), 2)
        else:
            temperatura = round(random.uniform(20, 50), 2)

        SpO2 = round(random.uniform(60, 100), 2)
        timestamp = datetime.now(timezone.utc).isoformat()
        data.append({"paciente_id":id_paciente, "temperatura": temperatura, "SpO2": SpO2, "timestamp": timestamp })

    df = pd.DataFrame(data)

    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)


    
    if not subfolder_name:
        file_path = f"{folder_name}/"
    else:
        file_path = f"{folder_name}/{subfolder_name}/"

    date_file=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00", "Z")
    file_name= f"{file_path}datos_{date_file}_{i}.csv"


    bucket_name = 'spark'
    found = blob_service_client.bucket_exists(bucket_name)
    if not found:
        blob_service_client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' creado.")


    blob_service_client.put_object(
        bucket_name=bucket_name,
        object_name=file_name,
        data=csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv"
    )

    print(f"Archivo CSV subido a s3a://{file_name}")


    if i < num_files - 1:
        time.sleep(latency/1000)