import json
import boto3
from datetime import datetime
from zoneinfo import ZoneInfo   # ← Agregado

sf = boto3.client('stepfunctions', region_name='us-east-2')

def lambda_handler(event, context):

    # Extraer datos del evento S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']            # ruta completa dentro del bucket

    # Obtener solo el nombre del archivo
    input_file_only = key.split('/')[-1]                        # "archivo1.csv"

    # Timestamp para generar output único (hora Perú)
    timestamp = datetime.now(ZoneInfo("America/Lima")).strftime('%Y-%m-%d_%H-%M-%S')

    # Definir rutas explícitas
    input_filename = f's3://data-analitycs-v2/raw/{input_file_only}'          
    output_filename = f's3://data-analitycs-v2/procesed/{timestamp}'

    # Construir JSON para Step Functions
    step_input = {
        "input_filename": input_filename,
        "output_filename": output_filename,
    }

    # Ejecutar Step Function
    response = sf.start_execution(
        stateMachineArn='arn:aws:states:us-east-2:720830544401:stateMachine:Mystate_machine',
        input=json.dumps(step_input)
    )

    print("Ejecución iniciada:", response)

    # Retornar solo datos serializables
    return {
        "status": "OK",
        "executionArn": response.get("executionArn")
    }
