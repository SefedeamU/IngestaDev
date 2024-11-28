import boto3
import pandas as pd
import json
import os
import logging
from botocore.config import Config
from botocore.exceptions import BotoCoreError, NoCredentialsError
from dotenv import load_dotenv

# Configurar el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(f"/logs/{os.getenv('CONTAINER_NAME')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def create_boto3_session():
    """Crea una sesión de boto3 usando un rol IAM y una región específica."""
    try:
        # Variables de entorno necesarias
        role_arn = os.getenv('AWS_ROLE_ARN')
        region = os.getenv('AWS_REGION', 'us-east-1')
        
        # Si el ARN del rol está definido, asume el rol
        if role_arn:
            sts_client = boto3.client('sts', region_name=region)
            assumed_role = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName='DataIngestionSession'
            )
            credentials = assumed_role['Credentials']
            
            session = boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
                region_name=region
            )
        else:
            # Usar credenciales por defecto si no se especifica un rol
            session = boto3.Session(region_name=region)
        
        return session
    except (BotoCoreError, NoCredentialsError) as e:
        logger.error(f"Error al crear la sesión de boto3: {e}")
        raise

def scan_dynamodb_table(session, table_name):
    """Realiza un scan de una tabla DynamoDB con paginación."""
    dynamodb = session.client('dynamodb')
    paginator = dynamodb.get_paginator('scan')
    response_iterator = paginator.paginate(TableName=table_name)
    
    items = []
    for page in response_iterator:
        items.extend(page['Items'])
    
    return items

def save_to_s3(session, data, bucket_name, file_name):
    """Guarda los datos en un bucket S3."""
    s3 = session.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=data)

def create_glue_crawler(session, crawler_name, s3_target, role, database_name):
    """Crea un crawler de AWS Glue."""
    glue = session.client('glue')
    try:
        glue.create_crawler(
            Name=crawler_name,
            Role=role,
            DatabaseName=database_name,
            Targets={'S3Targets': [{'Path': s3_target}]},
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        logger.info(f"Crawler {crawler_name} creado exitosamente.")
    except glue.exceptions.AlreadyExistsException:
        logger.warning(f"Crawler {crawler_name} ya existe.")

def start_glue_crawler(session, crawler_name):
    """Inicia un crawler de AWS Glue."""
    glue = session.client('glue')
    glue.start_crawler(Name=crawler_name)
    logger.info(f"Crawler {crawler_name} iniciado.")

def main():
    # Variables de entorno para la configuración
    table_name = os.getenv('DYNAMODB_TABLE_5_DEV')
    bucket_name = os.getenv('S3_BUCKET_DEV')
    file_format = os.getenv('FILE_FORMAT', 'csv')
    role = os.getenv('AWS_ROLE_ARN')
    glue_database = f"glue_database_{table_name}_DEV"
    glue_crawler_name = f"crawler_{table_name}_DEV"
    
    if not table_name or not bucket_name:
        logger.error("Error: DYNAMODB_TABLE_5_DEV y S3_BUCKET_DEV son obligatorios.")
        return

    logger.info("Iniciando sesión de boto3...")
    session = create_boto3_session()
    
    logger.info(f"Escaneando la tabla DynamoDB: {table_name}...")
    items = scan_dynamodb_table(session, table_name)
    
    if file_format == 'csv':
        df = pd.DataFrame(items)
        data = df.to_csv(index=False)
        file_name = f'{table_name}.csv'
    else:
        data = json.dumps(items, indent=4)
        file_name = f'{table_name}.json'
    
    logger.info(f"Guardando datos en el bucket S3: {bucket_name}...")
    save_to_s3(session, data, bucket_name, file_name)
    
    logger.info(f"Ingesta de datos completada. Archivo subido a S3: {file_name}")
    
    # Crear y ejecutar el crawler de AWS Glue
    s3_target = f"s3://{bucket_name}/{file_name}"
    create_glue_crawler(session, glue_crawler_name, s3_target, role, glue_database)
    start_glue_crawler(session, glue_crawler_name)

if __name__ == "__main__":
    main()