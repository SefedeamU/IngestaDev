import time
import boto3
import pandas as pd  # Asegúrate de importar pandas
from botocore.exceptions import BotoCoreError, NoCredentialsError
from dotenv import load_dotenv
import logging
import os
import mysql.connector

# Configurar el logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def create_boto3_session():
    """Crea una sesión de boto3 usando las credenciales especificadas en el archivo de configuración."""
    try:
        session = boto3.Session(region_name=os.getenv('AWS_REGION', 'us-east-1'))
        return session
    except (BotoCoreError, NoCredentialsError) as e:
        logger.error(f"Error al crear la sesión de boto3: {e}")
        raise

def wait_for_catalogs(glue_client, databases, retries=5, delay=10):
    """Espera a que los catálogos de datos estén disponibles en AWS Glue."""
    for _ in range(retries):
        all_available = True
        for database in databases:
            try:
                response = glue_client.get_database(Name=database)
                logger.info(f"Catálogo de datos {database} está disponible.")
            except glue_client.exceptions.EntityNotFoundException:
                logger.info(f"Esperando a que el catálogo de datos {database} esté disponible...")
                all_available = False
                break
        if all_available:
            return True
        time.sleep(delay)
    raise Exception("Los catálogos de datos no están disponibles después de varios intentos.")

def query_athena(session, query, database, output_location):
    athena = session.client('athena')
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        query_execution_id = response['QueryExecutionId']
        
        # Esperar a que la consulta se complete
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(5)
        
        if status == 'SUCCEEDED':
            result = athena.get_query_results(QueryExecutionId=query_execution_id)
            rows = result['ResultSet']['Rows']
            columns = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            data = [[col.get('VarCharValue', None) for col in row['Data']] for row in rows[1:]]
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown reason')
            logger.error(f"Query failed with status: {status}, reason: {reason}")
            raise Exception(f"Query failed with status: {status}, reason: {reason}")
    except Exception as e:
        logger.error(f"Error al ejecutar la consulta en Athena: {e}")
        raise

def save_to_mysql(df, table_name):
    try:
        conn = mysql.connector.connect(
            host='mysql',
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )
        cursor = conn.cursor()
        
        # Crear la tabla si no existe
        columns = ', '.join([f'{col} TEXT' for col in df.columns])
        create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns})'
        logger.info(f"Creando tabla con la consulta: {create_table_query}")
        cursor.execute(create_table_query)
        
        # Insertar los datos
        for _, row in df.iterrows():
            values = ', '.join([f"'{val}'" for val in row])
            insert_query = f'INSERT INTO {table_name} VALUES ({values})'
            logger.info(f"Insertando datos con la consulta: {insert_query}")
            cursor.execute(insert_query)
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Datos guardados en MySQL, tabla: {table_name}.")
    except mysql.connector.Error as err:
        logger.error(f"Error al guardar datos en MySQL: {err}")

def main():
    logger.info("Iniciando sesión de boto3...")
    session = create_boto3_session()
    glue_client = session.client('glue')
    s3_bucket = os.getenv('S3_BUCKET_DEV')
    output_location = f"s3://{s3_bucket}/athena-results/"
    
    # Construir la lista de bases de datos Glue utilizando las variables de entorno
    dynamodb_tables = [
        os.getenv('DYNAMODB_TABLE_1_DEV'),
        os.getenv('DYNAMODB_TABLE_2_DEV'),
        os.getenv('DYNAMODB_TABLE_3_DEV'),
        os.getenv('DYNAMODB_TABLE_4_DEV'),
        os.getenv('DYNAMODB_TABLE_5_DEV')
    ]
    
    glue_databases = [f"glue_database_{table}_DEV" for table in dynamodb_tables]
    glue_tables = [f"{table.replace('-', '_')}_csv" for table in dynamodb_tables]  # Derivar el nombre de la tabla de Glue
    
    # Esperar a que los catálogos de datos estén disponibles
    wait_for_catalogs(glue_client, glue_databases)

    for glue_database, glue_table in zip(glue_databases, glue_tables):
        query = f"SELECT * FROM {glue_table}"  # Usar el nombre de la tabla derivado del archivo CSV
        logger.info(f"Ejecutando consulta en Athena para la base de datos: {glue_database}...")
        try:
            df = query_athena(session, query, glue_database, output_location)
            table_name = f"summary_table_{glue_database.split('_')[2]}"  # Generar un nombre de tabla único
            logger.info(f"Guardando resultados en MySQL, tabla: {table_name}...")
            save_to_mysql(df, table_name)
        except Exception as e:
            logger.error(f"Error al procesar la base de datos {glue_database}: {e}")

if __name__ == "__main__":
    main()