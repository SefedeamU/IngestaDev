import time
import boto3
import pandas as pd
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

def wait_for_crawler(glue_client, crawler_name, retries=20, delay=60):
    """Espera a que el crawler de AWS Glue complete su ejecución."""
    for _ in range(retries):
        try:
            response = glue_client.get_crawler(Name=crawler_name)
            state = response['Crawler']['State']
            logger.info(f"Estado del crawler {crawler_name}: {state}")
            if state == 'READY':
                return True
        except glue_client.exceptions.EntityNotFoundException as e:
            logger.error(f"Error al obtener el estado del crawler {crawler_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error al obtener el estado del crawler {crawler_name}: {e}")
        time.sleep(delay)
    raise Exception(f"El crawler {crawler_name} no completó su ejecución después de varios intentos.")

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
            logger.info(f"Estado de la consulta en Athena: {status}")
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(5)
        
        if status == 'SUCCEEDED':
            result = athena.get_query_results(QueryExecutionId=query_execution_id)
            rows = result['ResultSet']['Rows']
            columns = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            logger.info(f"Esquema de la tabla en Athena: {columns}")  # Agregar log para verificar el esquema de la tabla
            data = [[col.get('VarCharValue', None) for col in row['Data']] for row in rows[1:]]
            df = pd.DataFrame(data, columns=columns)
            logger.info(f"DataFrame obtenido de Athena:\n{df.head()}")  # Agregar log para verificar el contenido del DataFrame
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
        columns = ', '.join([f'`{col}` TEXT' for col in df.columns])
        create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
        logger.info(f"Creando tabla con la consulta: {create_table_query}")
        cursor.execute(create_table_query)
        
        # Insertar los datos
        for _, row in df.iterrows():
            values = ', '.join([f"'{val}'" for val in row])
            insert_query = f'INSERT INTO `{table_name}` VALUES ({values})'
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
    ingest_services = [
        'ingest-service-1',
        'ingest-service-2',
        'ingest-service-3',
        'ingest-service-4',
        'ingest-service-5'
    ]
    
    glue_databases = [f"glue_database_{service}_dev-usuarios_dev" for service in ingest_services]
    glue_tables = [f"{service.replace('-', '_')}" for service in ingest_services]  # Derivar el nombre de la tabla de Glue
    
    # Esperar a que los catálogos de datos estén disponibles
    wait_for_catalogs(glue_client, glue_databases)

    for glue_database, glue_table in zip(glue_databases, glue_tables):
        # Esperar a que el crawler complete su ejecución
        crawler_name = f"crawler_{glue_table}_dev-usuarios_dev"
        if not wait_for_crawler(glue_client, crawler_name):
            continue
        
        query = f"SELECT * FROM {glue_table}"  # Usar el nombre de la tabla derivado del archivo CSV
        logger.info(f"Ejecutando consulta en Athena para la base de datos: {glue_database}...")
        try:
            df = query_athena(session, query, glue_database, output_location)
            table_name = f"summary_table_{glue_table}"  # Generar un nombre de tabla único
            logger.info(f"Guardando resultados en MySQL, tabla: {table_name}...")
            save_to_mysql(df, table_name)
        except Exception as e:
            logger.error(f"Error al procesar la base de datos {glue_database}: {e}")

if __name__ == "__main__":
    main()