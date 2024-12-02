import boto3
import pandas as pd
import mysql.connector
import os
import logging
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener el nombre del contenedor desde las variables de entorno
container_name = os.getenv('CONTAINER_NAME', 'etl')

# Configurar el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(f"/logs/{container_name}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def create_boto3_session():
    """Crea una sesión de boto3 usando las credenciales especificadas en el archivo de configuración."""
    try:
        # Crear la sesión de boto3 usando las credenciales especificadas en el archivo de configuración
        session = boto3.Session(region_name=os.getenv('AWS_REGION', 'us-east-1'))
        return session
    except (BotoCoreError, NoCredentialsError) as e:
        logger.error(f"Error al crear la sesión de boto3: {e}")
        raise

def query_athena(session, query, database, output_location):
    """Ejecuta una consulta en Athena y devuelve los resultados como un DataFrame."""
    athena = session.client('athena')
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    query_execution_id = response['QueryExecutionId']
    
    # Esperar a que la consulta se complete
    status = 'RUNNING'
    while status in ['RUNNING', 'QUEUED']:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
    
    # Descargar los resultados
    result = athena.get_query_results(QueryExecutionId=query_execution_id)
    rows = result['ResultSet']['Rows']
    columns = [col['VarCharValue'] for col in rows[0]['Data']]
    data = [[col.get('VarCharValue', None) for col in row['Data']] for row in rows[1:]]
    
    df = pd.DataFrame(data, columns=columns)
    return df

def save_to_mysql(df, table_name):
    """Guarda un DataFrame en una tabla MySQL."""
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
    s3_bucket = os.getenv('S3_BUCKET_DEV')
    output_location = f"s3://{s3_bucket}/"
    
    # Construir la lista de bases de datos Glue utilizando las variables de entorno
    dynamodb_tables = [
        os.getenv('DYNAMODB_TABLE_1_DEV'),
        os.getenv('DYNAMODB_TABLE_2_DEV'),
        os.getenv('DYNAMODB_TABLE_3_DEV'),
        os.getenv('DYNAMODB_TABLE_4_DEV'),
        os.getenv('DYNAMODB_TABLE_5_DEV')
    ]
    
    glue_databases = [f"glue_database_{table}_DEV" for table in dynamodb_tables]
    
    for glue_database in glue_databases:
        query = "SELECT * FROM your_table"  # Reemplaza con tu consulta específica
        logger.info(f"Ejecutando consulta en Athena para la base de datos: {glue_database}...")
        df = query_athena(session, query, glue_database, output_location)
        table_name = f"summary_table_{glue_database.split('_')[2]}"  # Generar un nombre de tabla único
        logger.info(f"Guardando resultados en MySQL, tabla: {table_name}...")
        save_to_mysql(df, table_name)

if __name__ == "__main__":
    main()