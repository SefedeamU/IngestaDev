import boto3
import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def create_boto3_session():
    """Crea una sesión de boto3 usando un rol IAM y una región específica."""
    role_arn = os.getenv('AWS_ROLE_ARN')
    region = os.getenv('AWS_REGION', 'us-east-1')
    
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
    
    return session

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
    conn = mysql.connector.connect(
        host='mysql',
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE')
    )
    cursor = conn.cursor()
    
    # Crear la tabla si no existe
    columns = ', '.join([f'{col} TEXT' for col in df.columns])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({columns})')
    
    # Insertar los datos
    for _, row in df.iterrows():
        values = ', '.join([f"'{val}'" for val in row])
        cursor.execute(f'INSERT INTO {table_name} VALUES ({values})')
    
    conn.commit()
    cursor.close()
    conn.close()

def main():
    session = create_boto3_session()
    output_location = "s3://your-output-bucket/"  # Reemplaza con tu bucket de salida
    
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
        df = query_athena(session, query, glue_database, output_location)
        table_name = f"summary_table_{glue_database.split('_')[2]}"  # Generar un nombre de tabla único
        save_to_mysql(df, table_name)

if __name__ == "__main__":
    main()