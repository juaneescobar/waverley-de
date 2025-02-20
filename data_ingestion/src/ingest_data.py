import os
import boto3
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging


# Cargar las variables del archivo .env
load_dotenv()
# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configurar PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "mydb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Configurar conexión a PostgreSQL
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Configurar AWS S3
S3_BUCKET = "my-bucket"

LOCAL_PATH = "/tmp"  # Directorio temporal

s3_client = boto3.client("s3")

def download_from_s3(filename):
    """ Descarga un archivo de S3 al directorio local """
    local_file = os.path.join(LOCAL_PATH, filename)
    try:
        s3_client.download_file(S3_BUCKET, filename, local_file)
        logging.info(f"Descargado: {filename}")
        return local_file
    except Exception as e:
        logging.error(f"Error descargando {filename}: {e}")
        return None

def ingest_data(file_path, table_name, force_schema):
    try:
        conn = engine.connect()
        df = pd.read_csv(file_path, on_bad_lines="skip", usecols=force_schema)
        df.to_sql(table_name, conn, schema="stg", if_exists="replace", index=False)
        logging.info(f"Datos insertados en {table_name} ({len(df)} filas)")
    except Exception as e:
        logging.error(f"Error insertando en {table_name}: {e}")

