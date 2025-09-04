import os
import json
import time
import io
import logging
import socket
import ssl
from dotenv import load_dotenv
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, ResponseStreamingError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

# --- Cargar variables desde archivo .env ---
load_dotenv()

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Variables de entorno ---
AWS_PROFILE = os.getenv("AWS_PROFILE")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = os.getenv("S3_FILE_KEY")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

# --- Configurar perfil AWS ---
if AWS_PROFILE:
    boto3.setup_default_session(profile_name=AWS_PROFILE)

# --- Clase de progreso (opcional) ---
class ProgressPercentage:
    def __init__(self, filename, filesize):
        self._filename = filename
        self._filesize = filesize
        self._seen_so_far = 0
        self._last_logged_time = time.time()

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        now = time.time()
        if now - self._last_logged_time >= 10:
            self._last_logged_time = now
            percent = (self._seen_so_far / self._filesize) * 100
            logging.info(f"📥 {self._filename} - {percent:.2f}% descargado")

# --- Descargar archivo desde S3 ---
def download_file_from_s3(bucket_name, s3_key, local_filename, max_retries=5):
    if os.path.exists(local_filename):
        logging.info(f"📦 Archivo ya existe localmente: {local_filename}, no se descargará de nuevo.")
        return True

    s3 = boto3.client('s3')
    config = TransferConfig(
        multipart_threshold=1024 * 1024 * 50,
        max_concurrency=10,
        multipart_chunksize=1024 * 1024 * 50,
        use_threads=True
    )

    try:
        metadata = s3.head_object(Bucket=bucket_name, Key=s3_key)
        filesize = metadata['ContentLength']
    except ClientError as e:
        logging.critical(f"❌ No se pudo obtener metadata del archivo: {e}")
        return False

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"📥 Intento {attempt}/{max_retries} de descarga desde S3...")
            with open(local_filename, "wb") as f:
                s3.download_fileobj(
                    bucket_name,
                    s3_key,
                    f,
                    Config=config,
                    Callback=ProgressPercentage(local_filename, filesize)
                )
            logging.info("✅ Archivo descargado correctamente.")
            return True
        except Exception as e:
            logging.warning(f"⚠️ Error durante la descarga: {e}")
            time.sleep(2 ** attempt)

    logging.critical("❌ Descarga fallida tras varios intentos.")
    return False

# --- Subida a Google Drive con reintentos mejorados ---
def upload_to_drive(service, filename, folder_id, max_retries=5):
    if not os.path.exists(filename):
        logging.critical(f"❌ El archivo '{filename}' no existe localmente.")
        return False

    file_metadata = {
        'name': os.path.basename(filename),
        'parents': [folder_id]
    }

    media = MediaFileUpload(
        filename,
        mimetype='application/gzip',
        chunksize=1024 * 1024 * 10,  # 10MB
        resumable=True
    )

    request = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id',
        supportsAllDrives=True
    )

    logging.info(f"📤 Iniciando subida de '{filename}' a Google Drive...")

    response = None
    for attempt in range(1, max_retries + 1):
        try:
            while response is None:
                status, response = request.next_chunk()
                if status:
                    logging.info(f"📤 Subiendo: {int(status.progress() * 100)}%")
            logging.info(f"✅ Archivo subido con ID: {response.get('id')}")
            return True
        except (HttpError, socket.timeout, ssl.SSLError, Exception) as e:
            logging.warning(f"⚠️ Error en intento {attempt}: {e}")
            sleep_time = 2 ** attempt
            logging.info(f"⏳ Esperando {sleep_time}s antes de reintentar...")
            time.sleep(sleep_time)

    logging.critical("❌ Subida fallida tras varios intentos.")
    return False

# --- Autenticación con Google Drive ---
def authenticate_gdrive():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

# --- Ejecución principal ---
def main():
    logging.info("--- Iniciando el proceso de S3 a Google Drive ---")
    logging.info(f"🔍 Archivo S3 configurado: {S3_FILE_KEY}")

    gdrive_service = authenticate_gdrive()
    logging.info("🔑 Autenticación con Google Drive completada.")

    local_filename = os.path.basename(S3_FILE_KEY)

    if not download_file_from_s3(S3_BUCKET_NAME, S3_FILE_KEY, local_filename):
        logging.critical("❌ No se pudo descargar el archivo desde S3.")
        return

    if not upload_to_drive(gdrive_service, local_filename, GDRIVE_FOLDER_ID):
        logging.critical("❌ No se pudo subir el archivo a Google Drive.")
        return

    logging.info("--- ✅ Proceso completado ---")

if __name__ == '__main__':
    main()