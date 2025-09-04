import hashlib
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Parámetros
LOCAL_FILE = "C:\\Users\\Cristian Perez\\Documents\\python bueno\\python-3.13.5-amd64.exe"  # Ajusta a tu ruta
GDRIVE_FILE_ID = "1dUADhMmPQO7kft1CdOi0TpL_btUr1V8e"
GOOGLE_CREDENTIALS_FILE = "credenciales_google.json"

# Función para calcular MD5 local
def calcular_md5(filepath, buffer_size=8192):
    md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        while chunk := f.read(buffer_size):
            md5.update(chunk)
    return md5.hexdigest()

print("🔍 Calculando MD5 local...")
md5_local = calcular_md5(LOCAL_FILE)
print(f"MD5 local: {md5_local}")

# Conexión a Google Drive con cuenta de servicio
creds = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
service = build('drive', 'v3', credentials=creds)

print("🔍 Consultando MD5 en Google Drive...")
file = service.files().get(
    fileId=GDRIVE_FILE_ID,
    fields="name, md5Checksum",
    supportsAllDrives=True  # <-- IMPORTANTE para Shared Drives
).execute()

md5_drive = file.get('md5Checksum')
print(f"MD5 Google Drive: {md5_drive}")

print("\n📊 Resultado de la verificación:")
if md5_local == md5_drive:
    print("✅ Integridad verificada: Los hashes coinciden.")
else:
    print("❌ Integridad comprometida: Los hashes NO coinciden.")