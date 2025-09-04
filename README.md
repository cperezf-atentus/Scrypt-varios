# S3 to Google Drive Backup

Este proyecto permite descargar un archivo desde un bucket S3 en AWS y subirlo automáticamente a una carpeta de Google Drive usando una cuenta de servicio.

## Archivos importantes

- `s3_to_gdrive_v2.py`: Script principal en Python.
- `.env`: Contiene credenciales y configuraciones (NO se sube a GitHub).
- `credenciales_google.json`: Credenciales de Google Service Account (NO se sube a GitHub).
- `example.env`: Ejemplo de configuración de variables de entorno.

## Uso

1. Crear un archivo `.env` basado en `example.env` con tus credenciales reales.
2. Colocar tu archivo `credenciales_google.json` en la carpeta raíz del proyecto.
3. Instalar dependencias necesarias:
   ```bash
   pip install -r requirements.txt
   ```
4. Ejecutar el script:
   ```bash
   python s3_to_gdrive_v2.py
   ```

## Seguridad

⚠️ **Nunca subas tus credenciales reales (`.env` y `credenciales_google.json`) a GitHub.**
