import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Cargar variables de entorno del archivo .env
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env'))

class Settings(BaseSettings):
    DATABASE_URL: str
    SECRET_KEY: str = "default_secret" # Un default por si acaso

    class Config:
        env_file = '.env' # Especifica que lea desde .env
        env_file_encoding = 'utf-8'

# Crear una instancia global de la configuración
settings = Settings()