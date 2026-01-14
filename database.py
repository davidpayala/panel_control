import os
import urllib.parse
from sqlalchemy import create_engine

def get_connection():
    try:
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASS')
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        dbname = os.getenv('DB_NAME')
        # Si usas Railway a veces la URL viene completa en DATABASE_URL
        # Si usas variables separadas:
        password_encoded = urllib.parse.quote_plus(password)
        return create_engine(f'postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{dbname}')
    except Exception as e:
        print(f"Error BD: {e}")
        return None

# Instancia global para importar
engine = get_connection()