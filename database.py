import os
import urllib.parse
from sqlalchemy import create_engine

def get_connection():
    try:
        # Traemos la URL completa directamente desde el archivo .env
        database_url = os.getenv('DATABASE_URL')
        
        # Opcional pero recomendado: SQLAlchemy a veces requiere que diga "postgresql://"
        if database_url and database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
            
        return create_engine(database_url)
    except Exception as e:
        print(f"Error BD: {e}")
        return None

# Instancia global para importar
engine = get_connection()