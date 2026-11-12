import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. Cargar las variables del archivo .env al entorno
load_dotenv()

# 2. Definir la URL globalmente
DATABASE_URL = os.getenv('DATABASE_URL')

# 3. SQLAlchemy requiere "postgresql://" en lugar de "postgres://"
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# 4. Instancia global lista para importar en otros archivos (ej: from database import engine)
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True, 
    pool_recycle=1800 
)

def get_connection():
    """Retorna el motor global, manejando posibles errores básicos."""
    try:
        if not DATABASE_URL:
            raise ValueError("No se encontró DATABASE_URL en el archivo .env")
        return engine
    except Exception as e:
        print(f"Error BD: {e}")
        return None