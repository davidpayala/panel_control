import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. Cargar variables del archivo .env con ruta absoluta segura
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# 2. Definir la URL globalmente
DATABASE_URL = os.getenv('DATABASE_URL')

if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# 3. Instancia global blindada contra caídas de RAM y modo de recuperación
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True, 
    pool_recycle=1800,
    pool_size=5,          # Limita conexiones simultáneas para ahorrar RAM
    max_overflow=10,
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        # Limita la RAM por operación a 16MB y cancela consultas que tarden más de 15 seg
        "options": "-c work_mem=16MB -c statement_timeout=15000"
    }
)

def get_connection():
    try:
        if not DATABASE_URL:
            raise ValueError("No se encontró DATABASE_URL en el archivo .env")
        return engine
    except Exception as e:
        print(f"Error BD: {e}")
        return None