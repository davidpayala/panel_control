import psutil
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Cargar variables de entorno
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# Conectar a la base de datos
engine = create_engine(os.getenv("DATABASE_URL"))

def registrar_pulso():
    try:
        with engine.begin() as conn:
            # 1. Asegurar que la tabla existe
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS Servidor_Metricas (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT (NOW() - INTERVAL '5 hours'),
                    cpu_pct NUMERIC(5,2),
                    ram_pct NUMERIC(5,2)
                )
            """))
            
            # 2. Leer sensores (El intervalo=1 hace que calcule el uso real del último segundo)
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory().percent
            
            # 3. Guardar en base de datos
            conn.execute(text("""
                INSERT INTO Servidor_Metricas (cpu_pct, ram_pct) 
                VALUES (:c, :r)
            """), {"c": cpu, "r": ram})
            
            # 4. Limpieza automática: Borrar registros de hace más de 7 días para no llenar el disco
            conn.execute(text("DELETE FROM Servidor_Metricas WHERE fecha < NOW() - INTERVAL '7 days'"))
            
    except Exception as e:
        print(f"Error al registrar recursos: {e}")

if __name__ == "__main__":
    registrar_pulso()