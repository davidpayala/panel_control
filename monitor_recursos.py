import os
import subprocess

import psutil
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cargar variables de entorno
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# Conectar a la base de datos (pool mínimo: este script corre cada minuto y termina)
engine = create_engine(os.getenv("DATABASE_URL"), pool_size=1, max_overflow=0)

CGROUP = "/sys/fs/cgroup/system.slice"
# Columna en la tabla -> unidad de systemd (cgroup)
SERVICIOS = {
    "panel_mb": "panel.service",
    "webhook_mb": "webhook.service",
    "ollama_mb": "ollama.service",
    "postgres_mb": "system-postgresql.slice",
}
# Columna en la tabla -> nombre del contenedor Docker
CONTENEDORES = {"waha_mb": "waha", "waha_estados_mb": "waha-estados", "waha_gows_mb": "waha-gows"}


def ram_cgroup_mb(ruta):
    """RAM real (memoria anónima, sin contar caché de disco) de un cgroup, en MB."""
    try:
        with open(os.path.join(ruta, "memory.stat")) as f:
            for linea in f:
                if linea.startswith("anon "):
                    return round(int(linea.split()[1]) / 1024 / 1024)
    except OSError:
        pass
    return None


def ram_por_componente():
    datos = {col: ram_cgroup_mb(os.path.join(CGROUP, unidad)) for col, unidad in SERVICIOS.items()}
    try:
        salida = subprocess.run(["docker", "ps", "--no-trunc", "--format", "{{.ID}} {{.Names}}"],
                                capture_output=True, text=True, timeout=10).stdout
        ids = dict(reversed(l.split(maxsplit=1)) for l in salida.splitlines() if " " in l)
    except Exception:
        ids = {}
    for col, nombre in CONTENEDORES.items():
        cid = ids.get(nombre)
        datos[col] = ram_cgroup_mb(os.path.join(CGROUP, f"docker-{cid}.scope")) if cid else None
    return datos


def registrar_pulso():
    try:
        with engine.begin() as conn:
            # 1. Asegurar que la tabla y las columnas por componente existen
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS Servidor_Metricas (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT (NOW() - INTERVAL '5 hours'),
                    cpu_pct NUMERIC(5,2),
                    ram_pct NUMERIC(5,2)
                )
            """))
            columnas = list(SERVICIOS) + list(CONTENEDORES)
            existentes = {r[0] for r in conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'servidor_metricas'"))}
            for col in columnas:
                if col not in existentes:
                    conn.execute(text(f"ALTER TABLE Servidor_Metricas ADD COLUMN IF NOT EXISTS {col} INTEGER"))

            # 2. Leer sensores (El intervalo=1 hace que calcule el uso real del último segundo)
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory().percent
            detalle = ram_por_componente()

            # 3. Guardar en base de datos
            conn.execute(text(f"""
                INSERT INTO Servidor_Metricas (cpu_pct, ram_pct, {", ".join(columnas)})
                VALUES (:c, :r, {", ".join(":" + c for c in columnas)})
            """), {"c": cpu, "r": ram, **detalle})

            # 4. Limpieza automática: Borrar registros de hace más de 7 días para no llenar el disco
            conn.execute(text("DELETE FROM Servidor_Metricas WHERE fecha < NOW() - INTERVAL '7 days'"))

    except Exception as e:
        print(f"Error al registrar recursos: {e}")


if __name__ == "__main__":
    registrar_pulso()
