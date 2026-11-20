"""
respaldo_semanal.py — Respaldo semanal del ERP hacia el Synology (NAS local).

Qué hace (domingos 4:00 am, desde cron):
  1. Saca un pg_dump (formato custom, comprimido) de la base de datos y verifica que se pueda leer.
  2. Empaqueta las sesiones de WhatsApp de los contenedores WAHA encendidos (waha-gows; antes waha y waha-estados) SIN los cachés de Chromium.
  3. Sube todo al Synology por SSH (llave dedicada, sin contraseñas).
  4. En el NAS conserva las últimas 8 copias de la base y las últimas 2 de WhatsApp.
  5. En el servidor deja solo la copia más reciente de la base (para restaurar rápido).
  6. Registra todo en la tabla logs_respaldo (Diagnóstico → Logs Respaldo).

Seguridad:
  - La contraseña de la base se lee de DATABASE_URL (.env) y se pasa a pg_dump por variable de
    entorno del proceso: no queda en archivos, en el historial ni en la lista de procesos.
  - La conexión al NAS usa ~/.ssh/synology_respaldo (alias «synology-respaldo» en ~/.ssh/config).
  - Los archivos de respaldo se crean con permisos 600 en una carpeta 700.

Parámetros opcionales en .env (todos tienen valor por defecto):
    NAS_SSH=synology-respaldo                         alias SSH del NAS
    NAS_RUTA=/volume1/Neko/Panel-Control/Respaldo     carpeta destino en el NAS
    RESPALDO_LOCAL=~/respaldos                        carpeta temporal en el servidor
    RESPALDO_SEMANAS_BD=8                             copias de la base a conservar en el NAS
    RESPALDO_SEMANAS_WSP=2                            copias de WhatsApp a conservar en el NAS

Uso:
    python recurrente/respaldo_semanal.py            # respaldo completo
    python recurrente/respaldo_semanal.py --sin-wsp  # solo la base de datos
    python recurrente/respaldo_semanal.py --prueba   # solo verifica conexión con el NAS
"""
import hashlib
import os
import shlex
import tarfile
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402

from database import engine, DATABASE_URL  # noqa: E402  (database.py ya carga tu .env)

NAS_SSH = os.getenv("NAS_SSH", "synology-respaldo")
NAS_RUTA = os.getenv("NAS_RUTA", "/volume1/Neko/Panel-Control/Respaldo").rstrip("/")
LOCAL = Path(os.path.expanduser(os.getenv("RESPALDO_LOCAL", "~/respaldos")))
SEMANAS_BD = int(os.getenv("RESPALDO_SEMANAS_BD", "8"))
SEMANAS_WSP = int(os.getenv("RESPALDO_SEMANAS_WSP", "2"))

# Contenedores WAHA posibles; se respaldan solo los que están encendidos (docker exec no funciona en uno apagado).
# waha-gows es el motor en uso desde el 20/11/2026; waha (WEBJS) y waha-estados (NOWEB) quedaron apagados.
CONTENEDORES_WSP = ["waha-gows", "waha", "waha-estados"]
# Cachés de Chromium: se regeneran solos y pesan GB. La sesión vinculada está en IndexedDB/Local Storage.
EXCLUIR_WSP = ["Cache", "Code Cache", "GPUCache", "GrShaderCache", "ShaderCache", "DawnGraphiteCache",
               "DawnWebGPUCache", "GPUPersistentCache", "CacheStorage", "component_crx_cache",
               "extensions_crx_cache", "Safe Browsing", "OptimizationGuideModelsManifest",
               "segmentation_platform", "BrowserMetrics*", "Singleton*"]

LOG_EN_BD = True


def log(msg):
    """Imprime y guarda el mensaje en logs_respaldo (Diagnóstico → Logs Respaldo)."""
    texto = str(msg)
    print(texto, flush=True)
    if not LOG_EN_BD or not texto.strip():
        return
    try:
        with engine.begin() as c:
            c.execute(text("CREATE TABLE IF NOT EXISTS logs_respaldo ("
                           "id SERIAL PRIMARY KEY, fecha TIMESTAMP DEFAULT NOW(), mensaje TEXT)"))
            c.execute(text("INSERT INTO logs_respaldo (fecha, mensaje) VALUES (NOW(), :m)"), {"m": texto})
    except Exception as e:
        print(f"(no se pudo guardar el log en la base: {e})", flush=True)


def correr(cmd, env=None, stdout=None, stdin=None):
    """Ejecuta un comando (lista, sin shell) con prioridad baja; lanza error con el stderr si falla."""
    r = subprocess.run(["nice", "-n", "10", "ionice", "-c3", *cmd], env=env, stdout=stdout or subprocess.PIPE,
                       stdin=stdin, stderr=subprocess.PIPE, text=stdout is None)
    if r.returncode != 0:
        err = r.stderr if isinstance(r.stderr, str) else r.stderr.decode(errors="replace")
        raise RuntimeError(f"{cmd[0]} terminó con código {r.returncode}: {err.strip()[-800:]}")
    return r.stdout


def ssh_nas(comando_remoto):
    return correr(["ssh", NAS_SSH, comando_remoto])


def tam(p):
    b = p.stat().st_size
    for u in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"


def nuevo_archivo(ruta):
    """Crea el archivo vacío con permisos 600 antes de escribir datos en él."""
    fd = os.open(ruta, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.close(fd)


# ==============================================================================
def respaldar_bd(sello):
    url = make_url(DATABASE_URL)
    env = {**os.environ, "PGPASSWORD": url.password or ""}
    destino = LOCAL / f"erp_{sello}.dump"
    parcial = destino.with_suffix(".dump.parcial")
    nuevo_archivo(parcial)
    correr(["pg_dump", "-h", url.host or "localhost", "-p", str(url.port or 5432), "-U", url.username,
            "-Fc", "-Z", "6", "-f", str(parcial), url.database], env=env)
    if parcial.stat().st_size == 0:
        raise RuntimeError("pg_dump generó un archivo vacío")
    # Verificación: si pg_restore puede leer el índice, el archivo está completo
    n = len(correr(["pg_restore", "--list", str(parcial)]).splitlines())
    parcial.rename(destino)
    log(f"🗄️ Base de datos respaldada: {destino.name} ({tam(destino)}, {n} objetos verificados)")
    return destino


def respaldar_wsp(sello):
    """Un .tar por contenedor (generado dentro del contenedor, así no hay problemas de permisos de root),
    escrito directo a disco y luego agrupado en un solo .tar.gz. No carga nada grande en RAM."""
    destino = LOCAL / f"whatsapp_{sello}.tar.gz"
    parcial = Path(str(destino) + ".parcial")
    excl = " ".join(f"--exclude={shlex.quote(e)}" for e in EXCLUIR_WSP)
    nuevo_archivo(parcial)
    with tarfile.open(parcial, mode="w:gz", compresslevel=6) as paquete:
        encendidos = set(subprocess.run(["docker", "ps", "--format", "{{.Names}}"],
                                        capture_output=True, text=True, timeout=30).stdout.split())
        for cont in [c for c in CONTENEDORES_WSP if c in encendidos]:
            tmp = LOCAL / f"{cont}.tar.parcial"
            nuevo_archivo(tmp)
            try:
                with open(tmp, "wb") as f:
                    correr(["docker", "exec", cont, "sh", "-c",
                            f"tar -C /app/.sessions -cf - {excl} --warning=no-file-changed --ignore-failed-read . ;"
                            " r=$?; [ $r -le 1 ] && exit 0 || exit $r"], stdout=f)
                paquete.add(tmp, arcname=f"{cont}.tar")
                log(f"💬 Sesiones de {cont} empaquetadas ({tam(tmp)} sin comprimir)")
            except Exception as e:
                log(f"⚠️ No se pudo empaquetar {cont}: {e}")
            finally:
                tmp.unlink(missing_ok=True)
    parcial.rename(destino)
    log(f"💬 WhatsApp respaldado: {destino.name} ({tam(destino)})")
    return destino


def sha256_local(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(bloque)
    return h.hexdigest()


def subir(archivos):
    """Copia por SSH puro (el rsync de Synology exige activar su servicio rsync aparte).
    Se escribe como .parcial, se verifica el SHA-256 en el NAS y recién entonces se renombra."""
    ssh_nas(f"mkdir -p {shlex.quote(NAS_RUTA)}")
    for a in archivos:
        final = shlex.quote(f"{NAS_RUTA}/{a.name}")
        parcial = shlex.quote(f"{NAS_RUTA}/{a.name}.parcial")
        with open(a, "rb") as f:
            correr(["ssh", NAS_SSH, f"umask 077 && cat > {parcial}"], stdin=f)
        remoto = ssh_nas(f"sha256sum {parcial}").split()[0]
        if remoto != sha256_local(a):
            ssh_nas(f"rm -f {parcial}")
            raise RuntimeError(f"{a.name}: la copia en el NAS no coincide (SHA-256 distinto); se descartó")
        ssh_nas(f"mv -f {parcial} {final}")
        log(f"☁️ Subido y verificado (SHA-256) en el Synology: {NAS_RUTA}/{a.name} ({tam(a)})")


def rotar_nas(prefijo, conservar):
    """Deja en el NAS solo las `conservar` copias más recientes (los nombres llevan fecha y ordenan bien)."""
    lista = ssh_nas(f"cd {shlex.quote(NAS_RUTA)} && ls -1 | grep -E '^{prefijo}_[0-9]{{4}}-' || true").split()
    viejos = sorted(lista)[:-conservar] if len(lista) > conservar else []
    for v in viejos:
        ssh_nas(f"rm -f -- {shlex.quote(NAS_RUTA + '/' + v)}")
    if viejos:
        log(f"🧹 NAS: eliminadas {len(viejos)} copias antiguas de «{prefijo}» (se conservan {conservar})")


def limpiar_local(ultimo_dump):
    """En el servidor solo se queda el último dump de la base; lo demás ya está en el NAS."""
    borrados = 0
    for f in LOCAL.iterdir():
        if f.is_file() and f != ultimo_dump and (f.name.startswith(("erp_", "whatsapp_"))):
            f.unlink()
            borrados += 1
    if borrados:
        log(f"🧹 Servidor: eliminados {borrados} archivos de respaldo locales antiguos")


# ==============================================================================
def main():
    global LOG_EN_BD
    args = set(sys.argv[1:])
    if "--prueba" in args:
        LOG_EN_BD = False
        print(ssh_nas(f"mkdir -p {shlex.quote(NAS_RUTA)} && cd {shlex.quote(NAS_RUTA)} && pwd && df -h . | tail -1"))
        print("✅ Conexión con el Synology OK")
        return

    os.umask(0o077)
    LOCAL.mkdir(parents=True, exist_ok=True)
    LOCAL.chmod(0o700)
    sello = datetime.now().strftime("%Y-%m-%d_%H%M")
    inicio = time.time()
    log(f"💾 Respaldo semanal iniciado ({sello})")

    try:
        dump = respaldar_bd(sello)
        archivos = [dump]
        if "--sin-wsp" not in args:
            try:
                archivos.append(respaldar_wsp(sello))
            except Exception as e:
                log(f"⚠️ Falló el respaldo de WhatsApp (la base sí se respaldará): {e}")
        subir(archivos)
        rotar_nas("erp", SEMANAS_BD)
        rotar_nas("whatsapp", SEMANAS_WSP)
        limpiar_local(dump)
        log(f"✅ Respaldo semanal completo en {int(time.time() - inicio)} s")
    except Exception:
        log("❌ El respaldo semanal falló:\n" + traceback.format_exc()[-1500:])
        # Borrar archivos a medio hacer
        for f in LOCAL.glob("*.parcial"):
            f.unlink(missing_ok=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
