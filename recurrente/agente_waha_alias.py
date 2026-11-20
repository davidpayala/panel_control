"""
agente_waha_alias.py — Vigila si WAHA ya permite LEER el nuevo «usuario» de WhatsApp (el alias que empieza con @).

Contexto: WhatsApp lanzó los nombres de usuario (@usuario) para quienes ocultan su número. En nuestra base se
guardarán en telefonoscliente.alias, pero WAHA aún no los entrega, así que el webhook no llena ese campo.

Qué revisa (cada 2 semanas, por cron, días 1 y 15):
  1. PRUEBA REAL en tu WAHA instalado: pide a cada motor en uso (NOWEB / WEBJS) los datos de varios LIDs reales
     (/api/{sesion}/lids/{lid} y /api/contacts) y busca campos tipo username/alias/handle o valores que empiecen con @.
     → Es la prueba definitiva: si aparece, ya se puede implementar en el webhook.
  2. NOVEDADES de WAHA: notas de las versiones publicadas en GitHub (devlikeapro/waha) y la página de changelog,
     buscando menciones a usernames. → Avisa que ya existe aunque tu WAHA todavía no esté actualizado.
  3. Compara tu versión instalada con la última publicada.

NO modifica clientes ni mensajes. Solo guarda el resultado en la tabla agente_waha_revisiones y en logs_agente_waha,
que se ven en el panel: Diagnóstico → 🔎 Agente WAHA (@alias).

Uso:
    python recurrente/agente_waha_alias.py            # revisión completa (guarda resultado)
    python recurrente/agente_waha_alias.py --prueba   # solo muestra, no guarda nada
    python recurrente/agente_waha_alias.py --estado   # JSON: última revisión automática + último análisis de Claude
    python recurrente/agente_waha_alias.py --guardar-analisis informe.json
        Guarda el análisis que hace la tarea programada de Claude (cada 2 semanas). El JSON lleva:
        {"version_revisada": "2026.11.1", "alias_disponible": false, "nivel": "ok|mejora|riesgo",
         "resumen": "una línea", "informe": "texto en markdown"}
"""
import html
import json
import os
import re
import sys
from datetime import datetime

RAIZ_PROYECTO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RAIZ_PROYECTO not in sys.path:
    sys.path.insert(0, RAIZ_PROYECTO)

import requests
from sqlalchemy import text

from database import engine  # database.py ya carga el .env

REPO_GITHUB = "devlikeapro/waha"
URL_CHANGELOG = "https://waha.devlike.pro/docs/overview/changelog/"
TIMEOUT = 15

# Palabras que indicarían soporte del @usuario. Se ignoran líneas sobre el login del panel/Swagger de WAHA,
# que también usan la palabra «username» (WHATSAPP_SWAGGER_USERNAME, WAHA_DASHBOARD_USERNAME...).
PATRON_MENCION = re.compile(r"(?i)(\busernames?\b|\buser[\s_-]?name\b|@username|\busername[\s_-]?handle\b)")
PATRON_IGNORAR = re.compile(r"(?i)(swagger|dashboard|basic[\s_-]?auth|password|_USERNAME|credential|login)")
CLAVES_CANDIDATAS = re.compile(r"(?i)(user.?name|alias|handle)")

ES_PRUEBA = "--prueba" in sys.argv


def log(msg):
    print(msg, flush=True)
    if ES_PRUEBA:
        return
    try:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO logs_agente_waha (fecha, mensaje) VALUES (NOW(), :m)"), {"m": str(msg)})
    except Exception:
        pass


def asegurar_tablas():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS logs_agente_waha (
                id SERIAL PRIMARY KEY, fecha TIMESTAMP DEFAULT NOW(), mensaje TEXT)
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS agente_waha_revisiones (
                id SERIAL PRIMARY KEY,
                fecha TIMESTAMP DEFAULT NOW(),
                version_local TEXT,
                ultima_version TEXT,
                soportado_local BOOLEAN,      -- TRUE = tu WAHA ya devuelve el @usuario
                mencionado_upstream BOOLEAN,  -- TRUE = las novedades de WAHA lo mencionan
                resumen TEXT,
                detalle TEXT                  -- JSON con la evidencia
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS agente_waha_analisis (
                id SERIAL PRIMARY KEY,
                fecha TIMESTAMP DEFAULT NOW(),
                version_revisada TEXT,        -- última versión de WAHA incluida en el análisis
                alias_disponible BOOLEAN,     -- ¿WAHA ya permite leer el @usuario?
                nivel TEXT,                   -- ok | mejora | riesgo
                resumen TEXT,
                informe TEXT                  -- markdown escrito por Claude
            )
        """))


# ------------------------------------------------------------------------------
# 1. Prueba real contra el WAHA instalado
# ------------------------------------------------------------------------------
def _buscar_evidencia(obj, ruta=""):
    """Devuelve [(ruta, valor)] de claves tipo username/alias o strings que parecen @usuario."""
    hallazgos = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            r = f"{ruta}.{k}" if ruta else str(k)
            if CLAVES_CANDIDATAS.search(str(k)) and v not in (None, "", [], {}):
                hallazgos.append((r, v))
            else:
                hallazgos.extend(_buscar_evidencia(v, r))
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:20]):
            hallazgos.extend(_buscar_evidencia(v, f"{ruta}[{i}]"))
    elif isinstance(obj, str):
        v = obj.strip()
        if re.fullmatch(r"@[A-Za-z0-9._]{3,40}", v):  # @usuario (no @lid ni @c.us, que llevan dígitos antes)
            hallazgos.append((ruta, v))
    return hallazgos


def revisar_waha_local():
    from waha_motor import MOTORES, url_motor, headers_waha
    sesion_de = {"WEBJS": "default", "NOWEB": "principal", "GOWS": "principal"}

    with engine.connect() as conn:
        lids = [r[0] for r in conn.execute(text("""
            SELECT lid FROM telefonoscliente
            WHERE lid LIKE '%@lid' AND activo = TRUE
            ORDER BY id_telefono DESC LIMIT 5
        """)).fetchall()]

    resultado = {"versiones": {}, "claves_vistas": {}, "evidencia": [], "errores": []}
    try:
        cabeceras = headers_waha()
    except Exception:
        cabeceras = {"X-Api-Key": os.getenv("WAHA_KEY", "")}

    # Se prueban TODOS los motores que respondan (aunque una acción no lo use): cualquiera podría soportarlo primero.
    for motor in MOTORES:
        base, sesion = url_motor(motor), sesion_de.get(motor, "default")
        try:
            v = requests.get(f"{base}/api/version", headers=cabeceras, timeout=TIMEOUT).json()
            resultado["versiones"][motor] = v.get("version")
        except Exception as e:
            resultado["errores"].append(f"{motor} /api/version: {e}")
            continue

        for lid in lids:
            lid_url = lid.replace("@", "%40")
            for nombre, url, params in (
                ("lids", f"{base}/api/{sesion}/lids/{lid_url}", None),
                ("contacts", f"{base}/api/contacts", {"contactId": lid, "session": sesion}),
            ):
                try:
                    r = requests.get(url, params=params, headers=cabeceras, timeout=TIMEOUT)
                    if r.status_code != 200 or "json" not in r.headers.get("Content-Type", ""):
                        continue
                    datos = r.json()
                    claves = sorted(datos.keys()) if isinstance(datos, dict) else []
                    resultado["claves_vistas"].setdefault(f"{motor}:{nombre}", sorted(
                        set(resultado["claves_vistas"].get(f"{motor}:{nombre}", [])) | set(claves)))
                    for ruta, valor in _buscar_evidencia(datos):
                        resultado["evidencia"].append({"motor": motor, "endpoint": nombre, "lid": lid,
                                                       "campo": ruta, "valor": str(valor)[:80]})
                except Exception as e:
                    resultado["errores"].append(f"{motor} {nombre}: {str(e)[:120]}")
    return resultado


# ------------------------------------------------------------------------------
# 2. Novedades publicadas de WAHA
# ------------------------------------------------------------------------------
def _menciones(texto, origen):
    encontradas = []
    for linea in texto.splitlines():
        linea = linea.strip()
        if linea and PATRON_MENCION.search(linea) and not PATRON_IGNORAR.search(linea):
            encontradas.append({"origen": origen, "texto": linea[:240]})
    return encontradas


def revisar_novedades():
    res = {"ultima_version": None, "menciones": [], "errores": []}
    try:
        r = requests.get(f"https://api.github.com/repos/{REPO_GITHUB}/releases", params={"per_page": 10},
                         headers={"Accept": "application/vnd.github+json"}, timeout=TIMEOUT)
        r.raise_for_status()
        releases = r.json()
        if releases:
            res["ultima_version"] = releases[0].get("tag_name")
        for rel in releases:
            res["menciones"] += _menciones(rel.get("body") or "", f"GitHub {rel.get('tag_name')}")
    except Exception as e:
        res["errores"].append(f"GitHub: {str(e)[:160]}")

    try:
        r = requests.get(URL_CHANGELOG, timeout=TIMEOUT)
        r.raise_for_status()
        texto = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", r.text, flags=re.S | re.I)
        texto = html.unescape(re.sub(r"<br\s*/?>|</(p|li|h\d|div)>", "\n", texto, flags=re.I))
        texto = re.sub(r"<[^>]+>", " ", texto)
        res["menciones"] += _menciones(texto, "Changelog")
    except Exception as e:
        res["errores"].append(f"Changelog: {str(e)[:160]}")

    # sin repetidos
    vistos, unicas = set(), []
    for m in res["menciones"]:
        if m["texto"] not in vistos:
            vistos.add(m["texto"])
            unicas.append(m)
    res["menciones"] = unicas[:30]
    return res


# ------------------------------------------------------------------------------
def main():
    if not ES_PRUEBA:
        asegurar_tablas()
    log("🔎 Agente WAHA: revisando si ya se puede leer el @usuario (alias) de WhatsApp...")

    local = revisar_waha_local()
    novedades = revisar_novedades()

    version_local = ", ".join(f"{m} {v}" for m, v in local["versiones"].items()) or "desconocida"
    ultima = novedades["ultima_version"] or "desconocida"
    soportado_local = bool(local["evidencia"])
    mencionado = bool(novedades["menciones"])
    versiones_inst = set(local["versiones"].values())
    hay_actualizacion = bool(novedades["ultima_version"]) and novedades["ultima_version"] not in versiones_inst

    if soportado_local:
        resumen = "✅ ¡Tu WAHA YA devuelve el @usuario! Se puede implementar el llenado de telefonoscliente.alias en el webhook."
    elif mencionado and hay_actualizacion:
        resumen = f"🟡 WAHA menciona usernames en sus novedades y hay versión nueva ({ultima}). Revisa y actualiza WAHA."
    elif mencionado:
        resumen = "🟡 WAHA menciona usernames en sus novedades, pero tu WAHA aún no lo devuelve. Revisa las menciones."
    else:
        resumen = "⏳ Aún no disponible: WAHA no entrega el @usuario ni lo menciona en sus novedades."
    if hay_actualizacion and not soportado_local:
        resumen += f" (Instalado: {version_local} · Última: {ultima})"

    log(resumen)
    for ev in local["evidencia"][:5]:
        log(f"   • {ev['motor']} {ev['endpoint']}: {ev['campo']} = {ev['valor']}")
    for m in novedades["menciones"][:5]:
        log(f"   • {m['origen']}: {m['texto']}")
    for err in local["errores"] + novedades["errores"]:
        log(f"   ⚠️ {err}")

    detalle = {"local": local, "novedades": novedades}
    if ES_PRUEBA:
        print(json.dumps(detalle, ensure_ascii=False, indent=2, default=str)[:3000])
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO agente_waha_revisiones
                (fecha, version_local, ultima_version, soportado_local, mencionado_upstream, resumen, detalle)
            VALUES (:f, :vl, :uv, :sl, :mu, :r, :d)
        """), {"f": datetime.now(), "vl": version_local, "uv": ultima, "sl": soportado_local,
               "mu": mencionado, "r": resumen, "d": json.dumps(detalle, ensure_ascii=False, default=str)})


def estado():
    """Lo que necesita la tarea programada de Claude para no repetir trabajo."""
    asegurar_tablas()
    with engine.connect() as conn:
        rev = conn.execute(text("SELECT fecha, version_local, ultima_version, soportado_local, mencionado_upstream, resumen "
                                "FROM agente_waha_revisiones ORDER BY id DESC LIMIT 1")).mappings().fetchone()
        ana = conn.execute(text("SELECT fecha, version_revisada, alias_disponible, nivel, resumen "
                                "FROM agente_waha_analisis ORDER BY id DESC LIMIT 1")).mappings().fetchone()
    print(json.dumps({"ultima_revision_automatica": dict(rev) if rev else None,
                      "ultimo_analisis_claude": dict(ana) if ana else None}, ensure_ascii=False, indent=2, default=str))


def guardar_analisis(ruta):
    with open(ruta, encoding="utf-8") as f:
        d = json.load(f)
    nivel = str(d.get("nivel", "ok")).lower()
    if nivel not in ("ok", "mejora", "riesgo"):
        nivel = "ok"
    asegurar_tablas()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO agente_waha_analisis (version_revisada, alias_disponible, nivel, resumen, informe)
            VALUES (:v, :a, :n, :r, :i)
        """), {"v": d.get("version_revisada"), "a": bool(d.get("alias_disponible")), "n": nivel,
               "r": str(d.get("resumen", ""))[:500], "i": str(d.get("informe", ""))})
    log(f"🧠 Análisis de Claude guardado ({nivel}): {str(d.get('resumen', ''))[:200]}")


if __name__ == "__main__":
    if "--estado" in sys.argv:
        estado()
        sys.exit(0)
    if "--guardar-analisis" in sys.argv:
        guardar_analisis(sys.argv[sys.argv.index("--guardar-analisis") + 1])
        sys.exit(0)
    try:
        main()
    except Exception as e:
        log(f"🔥 Error en el agente WAHA: {e}")
        raise
