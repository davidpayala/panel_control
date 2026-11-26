"""
📥 Descarga las fotos del catálogo del proveedor desde Drive (carpeta "Catalogo", compartida por enlace).

Estructura en Drive:  Catalogo/<NNN_Marca_Modelo_Color>/{1_alto_valor,2_regular_valor,3_bajo_valor}/*.jpg
Se copia igual en  /home/panel-control/catalogo_proveedor/  y solo baja lo que falta (se puede repetir).

Uso:
    .venv/bin/python recurrente/descargar_catalogo.py                 # solo los modelos de la propuesta de códigos
    .venv/bin/python recurrente/descargar_catalogo.py --todos         # las 373 carpetas
"""
import html
import os
import re
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARPETA_DRIVE = "1OuNUT400gPbsShcRWWehj1nlnS4h0-tQ"
DESTINO = "/home/panel-control/catalogo_proveedor"
PROPUESTA = os.path.join(_raiz, "documentos", "codigos_propuestos.xlsx")
UA = {"User-Agent": "Mozilla/5.0"}


def get(url, timeout=60):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return r.read()


def listar(folder_id):
    """[(id, nombre, es_carpeta)] de una carpeta pública de Drive."""
    page = get(f"https://drive.google.com/embeddedfolderview?id={folder_id}").decode("utf-8", "replace")
    items = []
    for m in re.finditer(r'href="https://drive\.google\.com/(drive/folders|file/d)/([A-Za-z0-9_-]+)[^"]*".*?'
                         r'flip-entry-title">([^<]+)<', page, re.S):
        items.append((m.group(2), html.unescape(m.group(3)).strip(), m.group(1) == "drive/folders"))
    return items


def bajar(file_id, ruta):
    if os.path.exists(ruta) and os.path.getsize(ruta) > 0:
        return "ya"
    datos = get(f"https://drive.usercontent.google.com/download?id={file_id}&export=download", timeout=120)
    if datos[:15].lower().startswith(b"<!doctype html") or datos[:5].lower() == b"<html":
        return "error"
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "wb") as f:
        f.write(datos)
    return "ok"


def carpetas_propuesta():
    from openpyxl import load_workbook
    ws = load_workbook(PROPUESTA, read_only=True).worksheets[0]
    filas = list(ws.iter_rows(values_only=True))
    i = list(filas[0]).index("Carpeta Drive (imágenes)")
    return {str(r[i]).split("/")[-1] for r in filas[1:] if r[i]}


def main():
    raiz = listar(CARPETA_DRIVE)
    quiero = None if "--todos" in sys.argv else carpetas_propuesta()
    modelos = [(fid, nom) for fid, nom, es in raiz if es and (quiero is None or nom in quiero)]
    print(f"📁 {len(modelos)} carpetas de modelo")
    tareas = []
    for fid, nom in modelos:
        for sid, sub, es in listar(fid):
            if es:
                for iid, archivo, es2 in listar(sid):
                    if not es2:
                        tareas.append((iid, os.path.join(DESTINO, nom, sub, archivo)))
            else:
                tareas.append((sid, os.path.join(DESTINO, nom, sub)))
    print(f"🖼️ {len(tareas)} imágenes")
    with ThreadPoolExecutor(8) as ex:
        res = list(ex.map(lambda t: bajar(*t), tareas))
    print({k: res.count(k) for k in set(res)})


if __name__ == "__main__":
    main()
