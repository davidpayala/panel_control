"""
Sube los artículos de contenido_blog/*.html a kmlentes.pe como BORRADORES.

Necesita en .env una contraseña de aplicación de WordPress (no tu contraseña normal):
  WP_USER=tu_usuario_de_wordpress
  WP_APP_PASSWORD=xxxx xxxx xxxx xxxx xxxx xxxx
Se crea en WordPress → Usuarios → Perfil → "Contraseñas de aplicación".

  • No publica nada: deja borradores para que los revises y publiques tú.
  • Si un artículo ya existe (mismo slug) lo actualiza en vez de duplicarlo.
  • Los enlaces https://kmlentes.pe/blog/<slug>/ entre artículos se cambian por
    la URL real que tendrá cada entrada según tus enlaces permanentes.
  • La meta descripción va en el extracto, que Rank Math usa por defecto.

Uso:  python publicar_blog.py
"""
import glob
import os
import re

import requests
from dotenv import load_dotenv

BASE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE, ".env"))

SITIO = os.getenv("WOO_LENTES_URL", "https://kmlentes.pe").rstrip("/")
API = f"{SITIO}/wp-json/wp/v2"


def leer_articulo(ruta):
    texto = open(ruta, encoding="utf-8").read()
    cab = re.match(r"\s*<!--(.*?)-->", texto, re.S)
    datos = dict(re.findall(r"^\s*(\w+):\s*(.+?)\s*$", cab.group(1), re.M)) if cab else {}
    datos["contenido"] = texto[cab.end():].strip() if cab else texto
    return datos


def main():
    usuario, clave = os.getenv("WP_USER"), os.getenv("WP_APP_PASSWORD")
    if not (usuario and clave):
        print("❌ Falta WP_USER y/o WP_APP_PASSWORD en .env (ver instrucciones arriba).")
        return
    s = requests.Session()
    s.auth = (usuario, clave)
    s.headers["User-Agent"] = "Mozilla/5.0 (KM Panel)"

    yo = s.get(f"{API}/users/me", timeout=30)
    if yo.status_code != 200:
        print(f"❌ WordPress no aceptó las credenciales (HTTP {yo.status_code}): {yo.text[:200]}")
        return

    articulos = [leer_articulo(r) for r in sorted(glob.glob(os.path.join(BASE, "contenido_blog", "*.html")))]

    # 1) Crear/actualizar todos como borrador y averiguar la URL definitiva de cada uno
    ids, urls = {}, {}
    for a in articulos:
        existe = s.get(f"{API}/posts", params={"slug": a["slug"], "status": "any", "context": "edit"}, timeout=30).json()
        cuerpo = {"title": a["titulo"], "slug": a["slug"], "excerpt": a.get("meta", ""), "content": a["contenido"]}
        if existe:
            post = existe[0]
            if post.get("status") == "publish":
                cuerpo.pop("content")   # ya publicado: no se pisa lo que hayas editado
            r = s.post(f"{API}/posts/{post['id']}", json=cuerpo, timeout=60)
        else:
            r = s.post(f"{API}/posts", json={**cuerpo, "status": "draft"}, timeout=60)
        if r.status_code not in (200, 201):
            print(f"❌ {a['slug']}: HTTP {r.status_code} {r.text[:200]}")
            continue
        post = s.get(f"{API}/posts/{r.json()['id']}", params={"context": "edit"}, timeout=30).json()
        ids[a["slug"]] = post["id"]
        plantilla = post.get("permalink_template") or f"{SITIO}/%postname%/"
        urls[a["slug"]] = plantilla.replace("%postname%", a["slug"]).replace("%pagename%", a["slug"])

    # 2) Reescribir los enlaces entre artículos con la URL real
    for a in articulos:
        if a["slug"] not in ids:
            continue
        contenido = re.sub(r"https://kmlentes\.pe/blog/([a-z0-9-]+)/",
                           lambda m: urls.get(m.group(1), m.group(0)), a["contenido"])
        if contenido != a["contenido"]:
            post = s.get(f"{API}/posts/{ids[a['slug']]}", params={"context": "edit"}, timeout=30).json()
            if post.get("status") != "publish":
                s.post(f"{API}/posts/{ids[a['slug']]}", json={"content": contenido}, timeout=60)
        print(f"✅ Borrador listo: {a['titulo']}\n   Editar: {SITIO}/wp-admin/post.php?post={ids[a['slug']]}&action=edit"
              f"\n   URL al publicar: {urls[a['slug']]}")


if __name__ == "__main__":
    main()
