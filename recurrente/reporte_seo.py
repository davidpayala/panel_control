"""
reporte_seo.py — Revisión semanal del SEO de kmlentes.pe.

Qué hace (una vez por semana, lo dispara la tarea programada de Claude del fin de semana):
  1. Mide la salud del sitio y del catálogo: productos visibles/ocultos, fichas sin
     foto o sin descripción, SEO pendiente (títulos, alts, noindex), marca para Google.
  2. Google Merchant Center (plugin Google for WooCommerce): productos activos,
     rechazados, avisos, y clics/impresiones de Shopping semana vs semana anterior.
  3. Search Console (vía Site Kit): clics, impresiones y búsquedas principales,
     si Site Kit tiene los permisos.
  4. Reseñas nuevas y pendientes de aprobar, agradecimientos postventa enviados.
  5. Blog: publicados, programados y días desde el último artículo.
  6. Revisa enlaces internos rotos en el blog y en las páginas principales.
  7. Arma una lista de acciones separadas en "Claude" y "Tú", y guarda todo en la
     tabla seo_reportes para verlo en el panel (REPORTE → SEO web) y comparar semanas.

SOLO LECTURA sobre la web. No cambia nada en WooCommerce ni en WordPress.

Uso:
    python recurrente/reporte_seo.py                 # genera y guarda el reporte
    python recurrente/reporte_seo.py --prueba        # no guarda, solo muestra
    python recurrente/reporte_seo.py --analisis-archivo informe.md
        # adjunta al último reporte el análisis/resumen que escribió Claude
"""
import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone

_AQUI = os.path.dirname(os.path.abspath(__file__))
_RAIZ = os.path.dirname(_AQUI)
if _RAIZ not in sys.path:
    sys.path.insert(0, _RAIZ)

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from sqlalchemy import text  # noqa: E402

load_dotenv(os.path.join(_RAIZ, ".env"))

from database import engine  # noqa: E402
import seo_woo  # noqa: E402

SITIO = "https://kmlentes.pe"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/140 Safari/537.36"
PAGINAS_CLAVE = {
    "/": "index", "/lentes-naturales/": "index", "/lentes-fantasia/": "index",
    "/lentes-para-miopia/": "index", "/accesorios/": "index", "/envios/": "index",
    "/reembolso_devoluciones/": "index", "/cart/": "noindex", "/my-account/": "noindex",
}


def log(msg):
    print(msg, flush=True)


def _wp():
    s = requests.Session()
    s.auth = (os.getenv("WP_USER", ""), os.getenv("WP_APP_PASSWORD", ""))
    s.headers["User-Agent"] = UA
    return s


def _get_json(s, url, **kw):
    try:
        r = s.get(url, timeout=90, **kw)
        return r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else None)
    except Exception as e:
        return 0, {"error": str(e)}


# ------------------------------------------------------------------------------
# 1. Catálogo
# ------------------------------------------------------------------------------
def revisar_catalogo(wcapi):
    from mejoras_merchant import calcular as calcular_merchant, marcas_db
    productos = seo_woo.descargar_productos(wcapi)
    visibles = [p for p in productos if p.get("catalog_visibility") != "hidden"]
    hace7 = datetime.now() - timedelta(days=7)

    seo_pend = sum(1 for p, c in seo_woo._con_desempate(productos) if seo_woo.calcular_cambios(p, con_sku=c)[0])
    marcas = marcas_db()
    merch_pend = sum(1 for p in productos if calcular_merchant(p, marcas)[0])
    sin_foto = [p["sku"] for p in visibles if not p.get("images")]
    sin_desc = [p["sku"] for p in visibles
                if not seo_woo._limpiar(p.get("description")) and not seo_woo._limpiar(p.get("short_description"))]
    nuevos = [p["sku"] for p in productos
              if p.get("date_created") and datetime.fromisoformat(p["date_created"]) >= hace7]
    nombres_ingles = [p["sku"] for p in productos if re.match(r"^(lens|lente)\s", p.get("name", ""), re.I)]

    return {
        "productos_total": len(productos),
        "productos_visibles": len(visibles),
        "productos_ocultos": len(productos) - len(visibles),
        "seo_pendientes": seo_pend,
        "merchant_pendientes": merch_pend,
        "visibles_sin_foto": sin_foto,
        "visibles_sin_descripcion": sin_desc,
        "productos_nuevos_7d": nuevos,
        "nombres_sin_espanol": nombres_ingles,
    }


# ------------------------------------------------------------------------------
# 2. Merchant Center (Google for WooCommerce)
# ------------------------------------------------------------------------------
def revisar_merchant(s):
    base = f"{SITIO}/wp-json/wc/gla/mc"
    res = {}
    for _ in range(8):   # las estadísticas a veces vienen "cargando"
        _, st = _get_json(s, f"{base}/product-statistics")
        if st and st.get("statistics"):
            res["estadisticas"] = st["statistics"]
            break
        time.sleep(15)
    issues, pag = [], 1
    while pag <= 5:
        _, d = _get_json(s, f"{base}/issues/product", params={"per_page": 100, "page": pag})
        lote = (d or {}).get("issues", [])
        issues += lote
        if len(lote) < 100:
            break
        pag += 1
    res["rechazados"] = sorted({f"{i['product']}" for i in issues if i.get("severity") == "error"})
    res["avisos"] = dict(Counter(i["issue"] for i in issues if i.get("severity") != "error"))
    _, pol = _get_json(s, f"{base}/policy_check")
    res["politicas_ok"] = {k: v for k, v in (pol or {}).items()}

    hoy = date.today()
    for nombre, ini, fin in (("semana", hoy - timedelta(days=7), hoy - timedelta(days=1)),
                             ("semana_anterior", hoy - timedelta(days=14), hoy - timedelta(days=8))):
        _, rep = _get_json(s, f"{base}/reports/programs",
                           params={"after": ini.isoformat(), "before": fin.isoformat(), "fields": "clicks,impressions"})
        res[f"shopping_{nombre}"] = (rep or {}).get("totals") or {}
    _, top = _get_json(s, f"{base}/reports/products",
                       params={"after": (hoy - timedelta(days=28)).isoformat(), "before": (hoy - timedelta(days=1)).isoformat(),
                               "fields": "clicks,impressions", "orderby": "clicks", "order": "desc", "per_page": 5})
    res["shopping_top_28d"] = [{"producto": p.get("name"), **(p.get("subtotals") or {})} for p in (top or {}).get("products") or []]
    return res


# ------------------------------------------------------------------------------
# 3. Search Console vía Site Kit
# ------------------------------------------------------------------------------
def revisar_search_console(s):
    base = f"{SITIO}/wp-json/google-site-kit/v1/modules/search-console/data/searchanalytics"
    hoy = date.today()
    # Search Console tiene ~3 días de retraso
    ini, fin = hoy - timedelta(days=10), hoy - timedelta(days=4)
    code, tot = _get_json(s, base, params={"startDate": ini.isoformat(), "endDate": fin.isoformat(), "dimensions": "date"})
    if code != 200:
        return {"disponible": False, "motivo": (tot or {}).get("code", f"HTTP {code}")}
    filas = tot if isinstance(tot, list) else (tot or {}).get("rows", [])
    clics = sum(f.get("clicks", 0) for f in filas)
    imp = sum(f.get("impressions", 0) for f in filas)
    pos = (sum(f.get("position", 0) * f.get("impressions", 0) for f in filas) / imp) if imp else None
    _, q = _get_json(s, base, params={"startDate": ini.isoformat(), "endDate": fin.isoformat(), "dimensions": "query", "limit": 10})
    qf = q if isinstance(q, list) else (q or {}).get("rows", [])
    _, pg = _get_json(s, base, params={"startDate": ini.isoformat(), "endDate": fin.isoformat(), "dimensions": "page", "limit": 10})
    pf = pg if isinstance(pg, list) else (pg or {}).get("rows", [])
    return {
        "disponible": True, "periodo": f"{ini} a {fin}", "clics": clics, "impresiones": imp,
        "ctr": round(clics / imp * 100, 2) if imp else 0, "posicion_media": round(pos, 1) if pos else None,
        "top_busquedas": [{"busqueda": f["keys"][0], "clics": f.get("clicks"), "impresiones": f.get("impressions"),
                           "posicion": round(f.get("position", 0), 1)} for f in qf],
        "top_paginas": [{"pagina": f["keys"][0], "clics": f.get("clicks"), "impresiones": f.get("impressions")} for f in pf],
    }


# ------------------------------------------------------------------------------
# 4. Reseñas, postventa y blog
# ------------------------------------------------------------------------------
def revisar_resenas(wcapi):
    hace7 = datetime.now() - timedelta(days=7)
    todas, pag = [], 1
    while pag <= 10:
        r = wcapi.get("products/reviews", params={"per_page": 100, "page": pag, "status": "all"})
        lote = r.json() if r.status_code == 200 else []
        todas += lote
        if len(lote) < 100:
            break
        pag += 1
    aprobadas = [x for x in todas if x.get("status") == "approved"]
    return {
        "resenas_aprobadas": len(aprobadas),
        "resenas_nuevas_7d": sum(1 for x in todas if datetime.fromisoformat(x["date_created"]) >= hace7),
        "resenas_pendientes": sum(1 for x in todas if x.get("status") == "hold"),
        "calificacion_media": round(sum(x.get("rating", 0) for x in aprobadas) / len(aprobadas), 2) if aprobadas else None,
    }


def revisar_postventa():
    try:
        with engine.connect() as c:
            n = c.execute(text("SELECT COUNT(*) FROM logs_postventa WHERE fecha >= NOW() - INTERVAL '7 days' "
                               "AND mensaje LIKE '%%actualizado a%%'")).scalar()
        return {"agradecimientos_7d": int(n or 0)}
    except Exception as e:
        return {"agradecimientos_7d": None, "error": str(e)[:120]}


def revisar_blog(s):
    _, pub = _get_json(s, f"{SITIO}/wp-json/wp/v2/posts", params={"per_page": 100, "status": "publish", "context": "edit"})
    _, fut = _get_json(s, f"{SITIO}/wp-json/wp/v2/posts", params={"per_page": 100, "status": "future", "context": "edit"})
    _, bor = _get_json(s, f"{SITIO}/wp-json/wp/v2/posts", params={"per_page": 100, "status": "draft", "context": "edit"})
    pub, fut, bor = (x if isinstance(x, list) else [] for x in (pub, fut, bor))
    ultima = max((p["date"] for p in pub), default=None)
    return {
        "publicados": len(pub), "programados": len(fut), "borradores": len(bor),
        "proximo": min(((p["date"][:10], p["title"]["raw"]) for p in fut), default=None),
        "dias_desde_ultimo": (datetime.now() - datetime.fromisoformat(ultima)).days if ultima else None,
        "sin_imagen_destacada": [p["title"]["raw"] for p in pub + fut if not p.get("featured_media")],
        "urls_publicadas": [p["link"] for p in pub],
    }


# ------------------------------------------------------------------------------
# 5. Salud técnica del sitio
# ------------------------------------------------------------------------------
def revisar_sitio(urls_blog):
    s = requests.Session()
    s.headers["User-Agent"] = UA
    res = {"paginas": {}, "enlaces_rotos": []}
    t0 = time.time()
    r = s.get(f"{SITIO}/?v={int(t0)}", timeout=60)
    res["tiempo_home_s"] = round(time.time() - t0, 2)
    res["h1_home"] = len(re.findall(r"<h1[\s>]", r.text))

    for ruta, esperado in PAGINAS_CLAVE.items():
        try:
            rr = s.get(f"{SITIO}{ruta}?v={int(time.time())}", timeout=60, allow_redirects=True)
            robots = re.search(r'<meta name="robots" content="([^"]*)"', rr.text)
            robots = robots.group(1) if robots else ""
            ok = rr.status_code == 200 and (("noindex" in robots) == (esperado == "noindex"))
            res["paginas"][ruta] = {"estado": rr.status_code, "robots": robots, "ok": ok}
        except Exception as e:
            res["paginas"][ruta] = {"estado": 0, "error": str(e)[:80], "ok": False}

    # Sitemap
    try:
        idx = s.get(f"{SITIO}/sitemap_index.xml", timeout=60).text
        mapas = re.findall(r"<loc>([^<]+)</loc>", idx)
        res["sitemaps"] = {m.rsplit("/", 1)[-1]: len([u for u in re.findall(r"<loc>([^<]+)</loc>", s.get(m, timeout=60).text)
                                                      if "/wp-content/" not in u]) for m in mapas}
    except Exception as e:
        res["sitemaps"] = {"error": str(e)[:80]}

    # Enlaces internos rotos en blog + páginas principales
    origenes = list(urls_blog) + [f"{SITIO}{r}" for r, e in PAGINAS_CLAVE.items() if e == "index"]
    revisados = {}
    for origen in origenes:
        try:
            html = s.get(origen, timeout=60).text
        except Exception:
            continue
        for u in set(re.findall(r'href="(https://kmlentes\.pe/[^"#?]*)"', html)):
            if "/wp-" in u or u.endswith((".xml", ".css", ".js")) or "/feed" in u:
                continue
            if u not in revisados:
                try:
                    revisados[u] = s.head(u, timeout=30, allow_redirects=True).status_code
                except Exception:
                    revisados[u] = 0
            if revisados[u] >= 400 or revisados[u] == 0:
                res["enlaces_rotos"].append({"en": origen, "enlace": u, "estado": revisados[u]})
        if len(revisados) > 250:
            break
    res["enlaces_revisados"] = len(revisados)
    return res


# ------------------------------------------------------------------------------
# 6. Acciones
# ------------------------------------------------------------------------------
def construir_acciones(d, anterior):
    claude, usuario = [], []
    cat, mc, sc, res, blog, sitio = d["catalogo"], d["merchant"], d["search_console"], d["resenas"], d["blog"], d["sitio"]

    if cat["seo_pendientes"]:
        claude.append(f"Aplicar SEO (títulos, meta, alts, index/noindex) a {cat['seo_pendientes']} productos: python seo_woo.py --aplicar")
    if cat["merchant_pendientes"]:
        claude.append(f"Aplicar marca/nombre/descripción para Google a {cat['merchant_pendientes']} productos: python mejoras_merchant.py --aplicar")
    if cat["visibles_sin_foto"]:
        usuario.append(f"Subir foto a productos visibles sin imagen: {', '.join(cat['visibles_sin_foto'][:15])}")
    if mc.get("rechazados"):
        usuario.append(f"Google rechaza {len(mc['rechazados'])} productos (revisar foto): {', '.join(mc['rechazados'][:10])}")
    if mc.get("avisos"):
        top = ", ".join(f"{k} ({v})" for k, v in sorted(mc["avisos"].items(), key=lambda x: -x[1])[:3])
        usuario.append(f"Avisos de Merchant Center a revisar cuando puedas: {top}")
    # en robots_restriction / page_not_found_error / page_redirects, True significa que HAY problema
    problemas = {"robots_restriction", "page_not_found_error", "page_redirects"}
    malas = [k for k, v in (mc.get("politicas_ok") or {}).items() if (bool(v) if k in problemas else not v)]
    if malas:
        claude.append(f"Revisar requisitos de Merchant Center que fallan: {', '.join(malas)}")
    if not sc.get("disponible"):
        usuario.append("Dar permisos de Search Console y Analytics a Site Kit (WordPress → Site Kit → Panel → "
                       "'Conceder permisos') con el usuario de la contraseña de aplicación, para medir búsquedas y visitas.")
    if res["resenas_pendientes"]:
        usuario.append(f"Aprobar {res['resenas_pendientes']} reseñas pendientes en WordPress → Productos → Valoraciones.")
    if res["resenas_nuevas_7d"] == 0 and (d["postventa"].get("agradecimientos_7d") or 0) >= 10:
        claude.append("Hubo agradecimientos postventa pero 0 reseñas nuevas: proponer un mensaje de reseña más directo o un incentivo.")
    if blog["programados"] < 2:
        claude.append("Quedan menos de 2 artículos programados: redactar 2 artículos nuevos y subirlos como borradores.")
    if blog["borradores"]:
        usuario.append(f"Revisar y programar {blog['borradores']} borradores del blog.")
    if blog["sin_imagen_destacada"]:
        claude.append(f"Crear imagen destacada para: {', '.join(blog['sin_imagen_destacada'][:5])}")
    if sitio["enlaces_rotos"]:
        claude.append(f"Corregir {len(sitio['enlaces_rotos'])} enlaces internos rotos (ver detalle).")
    malas = [r for r, v in sitio["paginas"].items() if not v.get("ok")]
    if malas:
        claude.append(f"Páginas clave con estado o robots inesperado: {', '.join(malas)}")
    if sitio.get("h1_home", 1) != 1:
        usuario.append(f"La página de inicio tiene {sitio['h1_home']} títulos H1 (debe ser 1).")

    # Tendencias
    s_act = mc.get("shopping_semana", {}).get("clicks")
    s_ant = mc.get("shopping_semana_anterior", {}).get("clicks")
    if s_act is not None and s_ant and s_ant >= 20 and s_act < s_ant * 0.7:
        claude.append(f"Los clics de Shopping bajaron de {s_ant} a {s_act}: revisar productos que perdieron visibilidad.")
    return {"claude": claude, "usuario": usuario}


def kpis(d):
    mc, sc = d["merchant"], d["search_console"]
    st = mc.get("estadisticas") or {}
    return {
        "productos_visibles": d["catalogo"]["productos_visibles"],
        "productos_en_sitemap": sum(v for k, v in d["sitio"].get("sitemaps", {}).items() if k.startswith("product")),
        "google_activos": st.get("active"),
        "google_rechazados": st.get("disapproved"),
        "shopping_clics_7d": mc.get("shopping_semana", {}).get("clicks"),
        "shopping_impresiones_7d": mc.get("shopping_semana", {}).get("impressions"),
        "organico_clics_7d": sc.get("clics") if sc.get("disponible") else None,
        "organico_impresiones_7d": sc.get("impresiones") if sc.get("disponible") else None,
        "organico_posicion": sc.get("posicion_media") if sc.get("disponible") else None,
        "resenas_aprobadas": d["resenas"]["resenas_aprobadas"],
        "articulos_publicados": d["blog"]["publicados"],
        "enlaces_rotos": len(d["sitio"]["enlaces_rotos"]),
    }


# ------------------------------------------------------------------------------
# 7. Guardado
# ------------------------------------------------------------------------------
def _tabla(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS seo_reportes (
            id SERIAL PRIMARY KEY,
            fecha TIMESTAMP DEFAULT NOW(),
            kpis JSONB, detalle JSONB, acciones JSONB,
            analisis TEXT
        )
    """))


def ultimo_reporte():
    with engine.begin() as conn:
        _tabla(conn)
        fila = conn.execute(text("SELECT id, fecha, kpis, acciones FROM seo_reportes ORDER BY fecha DESC LIMIT 1")).fetchone()
    return fila


def generar(guardar=True):
    wcapi = seo_woo.conectar_woo()
    s = _wp()
    log("🔎 Revisando catálogo...")
    d = {"generado": datetime.now(timezone.utc).isoformat()}
    d["catalogo"] = revisar_catalogo(wcapi)
    log("🛒 Revisando Merchant Center...")
    d["merchant"] = revisar_merchant(s)
    log("📈 Revisando Search Console...")
    d["search_console"] = revisar_search_console(s)
    log("⭐ Reseñas, postventa y blog...")
    d["resenas"] = revisar_resenas(wcapi)
    d["postventa"] = revisar_postventa()
    d["blog"] = revisar_blog(s)
    log("🌐 Revisando el sitio y enlaces...")
    d["sitio"] = revisar_sitio(d["blog"]["urls_publicadas"])

    anterior = ultimo_reporte()
    acciones = construir_acciones(d, anterior)
    k = kpis(d)
    if anterior and anterior.kpis:
        k["_semana_anterior"] = {x: anterior.kpis.get(x) for x in k if not x.startswith("_")}

    if guardar:
        with engine.begin() as conn:
            _tabla(conn)
            conn.execute(text("INSERT INTO seo_reportes (kpis, detalle, acciones) VALUES (:k, :d, :a)"),
                         {"k": json.dumps(k), "d": json.dumps(d, default=str), "a": json.dumps(acciones, ensure_ascii=False)})
        log("💾 Reporte guardado en seo_reportes.")
    return k, acciones, d


def adjuntar_analisis(ruta):
    texto = open(ruta, encoding="utf-8").read()
    with engine.begin() as conn:
        _tabla(conn)
        conn.execute(text("UPDATE seo_reportes SET analisis = :t WHERE id = (SELECT MAX(id) FROM seo_reportes)"), {"t": texto})
    log("📝 Análisis adjuntado al último reporte.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prueba", action="store_true")
    ap.add_argument("--analisis-archivo")
    a = ap.parse_args()
    if a.analisis_archivo:
        adjuntar_analisis(a.analisis_archivo)
        sys.exit(0)
    k, acc, d = generar(guardar=not a.prueba)
    print("\n=== KPIs ===")
    print(json.dumps(k, indent=2, ensure_ascii=False, default=str))
    print("\n=== Acciones para Claude ===")
    for x in acc["claude"]:
        print(" -", x)
    print("\n=== Acciones para ti ===")
    for x in acc["usuario"]:
        print(" -", x)
    print("\n=== Detalle ===")
    detalle = {k2: v for k2, v in d.items() if k2 != "blog"} | {"blog": {k3: v3 for k3, v3 in d["blog"].items() if k3 != "urls_publicadas"}}
    print(json.dumps(detalle, indent=1, ensure_ascii=False, default=str)[:6000])
