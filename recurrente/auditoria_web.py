"""
auditoria_web.py — Auditoría técnica semanal de kmlentes.pe (fluidez + SEO).

Repite cada semana los chequeos de la auditoría profunda del 21-nov-2026 y avisa
si algo se rompió o volvió (lo usa recurrente/reporte_seo.py):

  Fluidez (navegador real, celular con 4G y CPU media, si Playwright está instalado):
    • LCP, CLS, bloqueo (TBT), peticiones y peso de las páginas clave.
    • Doble carga de la página en la 1.ª visita (Guest Mode de LiteSpeed).
    • Visitas duplicadas en Google Analytics y píxel de Meta cargado dos veces.
  Fluidez (HTML, siempre):
    • Tiempo de respuesta, peso del HTML y datos JSON incrustados en categorías.
    • Precargas de imagen repetidas, GTM, "Acceder con Google".
  SEO:
    • Marca del schema de producto (debe ser la marca, no la categoría).
    • Páginas de marca: título en español y en el sitemap.
    • Blog: /blog/ responde, está en el menú y no hay artículos en "Uncategorized".
    • Descripciones: promociones/precios, errores ("Aquoso", "Lens"), campos vacíos,
      descripción corta vacía.
    • Portada con imagen para compartir (og:image), título de la tienda.
    • Snippets KM activos y plugins nuevos/desactivados desde la semana pasada.

SOLO LECTURA. Uso directo:  python recurrente/auditoria_web.py   (imprime el JSON)
"""
import json
import os
import re
import sys
import time
import urllib.parse as U

import requests

SITIO = "https://kmlentes.pe"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/140 Safari/537.36"
UA_MOVIL = ("Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0 Mobile Safari/537.36")
LIBS_EXTRA = os.path.expanduser("~/.local/lib/km-pw")  # libasound para Chromium sin root
GA4 = "G-4M61329WLZ"
PIXEL_META = "1744386953124404"
SNIPPETS_KM = ["KM SEO: marcas y schema", "KM: purgar caché"]

# Límites a partir de los cuales se avisa (celular 4G, CPU x4)
LIMITE_LCP_MS = 2500
LIMITE_TBT_MS = 600
LIMITE_CLS = 0.1
LIMITE_HTML_KB = 500


def _sesion_wp():
    s = requests.Session()
    s.auth = (os.getenv("WP_USER", ""), os.getenv("WP_APP_PASSWORD", ""))
    s.headers["User-Agent"] = UA
    return s


def _texto(h):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", h or "")).strip()


# ------------------------------------------------------------------------------
# Páginas a medir
# ------------------------------------------------------------------------------
def paginas_a_medir(productos):
    prod = next((p["permalink"] for p in productos
                 if p.get("catalog_visibility") != "hidden" and p.get("stock_status") == "instock"
                 and p.get("type") == "variable"), None) or f"{SITIO}/shop/"
    return {"portada": f"{SITIO}/", "categoría": f"{SITIO}/lentes-naturales/",
            "producto": prod, "blog": f"{SITIO}/blog/"}


# ------------------------------------------------------------------------------
# 1. Fluidez en navegador real
# ------------------------------------------------------------------------------
_JS_METRICAS = """() => new Promise(res => {
  const o = {lcp: 0, cls: 0, tbt: 0, lcp_el: ''};
  new PerformanceObserver(l => { for (const e of l.getEntries()) { o.lcp = e.startTime;
      o.lcp_el = e.element ? (e.element.tagName + ' ' + (e.element.currentSrc || e.element.className || '')).slice(0, 140) : (e.url || '') } })
    .observe({type: 'largest-contentful-paint', buffered: true});
  new PerformanceObserver(l => { for (const e of l.getEntries()) if (!e.hadRecentInput) o.cls += e.value })
    .observe({type: 'layout-shift', buffered: true});
  try { new PerformanceObserver(l => { for (const e of l.getEntries()) o.tbt += Math.max(0, e.duration - 50) })
    .observe({type: 'longtask', buffered: true}) } catch (e) {}
  setTimeout(() => {
    const n = performance.getEntriesByType('navigation')[0];
    const r = performance.getEntriesByType('resource');
    let kb = n.transferSize; for (const x of r) kb += x.transferSize;
    const pesadas = r.filter(x => x.initiatorType === 'img' && x.transferSize > 300 * 1024)
      .map(x => [x.name.split('?')[0], Math.round(x.transferSize / 1024)]);
    const terceros = new Set(r.map(x => new URL(x.name).host).filter(h => !h.endsWith('kmlentes.pe')));
    res({...o, ttfb: n.responseStart, carga: n.loadEventEnd, peticiones: r.length + 1,
         kb: Math.round(kb / 1024), dominios_externos: [...terceros], pesadas});
  }, 8000);
})"""


def medir_navegador(urls):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"disponible": False, "motivo": "Playwright no está instalado en .venv"}
    env = dict(os.environ)
    if os.path.isdir(LIBS_EXTRA):
        env["LD_LIBRARY_PATH"] = LIBS_EXTRA + ":" + env.get("LD_LIBRARY_PATH", "")
    res = {"disponible": True, "paginas": {}}
    try:
        with sync_playwright() as p:
            b = p.chromium.launch(env=env)
            for nombre, url in urls.items():
                ctx = b.new_context(viewport={"width": 412, "height": 823}, is_mobile=True, has_touch=True,
                                    user_agent=UA_MOVIL)
                pg = ctx.new_page()
                docs, ga, meta, errores = [], [], [], []

                def al_pedir(r, docs=docs, ga=ga, meta=meta):
                    u = r.url
                    if r.resource_type == "document" and u.split("?")[0].rstrip("/") == url.rstrip("/"):
                        docs.append(u)
                    if "/g/collect" in u and GA4 in u:
                        q = dict(U.parse_qsl(U.urlparse(u).query))
                        eventos = [q.get("en")] if q.get("en") else []
                        eventos += [dict(U.parse_qsl(l)).get("en") for l in (r.post_data or "").split("\n") if l]
                        ga.extend(e for e in eventos if e)
                    if "facebook.com/tr" in u and "ev=PageView" in u:
                        meta.append(u)

                pg.on("request", al_pedir)
                pg.on("pageerror", lambda e, errores=errores: errores.append(str(e)[:150]))
                cdp = ctx.new_cdp_session(pg)
                cdp.send("Network.enable")
                cdp.send("Network.emulateNetworkConditions", {"offline": False, "latency": 150,
                                                              "downloadThroughput": 1.6 * 1024 * 1024 / 8,
                                                              "uploadThroughput": 750 * 1024 / 8})
                cdp.send("Emulation.setCPUThrottlingRate", {"rate": 4})
                try:
                    pg.goto(url, wait_until="load", timeout=120000)
                    m = None
                    for _ in range(2):  # si la página se recarga sola, el contexto cambia: reintenta
                        try:
                            m = pg.evaluate(_JS_METRICAS)
                            break
                        except Exception:
                            pg.wait_for_load_state("load")
                    m = m or {}
                    res["paginas"][nombre] = {
                        "url": url,
                        "lcp_ms": round(m.get("lcp", 0)), "lcp_elemento": m.get("lcp_el", ""),
                        "cls": round(m.get("cls", 0), 3),
                        "tbt_ms": round(m.get("tbt", 0)), "ttfb_ms": round(m.get("ttfb", 0)),
                        "carga_ms": round(m.get("carga", 0)), "peticiones": m.get("peticiones"),
                        "kb": m.get("kb"), "dominios_externos": sorted(m.get("dominios_externos", [])),
                        "imagenes_pesadas": m.get("pesadas", [])[:5],
                        "cargas_del_documento": len(docs),
                        "ga4_page_view": ga.count("page_view"),
                        "ga4_eventos_basura": sum(1 for e in ga if e.startswith("gtm.")),
                        "meta_pageview": len(meta),
                        "errores_js": errores[:5],
                    }
                except Exception as e:
                    res["paginas"][nombre] = {"url": url, "error": str(e)[:150]}
                ctx.close()
            b.close()
    except Exception as e:
        return {"disponible": False, "motivo": f"No se pudo abrir el navegador: {str(e)[:150]}"}
    return res


# ------------------------------------------------------------------------------
# 2. Fluidez por HTML
# ------------------------------------------------------------------------------
def revisar_html(urls):
    s = requests.Session()
    s.headers["User-Agent"] = UA
    out = {}
    for nombre, url in urls.items():
        try:
            t0 = time.time()
            r = s.get(url, timeout=60)
            ttfb = round(time.time() - t0, 2)
            h = r.text
            js_iapi = re.search(r'id="wp-script-module-data-@wordpress/interactivity"[^>]*>(.*?)</script>', h, re.S)
            preloads = re.findall(r'<link[^>]+rel=.preload[^>]+as="image"[^>]+href="([^"]+)"', h)
            norm = [U.urlparse(x).path for x in preloads]
            banner = "/wp-content/uploads/2025/03/lentes-de-contacto.webp"
            out[nombre] = {
                "estado": r.status_code, "respuesta_s": ttfb, "html_kb": round(len(r.content) / 1024),
                "cache_litespeed": r.headers.get("x-litespeed-cache", ""),
                "json_incrustado_kb": round(len(js_iapi.group(1)) / 1024) if js_iapi else 0,
                "precargas_imagen": len(preloads),
                "precargas_repetidas": len(norm) - len(set(norm)),
                "precarga_banner_portada": nombre != "portada" and banner in norm,
                "gtm": bool(re.search(r"GTM-[A-Z0-9]{6,}", h)),
                "acceder_con_google": "accounts.google.com/gsi/client" in h,
                "guest_mode": "guest.vary.php" in h,
                "scripts_externos": len(re.findall(r'<script[^>]+src="https?://(?!kmlentes\.pe)', h)),
                "og_image": bool(re.search(r'property="og:image"', h)),
                "titulo": (re.search(r"<title>(.*?)</title>", h, re.S) or [None, ""])[1].strip(),
            }
        except Exception as e:
            out[nombre] = {"estado": 0, "error": str(e)[:120]}
    return out


# ------------------------------------------------------------------------------
# 3. SEO
# ------------------------------------------------------------------------------
def _schema_marca(url):
    try:
        h = requests.get(url, headers={"User-Agent": UA}, timeout=60).text
    except Exception:
        return None
    for b in re.findall(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', h, re.S):
        try:
            d = json.loads(b)
        except Exception:
            continue
        nodos = d.get("@graph", [d]) if isinstance(d, dict) else []
        for n in nodos:
            if isinstance(n, dict) and "Product" in str(n.get("@type")):
                return (n.get("brand") or {}).get("name")
    return ""


def revisar_seo(productos, s_wp):
    visibles = [p for p in productos if p.get("catalog_visibility") != "hidden"]
    res = {}

    # Marca en el schema: comparar con la taxonomía Marcas en 5 productos
    malas = []
    for p in [x for x in visibles if x.get("brands")][:5]:
        esperado = p["brands"][0]["name"]
        visto = _schema_marca(p["permalink"])
        if visto is not None and esperado != "Otros" and visto != esperado:
            malas.append(f"{p['sku']}: «{visto}» en vez de «{esperado}»")
    res["schema_marca_incorrecta"] = malas

    # Descripciones
    pat_promo = re.compile(r"promoci[oó]n|ll[eé]vate|\bS/\.?\s?\d|\b2x\d|descuento|oferta especial", re.I)
    pat_error = re.compile(r"Aquoso|Diametro\b|\bLens\b")
    pat_vacio = re.compile(r"<strong>[^<]*:\s*</strong>\s*</li>|<li>\s*</li>")
    res["desc_con_promos"] = [p["sku"] for p in visibles
                              if pat_promo.search(_texto(p.get("description")) + " " + _texto(p.get("short_description")))]
    res["desc_con_errores"] = [p["sku"] for p in visibles if pat_error.search(p.get("description") or "")]
    res["desc_campos_vacios"] = [p["sku"] for p in visibles if pat_vacio.search(p.get("description") or "")]
    res["sin_descripcion_corta"] = [p["sku"] for p in visibles if not _texto(p.get("short_description"))]
    res["visibles_sin_marca"] = [p["sku"] for p in visibles if not p.get("brands")
                                 and not (p.get("sku") or "").upper().startswith("EL")]

    # Páginas de marca
    s = requests.Session()
    s.headers["User-Agent"] = UA
    try:
        idx = s.get(f"{SITIO}/sitemap_index.xml", timeout=60).text
        res["marcas_en_sitemap"] = "product_brand-sitemap" in idx
        urls_marca = re.findall(r"<loc>([^<]+)</loc>", s.get(f"{SITIO}/product_brand-sitemap.xml", timeout=60).text) \
            if res["marcas_en_sitemap"] else []
    except Exception:
        res["marcas_en_sitemap"], urls_marca = None, []
    titulos_mal = []
    for u in urls_marca[:4]:
        try:
            t = re.search(r"<title>(.*?)</title>", s.get(u, timeout=60).text, re.S)
            if t and ("Archives" in t.group(1) or not t.group(1).startswith("Lentes de Contacto")):
                titulos_mal.append(f"{u} → {t.group(1).strip()}")
        except Exception:
            pass
    res["paginas_marca_titulo_mal"] = titulos_mal

    # Blog
    try:
        rb = s.get(f"{SITIO}/blog/", timeout=60)
        res["blog_estado"] = rb.status_code
        portada = s.get(f"{SITIO}/", timeout=60).text
        res["blog_en_menu"] = 'href="/blog/"' in portada or f'href="{SITIO}/blog/"' in portada
    except Exception:
        res["blog_estado"], res["blog_en_menu"] = 0, None
    _, cats = _get(s_wp, f"{SITIO}/wp-json/wp/v2/categories", params={"slug": "uncategorized"})
    res["posts_sin_categoria"] = cats[0]["count"] if isinstance(cats, list) and cats else 0

    # Reseñas: productos visibles con al menos una
    res["visibles_con_resenas"] = sum(1 for p in visibles if (p.get("rating_count") or 0) > 0)
    return res


def _get(s, url, **kw):
    try:
        r = s.get(url, timeout=60, **kw)
        return r.status_code, r.json()
    except Exception as e:
        return 0, {"error": str(e)[:120]}


def revisar_config(s_wp):
    res = {}
    st, plugins = _get(s_wp, f"{SITIO}/wp-json/wp/v2/plugins", params={"_fields": "plugin,status,name"})
    if isinstance(plugins, list):
        res["plugins_activos"] = sorted(p["plugin"] for p in plugins if p.get("status") == "active")
    st, sn = _get(s_wp, f"{SITIO}/wp-json/code-snippets/v1/snippets")
    if isinstance(sn, list):
        activos = {x["name"] for x in sn if x.get("active")}
        res["snippets_km_inactivos"] = [n for n in SNIPPETS_KM if n not in activos]
    return res


# ------------------------------------------------------------------------------
# 4. Acciones
# ------------------------------------------------------------------------------
def acciones(a, anterior=None):
    """a = resultado de revisar(); anterior = auditoría de la semana pasada (o None)."""
    claude, usuario = [], []
    nav, html, seo, cfg = a.get("navegador", {}), a.get("html", {}), a.get("seo", {}), a.get("config", {})

    # --- Fluidez
    for nombre, m in (nav.get("paginas") or {}).items():
        if m.get("error"):
            continue
        if (m.get("cargas_del_documento") or 0) > 1:
            usuario.append(f"La página {nombre} se carga {m['cargas_del_documento']} veces en la primera visita. "
                           "Apagar Guest Mode y Guest Optimization en LiteSpeed Cache → General y purgar todo.")
        if (m.get("ga4_page_view") or 0) > 1:
            claude.append(f"Google Analytics recibe {m['ga4_page_view']} visitas por cada visita en {nombre}: "
                          "buscar la etiqueta duplicada (GTM, Site Kit u otro plugin) y proponer cuál quitar.")
        if (m.get("meta_pageview") or 0) > 1:
            claude.append(f"El píxel de Meta envía {m['meta_pageview']} PageView por visita en {nombre}: revisar duplicado.")
        if (m.get("lcp_ms") or 0) > LIMITE_LCP_MS:
            claude.append(f"{nombre}: lo principal tarda {m['lcp_ms'] / 1000:.1f} s en aparecer en celular "
                          f"(meta < {LIMITE_LCP_MS / 1000:.1f} s; elemento: {m.get('lcp_elemento') or '?'}). "
                          "Analizar la causa y proponer el arreglo.")
        if (m.get("tbt_ms") or 0) > LIMITE_TBT_MS:
            claude.append(f"{nombre}: el celular queda bloqueado {m['tbt_ms']} ms por scripts (meta < {LIMITE_TBT_MS} ms). "
                          "Revisar qué scripts de terceros se agregaron.")
        if (m.get("cls") or 0) > LIMITE_CLS:
            claude.append(f"{nombre}: la página salta al cargar (CLS {m['cls']}). Buscar imágenes sin tamaño o banners.")
        if m.get("imagenes_pesadas"):
            img, kb = m["imagenes_pesadas"][0]
            claude.append(f"{nombre}: se descarga una imagen de {kb} KB ({img.rsplit('/', 1)[-1]}). Si es el zoom de la "
                          "galería, proponer desactivarlo en celular; si es una foto subida pesada, recomprimirla.")
        if m.get("errores_js"):
            claude.append(f"{nombre}: errores de JavaScript: {m['errores_js'][0]}")
    if nav and not nav.get("disponible"):
        claude.append(f"No se pudo medir en navegador ({nav.get('motivo')}). Reinstalar: "
                      ".venv/bin/pip install playwright && .venv/bin/playwright install chromium-headless-shell")

    for nombre, h in html.items():
        if h.get("estado") != 200:
            claude.append(f"La página {nombre} responde {h.get('estado')}: revisar.")
            continue
        if h.get("guest_mode") and not any((m.get("cargas_del_documento") or 0) > 1
                                           for m in (nav.get("paginas") or {}).values()):
            usuario.append("LiteSpeed Guest Mode volvió a estar activo: apagarlo en LiteSpeed Cache → General.")
        if h.get("gtm"):
            claude.append(f"Volvió a aparecer un contenedor de Google Tag Manager en {nombre}: verificar que no duplique Analytics/Meta.")
        if (h.get("html_kb") or 0) > LIMITE_HTML_KB:
            usuario.append(f"La página {nombre} pesa {h['html_kb']} KB de HTML ({h.get('json_incrustado_kb')} KB son datos de productos). "
                           "En el bloque de productos, cambiar el botón 'Añadir al carrito' por 'Ver producto' o mostrar menos productos.")
        if h.get("precargas_repetidas"):
            usuario.append(f"{nombre}: {h['precargas_repetidas']} imágenes se precargan dos veces. En LiteSpeed → Page Optimization → "
                           "Media → Preload images, y en el snippet 'banner optimi', dejar una sola.")
    if any(h.get("precarga_banner_portada") for h in html.values()):
        usuario.append("La foto del banner de la portada (103 KB) se precarga en todas las páginas y retrasa la foto de cada "
                       "producto. En Code Snippets → 'banner optimi': cambiarlo a PHP y que solo cargue en la portada "
                       "(Claude te deja el código listo en web_snippets/ si se lo pides).")
    if any(h.get("acceder_con_google") for h in html.values()):
        usuario.append("'Acceder con Google' de Site Kit carga en todas las páginas (script de Google + error en consola). "
                       "Si los clientes no lo usan: Site Kit → Configuración → Sign in with Google → desactivar.")
    port = html.get("portada", {})
    if port and not port.get("og_image"):
        claude.append("La portada no tiene imagen para compartir (og:image): al pegar el enlace en WhatsApp sale sin foto. "
                      "Proponer imagen y configurarla en Rank Math.")

    # --- SEO
    if seo.get("schema_marca_incorrecta"):
        claude.append("La marca del schema de producto volvió a fallar: " + "; ".join(seo["schema_marca_incorrecta"][:3]) +
                      ". Revisar que el snippet 'KM SEO: marcas y schema' siga activo.")
    if seo.get("desc_con_promos"):
        claude.append(f"{len(seo['desc_con_promos'])} productos tienen promociones o precios en la descripción "
                      f"({', '.join(seo['desc_con_promos'][:8])}): python mejorar_descripciones.py --aplicar")
    if seo.get("desc_con_errores") or seo.get("desc_campos_vacios"):
        n = len(set(seo.get("desc_con_errores", [])) | set(seo.get("desc_campos_vacios", [])))
        claude.append(f"{n} descripciones con errores de plantilla o campos vacíos (productos nuevos): "
                      "python mejorar_descripciones.py --aplicar")
    if seo.get("sin_descripcion_corta"):
        claude.append(f"{len(seo['sin_descripcion_corta'])} productos visibles sin descripción corta: "
                      "python mejorar_descripciones.py --aplicar")
    if seo.get("visibles_sin_marca"):
        usuario.append(f"Asignar marca en WooCommerce a: {', '.join(seo['visibles_sin_marca'][:12])}")
    if seo.get("marcas_en_sitemap") is False:
        claude.append("Las páginas de marca salieron del sitemap: subir de nuevo los snippets (python web_snippets/subir.py) "
                      "con un token de purga nuevo.")
    if seo.get("paginas_marca_titulo_mal"):
        claude.append("Páginas de marca con título incorrecto: " + "; ".join(seo["paginas_marca_titulo_mal"][:2]))
    if seo.get("blog_estado") not in (200, None):
        claude.append(f"La página /blog/ responde {seo.get('blog_estado')}: revisarla.")
    if seo.get("blog_en_menu") is False:
        claude.append("El blog ya no está en el menú principal: volver a agregar el enlace.")
    if seo.get("posts_sin_categoria"):
        claude.append(f"{seo['posts_sin_categoria']} artículos quedaron en 'Uncategorized': moverlos a 'Guías de lentes de contacto'.")
    if cfg.get("snippets_km_inactivos"):
        claude.append(f"Snippets KM desactivados: {', '.join(cfg['snippets_km_inactivos'])}. Reactivar con python web_snippets/subir.py.")

    # --- Cambios de plugins desde la semana pasada
    if anterior and anterior.get("config", {}).get("plugins_activos") and cfg.get("plugins_activos"):
        nuevos = sorted(set(cfg["plugins_activos"]) - set(anterior["config"]["plugins_activos"]))
        quitados = sorted(set(anterior["config"]["plugins_activos"]) - set(cfg["plugins_activos"]))
        if nuevos:
            claude.append(f"Plugins activados esta semana: {', '.join(nuevos)}. Medir si afectan la velocidad o duplican seguimiento.")
        if quitados:
            usuario.append(f"Plugins desactivados esta semana: {', '.join(quitados)}. Confirmar que fue a propósito.")
    return {"claude": claude, "usuario": usuario}


def kpis(a):
    """Cifras para comparar semana a semana (peor caso entre las páginas medidas)."""
    pags = [m for m in (a.get("navegador", {}).get("paginas") or {}).values() if not m.get("error")]
    if not pags:
        return {}
    cat = a.get("html", {}).get("categoría", {})
    return {
        "lcp_movil_ms": max(m.get("lcp_ms") or 0 for m in pags),
        "bloqueo_movil_ms": max(m.get("tbt_ms") or 0 for m in pags),
        "peticiones_max": max(m.get("peticiones") or 0 for m in pags),
        "visitas_ga4_por_vista": max(m.get("ga4_page_view") or 0 for m in pags),
        "cargas_por_vista": max(m.get("cargas_del_documento") or 0 for m in pags),
        "html_categoria_kb": cat.get("html_kb"),
    }


def revisar(productos, log=print):
    urls = paginas_a_medir(productos)
    s_wp = _sesion_wp()
    log("   · HTML de páginas clave...")
    a = {"urls": urls, "html": revisar_html(urls)}
    log("   · Medición en navegador (celular 4G)...")
    a["navegador"] = medir_navegador(urls)
    log("   · SEO técnico y descripciones...")
    a["seo"] = revisar_seo(productos, s_wp)
    a["config"] = revisar_config(s_wp)
    return a


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
    import seo_woo
    prods = seo_woo.descargar_productos(seo_woo.conectar_woo())
    r = revisar(prods)
    print(json.dumps(r, indent=1, ensure_ascii=False, default=str)[:8000])
    print(json.dumps(acciones(r), indent=1, ensure_ascii=False))
    print(json.dumps(kpis(r), indent=1, ensure_ascii=False))
