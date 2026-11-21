"""
Descripciones de producto de kmlentes.pe (WooCommerce).

Qué hace:
  • Lentes con la plantilla "Características detalladas ..." (NL/FL/KL/SL/CL):
      - Quita la sección "Promociones" y la imagen del aplicador (las promos cambian
        y no deben vivir en la descripción).
      - Corrige ortografía ("Diámetro", "acuoso", "Lens" -> "Lentes") y quita los
        campos vacíos (Material, Laboratorio, "Personajes" sin contenido).
      - Agrega un párrafo de presentación armado con los datos del propio lente.
      - El enlace de la política pasa a la página de kmlentes (/reembolso_devoluciones/).
  • Descripción corta (la que sale junto al precio): se rehace en todos los lentes
    con los datos del producto, sin promociones. En accesorios se usa el primer
    párrafo de su descripción.

Reglas de seguridad:
  • Por defecto NO escribe nada (simulación).
  • Con --aplicar guarda antes un respaldo JSON con los textos originales en
    respaldos_web/ y escribe por lotes.
  • --restaurar ARCHIVO vuelve a poner los textos de un respaldo.

Uso:
  python mejorar_descripciones.py                  -> simulación + ejemplos
  python mejorar_descripciones.py --sku NL-MC155   -> ver uno (o varios, con coma)
  python mejorar_descripciones.py --aplicar        -> aplica en kmlentes.pe
  python mejorar_descripciones.py --restaurar respaldos_web/descripciones_XXXX.json
"""
import os
import re
import sys
import json
import html as _html
from datetime import datetime

from seo_woo import conectar_woo

RAIZ = os.path.dirname(os.path.abspath(__file__))
URL_POLITICA = "https://kmlentes.pe/reembolso_devoluciones/"

COLORES_EN = {"gray": "Gris", "grey": "Gris", "green": "Verde", "brown": "Marrón", "blue": "Azul",
              "red": "Rojo", "marron": "Marrón", "violet": "Violeta", "pink": "Rosado", "black": "Negro",
              "white": "Blanco", "yellow": "Amarillo", "purple": "Morado", "hazel": "Miel",
              "orange": "Naranja"}
PALABRAS_COLOR = {"azul", "rojo", "gris", "marrón", "verde", "rosado", "morado", "miel", "blanco", "negro",
                  "dorado", "amarillo", "naranja", "violeta", "café", "caramelo", "chocolate", "ámbar",
                  "ambar", "turquesa", "plateado", "celeste", "lila", "fucsia"}


# ------------------------------------------------------------------------------
# Utilidades de texto
# ------------------------------------------------------------------------------
def _texto(h):
    h = re.sub(r"<br\s*/?>", " ", h or "")
    return re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", h))).strip()


def _esc(s):
    return _html.escape(s, quote=False)


def _lista_es(items):
    items = [i for i in items if i]
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " y " + items[-1]


def _atributo(prod, nombre):
    for a in prod.get("attributes") or []:
        if a.get("name") == nombre:
            return [o for o in a.get("options") or [] if o]
    return []


def _seccion(desc, titulo):
    """Contenido HTML entre <h2>titulo...</h2> y el siguiente <h2> (o el final)."""
    m = re.search(r"<h2>\s*" + titulo + r".*?</h2>(.*?)(?=<h2|\Z)", desc, re.S | re.I)
    return m.group(1) if m else ""


# ------------------------------------------------------------------------------
# Lectura de la plantilla vieja
# ------------------------------------------------------------------------------
def leer_ficha(desc):
    ficha = {}
    for li in re.findall(r"<li>(.*?)</li>", _seccion(desc, "Características"), re.S):
        s = _texto(li)
        if ":" in s:
            k, v = s.split(":", 1)
            ficha[k.strip().lower()] = v.strip()
    return ficha


def _diametro(v):
    m = re.search(r"\d+(?:[.,]\d+)?", v or "")
    if not m:
        return ""
    n = float(m.group(0).replace(",", "."))
    return f"{n:.1f} mm" if n < 20 else f"{n:.0f} mm"


def _acuoso(v):
    nums = [float(x.replace(",", ".")) for x in re.findall(r"\d+(?:[.,]\d+)?", v or "")]
    if not nums:
        return ""
    nums = [round(n * 100) if n <= 1 else round(n) for n in nums]
    return f"{nums[0]}%" if len(nums) == 1 or nums[0] == nums[1] else f"{nums[0]}% – {nums[1]}%"


def _duracion(v):
    v = re.sub(r"¡.*?!", "", v or "").strip(" .")
    return v[:1].lower() + v[1:] if v else ""


def _color(ficha, prod):
    crudo = [c.strip() for c in re.split(r"[,/]", ficha.get("color", "")) if c.strip()]
    colores = []
    for c in crudo:
        c = COLORES_EN.get(c.lower(), c)
        if c.lower() not in [x.lower() for x in colores]:
            colores.append(c)
    if not colores or not any(w in PALABRAS_COLOR for c in colores for w in c.lower().split()):
        colores = [c.split("–")[0].split("-")[0].strip() for c in _atributo(prod, "Color")]
    # Si el nombre dice un color concreto y la ficha tiene uno solo distinto, manda el nombre
    # (p. ej. "Werewolf Orange" con ficha "Rojo"): es lo que el cliente ve en el título.
    del_nombre = None
    for w in reversed(_modelo_desde_nombre(prod).lower().split()):
        w = COLORES_EN.get(w, w)
        if w.lower() in PALABRAS_COLOR:
            del_nombre = w.capitalize()
            break
    if del_nombre and len(colores) == 1 and del_nombre.lower() not in colores[0].lower():
        colores = [del_nombre]
    return _lista_es(colores)


def _medida(prod):
    graduaciones = sorted({float(o) for o in _atributo(prod, "Medida Miopía")
                           if re.fullmatch(r"-\d+(?:\.\d+)?", o.strip())}, reverse=True)
    if graduaciones:
        return {"corta": f"Sin medida o con medida para miopía de {graduaciones[0]:.2f} a {graduaciones[-1]:.2f}",
                "ficha": f"Sin medida o con medida para miopía ({graduaciones[0]:.2f} a {graduaciones[-1]:.2f})",
                "frase": f"Puedes pedirlos sin medida o con medida para miopía, desde {graduaciones[0]:.2f} "
                         f"hasta {graduaciones[-1]:.2f}."}
    return {"corta": "Sin medida (medida cero)", "ficha": "Sin medida (medida cero)",
            "frase": "No tienen graduación (medida cero), son de uso estético."}


def _modelo_desde_nombre(prod):
    """'Lentes de Contacto Fantasía Ghost Green (FL027I)' -> 'Ghost Green'."""
    n = re.sub(r"\s*\([^)]*\)\s*$", "", prod.get("name") or "").strip()
    n = re.sub(r"^Lentes de Contacto\s+(de\s+)?(Fantas[ií]a\s+|Naturales?\s+)?", "", n, flags=re.I)
    return n.strip()


def _es_fantasia(prod):
    return any("fantasia" in (c.get("slug") or "") for c in prod.get("categories") or [])


# ------------------------------------------------------------------------------
# Construcción de los textos nuevos
# ------------------------------------------------------------------------------
def datos_lente(prod):
    desc = prod.get("description") or ""
    f = leer_ficha(desc)
    marca = (f.get("marca") or "").strip(" ,")
    marcas_tax = [b["name"] for b in prod.get("brands") or [] if b.get("name") and b["name"] != "Otros"]
    if marca.lower() in ("otros", "otro", "genérico", "generico"):
        marca = ""
    if not marca and marcas_tax:
        marca = ", ".join(marcas_tax)
    for b in marcas_tax:  # misma escritura que la taxonomía (Freshlady -> FreshLady)
        marca = re.sub(re.escape(b), b, marca, flags=re.I)
    personajes = [_texto(li) for li in re.findall(r"<li>(.*?)</li>", _seccion(desc, "Personajes"), re.S)]
    incluye = [_texto(li) for li in re.findall(r"<li>(.*?)</li>", _seccion(desc, r"¿Qué incluye"), re.S)]
    return {
        "nombre": _modelo_desde_nombre(prod),
        "titulo": re.sub(r"\s*\([^)]*\)\s*$", "", prod.get("name") or "").strip(),
        "sku": prod.get("sku") or "",
        "marca": marca,
        "modelo": (f.get("modelo") or "").strip(" ,"),
        "color": _color(f, prod),
        "diametro": _diametro(f.get("diametro") or f.get("diámetro") or " ".join(_atributo(prod, "Diametro"))),
        "contorno": (f.get("contorno") or " ".join(_atributo(prod, "Contorno"))).strip().capitalize(),
        "acuoso": _acuoso(f.get("contenido aquoso") or f.get("contenido acuoso")),
        "duracion": _duracion(f.get("duración")),
        "medida": _medida(prod),
        "personajes": [p for p in personajes if p],
        "incluye": [re.sub(r"lentes de Contacto", "lentes de contacto", i) for i in incluye if i],
        "fantasia": _es_fantasia(prod),
    }


def intro_lente(d):
    marca_txt = "" if (not d["marca"] or d["marca"].lower() in d["nombre"].lower()) else f" de la marca {d['marca']}"
    color = d["color"].lower() if d["color"] else ""
    if d["fantasia"]:
        frase1 = (f"Los {d['nombre']} son lentes de contacto de fantasía"
                  f"{' de color ' + color if color else ''}{marca_txt}, ideales para cosplay, Halloween, "
                  f"maquillaje artístico y sesiones de fotos.")
    else:
        frase1 = (f"Los {d['nombre']} son lentes de contacto{' de color ' + color if color else ''}{marca_txt}, "
                  f"pensados para un look natural.")
    partes = []
    if d["diametro"]:
        n = float(d["diametro"].split()[0])
        partes.append(f"son tipo esclera de {d['diametro']}, cubren todo el ojo" if n >= 20
                      else f"miden {d['diametro']} de diámetro")
    if d["contorno"].lower().startswith("con"):
        partes.append("tienen contorno para resaltar el iris")
    elif d["contorno"].lower().startswith("sin"):
        partes.append("son sin contorno para un cambio de color más suave")
    if d["duracion"]:
        partes.append(f"duran {d['duracion']}")
    frase2 = (_lista_es(partes)[:1].upper() + _lista_es(partes)[1:] + ".") if partes else ""
    frase3 = f"Perfectos para caracterizar a {_lista_es(d['personajes'])}." if d["personajes"] else ""
    return " ".join(x for x in [frase1, frase2, d["medida"]["frase"], frase3] if x)


def descripcion_lente(d):
    filas = [("Marca", d["marca"]), ("Modelo", d["modelo"]), ("Color", d["color"]),
             ("Diámetro", d["diametro"]), ("Medida", d["medida"]["ficha"]), ("Contorno", d["contorno"]),
             ("Contenido acuoso", d["acuoso"]), ("Duración", d["duracion"][:1].upper() + d["duracion"][1:]),
             ("Código", d["sku"])]
    h = [f"<p>{_esc(intro_lente(d))}</p>",
         f"<h2>Características de los {_esc(d['titulo'])}</h2>",
         "<ul>" + "".join(f"<li><strong>{k}:</strong> {_esc(v)}</li>" for k, v in filas if v) + "</ul>"]
    if d["personajes"]:
        h += ["<h2>Ideales para cosplay de</h2>",
              "<ul>" + "".join(f"<li>{_esc(p)}</li>" for p in d["personajes"]) + "</ul>"]
    if d["incluye"]:
        h += ["<h2>¿Qué incluye la compra?</h2>",
              "<ul>" + "".join(f"<li>{_esc(i)}</li>" for i in d["incluye"]) + "</ul>"]
    h += ["<h2>Cambios y devoluciones</h2>",
          "<p>Si tu pedido llegó con algún problema, tienes 24 horas desde que lo recibes para escribirnos. "
          "El producto no debe haber sido usado. Revisa los detalles en nuestra "
          f'<a href="{URL_POLITICA}">política de cambios y devoluciones</a>.</p>']
    return "\n".join(h)


def corta_lente(d):
    tipo = "de fantasía" if d["fantasia"] else "de look natural"
    marca_modelo = d["nombre"] if (not d["marca"] or d["marca"].lower() in d["nombre"].lower()) \
        else f"{d['marca']} {d['nombre']}"
    frase1 = f"Lentes de contacto {tipo} {marca_modelo}" + (f", color {d['color'].lower()}." if d["color"] else ".")
    datos = [d["medida"]["corta"]]
    if d["diametro"]:
        datos.append(f"diámetro {d['diametro']}")
    if d["contorno"]:
        datos.append(d["contorno"].lower())
    if d["duracion"]:
        datos.append(f"duración {d['duracion']}")
    frase2 = ", ".join(datos) + "."
    frase2 = frase2[:1].upper() + frase2[1:]
    extra = "Incluye estuche. Envío a todo el Perú." if any("estuche" in i.lower() for i in d["incluye"]) \
        else "Envío a todo el Perú."
    return f"<p>{_esc(frase1 + ' ' + frase2 + ' ' + extra)}</p>"


def corta_generica(prod):
    """Lentes sin plantilla (pocos) y accesorios: datos del producto o primer párrafo de su descripción."""
    desc = prod.get("description") or ""
    sku = (prod.get("sku") or "").upper()
    if not sku.startswith("EL") and (prod.get("attributes") or []):
        color = _lista_es([c.split("–")[0].strip() for c in _atributo(prod, "Color")])
        diam = _diametro(" ".join(_atributo(prod, "Diametro")))
        contorno = " ".join(_atributo(prod, "Contorno")).lower()
        tipo = "de fantasía" if _es_fantasia(prod) else "de look natural"
        datos = ", ".join(x for x in [_medida(prod)["corta"], f"diámetro {diam}" if diam else "", contorno] if x)
        return f"<p>{_esc(f'Lentes de contacto {tipo} {_modelo_desde_nombre(prod)}' + (f', color {color.lower()}.' if color else '.') + ' ' + datos[:1].upper() + datos[1:] + '. Envío a todo el Perú.')}</p>"
    base = None
    for p in re.findall(r"<p>(.*?)</p>", desc, re.S) or [desc]:
        t = _texto(p)
        if len(t) >= 60:
            if len(t) > 240:
                frases = re.split(r"(?<=[.!?])\s+", t)
                t = ""
                for f in frases:
                    if len(t) + len(f) > 240:
                        break
                    t = (t + " " + f).strip()
                t = t or _texto(p)[:237].rsplit(" ", 1)[0] + "..."
            base = t
            break
    if not base:
        return None
    return f"<p>{_esc((base + ' ' + _opciones_accesorio(prod)).strip())}</p>"


ETIQUETAS_OPCION = {"color 2": "colores", "color": "colores", "mascota": "diseños", "forma": "diseños",
                    "gato": "diseños", "fruta": "diseños", "mirada": "diseños", "tamaño": "presentación"}


def _opciones_accesorio(prod):
    """'Disponible en colores: Blanco, Verde y Rosa.' a partir de los atributos de la variación."""
    frases = []
    for a in prod.get("attributes") or []:
        etiqueta = ETIQUETAS_OPCION.get((a.get("name") or "").lower())
        opciones = [COLORES_EN.get(o.strip().lower(), o.strip()) for o in a.get("options") or []]
        opciones = [o[:1].upper() + o[1:] for o in opciones
                    if o and o.lower() not in ("mix", "mixted", "mixto") and not re.fullmatch(r"v\d+", o.lower())]
        legibles = [o for o in opciones if re.search(r"[a-záéíóúñ]{3}", o, re.I)]
        if len(legibles) * 2 < len(opciones):  # caritas tipo ":D", "owo": no se describen
            continue
        opciones = legibles
        if etiqueta and opciones:
            if etiqueta == "presentación":
                frases.append(f"Presentación de {_lista_es(opciones)}.")
            elif len(opciones) == 1:
                frases.append(f"Disponible en {opciones[0].lower() if etiqueta == 'colores' else opciones[0]}.")
            else:
                frases.append(f"Disponible en {etiqueta}: {_lista_es(opciones)}.")
    return " ".join(frases)


def quitar_promos(desc):
    desc = re.sub(r"<h2>\s*Promociones.*?</h2>.*?(?=<h2|\Z)", "", desc, flags=re.S | re.I)
    return re.sub(r"<p>\s*<img[^>]*aplicador[^>]*>\s*</p>", "", desc, flags=re.I)


def calcular(prod):
    """Devuelve (payload, info) o (None, info) si no hay cambios."""
    desc = prod.get("description") or ""
    payload, info = {}, {"sku": prod.get("sku"), "id": prod.get("id")}
    if "Características detalladas" in desc:
        d = datos_lente(prod)
        payload["description"] = descripcion_lente(d)
        payload["short_description"] = corta_lente(d)
        info["tipo"] = "lente"
    else:
        nueva_desc = quitar_promos(desc)
        if nueva_desc != desc:
            payload["description"] = nueva_desc
        corta = corta_generica(prod)
        if corta:
            payload["short_description"] = corta
        info["tipo"] = "otro"
    for k in list(payload):
        if _texto(payload[k]) == _texto(prod.get(k)):
            del payload[k]
    return (payload or None), info


# ------------------------------------------------------------------------------
# Ejecución
# ------------------------------------------------------------------------------
def descargar_todo(wcapi):
    productos, pagina = [], 1
    while True:
        r = wcapi.get("products", params={"per_page": 100, "page": pagina, "status": "publish"})
        if r.status_code != 200:
            raise RuntimeError(f"WooCommerce respondió HTTP {r.status_code}: {r.text[:200]}")
        lote = r.json()
        if not lote:
            return productos
        productos += lote
        pagina += 1


def _enviar_lotes(wcapi, cambios, log):
    ok, errores = 0, []
    for i in range(0, len(cambios), 25):
        lote = cambios[i:i + 25]
        for intento in range(3):
            try:
                r = wcapi.post("products/batch", {"update": lote})
                if r.status_code == 200:
                    res = r.json().get("update", [])
                    ok += sum(1 for x in res if not x.get("error"))
                    errores += [(x.get("id"), x["error"].get("message")) for x in res if x.get("error")]
                    break
                log(f"  lote {i // 25 + 1}: HTTP {r.status_code}, reintento")
            except Exception as e:  # red / timeout
                log(f"  lote {i // 25 + 1}: {e}, reintento")
        else:
            errores += [(c["id"], "lote fallido") for c in lote]
        log(f"  {min(i + 25, len(cambios))}/{len(cambios)}")
    return ok, errores


def ejecutar(aplicar=False, skus=None, log=print):
    wcapi = conectar_woo()
    productos = descargar_todo(wcapi)
    if skus:
        productos = [p for p in productos if (p.get("sku") or "").upper() in skus]
    cambios, respaldo, tipos = [], [], {}
    for p in productos:
        payload, info = calcular(p)
        if not payload:
            continue
        tipos[info["tipo"]] = tipos.get(info["tipo"], 0) + 1
        cambios.append({"id": p["id"], **payload})
        respaldo.append({"id": p["id"], "sku": p.get("sku"), "description": p.get("description"),
                         "short_description": p.get("short_description")})
    log(f"📦 {len(productos)} productos revisados, {len(cambios)} con cambios {tipos}")
    if skus or not aplicar:
        for c in cambios[: (len(cambios) if skus else 3)]:
            log(f"\n===== {next(r['sku'] for r in respaldo if r['id'] == c['id'])}")
            for k in ("short_description", "description"):
                if k in c:
                    log(f"--- {k}:\n{c[k]}")
    if not aplicar:
        log("\n(simulación: no se escribió nada; usa --aplicar)")
        return cambios
    os.makedirs(os.path.join(RAIZ, "respaldos_web"), exist_ok=True)
    ruta = os.path.join(RAIZ, "respaldos_web", f"descripciones_{datetime.now():%Y%m%d_%H%M%S}.json")
    with open(ruta, "w", encoding="utf-8") as fh:
        json.dump(respaldo, fh, ensure_ascii=False)
    log(f"💾 respaldo: {ruta}")
    ok, errores = _enviar_lotes(wcapi, cambios, log)
    log(f"✅ {ok} actualizados, {len(errores)} errores {errores[:5]}")
    return cambios


def restaurar(ruta, log=print):
    wcapi = conectar_woo()
    datos = json.load(open(ruta, encoding="utf-8"))
    cambios = [{"id": d["id"], "description": d["description"], "short_description": d["short_description"]}
               for d in datos]
    ok, errores = _enviar_lotes(wcapi, cambios, log)
    log(f"↩️ {ok} restaurados, {len(errores)} errores")


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--restaurar" in args:
        restaurar(args[args.index("--restaurar") + 1])
    else:
        skus = None
        if "--sku" in args:
            skus = {s.strip().upper() for s in args[args.index("--sku") + 1].split(",") if s.strip()}
        ejecutar(aplicar="--aplicar" in args, skus=skus)
