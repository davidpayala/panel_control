"""
🏷️ Propone códigos KM para los modelos del proveedor que aún no existen en el panel.

Regla de códigos de lentes naturales:  NL + número (3 cifras) + letra
    número = el modelo (FreshLady Russian = 032)      letra = el color (A, B, C…)
    NL032A Russian Blue · NL032B Russian Gray · NL032C Russian Brown

Para cada modelo de proveedor_no_mapeados decide una acción:
    VINCULAR      el mismo modelo y color ya existe en el panel → solo hay que conectarlo (no se crea nada)
    NUEVO COLOR   el modelo ya existe con número NL → se usa ese número y la siguiente letra libre
    MODELO NUEVO  el modelo no existe → número nuevo (después del más alto usado) y letras A, B, C…
FreshLady y Meetone son independientes: el número es por MARCA + modelo (Sorayama FreshLady y Sorayama
Meetone llevan números distintos) y solo se vincula con productos de la misma marca.
El estado "Freshlady" de la lista Meetone (sugerencia de un similar) se trata como AGOTADO.
Se fusiona con documentos/catalogo_proveedor.xlsx (diámetro, imágenes y carpeta de Drive de cada modelo).
Los modelos se agrupan por nombre (tolerando errores: Roccoco = Rococo) y por familia de código del
proveedor (FA37-1 y FA37-2 son el mismo modelo).  Nunca propone un SKU que ya exista en Variantes.

No modifica la base de datos: solo genera el Excel de propuesta para revisar.

Uso:
    .venv/bin/python recurrente/proponer_codigos.py [--salida=documentos/codigos_propuestos.xlsx]
"""
import os
import re
import sys
import difflib
from collections import defaultdict

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from dotenv import load_dotenv
load_dotenv(os.path.join(_raiz, ".env"))

from sqlalchemy import text
from database import engine

LETRAS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
ORDEN_COLOR = {"1": 0, "a": 1, "2": 2, "3": 3, "4": 4, "5": 5}  # brown, hazel/coffee, green, blue, violet, gray


def norm(s):
    s = re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())
    return re.sub(r"\s+", " ", s).strip()


def sin_dobles(s):
    return re.sub(r"(.)\1+", r"\1", s)


def parecido(a, b):
    a, b = sin_dobles(norm(a)), sin_dobles(norm(b))
    return a == b or difflib.SequenceMatcher(None, a, b).ratio() >= 0.92


def familia(codigo):
    """FA37-1 → FA37 · FD92-3-1 → FD92 · FA-39-2 → FA39. Códigos sin '-color' (ME22, JY744) no forman familia."""
    c = (codigo or "").upper().replace("FA-", "FA").split("--")[0]
    m = re.match(r"^((?:FA|FD|FR|C|MI|BL)\d+)-\w", c)
    return m.group(1) if m else None


def color_km(modelo, color):
    """'CALIFORNIA BLUE' del modelo 'California' → 'Blue'.  Quita la 'm' final (versión con medida)."""
    c = re.sub(r"\s+m$", "", (color or "").strip(), flags=re.I)
    for palabra in norm(modelo).split():
        primera = (c.split() or [""])[0]
        if norm(primera) == palabra or (len(palabra) > 3 and difflib.SequenceMatcher(
                None, sin_dobles(norm(primera)), sin_dobles(palabra)).ratio() >= 0.75):
            c = c[len(primera):].strip()
    c = c.strip(" -") or color.strip()
    return c[:1].upper() + c[1:].lower() if c.isupper() or c.islower() else c


def orden_color(codigo):
    m = re.search(r"-(\w)", (codigo or "").replace("FA-", "FA").split("--")[0])
    return ORDEN_COLOR.get(m.group(1).lower(), 9) if m else 9


COLORES = {"brown", "gray", "grey", "hazel", "green", "blue", "violet", "black", "m"}
PREFIJO_LENTE = re.compile(r"^(NL|FL|KL|CL|SL)(\d+)([A-Z]?)$")
# Nombres del proveedor que en el panel son otro modelo/color
ALIAS = {"x honey flare": ("X Flare", "Honey")}


def norm_marca(m):
    return norm(m).replace(" ", "")


def nombre_modelo(f):
    """Modelo del proveedor sin colores pegados: 'PLUM GLOW BROWN' → 'Plum Glow'; sin modelo → del color."""
    base = f.modelo or f.color or ""
    palabras = base.split()
    while len(palabras) > 1 and norm(palabras[-1]) in COLORES:
        palabras.pop()
    return " ".join(palabras)


def main():
    salida = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--salida=")),
                  os.path.join(_raiz, "documentos", "codigos_propuestos.xlsx"))

    with engine.connect() as c:
        variantes = c.execute(text("""
            SELECT v.sku, p.marca, p.modelo, v.nombre_variante
            FROM Variantes v JOIN productos p USING (id_producto)""")).fetchall()
        sin = c.execute(text("""
            SELECT tipo, codigo, modelo, color, estado FROM proveedor_no_mapeados
            WHERE tipo IN ('freshlady', 'meetone') ORDER BY tipo, codigo, modelo, color""")).fetchall()
        mapeo_cod = {fam: sku for fam, sku in c.execute(text(
            "SELECT upper(codigo), sku FROM proveedor_mapeo WHERE tipo = 'medida' AND codigo <> ''"))}

    todos_skus = {v.sku for v in variantes}
    numeros_nl = {int(m.group(1)) for s in todos_skus for m in [re.match(r"^NL(\d+)", s)] if m}
    siguiente = max(numeros_nl) + 1

    # Modelos de lentes del panel (NL, FL, KL, CL, SL y los antiguos NL-MC)
    panel = {}
    for v in variantes:
        if not re.match(r"^(NL|FL|KL|CL|SL)", v.sku):
            continue
        base = re.sub(r"-\d{4}$", "", v.sku)   # NL191A-0125 → NL191A (las letras con medida también cuentan)
        clave = (norm_marca(v.marca), sin_dobles(norm(v.modelo)))
        d = panel.setdefault(clave, {"modelo": v.modelo, "serie": None, "letras": set(),
                                     "colores": {}, "por_marca": {}, "sin_color": []})
        col = norm(color_km(v.modelo, v.nombre_variante or ""))
        if col:
            d["colores"].setdefault(col, base)
            d["por_marca"].setdefault((norm(v.marca), col), base)
        elif base not in d["sin_color"]:
            d["sin_color"].append(base)
        m = PREFIJO_LENTE.match(base)
        if m and m.group(3):
            serie = (m.group(1), int(m.group(2)))
            if d["serie"] is None or serie == d["serie"]:
                d["serie"] = serie
                d["letras"].add(m.group(3))

    def buscar_panel(marca, nombre):
        clave = (norm_marca(marca), sin_dobles(norm(nombre)))
        return panel.get(clave) or next((d for k, d in panel.items() if k[0] == clave[0] and parecido(k[1], clave[1])), None)

    # Agrupar filas del proveedor en modelos (nombre parecido o misma familia de código)
    grupos = []
    for f in sin:
        nombre, alias_color = nombre_modelo(f), None
        for k, (mod, col) in ALIAS.items():
            if k in norm(f"{f.modelo} {f.color}"):
                nombre, alias_color = mod, col
        fam = None if alias_color else familia(f.codigo)
        marca_f = "FreshLady" if f.tipo == "freshlady" else "Meetone"
        g = next((g for g in grupos if g["marca"] == marca_f
                  and (parecido(g["nombre"], nombre) or (fam and fam in g["familias"]))), None)
        if not g:
            g = {"marca": marca_f, "nombre": nombre, "familias": set(), "filas": []}
            grupos.append(g)
        if fam and not alias_color:
            g["familias"].add(fam)
        g["filas"].append((f, alias_color))

    propuestas = []
    for g in sorted(grupos, key=lambda g: (g["marca"], norm(g["nombre"]))):
        existente = buscar_panel(g["marca"], g["nombre"])
        serie = existente["serie"] if existente else None
        letras = set(existente["letras"]) if existente else set()
        colores_dados = {}
        for f, alias_color in sorted(g["filas"], key=lambda x: (orden_color(x[0].codigo), norm(x[0].color))):
            col = alias_color or color_km(g["nombre"], f.color)
            marca = "FreshLady" if f.tipo == "freshlady" else "Meetone"
            nota = ""
            estado = "AGOTADO" if (f.estado or "").lower() in ("freshlady", "ver en meetone") else f.estado
            fila = dict(lista=marca, codigo=f.codigo, modelo_prov=f.modelo, color_prov=f.color, estado=estado,
                        modelo_km=existente["modelo"] if existente else g["nombre"].strip().title(),
                        color_km=col, accion="", sku="", nota=nota)
            cod_base = (f.codigo or "").upper().split("--")[0]
            ya = None
            if existente:
                ya = existente["por_marca"].get((norm(marca), norm(col)))

            if marca == "FreshLady" and cod_base in mapeo_cod:   # la tabla con medida es toda FreshLady
                fila.update(accion="VINCULAR", sku=mapeo_cod[cod_base],
                            nota=fila["nota"] + f"Mismo código de proveedor que la tabla con medida ({cod_base}).")
            elif ya:
                fila.update(accion="VINCULAR", sku=ya, nota=fila["nota"] + f"Ya existe en el panel: {existente['modelo']} {col}.")
            elif (marca, norm(col)) in colores_dados:
                fila.update(accion="VINCULAR", sku=colores_dados[(marca, norm(col))],
                            nota=fila["nota"] + "El proveedor repite este color; usa el mismo código nuevo.")
            else:
                if serie is None:
                    while any(s.startswith(f"NL{siguiente:03d}") for s in todos_skus):
                        siguiente += 1
                    serie = ("NL", siguiente)
                    siguiente += 1
                    if existente:
                        viejos = sorted(set(existente["colores"].values()) | set(existente["sin_color"]))
                        fila["nota"] += (f"El modelo {existente['modelo']} ya existe con códigos sin letra ({', '.join(viejos[:4])}); "
                                 "se abre número nuevo. ")
                pref, num = serie
                letra = next(l for l in LETRAS if l not in letras
                             and not any(s == f"{pref}{num:03d}{l}" or s.startswith(f"{pref}{num:03d}{l}-") for s in todos_skus))
                letras.add(letra)
                sku = f"{pref}{num:03d}{letra}"
                colores_dados[(marca, norm(col))] = sku
                fila.update(accion="NUEVO COLOR" if existente and existente["serie"] else "MODELO NUEVO", sku=sku)
                if existente and existente["sin_color"]:
                    fila["nota"] += (f"Ojo: en el panel {', '.join(existente['sin_color'][:4])} no tiene color; "
                                     "revisa si es este mismo. ")
            propuestas.append(fila)

    fusionar_catalogo(propuestas)
    guardar(propuestas, salida)
    resumen = defaultdict(int)
    for p in propuestas:
        resumen[p["accion"]] += 1
    nuevos = sorted({p["sku"][:5] for p in propuestas if p["accion"] == "MODELO NUEVO"})
    print(f"📄 {len(propuestas)} filas → {salida}")
    print("   " + " · ".join(f"{k}: {v}" for k, v in sorted(resumen.items())))
    if nuevos:
        print(f"   Números nuevos: {nuevos[0]} a {nuevos[-1]} ({len(nuevos)} modelos)")


def fusionar_catalogo(propuestas):
    """Agrega diámetro, imágenes y carpeta de Drive desde documentos/catalogo_proveedor.xlsx."""
    ruta = os.path.join(_raiz, "documentos", "catalogo_proveedor.xlsx")
    if not os.path.exists(ruta):
        print("⚠️ No está documentos/catalogo_proveedor.xlsx: la propuesta sale sin diámetros ni imágenes")
        return
    from openpyxl import load_workbook
    ws = load_workbook(ruta, read_only=True, data_only=True).worksheets[0]
    filas = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]
    cab = [str(c) for c in filas[0]]
    cat, por_color = {}, {}
    sin_m = lambda c: re.sub(r" m$", "", norm(c))   # 'GLIMMER BROWN M' = 'GLIMMER BROWN'
    for r in filas[1:]:
        d = dict(zip(cab, r))
        clave = (norm(d["Marca"]), norm(d["Modelo"]), norm(d["Color"]))
        cat.setdefault(clave, d)
        por_color.setdefault((norm(d["Marca"]), sin_m(d["Color"])), d)
    sin_cat = 0
    for p in propuestas:
        d = (cat.get((norm(p["lista"]), norm(p["modelo_prov"]), norm(p["color_prov"])))
             or por_color.get((norm(p["lista"]), sin_m(p["color_prov"]))))
        if not d:
            sin_cat += 1
            continue
        diam = str(d.get("Diametro (mm)") or "")
        m = re.match(r"([\d.]+)\s*mm", diam)
        p["diametro"] = float(m.group(1)) if m else None
        p["diam_estado"] = "Confirmado" if m else diam
        p["fuente_diam"] = d.get("Fuentes Diametro") or ""
        p["img_alto"], p["img_regular"], p["img_bajo"] = (int(d.get(k) or 0) for k in ("1_alto_valor", "2_regular_valor", "3_bajo_valor"))
        p["carpeta"] = str(d.get("Carpeta") or "").replace("\\", "/")
    if sin_cat:
        print(f"⚠️ {sin_cat} filas no se encontraron en el catálogo de Drive")


def guardar(propuestas, ruta):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.worksheet.datavalidation import DataValidation

    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "Propuesta"
    cols = [("lista", "Lista proveedor", 11), ("codigo", "Código proveedor", 16), ("modelo_prov", "Modelo proveedor", 18),
            ("color_prov", "Color proveedor", 26), ("estado", "Estado", 13), ("accion", "Acción", 15),
            ("sku", "SKU KM propuesto", 15), ("modelo_km", "Modelo KM", 18), ("color_km", "Color KM", 20),
            ("diametro", "Diámetro (mm)", 12), ("diam_estado", "Diámetro estado", 15),
            ("img_alto", "Imágenes alto valor", 10), ("img_regular", "Imágenes regular", 10), ("img_bajo", "Imágenes bajo", 10),
            ("carpeta", "Carpeta Drive (imágenes)", 40), ("fuente_diam", "Fuente diámetro", 40),
            ("nota", "Nota", 70)]
    ws.append([c[1] for c in cols])
    for p in propuestas:
        ws.append([p.get(c[0], "") for c in cols])
    azul = PatternFill("solid", fgColor="1F3864")
    colores = {"MODELO NUEVO": "E2EFDA", "NUEVO COLOR": "DDEBF7", "VINCULAR": "FFF2CC", "NO CREAR": "EDEDED"}
    for i, (_, _, ancho) in enumerate(cols, 1):
        ws.column_dimensions[ws.cell(1, i).column_letter].width = ancho
        ws.cell(1, i).font = Font(bold=True, color="FFFFFF")
        ws.cell(1, i).fill = azul
    for r in range(2, ws.max_row + 1):
        fill = PatternFill("solid", fgColor=colores.get(ws.cell(r, 6).value, "FFFFFF"))
        for c in range(1, len(cols) + 1):
            ws.cell(r, c).fill = fill
            ws.cell(r, c).alignment = Alignment(vertical="top", wrap_text=(c == len(cols)))
        ws.cell(r, 7).font = Font(bold=True)
    dv = DataValidation(type="list", formula1='"MODELO NUEVO,NUEVO COLOR,VINCULAR,NO CREAR"')
    ws.add_data_validation(dv)
    dv.add(f"F2:F{ws.max_row}")
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    wb.save(ruta)


if __name__ == "__main__":
    main()
