"""
🖼️ Genera la imagen de producto con el formato de kmlentes.pe (1500×1500):
   marco azul marino redondeado · título del modelo arriba · etiqueta con el diámetro · código KM abajo a la izquierda.

Toma la "Mejor foto" de documentos/codigos_propuestos.xlsx (solo filas con "Fotos aptas" = Sí) desde
/home/panel-control/catalogo_proveedor/ y guarda en /home/panel-control/imagenes_km/<SKU>_1.webp

Recorte a cuadrado:
  · foto cuadrada → se usa entera
  · foto vertical u horizontal → cuadrado centrado en los ojos (OpenCV: cara o ojos);
    si no detecta nada, a la altura típica de los ojos (40 % del alto). AJUSTE_X/Y corrige a mano.

Uso:
    .venv/bin/python recurrente/generar_imagenes.py NL193A NL240B     # solo esos SKUs
    .venv/bin/python recurrente/generar_imagenes.py --todos
"""
import os
import sys

from openpyxl import load_workbook
from PIL import Image, ImageDraw, ImageFilter, ImageFont

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROPUESTA = os.path.join(_raiz, "documentos", "codigos_propuestos.xlsx")
FOTOS = "/home/panel-control/catalogo_proveedor"
SALIDA = "/home/panel-control/imagenes_km"
FUENTES = os.path.join(_raiz, "documentos", "fuentes")
MODELOS = os.path.join(_raiz, "documentos", "modelos_cv")

LADO = 1500
K = LADO / 800                      # medidas tomadas de las fotos actuales (800 px)
NAVY = (31, 56, 100)
NAVY_TAG = (30, 77, 121)
BLANCO = (255, 255, 255)

# Centro del recorte (0–1) por SKU, cuando el automático no centra bien los ojos
AJUSTE_Y = {}
AJUSTE_X = {}


def fuente(peso, tam):
    return ImageFont.truetype(os.path.join(FUENTES, f"Roboto-{peso}.ttf"), int(tam))


def centro_ojos(im):
    """(x, y) relativo (0–1) del centro de los ojos con OpenCV, o None si no encuentra."""
    try:
        import cv2
        import numpy as np
    except ImportError:
        return None
    w, h = im.size
    esc = 800 / max(w, h)
    gris = cv2.cvtColor(np.array(im.resize((int(w * esc), int(h * esc)))), cv2.COLOR_RGB2GRAY)
    gris = cv2.equalizeHist(gris)
    caras = cv2.CascadeClassifier(os.path.join(MODELOS, "haarcascade_frontalface_default.xml")) \
        .detectMultiScale(gris, 1.1, 6, minSize=(80, 80))
    if len(caras):
        x, y, cw, ch = max(caras, key=lambda c: c[2] * c[3])          # la cara más grande
        return ((x + cw / 2) / gris.shape[1], (y + ch * 0.42) / gris.shape[0])
    ojos = cv2.CascadeClassifier(os.path.join(MODELOS, "haarcascade_eye.xml")) \
        .detectMultiScale(gris, 1.1, 8, minSize=(40, 40))
    if len(ojos):
        ojos = sorted(ojos, key=lambda o: -o[2] * o[3])[:2]          # los dos ojos más grandes
        cx = sum(o[0] + o[2] / 2 for o in ojos) / len(ojos)
        cy = sum(o[1] + o[3] / 2 for o in ojos) / len(ojos)
        return (cx / gris.shape[1], cy / gris.shape[0])
    return None


def recortar(im, sku):
    w, h = im.size
    if abs(w - h) / max(w, h) < 0.04:          # casi cuadrada: se estira un poco, sin recortar
        return im.resize((LADO, LADO), Image.LANCZOS), "entera"
    lado = min(w, h)
    ojos = centro_ojos(im)
    modo = "ojos" if ojos else "fija"
    fx, fy = ojos or (0.5, 0.40)
    fx, fy = AJUSTE_X.get(sku, fx), AJUSTE_Y.get(sku, fy)
    if sku in AJUSTE_X or sku in AJUSTE_Y:
        modo = "manual"
    x = int(min(max(fx * w - lado / 2, 0), w - lado))
    y = int(min(max(fy * h - lado / 2, 0), h - lado))
    return im.crop((x, y, x + lado, y + lado)).resize((LADO, LADO), Image.LANCZOS), modo


def componer(foto, sku, titulo, diametro):
    lienzo = Image.new("RGB", (LADO, LADO), BLANCO)
    m, borde, radio = int(8 * K), int(4 * K), int(14 * K)
    # foto dentro de un rectángulo redondeado con borde azul
    interior = (m + borde, m + borde, LADO - m - borde, LADO - m - borde)
    iw, ih = interior[2] - interior[0], interior[3] - interior[1]
    foto = foto.resize((iw, ih), Image.LANCZOS)
    mascara = Image.new("L", (iw, ih), 0)
    ImageDraw.Draw(mascara).rounded_rectangle((0, 0, iw - 1, ih - 1), radio - borde, fill=255)
    d = ImageDraw.Draw(lienzo)
    d.rounded_rectangle((m, m, LADO - m, LADO - m), radio, fill=NAVY)
    lienzo.paste(foto, interior[:2], mascara)

    # título: recuadro blanco arriba al centro, letra azul cursiva gruesa
    f = fuente("BoldItalic", 34 * K)
    while d.textlength(titulo, font=f) > 440 * K and f.size > 26 * K:
        f = fuente("BoldItalic", f.size - 2)
    tw = d.textlength(titulo, font=f)
    bw, bh = max(tw + 60 * K, 300 * K), 52 * K
    bx, by = (LADO - bw) / 2, 18 * K
    sombra = Image.new("L", (LADO, LADO), 0)
    ImageDraw.Draw(sombra).rounded_rectangle((bx, by + 3 * K, bx + bw, by + bh + 3 * K), 8 * K, fill=90)
    lienzo.paste((0, 0, 0), (0, 0), sombra.filter(ImageFilter.GaussianBlur(6 * K)))
    d = ImageDraw.Draw(lienzo)
    d.rounded_rectangle((bx, by, bx + bw, by + bh), 8 * K, fill=BLANCO)
    # borde punteado gris claro, como en las fotos actuales
    paso, g = 6 * K, (200, 200, 200)
    x = bx + 8 * K
    while x < bx + bw - 8 * K:
        d.line((x, by + 2, min(x + paso / 2, bx + bw - 8 * K), by + 2), fill=g, width=2)
        d.line((x, by + bh - 2, min(x + paso / 2, bx + bw - 8 * K), by + bh - 2), fill=g, width=2)
        x += paso
    d.text((LADO / 2, by + bh / 2), titulo, font=f, fill=NAVY, anchor="mm")

    # etiqueta del diámetro, pegada abajo a la derecha del título
    if diametro:
        t = f"{float(diametro):.2f} mm"
        ft = fuente("Medium", 11.5 * K)
        tw2 = d.textlength(t, font=ft)
        tx2, ty = bx + bw - tw2 - 24 * K, by + bh - 8 * K
        d.rounded_rectangle((tx2, ty, tx2 + tw2 + 18 * K, ty + 17 * K), 8.5 * K, fill=NAVY_TAG)
        d.text((tx2 + 9 * K + tw2 / 2, ty + 8.5 * K), t, font=ft, fill=BLANCO, anchor="mm")

    # código KM abajo a la izquierda
    fc = fuente("Medium", 17 * K)
    cw = d.textlength(sku, font=fc)
    cx, cy = 24 * K, 742 * K
    d.rounded_rectangle((cx, cy, cx + cw + 20 * K, cy + 29 * K), 5 * K, fill=BLANCO, outline=NAVY, width=int(1.5 * K))
    d.text((cx + 10 * K + cw / 2, cy + 14.5 * K), sku, font=fc, fill=NAVY, anchor="mm")
    return lienzo


def filas():
    ws = load_workbook(PROPUESTA, read_only=True, data_only=True).worksheets[0]
    datos = list(ws.iter_rows(values_only=True))
    cab = list(datos[0])
    for r in datos[1:]:
        yield dict(zip(cab, r))


def main():
    pedidos = {a for a in sys.argv[1:] if not a.startswith("--")}
    os.makedirs(SALIDA, exist_ok=True)
    hechos = set()
    for f in filas():
        sku = f["SKU KM propuesto"]
        if not sku or sku in hechos or f["Fotos aptas"] != "Sí" or not f["Mejor foto"]:
            continue
        if pedidos and sku not in pedidos:
            continue
        hechos.add(sku)
        titulo = " ".join(w.capitalize() if w.isupper() and len(w) > 3 else w[:1].upper() + w[1:]
                          for w in f"{f['Modelo KM']} {f['Color KM']}".replace("Dna2.0", "DNA 2.0").split())
        im = Image.open(os.path.join(FOTOS, f["Mejor foto"])).convert("RGB")
        foto, modo = recortar(im, sku)
        out = componer(foto, sku, titulo, f["Diámetro (mm)"])
        ruta = os.path.join(SALIDA, f"{sku}_1.webp")
        out.save(ruta, "WEBP", quality=88)
        print("✅", ruta, f"(recorte: {modo})")
    print(f"{len(hechos)} imágenes")


if __name__ == "__main__":
    main()
