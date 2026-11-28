"""🎄 Plantilla «Navidad» — carrusel de lentes con la promo de packs (aprobada por el usuario el 27-nov-2026).

Escenas:
  1. Portada (3 s): ¡Feliz Navidad! + frase + kmlentes.pe
  2. Una foto por producto (2 s c/u por defecto): encabezado, foto con zoom, nombre, color, precio,
     «Envío gratis* · Contraentrega*»
  3. Packs (4 s): precios de la promo (tabla ajustes → promo_lentes). Se omite si la promo no está vigente.
  4. Cierre (3 s): envío, contraentrega, estuche, WhatsApp, web.
Solo cambian las fotos/productos; los textos fijos son los aprobados.
"""
import datetime
from PIL import Image, ImageDraw, ImageFilter

from videos import musica as musica_mod
from videos.motor import (FORMATOS, Nieve, ajustar_fuente, ease, fondo_degradado, mont,
                          pastilla, renderizar, script, texto_c)

NOMBRE = 'Navidad'
DESCRIPCION = 'Fondo rojo con nieve, ¡Feliz Navidad!, fotos de 2 s con precio y envío, packs de la promo y cierre.'

ROJO1, ROJO2 = (150, 16, 32), (70, 6, 16)
ORO = (244, 200, 90)
BLANCO = (255, 255, 255)
VERDE = (24, 110, 60)
ROSA = (255, 225, 225)
MESES = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre',
         'noviembre', 'diciembre']

T_INTRO, T_PACKS, T_CIERRE = 3.0, 4.0, 3.0


def soles(x):
    x = float(x)
    return f'S/{x:.0f}' if x == int(x) else f'S/{x:.2f}'


def vigente(hoy=None):
    """La plantilla dice «¡Feliz Navidad!»: se usa del 1-nov al 26-dic."""
    hoy = hoy or datetime.date.today()
    return (11, 1) <= (hoy.month, hoy.day) <= (12, 26)


def promo_vigente(promo, hoy=None):
    if not promo or not promo.get('activo'):
        return False
    hoy = (hoy or datetime.date.today()).isoformat()
    return (promo.get('desde') or '0000') <= hoy <= (promo.get('hasta') or '9999')


def generar(productos, salida, formato='vertical', segundos_foto=2.0, texto_modelos='Tenemos cientos de modelos naturales',
            promo=None, capturas=None, ruta_audio=None, musica=None):
    """productos: lista de dicts {imagen: PIL.Image, nombre, color, precio, encabezado}.
    musica: clave de videos.musica.CANCIONES (None = aleatoria).
    Devuelve {'duracion': s, 'musica': clave}."""
    W, H = FORMATOS[formato]
    S = W / 1080
    V = formato == 'vertical'
    n = len(productos)
    hay_packs = promo_vigente(promo)
    t_prod_fin = T_INTRO + segundos_foto * n
    t_cierre = t_prod_fin + (T_PACKS if hay_packs else 0)
    duracion = t_cierre + T_CIERRE

    fondo = fondo_degradado(W, H, ROJO1, ROJO2, marco=ORO).convert('RGBA')
    nieve = Nieve(W, H)

    LADO = int(820 * S) if V else int(640 * S)
    fotos = [p['imagen'].resize((int(LADO * 1.08),) * 2, Image.LANCZOS) for p in productos]
    mascara = Image.new('L', (LADO, LADO), 0)
    ImageDraw.Draw(mascara).rounded_rectangle([0, 0, LADO - 1, LADO - 1], radius=int(36 * S), fill=255)
    sombra = Image.new('RGBA', (LADO + 80, LADO + 80), (0, 0, 0, 0))
    ImageDraw.Draw(sombra).rounded_rectangle([40, 50, LADO + 40, LADO + 50], radius=int(40 * S), fill=(0, 0, 0, 140))
    sombra = sombra.filter(ImageFilter.GaussianBlur(22))

    def lienzo(t):
        img = fondo.copy()
        nieve.dibujar(img, t)
        return img, ImageDraw.Draw(img)

    def nota(dd):
        texto_c(dd, W, H - (70 if V else 50) * S, '*Consulta condiciones y cobertura', mont(26 * S, 'Medium'),
                fill=(255, 215, 215), sombra=False)

    def intro(t):
        img, dd = lienzo(t)
        a = ease(t / .8)
        cy = H * .40 if V else H * .38
        texto_c(dd, W, cy - 40 * (1 - a), '¡Feliz Navidad!', script(124 * S), fill=ORO)
        if t > .6:
            b = ease((t - .6) / .7)
            texto_c(dd, W, cy + 170 * S + 30 * (1 - b), 'Dale brillo a tu mirada', mont(56 * S))
            texto_c(dd, W, cy + 240 * S + 30 * (1 - b), '¡Pide los tuyos hoy!', mont(56 * S, 'ExtraBold'), fill=ORO)
        texto_c(dd, W, H - (150 if V else 110) * S, 'kmlentes.pe', mont(40 * S, 'SemiBold'))
        return img

    def producto(t, i):
        img, dd = lienzo(t + 10)
        p = productos[i]
        lt = t - (T_INTRO + i * segundos_foto)
        top = int(220 * S) if V else int(100 * S)
        enc = p.get('encabezado') or 'Lentes naturales'
        texto_c(dd, W, top, enc, ajustar_fuente(dd, enc, script, int(76 * S if V else 64 * S), W * .84), fill=ORO)
        z = 1.0 + .06 * (lt / segundos_foto)
        c = int(LADO * z)
        f = fotos[i].resize((c, c), Image.BILINEAR)
        off = (c - LADO) // 2
        f = f.crop((off, off, off + LADO, off + LADO))
        dx = int((1 - ease(lt / .35)) * W * .6)
        x0 = (W - LADO) // 2 + dx
        y0 = int(340 * S) if V else int(165 * S)
        img.alpha_composite(sombra, (x0 - 40, y0 - 40))
        capa = Image.new('RGBA', (LADO, LADO))
        capa.paste(f, (0, 0))
        capa.putalpha(mascara)
        img.alpha_composite(capa, (x0, y0))
        dd = ImageDraw.Draw(img)
        dd.rounded_rectangle([x0 - 4, y0 - 4, x0 + LADO + 4, y0 + LADO + 4], radius=int(38 * S), outline=BLANCO, width=6)
        nombre, color, precio = p.get('nombre') or '', p.get('color') or '', soles(p['precio'])
        if V:
            yb = y0 + LADO + 110 * S
            texto_c(dd, W, yb, nombre, ajustar_fuente(dd, nombre, lambda s: mont(s, 'ExtraBold'), int(54 * S), W * .86))
            if color:
                texto_c(dd, W, yb + 72 * S, color, ajustar_fuente(dd, color, lambda s: mont(s, 'Medium'), int(46 * S), W * .86), fill=ROSA)
            pastilla(dd, W / 2, yb + 220 * S, precio, mont(72 * S, 'Black'), ORO, ROJO2, 60, 12, S)
            texto_c(dd, W, yb + 340 * S, 'Envío gratis*  ·  Contraentrega*', mont(42 * S, 'Bold'))
        else:
            yb = y0 + LADO + 70 * S
            t2 = nombre + (f' · {color}' if color else '')
            texto_c(dd, W, yb, t2, ajustar_fuente(dd, t2, lambda s: mont(s, 'ExtraBold'), int(44 * S), W * .86))
            pastilla(dd, W / 2, yb + 88 * S, f'{precio}  ·  Envío gratis*  ·  Contraentrega*', mont(36 * S, 'ExtraBold'), ORO, ROJO2, S=S)
        nota(dd)
        return img

    filas = []
    if hay_packs:
        filas = [('1 par', soles(promo.get('precio_base', 55)))]
        for pk in promo.get('packs', []):
            filas.append((f"{pk['n']} pares", soles(pk['precio'])))
        hasta = datetime.date.fromisoformat(promo['hasta'])
        texto_hasta = f'Válido hasta el {hasta.day} de {MESES[hasta.month - 1]}'

    def packs(t):
        img, dd = lienzo(t + 30)
        lt = t - t_prod_fin
        top = H * .16 if V else H * .15
        texto_c(dd, W, top, 'Promo Navidad', script(120 * S), fill=ORO)
        texto_c(dd, W, top + 120 * S, texto_modelos,
                ajustar_fuente(dd, texto_modelos, lambda s: mont(s, 'ExtraBold'), int(44 * S if V else 38 * S), W * .86))
        texto_c(dd, W, top + (185 if V else 170) * S, '¡Combínalos y arma tu pack navideño!', mont(40 * S if V else 34 * S, 'Medium'), fill=ROSA)
        alto = 230 * S if V else 118 * S
        y = H * .36 if V else H * .345
        for k, (a, pr) in enumerate(filas):
            e = ease((lt - k * .25) / .4)
            if e <= 0:
                continue
            cw, cx = W * .78, W / 2 + (1 - e) * W
            yy = y + k * (alto + 40 * S)
            destacado = k == len(filas) - 1 and len(filas) > 1
            dd.rounded_rectangle([cx - cw / 2, yy, cx + cw / 2, yy + alto], radius=int(34 * S),
                                 fill=ORO if destacado else BLANCO, outline=ORO, width=5)
            dd.text((cx - cw / 2 + 60 * S, yy + alto / 2), a, font=mont(64 * S if V else 54 * S, 'ExtraBold'), fill=ROJO2, anchor='lm')
            dd.text((cx + cw / 2 - 60 * S, yy + alto / 2), pr, font=mont(96 * S if V else 76 * S, 'Black'), fill=(180, 20, 40), anchor='rm')
            if destacado:
                pastilla(dd, cx + cw / 2 - 90 * S, yy - 6 * S, 'MEJOR PRECIO', mont(26 * S, 'ExtraBold'), VERDE, BLANCO, 20, 10, S)
        pastilla(dd, W / 2, H - (330 if V else 175) * S, 'Envío gratis*  ·  Contraentrega*', mont(40 * S if V else 34 * S, 'ExtraBold'), BLANCO, ROJO2, S=S)
        texto_c(dd, W, H - (220 if V else 112) * S, texto_hasta, mont(40 * S if V else 34 * S, 'SemiBold'))
        nota(dd)
        return img

    def cierre(t):
        img, dd = lienzo(t + 50)
        lt = t - t_cierre
        y = H * .20 if V else H * .17
        texto_c(dd, W, y, 'KM Lentes', script(130 * S), fill=ORO)
        lineas = ['Envío GRATIS a todo el Perú*', 'Contraentrega: pagas al recibir*', 'Estuche de regalo con cada par']
        paso = 120 * S if V else 95 * S
        y2 = y + (230 if V else 170) * S
        for k, l in enumerate(lineas):
            e = ease((lt - k * .2) / .4)
            if e > 0:
                texto_c(dd, W, y2 + k * paso + 20 * (1 - e), l, mont(50 * S if V else 44 * S, 'Bold'))
        if lt > .8:
            yb = y2 + 3 * paso + (120 if V else 50) * S
            pastilla(dd, W / 2, yb, 'Pide por WhatsApp', mont(50 * S, 'ExtraBold'), (37, 211, 102), BLANCO, S=S)
            texto_c(dd, W, yb + 120 * S, '936 041 531', mont(78 * S, 'Black'))
            texto_c(dd, W, yb + 205 * S, 'kmlentes.pe', mont(46 * S, 'SemiBold'), fill=ORO)
        nota(dd)
        return img

    def cuadro(t):
        if t < T_INTRO:
            return intro(t)
        if t < t_prod_fin:
            return producto(t, min(n - 1, int((t - T_INTRO) // segundos_foto)))
        if t < t_cierre:
            return packs(t)
        return cierre(t)

    audio = ruta_audio or salida.rsplit('.', 1)[0] + '.wav'
    musica = musica or musica_mod.aleatoria()
    musica_mod.sintetizar(musica, duracion, audio)
    try:
        renderizar(cuadro, duracion, W, H, salida, audio=audio, capturas=capturas)
    finally:
        if not ruta_audio:
            import os
            try:
                os.remove(audio)
            except OSError:
                pass
    return {'duracion': duracion, 'musica': musica}
