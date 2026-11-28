"""🎬 Motor común de videos (carruseles para estados / Reels).

Todo corre en el servidor: Pillow dibuja cada cuadro, ffmpeg (imageio-ffmpeg) arma el MP4 y la
música se sintetiza en videos/musica.py (villancicos de dominio público), sin servicios externos.

Las plantillas (videos/plantillas/*.py) usan estas piezas: fuentes, fondo, nieve, textos,
pastillas, fotos de producto y `renderizar()`.
"""
import math
import os
import random
import subprocess
import hashlib

import requests
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import imageio_ffmpeg

DIR = os.path.dirname(os.path.abspath(__file__))
FUENTES = os.path.join(DIR, 'assets', 'fonts')
CACHE = os.path.expanduser('~/videos_auto/_cache')
FPS = 30

FORMATOS = {'vertical': (1080, 1920), 'cuadrado': (1080, 1080)}


# ---------------------------------------------------------------- fuentes
_fcache = {}
def mont(size, peso='Bold'):
    k = ('m', int(size), peso)
    if k not in _fcache:
        f = ImageFont.truetype(os.path.join(FUENTES, 'Montserrat-VF.ttf'), int(size))
        f.set_variation_by_name(peso)
        _fcache[k] = f
    return _fcache[k]

def script(size):
    k = ('s', int(size))
    if k not in _fcache:
        _fcache[k] = ImageFont.truetype(os.path.join(FUENTES, 'BerkshireSwash-Regular.ttf'), int(size))
    return _fcache[k]


# ---------------------------------------------------------------- utilidades de dibujo
def ease(x):
    x = max(0.0, min(1.0, x))
    return 1 - (1 - x) ** 3

def texto_c(dd, W, y, txt, font, fill=(255, 255, 255), sombra=True, x=None):
    x = W / 2 if x is None else x
    if sombra:
        dd.text((x + 3, y + 4), txt, font=font, fill=(0, 0, 0, 110), anchor='mm')
    dd.text((x, y), txt, font=font, fill=fill, anchor='mm')

def ajustar_fuente(dd, txt, maker, size, ancho_max):
    """Reduce el tamaño hasta que el texto entre en ancho_max."""
    while size > 18:
        f = maker(size)
        b = dd.textbbox((0, 0), txt, font=f)
        if b[2] - b[0] <= ancho_max:
            return f
        size -= 2
    return maker(size)

def pastilla(dd, cx, cy, txt, font, bg, fg, padx=34, pady=16, S=1.0):
    b = dd.textbbox((0, 0), txt, font=font, anchor='lt')
    w, h = b[2] - b[0] + padx * 2 * S, b[3] - b[1] + pady * 2 * S
    dd.rounded_rectangle([cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2], radius=h / 2, fill=bg)
    dd.text((cx, cy), txt, font=font, fill=fg, anchor='mm')


def fondo_degradado(W, H, c1, c2, brillo=(210, 40, 60), marco=None):
    S = W / 1080
    img = Image.new('RGB', (W, H))
    d = ImageDraw.Draw(img)
    for y in range(H):
        k = y / H
        d.line([(0, y), (W, y)], fill=tuple(int(c1[i] * (1 - k) + c2[i] * k) for i in range(3)))
    glow = Image.new('L', (W, H), 0)
    ImageDraw.Draw(glow).ellipse([W * .1, H * .2, W * .9, H * .8], fill=70)
    glow = glow.filter(ImageFilter.GaussianBlur(W * .15))
    img = Image.composite(Image.new('RGB', (W, H), brillo), img, glow)
    if marco:
        m = int(24 * S)
        ImageDraw.Draw(img).rounded_rectangle([m, m, W - m, H - m], radius=int(30 * S), outline=marco, width=3)
    return img


class Nieve:
    def __init__(self, W, H, n=90, semilla=3):
        self.W, self.H = W, H
        S = W / 1080
        rnd = random.Random(semilla)
        self.copos = [(rnd.random() * W, rnd.random() * H, rnd.uniform(2, 6) * S, rnd.uniform(40, 120) * S,
                       rnd.uniform(0, 6.28)) for _ in range(n)]
        self.S = S

    def dibujar(self, img, t):
        capa = Image.new('RGBA', (self.W, self.H), (0, 0, 0, 0))
        dd = ImageDraw.Draw(capa)
        for x, y, r, v, fase in self.copos:
            yy = (y + v * t) % (self.H + 20) - 10
            xx = x + math.sin(t * 1.2 + fase) * 18 * self.S
            a = int(120 + 100 * (r / (6 * self.S)))
            dd.ellipse([xx - r, yy - r, xx + r, yy + r], fill=(255, 255, 255, a))
        img.alpha_composite(capa)


# ---------------------------------------------------------------- fotos de producto
def descargar_imagen(url):
    """Descarga (con caché en disco) y devuelve PIL.Image RGB, o None si falla."""
    if not url:
        return None
    os.makedirs(CACHE, exist_ok=True)
    ruta = os.path.join(CACHE, hashlib.md5(url.encode()).hexdigest() + '.img')
    try:
        if not os.path.exists(ruta):
            r = requests.get(url, timeout=20, headers={'User-Agent': 'KM-panel-videos'})
            if r.status_code != 200 or len(r.content) < 2000:
                return None
            with open(ruta, 'wb') as f:
                f.write(r.content)
        im = Image.open(ruta).convert('RGB')
        if min(im.size) < 300:
            return None
        # recorte cuadrado centrado
        w, h = im.size
        s = min(w, h)
        return im.crop(((w - s) // 2, (h - s) // 2, (w - s) // 2 + s, (h - s) // 2 + s))
    except Exception:
        return None


# ---------------------------------------------------------------- render
def renderizar(cuadro, duracion, W, H, salida, audio=None, capturas=None):
    """cuadro(t) -> PIL.Image. Escribe MP4 H.264 + AAC. `capturas`: segundos a guardar como JPG (revisión)."""
    os.makedirs(os.path.dirname(os.path.abspath(salida)), exist_ok=True)
    ff = imageio_ffmpeg.get_ffmpeg_exe()
    cmd = [ff, '-y', '-loglevel', 'error', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-s', f'{W}x{H}', '-r', str(FPS), '-i', '-']
    if audio:
        cmd += ['-i', audio]
    cmd += ['-c:v', 'libx264', '-preset', 'medium', '-crf', '20', '-pix_fmt', 'yuv420p', '-threads', '4']
    if audio:
        cmd += ['-c:a', 'aac', '-b:a', '160k', '-shortest']
    cmd += ['-movflags', '+faststart', salida]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    capturas = set(capturas or [])
    try:
        for n in range(int(round(FPS * duracion))):
            t = n / FPS
            im = cuadro(t).convert('RGB')
            if n % FPS == 0 and int(t) in capturas:
                im.save(salida.rsplit('.', 1)[0] + f'_{int(t):02d}.jpg', quality=80)
            p.stdin.write(im.tobytes())
    finally:
        p.stdin.close()
        p.wait()
    if p.returncode != 0:
        raise RuntimeError(f'ffmpeg terminó con código {p.returncode}')
    return salida
