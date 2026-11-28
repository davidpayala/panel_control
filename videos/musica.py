"""🎵 Música navideña sintetizada en el servidor (villancicos de dominio público, siglos XVI–XIX).

Cada canción = melodía + arreglo (instrumento, tempo, percusión, colchón). El bajo y los acordes se
calculan solos a partir de la melodía (armonización I-IV-V-vi-ii por compás), así que agregar un
villancico nuevo es solo escribir su melodía.

Notación de melodía: "NOTA:duración" separados por espacios; '|' (barra de compás) se ignora.
NOTA = C4, F#4, Bb3… o R (silencio). Duración en tiempos (negra = 1).
"""
import random
import wave

import numpy as np

SR = 44100

CANCIONES = {
    'jingle_bells': dict(
        titulo='Jingle Bells (1857)', tonica='C', modo='mayor', compas=4, bpm=128, instrumento='campana',
        percusion='cascabeles', colchon=False, octava=0,
        melodia="""E5:1 E5:1 E5:2 | E5:1 E5:1 E5:2 | E5:1 G5:1 C5:1.5 D5:.5 | E5:4 |
                   F5:1 F5:1 F5:1.5 F5:.5 | F5:1 E5:1 E5:1 E5:.5 E5:.5 | E5:1 D5:1 D5:1 E5:1 | D5:2 G5:2 |
                   E5:1 E5:1 E5:2 | E5:1 E5:1 E5:2 | E5:1 G5:1 C5:1.5 D5:.5 | E5:4 |
                   F5:1 F5:1 F5:1.5 F5:.5 | F5:1 E5:1 E5:1 E5:.5 E5:.5 | G5:1 G5:1 F5:1 D5:1 | C5:4"""),
    'we_wish_you': dict(
        titulo='We Wish You a Merry Christmas (s. XVI)', tonica='G', modo='mayor', compas=3, bpm=168,
        instrumento='caja_musica', percusion='cascabeles_suave', colchon=False, octava=1,
        melodia="""R:2 D4:1 | G4:1 G4:.5 A4:.5 G4:.5 F#4:.5 | E4:1 E4:1 E4:1 | A4:1 A4:.5 B4:.5 A4:.5 G4:.5 |
                   F#4:1 D4:1 D4:1 | B4:1 B4:.5 C5:.5 B4:.5 A4:.5 | G4:1 E4:1 D4:.5 D4:.5 | E4:1 A4:1 F#4:1 | G4:2 D4:1 |
                   G4:1 G4:1 G4:1 | F#4:2 F#4:1 | G4:1 F#4:1 E4:1 | D4:2 A4:1 | B4:1 A4:1 G4:1 |
                   D5:1 D4:1 D4:.5 D4:.5 | E4:1 A4:1 F#4:1 | G4:3"""),
    'deck_the_halls': dict(
        titulo='Deck the Halls (s. XVI)', tonica='C', modo='mayor', compas=4, bpm=150, instrumento='marimba',
        percusion='cascabeles', colchon=False, octava=1,
        melodia="""G4:1.5 F4:.5 E4:1 D4:1 | C4:1 D4:1 E4:1 C4:1 | D4:.5 E4:.5 F4:.5 D4:.5 E4:1.5 D4:.5 | C4:1 B3:1 C4:2 |
                   G4:1.5 F4:.5 E4:1 D4:1 | C4:1 D4:1 E4:1 C4:1 | D4:.5 E4:.5 F4:.5 D4:.5 E4:1.5 D4:.5 | C4:1 B3:1 C4:2 |
                   D4:1.5 E4:.5 F4:1 D4:1 | E4:1.5 F4:.5 G4:1 D4:1 | E4:.5 F#4:.5 G4:1 A4:.5 B4:.5 C5:1 | B4:1 A4:1 G4:2 |
                   G4:1.5 F4:.5 E4:1 D4:1 | C4:1 D4:1 E4:1 C4:1 | A4:.5 A4:.5 A4:.5 A4:.5 G4:1.5 F4:.5 | E4:1 D4:1 C4:2"""),
    'joy_to_the_world': dict(
        titulo='Joy to the World (1839)', tonica='C', modo='mayor', compas=4, bpm=118, instrumento='piano',
        percusion='bombo_suave', colchon=True, octava=1,
        melodia="""C5:2 B4:1.5 A4:.5 | G4:3 F4:1 | E4:2 D4:2 | C4:3 G4:1 | A4:3 A4:1 | B4:3 B4:1 | C5:3 C5:1 |
                   C5:1 B4:1 A4:1 G4:1 | G4:1.5 F4:.5 E4:1 C5:1 | C5:1 B4:1 A4:1 G4:1 | G4:1.5 F4:.5 E4:1 E4:1 |
                   E4:1 E4:1 E4:1 E4:.5 F4:.5 | G4:3 F4:.5 E4:.5 | D4:1 D4:1 D4:1 D4:.5 E4:.5 | F4:3 E4:.5 D4:.5 |
                   C4:1 C5:2 A4:1 | G4:1.5 F4:.5 E4:1 F4:1 | E4:2 D4:2 | C4:4"""),
    'silent_night': dict(
        titulo='Noche de Paz (1818)', tonica='C', modo='mayor', compas=3, bpm=190, instrumento='caja_musica',
        percusion=None, colchon=True, octava=1,
        melodia="""G4:1.5 A4:.5 G4:1 | E4:3 | G4:1.5 A4:.5 G4:1 | E4:3 | D5:2 D5:1 | B4:3 | C5:2 C5:1 | G4:3 |
                   A4:2 A4:1 | C5:1.5 B4:.5 A4:1 | G4:1.5 A4:.5 G4:1 | E4:3 | A4:2 A4:1 | C5:1.5 B4:.5 A4:1 |
                   G4:1.5 A4:.5 G4:1 | E4:3 | D5:2 D5:1 | F5:1.5 D5:.5 B4:1 | C5:3 | E5:3 |
                   C5:1 G4:1 E4:1 | G4:1.5 F4:.5 D4:1 | C4:3 | R:3"""),
    'god_rest_ye': dict(
        titulo='God Rest Ye Merry Gentlemen (s. XVIII)', tonica='E', modo='menor', compas=4, bpm=150,
        instrumento='pluck', percusion='cascabeles', colchon=False, octava=1,
        melodia="""R:3 E4:1 | E4:1 B4:1 B4:1 A4:1 | G4:1 F#4:1 E4:1 D4:1 | E4:1 F#4:1 G4:1 A4:1 | B4:3 E4:1 |
                   E4:1 B4:1 B4:1 A4:1 | G4:1 F#4:1 E4:1 D4:1 | E4:1 F#4:1 G4:1 A4:1 | B4:3 B4:1 |
                   C5:1 A4:1 B4:1 C5:1 | D5:1 E5:1 B4:1 A4:1 | G4:1 E4:1 F#4:1 G4:1 | A4:2 G4:1 A4:1 |
                   B4:2 C5:1 B4:1 | A4:1 G4:1 F#4:1 E4:1 | G4:1 F#4:.5 E4:.5 D4:1 G4:1 | A4:2 B4:1 C5:1 |
                   B4:1 A4:1 G4:1 F#4:1 | E4:4"""),
    'good_king': dict(
        titulo='Good King Wenceslas (1582)', tonica='C', modo='mayor', compas=4, bpm=132, instrumento='campana',
        percusion='bombo_suave', colchon=True, octava=1,
        melodia="""C4:1 C4:1 C4:1 D4:1 | C4:1 C4:1 G3:2 | A3:1 G3:1 A3:1 B3:1 | C4:2 C4:2 |
                   C4:1 C4:1 C4:1 D4:1 | C4:1 C4:1 G3:2 | A3:1 G3:1 A3:1 B3:1 | C4:2 C4:2 |
                   G4:1 F4:1 E4:1 D4:1 | E4:1 F4:1 E4:2 | C4:1 B3:1 A3:1 B3:1 | C4:2 C4:2 |
                   G3:1 G3:1 A3:1 B3:1 | C4:1 C4:1 D4:2 | G4:1 F4:1 E4:1 D4:1 | C4:2 F4:2 | C4:4"""),
    'first_noel': dict(
        titulo='The First Noel (s. XVIII)', tonica='C', modo='mayor', compas=3, bpm=150, instrumento='piano',
        percusion=None, colchon=True, octava=1,
        melodia="""R:2 E4:.5 D4:.5 | C4:1.5 D4:.5 E4:.5 F4:.5 | G4:2 A4:.5 B4:.5 | C5:1 B4:1 A4:1 | G4:2 A4:.5 B4:.5 |
                   C5:1 B4:1 A4:1 | G4:1 A4:1 B4:1 | C5:1 G4:1 F4:1 | E4:2 E4:.5 D4:.5 |
                   C4:1.5 D4:.5 E4:.5 F4:.5 | G4:2 C5:.5 B4:.5 | A4:2 A4:1 | G4:2 C5:1 | B4:1 A4:1 G4:1 |
                   A4:1 B4:1 C5:1 | G4:1 F4:1 E4:1 | E4:3"""),
    'hark_the_herald': dict(
        titulo='Hark! The Herald Angels Sing (1840)', tonica='G', modo='mayor', compas=4, bpm=124, instrumento='celesta',
        percusion='cascabeles_suave', colchon=True, octava=1,
        melodia="""D4:1 G4:1 G4:1.5 F#4:.5 | G4:1 B4:1 B4:1 A4:1 | D5:1 D5:1 D5:1.5 C5:.5 | B4:1 A4:1 B4:2 |
                   D4:1 G4:1 G4:1.5 F#4:.5 | G4:1 B4:1 B4:1 A4:1 | D5:1 A4:1 A4:1.5 F#4:.5 | F#4:1 E4:1 D4:2 |
                   D5:1 D5:1 D5:1 G4:1 | C5:1 B4:1 B4:1 A4:1 | D5:1 D5:1 D5:1 G4:1 | C5:1 B4:1 B4:1 A4:1 |
                   E5:1 E5:1 E5:1 D5:1 | C5:1 B4:1 C5:2 | A4:1 B4:.5 C5:.5 D5:1.5 G4:.5 | G4:1 A4:1 B4:2 |
                   E5:1.5 E5:.5 E5:1 D5:1 | C5:1 B4:1 C5:2 | A4:1 B4:.5 C5:.5 D5:1.5 G4:.5 | G4:1 A4:1 G4:2"""),
    'angels_we_have_heard': dict(
        titulo='Ángeles cantando están (s. XVIII)', tonica='F', modo='mayor', compas=4, bpm=132, instrumento='marimba',
        percusion='cascabeles', colchon=True, octava=1,
        melodia="""A4:1 A4:1 A4:1 C5:1 | C5:1.5 Bb4:.5 A4:2 | A4:1 G4:1 A4:1 C5:1 | A4:1.5 G4:.5 F4:2 |
                   A4:1 A4:1 A4:1 C5:1 | C5:1.5 Bb4:.5 A4:2 | A4:1 G4:1 A4:1 C5:1 | A4:1.5 G4:.5 F4:2 |
                   C5:2 D5:.5 C5:.5 Bb4:.5 A4:.5 | Bb4:2 C5:.5 Bb4:.5 A4:.5 G4:.5 | A4:2 Bb4:.5 A4:.5 G4:.5 F4:.5 | G4:1.5 C4:.5 C4:2 |
                   F4:1 G4:1 A4:1 Bb4:1 | A4:2 G4:2 |
                   C5:2 D5:.5 C5:.5 Bb4:.5 A4:.5 | Bb4:2 C5:.5 Bb4:.5 A4:.5 G4:.5 | A4:2 Bb4:.5 A4:.5 G4:.5 F4:.5 | G4:1.5 C4:.5 C4:2 |
                   F4:1 G4:1 A4:1 Bb4:1 | A4:2 G4:2 | F4:4"""),
}

_PC = {'C': 0, 'D': 2, 'E': 4, 'F': 5, 'G': 7, 'A': 9, 'B': 11}


def midi(nombre):
    letra = nombre[0]
    resto = nombre[1:]
    alt = 0
    while resto and resto[0] in '#b':
        alt += 1 if resto[0] == '#' else -1
        resto = resto[1:]
    return 12 * (int(resto) + 1) + _PC[letra] + alt


def hz(m):
    return 440.0 * 2 ** ((m - 69) / 12)


def parsear(melodia, octava=0):
    notas, t = [], 0.0
    for tok in melodia.split():
        if tok == '|':
            continue
        n, d = tok.split(':')
        d = float(d)
        if n != 'R':
            notas.append((t, d, midi(n) + 12 * octava))
        t += d
    return notas, t


ACORDES = {
    'mayor': [('I', (0, 4, 7)), ('V', (7, 11, 2)), ('IV', (5, 9, 0)), ('vi', (9, 0, 4)), ('ii', (2, 5, 9))],
    'menor': [('i', (0, 3, 7)), ('V', (7, 11, 2)), ('iv', (5, 8, 0)), ('III', (3, 7, 10)), ('VII', (10, 2, 5))],
}


def armonizar(notas, total, tonica, modo, paso):
    """Un acorde cada `paso` tiempos: el que más cubre las notas de la melodía en ese tramo."""
    raiz = _PC[tonica]
    out = []
    t = 0.0
    previo = ACORDES[modo][0]
    while t < total - 1e-6:
        peso = [0.0] * 12
        for (ti, d, m) in notas:
            ini, fin = max(ti, t), min(ti + d, t + paso)
            if fin > ini:
                peso[(m - raiz) % 12] += (fin - ini) * (1.5 if abs(ti - t) < 1e-6 else 1)
        mejor, mejor_s = previo, -1
        for k, (nom, tonos) in enumerate(ACORDES[modo]):
            s = sum(peso[x] for x in tonos) - k * 0.01 + (0.05 if (nom, tonos) == previo else 0)
            if s > mejor_s:
                mejor, mejor_s = (nom, tonos), s
        if sum(peso) == 0:
            mejor = previo
        out.append((t, paso, [(raiz + x) % 12 for x in mejor[1]]))
        previo = mejor
        t += paso
    return out


# ---------------------------------------------------------------- instrumentos
def _env(t, ataque, caida):
    return np.minimum(1, t / ataque) * np.exp(-t * caida)


def tono(instr, f, dur):
    t = np.arange(int(SR * dur)) / SR
    w = 2 * np.pi * f * t
    if instr == 'campana':
        s = (np.sin(w) + .45 * np.sin(2 * w) * np.exp(-t * 6) + .25 * np.sin(3.01 * w) * np.exp(-t * 9)
             + .12 * np.sin(4.2 * w) * np.exp(-t * 14))
        return s * _env(t, .004, 3.2)
    if instr == 'caja_musica':
        s = np.sin(w) + .3 * np.sin(2 * w) + .15 * np.sin(5.4 * w) * np.exp(-t * 20)
        return s * _env(t, .002, 4.5)
    if instr == 'celesta':
        s = np.sin(w) + .5 * np.sin(2 * w) * np.exp(-t * 3) + .2 * np.sin(4 * w) * np.exp(-t * 8)
        return s * _env(t, .003, 2.6)
    if instr == 'marimba':
        s = np.sin(w) + .35 * np.sin(4 * w) * np.exp(-t * 25) + .1 * np.sin(10 * w) * np.exp(-t * 60)
        return s * _env(t, .002, 6.5)
    if instr == 'piano':
        s = sum((1 / k ** 1.4) * np.sin(k * w * (1 + .0004 * k)) * np.exp(-t * (1.6 + .9 * k)) for k in range(1, 7))
        return s * _env(t, .004, 1.2)
    if instr == 'pluck':  # Karplus-Strong (cuerda pulsada, tipo arpa/guitarra)
        n = len(t)
        p = max(2, int(SR / f))
        rng = np.random.default_rng(int(f))
        buf = rng.uniform(-1, 1, p)
        out = np.empty(n)
        for i in range(n):
            out[i] = buf[i % p]
            buf[i % p] = .5 * (buf[i % p] + buf[(i + 1) % p]) * .996
        return out * _env(t, .002, 1.5)
    return np.sin(w) * _env(t, .01, 3)


def bajo(f, dur):
    t = np.arange(int(SR * dur)) / SR
    return (np.sin(2 * np.pi * f * t) + .3 * np.sin(4 * np.pi * f * t)) * _env(t, .01, 3.5)


def colchon(fs, dur):
    t = np.arange(int(SR * dur)) / SR
    env = np.minimum(1, t / .25) * np.minimum(1, (dur - t) / .3).clip(0)
    s = sum(np.sin(2 * np.pi * f * t) + .2 * np.sin(4 * np.pi * f * t) for f in fs)
    return s * env


def cascabel(rng, dur=.12):
    k = int(SR * dur)
    r = np.diff(np.concatenate([[0], rng.standard_normal(k)]))
    return r * np.exp(-(np.arange(k) / SR) * 40)


def bombo(dur=.25):
    t = np.arange(int(SR * dur)) / SR
    f = 110 * np.exp(-t * 18) + 45
    return np.sin(2 * np.pi * np.cumsum(f) / SR) * np.exp(-t * 14)


# ---------------------------------------------------------------- mezcla
def sintetizar(clave, duracion, ruta):
    c = CANCIONES[clave]
    beat = 60 / c['bpm']
    notas, total = parsear(c['melodia'], c.get('octava', 0))
    compas = c['compas']
    total = (int(np.ceil(total / compas - 1e-6))) * compas  # completar el último compás
    paso = compas if compas == 3 else compas / 2
    acordes = armonizar(notas, total, c['tonica'], c['modo'], paso)
    n = int(SR * duracion)
    out = np.zeros(n)
    rng = np.random.default_rng(7)

    def poner(x, seg, vol):
        i = int(seg * SR)
        if i >= n:
            return
        s = x[: n - i]
        out[i:i + len(s)] += vol * s

    ciclo = total * beat
    base = 0.0
    cache = {}
    while base < duracion:
        for (t, d, m) in notas:
            k = (m, round(d, 3))
            if k not in cache:
                cache[k] = tono(c['instrumento'], hz(m), max(d * beat, .5) + .5)
            poner(cache[k], base + t * beat, .30)
            if c['instrumento'] in ('caja_musica', 'celesta', 'campana'):
                kk = ('o', m)
                if kk not in cache:
                    cache[kk] = tono(c['instrumento'], hz(m + 12), .5)
                poner(cache[kk], base + t * beat, .07)
        for (t, d, pcs) in acordes:
            r = pcs[0]
            raiz_m = 36 + r if r >= 4 else 48 + r  # bajo entre E2 y D#3
            quinta = raiz_m + 7
            if compas == 3:
                poner(bajo(hz(raiz_m), beat * .9), base + t * beat, .26)
                for k in (1, 2):
                    poner(colchon([hz(48 + p if p >= 7 else 60 + p) for p in pcs], beat * .6), base + (t + k) * beat, .025)
            else:
                poner(bajo(hz(raiz_m), beat * .9), base + t * beat, .26)
                poner(bajo(hz(quinta), beat * .9), base + (t + 1) * beat, .2)
            if c.get('colchon'):
                poner(colchon([hz(55 + ((p - 7) % 12)) for p in pcs], d * beat), base + t * beat, .035)
        perc = c.get('percusion')
        golpes = int(total * 2)
        for k in range(golpes):
            tt = base + k * beat / 2
            if perc == 'cascabeles':
                poner(cascabel(rng), tt, .08 if k % 2 == 0 else .05)
            elif perc == 'cascabeles_suave' and k % 2 == 0:
                poner(cascabel(rng, .1), tt, .045)
            elif perc == 'bombo_suave' and k % (2 * compas) == 0:
                poner(bombo(), tt, .22)
            if perc == 'bombo_suave' and k % 2 == 1:
                poner(cascabel(rng, .06), tt, .025)
        base += ciclo

    fade = np.ones(n)
    fi, fo = int(.05 * SR), int(min(1.5, duracion / 4) * SR)
    fade[:fi] = np.linspace(0, 1, fi)
    fade[-fo:] = np.linspace(1, 0, fo)
    out *= fade
    out /= (np.max(np.abs(out)) or 1) / .85
    L, R = out, np.roll(out, int(.008 * SR)) * .95
    data = (np.stack([L, R], 1) * 32767).astype(np.int16)
    with wave.open(ruta, 'wb') as w:
        w.setnchannels(2); w.setsampwidth(2); w.setframerate(SR); w.writeframes(data.tobytes())
    return clave


def aleatoria(rnd=None, evitar=()):
    claves = [k for k in CANCIONES if k not in evitar] or list(CANCIONES)
    return (rnd or random).choice(claves)
