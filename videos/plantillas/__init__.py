"""Registro de plantillas de video. Para agregar una: crear videos/plantillas/<clave>.py con
NOMBRE, DESCRIPCION, vigente(fecha) y generar(productos, salida, formato, segundos_foto, texto_modelos,
promo, capturas, musica) y sumarla a PLANTILLAS."""
import datetime
from videos.plantillas import navidad

PLANTILLAS = {
    'navidad': navidad,
}


def disponibles(claves, fecha=None):
    """Claves registradas y vigentes en la fecha (las de temporada se apagan solas)."""
    fecha = fecha or datetime.date.today()
    out = []
    for k in claves or []:
        m = PLANTILLAS.get(k)
        if m and (not hasattr(m, 'vigente') or m.vigente(fecha)):
            out.append(k)
    return out
