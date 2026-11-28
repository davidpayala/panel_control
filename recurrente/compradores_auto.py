"""
🌟 Persona compradora automática.

Pone la 🌟 a quien hizo 3 compras o más, o se llevó 10 productos o más con precio en los últimos
9 meses (no cuentan ventas anuladas, regalos a S/ 0 ni recargos de tarjeta) y se la quita a quien ya
no cumple. La marca se escribe al inicio del nombre del panel y de Google Contacts, para verla
también en el WhatsApp del celular. La regla está en utils.py (COMPRADOR_*).

Uso:  python recurrente/compradores_auto.py            → solo muestra qué cambiaría
      python recurrente/compradores_auto.py --aplicar  → aplica los cambios (lo corre el cron a diario)
"""
import os
import sys
from datetime import datetime

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

from dotenv import load_dotenv  # noqa: E402
load_dotenv(os.path.join(RAIZ, ".env"))

from utils import recalcular_compradores  # noqa: E402

if __name__ == "__main__":
    aplicar = "--aplicar" in sys.argv
    cambios = recalcular_compradores(aplicar=aplicar)
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] {'Aplicado' if aplicar else 'Simulación'}: {len(cambios)} cambio(s)")
    for id_c, nombre, nuevo in cambios:
        print(f"   {'🌟 pone ' if nuevo else '   quita'}  #{id_c} {nombre}")
