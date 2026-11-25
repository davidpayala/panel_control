"""
🔎 Verificador masivo de números de WhatsApp.

Revisa los teléfonos principales de los clientes activos y bloquea (igual que hace el bot de marketing)
a los que no tienen WhatsApp o cuyo número es basura (IDs de grupo, @usuario de Instagram, 105, 119...).

Bloquear = clientes.activo FALSE, excluir_publicidad TRUE, estado 'Sin WhatsApp' y el teléfono activo FALSE.

Cada número revisado queda anotado en la tabla verificacion_wa, así cada corrida sigue donde quedó.
Primero revisa a los que el bot de marketing tiene en cola (estado 'Sin empezar' y en bot_grupos).

Uso:
    .venv/bin/python recurrente/verificar_numeros.py              # simulación: solo muestra, no bloquea
    .venv/bin/python recurrente/verificar_numeros.py --aplicar    # bloquea de verdad
    opciones: --limite=300  --pausa=6-15  (segundos entre consultas, para no alertar a Meta)
              --hasta=03:00  (se detiene a esa hora; lo que falte sigue en la próxima corrida)
    Cron: todas las noches de 23:00 a 03:00 (--aplicar --limite=5000 --hasta=03:00).
"""
import os
import sys
import time
import random
from datetime import datetime, timedelta

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from dotenv import load_dotenv
load_dotenv(os.path.join(_raiz, ".env"))

from sqlalchemy import text
from database import engine
from utils import verificar_numero_waha, normalizar_telefono_maestro, es_lid


def _arg(nombre, defecto):
    for a in sys.argv[1:]:
        if a.startswith(f"--{nombre}="):
            return a.split("=", 1)[1].strip()
    return defecto


APLICAR = "--aplicar" in sys.argv
LIMITE = int(_arg("limite", "300"))
PAUSA_MIN, PAUSA_MAX = (int(x) for x in _arg("pausa", "6-15").split("-"))
HASTA = _arg("hasta", "")
MAX_ERRORES_SEGUIDOS = 5  # WAHA caído → se detiene en vez de seguir sin saber nada


def hora_limite():
    """Momento exacto en que debe parar (si --hasta=03:00 y son las 23:00, es mañana a las 3)."""
    if not HASTA:
        return None
    h, m = (int(x) for x in HASTA.split(":"))
    ahora = datetime.now()
    limite = ahora.replace(hour=h, minute=m, second=0, microsecond=0)
    if limite <= ahora:
        limite += timedelta(days=1)
    return limite


def preparar_columna():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS verificacion_wa (
                id_telefono INT PRIMARY KEY,
                fecha TIMESTAMP DEFAULT NOW(),
                resultado TEXT
            )
        """))


def pendientes():
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT t.id_telefono, t.id_cliente, t.telefono, c.nombre_corto
            FROM telefonoscliente t
            JOIN clientes c ON c.id_cliente = t.id_cliente
            WHERE c.activo = TRUE AND t.activo = TRUE AND t.es_principal = TRUE
              AND NOT EXISTS (SELECT 1 FROM verificacion_wa v WHERE v.id_telefono = t.id_telefono)
              AND NULLIF(TRIM(t.telefono), '') IS NOT NULL
            ORDER BY
              -- primero los que el bot de marketing tiene en cola
              (c.estado = 'Sin empezar' AND COALESCE(c.excluir_publicidad, FALSE) = FALSE
               AND EXISTS (SELECT 1 FROM bot_grupos bg WHERE bg.id_cliente = c.id_cliente)) DESC,
              t.id_telefono
            LIMIT :lim
        """), {"lim": LIMITE}).fetchall()


def tiene_entrantes(conn, telefono_db):
    """Si alguna vez nos escribió desde ese número, existe: no hace falta preguntarle a WAHA."""
    return conn.execute(text(
        "SELECT 1 FROM mensajes WHERE telefono = :t AND tipo = 'ENTRANTE' LIMIT 1"
    ), {"t": telefono_db}).first() is not None


def _anotar(conn, id_tel, resultado):
    conn.execute(text("""
        INSERT INTO verificacion_wa (id_telefono, fecha, resultado) VALUES (:id, NOW(), :res)
        ON CONFLICT (id_telefono) DO UPDATE SET fecha = NOW(), resultado = EXCLUDED.resultado
    """), {"id": id_tel, "res": resultado})


def marcar_verificado(id_tel):
    if APLICAR:
        with engine.begin() as conn:
            _anotar(conn, id_tel, "ok")


def bloquear(r, motivo):
    print(f"   🚫 BLOQUEAR {r.nombre_corto} ({r.telefono!r}): {motivo}")
    if not APLICAR:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE clientes SET excluir_publicidad = TRUE, activo = FALSE, estado = 'Sin WhatsApp'
            WHERE id_cliente = :idc
        """), {"idc": r.id_cliente})
        conn.execute(text("""
            UPDATE telefonoscliente SET activo = FALSE WHERE id_telefono = :id
        """), {"id": r.id_telefono})
        _anotar(conn, r.id_telefono, motivo)


def main():
    preparar_columna()
    lista = pendientes()
    print(f"🔎 Verificador de números — {'APLICANDO CAMBIOS' if APLICAR else 'SIMULACIÓN (usa --aplicar para bloquear)'}")
    print(f"📊 {len(lista)} teléfonos por revisar en esta corrida (límite {LIMITE}).\n")

    cont = {"ok": 0, "sin_wa": 0, "invalido": 0, "error": 0}
    errores_seguidos = 0
    limite = hora_limite()

    for i, r in enumerate(lista, 1):
        if limite and datetime.now() >= limite:
            print(f"\n⏰ Son las {HASTA}: me detengo. Faltan {len(lista) - i + 1}, siguen en la próxima corrida.")
            break
        tel = str(r.telefono).strip()
        print(f"[{i}/{len(lista)}] {r.nombre_corto} — {tel}")

        # Un LID existe porque esa persona nos escribió
        if es_lid(tel):
            cont["ok"] += 1
            marcar_verificado(r.id_telefono)
            continue

        norm = normalizar_telefono_maestro(tel)
        # basura: IDs de grupo (120363...), @usuario, números cortos tipo 105/119
        if not norm or tel.startswith("120363"):
            cont["invalido"] += 1
            bloquear(r, "número con formato inválido")
            continue

        with engine.connect() as conn:
            if tiene_entrantes(conn, norm["db"]):
                print("   ✅ Nos ha escrito antes (existe)")
                cont["ok"] += 1
                marcar_verificado(r.id_telefono)
                continue

        existe = verificar_numero_waha(norm["db"])
        if existe is True:
            print("   ✅ Tiene WhatsApp")
            cont["ok"] += 1
            errores_seguidos = 0
            marcar_verificado(r.id_telefono)
        elif existe is False:
            cont["sin_wa"] += 1
            errores_seguidos = 0
            bloquear(r, "no tiene WhatsApp")
        else:
            # None = WAHA no respondió: NO se bloquea ni se marca, se reintenta en la próxima corrida
            cont["error"] += 1
            errores_seguidos += 1
            print("   ⚠️ WAHA no respondió, se reintentará después")
            if errores_seguidos >= MAX_ERRORES_SEGUIDOS:
                print(f"\n🛑 {MAX_ERRORES_SEGUIDOS} errores seguidos de WAHA. Me detengo para no trabajar a ciegas.")
                break

        if i < len(lista):
            time.sleep(random.uniform(PAUSA_MIN, PAUSA_MAX))

    print("\n🏁 ===========================================")
    print(f"🏁 Con WhatsApp: {cont['ok']} | Sin WhatsApp: {cont['sin_wa']} | "
          f"Inválidos: {cont['invalido']} | Errores WAHA: {cont['error']}")
    if not APLICAR:
        print("🏁 Era una simulación: no se bloqueó a nadie. Repite con --aplicar.")
    print("🏁 ===========================================")


if __name__ == "__main__":
    main()
