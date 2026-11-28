"""
🤖 Perfiles de personalidad hechos por Claude (IA) a partir de los chats de WhatsApp.

Para cada cliente que compró en el periodo, arma un resumen de su chat (sin los mensajes del bot de
publicidad), sus compras y la nota del equipo, y le pide a Claude (CLI `claude -p`, sin herramientas)
un perfil corto útil para personalizar la publicidad. Si el cliente ya tenía perfil, se le pasa para
que lo actualice en vez de empezar de cero.

El resultado va en clientes.perfil_ia / perfil_ia_fecha: un campo APARTE de detalle_personalidad
(que es lo que escribe una persona del equipo). En el panel se muestra como "🤖 Perfil hecho por Claude".

Uso:
  python recurrente/perfiles_ia.py --dias 7 --aplicar      → compradores de la última semana (cron semanal)
  python recurrente/perfiles_ia.py --meses 9 --aplicar     → carga inicial
  python recurrente/perfiles_ia.py --id 708                → prueba con un cliente, sin guardar
Opciones: --solo-nuevos (salta a quien ya tiene perfil), --limite N, --modelo sonnet
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

from dotenv import load_dotenv  # noqa: E402
load_dotenv(os.path.join(RAIZ, ".env"))

from sqlalchemy import text  # noqa: E402
from database import engine  # noqa: E402
from utils import asegurar_columnas_perfil, quitar_marcas_perfil  # noqa: E402

CLAUDE = os.path.expanduser("~/.local/bin/claude")
MAX_CHARS_CHAT = 14000      # últimos caracteres del chat que se mandan por cliente
MAX_CHARS_MSG = 400         # cada mensaje se recorta a esto
MAX_CHARS_LOTE = 70000      # tamaño de cada consulta a Claude
MAX_CLIENTES_LOTE = 10

INSTRUCCIONES = """Eres analista de clientes de K&M, una tienda peruana que vende por WhatsApp lentes de contacto
cosméticos (kmlentes.pe) y pelucas/cosplay. "KM" son los mensajes de la tienda y "Cliente" los del cliente.

Para CADA cliente escribe un perfil breve, en español, que sirva para personalizar mensajes de publicidad.
Usa solo lo que se desprende del chat, las compras y la nota del equipo; no inventes. Si algo no se sabe, omítelo.
Formato: de 3 a 6 líneas, cada una empieza con "• " y una etiqueta, eligiendo las que apliquen:
• Trato: cómo escribe y cómo le gusta que le hablen (formal/informal, breve, audios, emojis, apodo, usted/tú).
• Le interesa: categorías, modelos, colores, marcas, medida/graduación, para qué los usa (diario, cosplay, eventos).
• Precio: sensibilidad al precio, si busca packs, promociones, regalos o regatea.
• Compra: cómo decide (rápido, pide fotos, compara), pago y entrega preferidos, zona, horarios en que responde.
• Ojo: quejas, reclamos, demoras o algo a cuidar (solo si existe).
• Para publicidad: qué ofrecerle y cómo decírselo (una línea concreta).
Máximo 550 caracteres por perfil. Nada de datos sensibles (DNI, direcciones exactas, salud más allá de la medida).
Si hay "Perfil anterior", actualízalo con lo nuevo (corrige lo que ya no aplique) en vez de repetirlo tal cual.
Si el chat casi no tiene información, escribe solo lo poco que se sepa (por ejemplo lo que compró).

Responde SOLO con JSON válido, sin texto extra, así:
{"perfiles": [{"id_cliente": 123, "perfil": "• Trato: ...\\n• Le interesa: ..."}]}
"""


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def clientes_del_periodo(conn, dias, ids, solo_nuevos, limite):
    if ids:
        filtro, params = "c.id_cliente = ANY(:ids)", {"ids": ids}
    else:
        filtro, params = ("EXISTS (SELECT 1 FROM ventas v WHERE v.id_cliente = c.id_cliente AND v.anulado IS NOT TRUE "
                          "AND v.fecha_venta >= NOW() - make_interval(days => :d))"), {"d": dias}
    if solo_nuevos:
        filtro += " AND c.perfil_ia IS NULL"
    sql = f"""SELECT c.id_cliente, c.nombre_corto, c.detalle_personalidad, c.perfil_ia
              FROM clientes c WHERE c.activo = TRUE AND {filtro} ORDER BY c.id_cliente"""
    if limite:
        sql += f" LIMIT {int(limite)}"
    return conn.execute(text(sql), params).fetchall()


def armar_ficha(conn, cli):
    ids_chat = [r[0] for r in conn.execute(text("""
        SELECT telefono FROM telefonoscliente WHERE id_cliente = :id AND telefono IS NOT NULL AND telefono <> ''
        UNION SELECT lid FROM telefonoscliente WHERE id_cliente = :id AND lid IS NOT NULL AND lid <> ''"""),
        {"id": cli.id_cliente})]
    msgs = conn.execute(text("""
        SELECT fecha, tipo, contenido FROM mensajes
        WHERE telefono = ANY(:t) AND tipo IN ('ENTRANTE', 'SALIENTE') AND COALESCE(contenido, '') <> ''
        ORDER BY fecha"""), {"t": ids_chat}).fetchall() if ids_chat else []
    lineas, dia = [], None
    for m in msgs:
        if m.fecha and m.fecha.date() != dia:
            dia = m.fecha.date()
            lineas.append(f"--- {dia:%d/%m/%Y} ---")
        quien = "Cliente" if m.tipo == "ENTRANTE" else "KM"
        txt = re.sub(r"\s+", " ", m.contenido).strip()[:MAX_CHARS_MSG]
        lineas.append(f"{m.fecha:%H:%M} {quien}: {txt}")
    chat = "\n".join(lineas)
    if len(chat) > MAX_CHARS_CHAT:
        chat = "[...]\n" + chat[-MAX_CHARS_CHAT:]

    compras = conn.execute(text("""
        SELECT v.fecha_venta, v.tipo_envio, d.descripcion, d.cantidad, d.precio_unitario
        FROM ventas v JOIN detalleventa d ON d.id_venta = v.id_venta
        WHERE v.id_cliente = :id AND v.anulado IS NOT TRUE ORDER BY v.fecha_venta"""), {"id": cli.id_cliente}).fetchall()
    txt_compras = "\n".join(
        f"{c.fecha_venta:%d/%m/%Y} ({c.tipo_envio or '-'}): {c.cantidad} x {c.descripcion} a S/ {float(c.precio_unitario or 0):.0f}"
        + (" (regalo)" if not c.precio_unitario else "") for c in compras) or "Sin compras registradas"

    partes = [f"### CLIENTE id_cliente={cli.id_cliente} — {quitar_marcas_perfil(cli.nombre_corto) or 'sin nombre'}",
              f"Compras:\n{txt_compras}"]
    if cli.detalle_personalidad:
        partes.append(f"Nota del equipo: {cli.detalle_personalidad}")
    if cli.perfil_ia:
        partes.append(f"Perfil anterior:\n{cli.perfil_ia}")
    partes.append(f"Chat:\n{chat or '(sin mensajes)'}")
    return "\n\n".join(partes)


def pedir_a_claude(fichas, modelo):
    prompt = INSTRUCCIONES + "\n\n" + "\n\n".join(fichas)
    with tempfile.TemporaryDirectory() as tmp:  # carpeta vacía: sin CLAUDE.md ni memoria del proyecto
        r = subprocess.run([CLAUDE, "-p", "--model", modelo, "--output-format", "json", "--tools", ""],
                           input=prompt, capture_output=True, text=True, timeout=900, cwd=tmp)
    if r.returncode != 0:
        raise RuntimeError(f"claude terminó con código {r.returncode}: {(r.stderr or r.stdout)[-500:]}")
    salida = json.loads(r.stdout)
    if salida.get("is_error"):
        raise RuntimeError(f"claude devolvió error: {salida.get('result', '')[:500]}")
    respuesta = salida.get("result", "")
    m = re.search(r"\{.*\}", respuesta, re.S)
    if not m:
        raise RuntimeError(f"respuesta sin JSON: {respuesta[:300]}")
    return json.loads(m.group(0)).get("perfiles", []), salida.get("total_cost_usd", 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dias", type=int, default=7)
    ap.add_argument("--meses", type=int)
    ap.add_argument("--id", type=int, action="append")
    ap.add_argument("--aplicar", action="store_true")
    ap.add_argument("--solo-nuevos", action="store_true")
    ap.add_argument("--limite", type=int)
    ap.add_argument("--modelo", default="sonnet")
    a = ap.parse_args()
    dias = a.meses * 30 if a.meses else a.dias

    asegurar_columnas_perfil()
    with engine.connect() as conn:
        clientes = clientes_del_periodo(conn, dias, a.id, a.solo_nuevos, a.limite)
        fichas = [(c.id_cliente, armar_ficha(conn, c)) for c in clientes]
    log(f"{len(fichas)} cliente(s) a perfilar ({'últimos ' + str(dias) + ' días' if not a.id else 'ids ' + str(a.id)})"
        f"{'' if a.aplicar else ' — PRUEBA, no se guarda'}")

    lotes, actual, tam = [], [], 0
    for idc, f in fichas:
        if actual and (tam + len(f) > MAX_CHARS_LOTE or len(actual) >= MAX_CLIENTES_LOTE):
            lotes.append(actual)
            actual, tam = [], 0
        actual.append((idc, f))
        tam += len(f)
    if actual:
        lotes.append(actual)

    hechos, costo = 0, 0.0
    for n, lote in enumerate(lotes, 1):
        ids_lote = {idc for idc, _ in lote}
        perfiles = None
        for intento in (1, 2):  # si la respuesta llega mal formada, se reintenta una vez
            try:
                perfiles, c = pedir_a_claude([f for _, f in lote], a.modelo)
                costo += c or 0
                break
            except Exception as e:
                log(f"{'⚠️' if intento == 1 else '❌'} Lote {n}/{len(lotes)} intento {intento} falló: {e}")
        if perfiles is None:
            continue
        validos = [(int(p["id_cliente"]), str(p.get("perfil") or "").strip()) for p in perfiles
                   if str(p.get("id_cliente", "")).isdigit() and int(p["id_cliente"]) in ids_lote and p.get("perfil")]
        if a.aplicar and validos:
            with engine.begin() as conn:
                for idc, perfil in validos:
                    conn.execute(text("UPDATE clientes SET perfil_ia = :p, perfil_ia_fecha = NOW() WHERE id_cliente = :id"),
                                 {"p": perfil[:1200], "id": idc})
        if not a.aplicar:
            for idc, perfil in validos:
                print(f"\n#{idc}\n{perfil}")
        hechos += len(validos)
        faltan = ids_lote - {i for i, _ in validos}
        log(f"Lote {n}/{len(lotes)}: {len(validos)} perfil(es)" + (f" · sin respuesta para {sorted(faltan)}" if faltan else ""))
    log(f"Listo: {hechos} perfil(es) {'guardados' if a.aplicar else 'generados (prueba)'} · costo aprox. US$ {costo:.2f}")


if __name__ == "__main__":
    main()
