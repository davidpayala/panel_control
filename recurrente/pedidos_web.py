"""
pedidos_web.py — Pedidos de kmlentes.pe → panel + WhatsApp (cron cada 5 minutos).

Por cada pedido nuevo de la web:
  1. Lo registra en el panel: cliente (lo crea si no existe), dirección, venta, detalle y descuento de stock.
     El stock se descuenta primero del interno; lo que falte se toma del proveedor (stock_externo).
  2. Le escribe al cliente por WhatsApp (sesión de KM Lentes) con el resumen y, en el delivery de Lima sin adelanto,
     le pide que responda "SÍ" para confirmar.
  3. Te avisa por WhatsApp (número en Ventas → Pedidos web) con alarmas de stock:
     🟠 no hay en tu stock pero sí en el proveedor (hay que pedirlo)  ·  🔴 no hay ni en el proveedor.
Además sigue las confirmaciones (respuestas del cliente en el chat) y sincroniza con la web la lista interna de
clientes que deben adelantar (lista_adelanto → opción km_lista_adelanto de WordPress).

Uso: python recurrente/pedidos_web.py            (lo corre el cron)
     python recurrente/pedidos_web.py --sin-wsp  (registra sin mandar WhatsApp; para pruebas)
"""
import json
import os
import random
import re
import sys
import hashlib
import unicodedata
from datetime import datetime

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import pytz
import requests
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv(os.path.join(RAIZ, ".env"))

from database import engine  # noqa: E402
from utils import enviar_mensaje_whatsapp, normalizar_telefono_maestro, sync_woo_background  # noqa: E402

TZ = pytz.timezone("America/Lima")
SIN_WSP = "--sin-wsp" in sys.argv
ALERTA_TEL_DEFECTO = "51941380271"  # Km Ventas Virtuales (se cambia en Ventas → Pedidos web)
SESION_DEFECTO = "default"          # KM Lentes 936 041 531, el número que figura en la web
HORAS_SIN_RESPUESTA = 3
YAPE = "986203398 (David Perez Ayala)"

ESTADOS_IGNORAR = {"pending", "failed", "cancelled", "checkout-draft", "trash", "refunded"}
ENVIOS = {  # clave del snippet KM: envíos → (tipo de envío en el panel, texto corto)
    "lima_dia": ("Motorizado", "Delivery Lima · mañana"),
    "lima_express": ("Express (Moto)", "Express Lima · hoy (InDriver)"),
    "shalom": ("Agencia", "Shalom"),
    "shalom_contra": ("Agencia (Pago Destino)", "Shalom contraentrega"),
    "shalom_express": ("Agencia", "Shalom Express · hoy"),
    "shalom_avion": ("Agencia", "Shalom aéreo · hoy"),
    "olva": ("Agencia", "Olva Courier"),
    "plaza_norte": ("Agencia", "Agencia en Plaza Norte"),
    "otra_agencia": ("Agencia", "Otra agencia"),
}
SI = re.compile(r"^(si+|sip|claro|ok+|okey|okay|dale|listo|confirmo|confirmado|correcto|de acuerdo|perfecto|ya|yes|va|vale|bueno|esta bien)\b")
NO = re.compile(r"\b(cancel\w*|anul\w*|ya no|no quiero|no lo quiero|no gracias|desisto)\b")


# ----------------------------------------------------------------------------------------------------------
def log(msg):
    print(f"[{datetime.now(TZ):%Y-%m-%d %H:%M}] {msg}", flush=True)
    try:
        with engine.begin() as c:
            c.execute(text("INSERT INTO logs_pedidos_web (fecha, mensaje) VALUES (NOW(), :m)"), {"m": str(msg)[:2000]})
    except Exception:
        pass


def crear_tablas():
    with engine.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS pedidos_web (
                id_pedido INTEGER PRIMARY KEY,
                id_venta INTEGER,
                id_cliente INTEGER,
                telefono TEXT,
                telefono2 TEXT,
                dni TEXT,
                nombre TEXT,
                envio TEXT,
                pago TEXT,
                total NUMERIC(10,2),
                confirmacion TEXT DEFAULT 'pendiente',
                alertas TEXT,
                fecha_pedido TIMESTAMP,
                fecha_registro TIMESTAMP DEFAULT NOW(),
                fecha_aviso TIMESTAMP,
                fecha_confirmacion TIMESTAMP,
                respuesta TEXT
            )"""))
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS lista_adelanto (
                id SERIAL PRIMARY KEY,
                telefono TEXT,
                dni TEXT,
                nombre TEXT,
                motivo TEXT,
                id_pedido INTEGER,
                fecha TIMESTAMP DEFAULT NOW(),
                activo BOOLEAN DEFAULT TRUE
            )"""))
        c.execute(text("CREATE TABLE IF NOT EXISTS logs_pedidos_web (id SERIAL PRIMARY KEY, fecha TIMESTAMP DEFAULT NOW(), mensaje TEXT)"))


def ajuste(clave, defecto=None):
    with engine.connect() as c:
        v = c.execute(text("SELECT valor FROM ajustes WHERE clave = :k"), {"k": clave}).scalar()
    return v if v not in (None, "") else defecto


def guardar_ajuste(clave, valor):
    with engine.begin() as c:
        c.execute(text("""INSERT INTO ajustes (clave, valor) VALUES (:k, :v)
                          ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor"""), {"k": clave, "v": valor})


def woo():
    from woocommerce import API
    return API(url=os.getenv("WOO_URL"), consumer_key=os.getenv("WOO_KEY"),
               consumer_secret=os.getenv("WOO_SECRET"), version="wc/v3", timeout=40)


def corto(tel):
    n = normalizar_telefono_maestro(tel) if tel else None
    return n["corto"] if n else ""


def sin_tildes(t):
    t = unicodedata.normalize("NFKD", str(t or "").lower())
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9\s]", " ", t).strip()


def soles(n):
    return f"S/ {float(n):,.2f}"


def meta(pedido, clave):
    for m in pedido.get("meta_data", []):
        if m.get("key") == clave:
            return str(m.get("value") or "").strip()
    return ""


def clave_envio(pedido):
    for s in pedido.get("shipping_lines", []):
        for m in s.get("meta_data", []):
            if m.get("key") == "_km_clave":
                return m.get("value"), s.get("method_title", "")
        return "", s.get("method_title", "")
    return "", ""


def avisar_dueno(mensaje):
    tel = ajuste("pedidos_web_alerta_tel", ALERTA_TEL_DEFECTO)
    if SIN_WSP:
        log(f"(sin WhatsApp) aviso al dueño:\n{mensaje}")
        return True
    return enviar_mensaje_whatsapp(tel, mensaje, session=ajuste("pedidos_web_sesion", SESION_DEFECTO))


def escribir_cliente(tel, mensaje):
    if SIN_WSP:
        log(f"(sin WhatsApp) mensaje al cliente {tel}:\n{mensaje}")
        return True
    return enviar_mensaje_whatsapp(tel, mensaje, session=ajuste("pedidos_web_sesion", SESION_DEFECTO))


def en_lista_adelanto(conn, telefonos, dni):
    tels = [t for t in telefonos if t]
    fila = conn.execute(text("""
        SELECT motivo FROM lista_adelanto
        WHERE activo AND ((telefono IS NOT NULL AND telefono = ANY(:tels)) OR (:dni <> '' AND dni = :dni))
        ORDER BY fecha DESC LIMIT 1"""), {"tels": tels or [""], "dni": dni or ""}).fetchone()
    return (fila.motivo or "sin motivo") if fila else None


# ----------------------------------------------------------------------------------------------------------
def buscar_o_crear_cliente(conn, tel_db, nombre, apellido, dni, tel2):
    fila = conn.execute(text("""
        SELECT t.id_cliente FROM telefonoscliente t JOIN Clientes c ON c.id_cliente = t.id_cliente
        WHERE t.telefono = :t ORDER BY t.es_principal DESC, t.id_telefono LIMIT 1"""), {"t": tel_db}).fetchone()
    id_venta_etapa = conn.execute(text(
        "SELECT id_etapa FROM EtapasCliente WHERE LOWER(TRIM(subgrupo)) = 'venta' LIMIT 1")).scalar()
    if fila:
        idc = fila.id_cliente
        conn.execute(text("""
            UPDATE Clientes SET estado = 'Venta', id_etapa = COALESCE(:et, id_etapa), activo = TRUE,
                   dni = COALESCE(NULLIF(dni, ''), NULLIF(:dni, '')),
                   telefono_secundario = COALESCE(NULLIF(telefono_secundario, ''), NULLIF(:t2, '')),
                   nombre = COALESCE(NULLIF(nombre, ''), :n), apellido = COALESCE(NULLIF(apellido, ''), :a)
            WHERE id_cliente = :id"""), {"et": id_venta_etapa, "dni": dni, "t2": tel2, "n": nombre, "a": apellido, "id": idc})
        return idc, False
    idc = conn.execute(text("""
        INSERT INTO Clientes (telefono, nombre_corto, nombre, apellido, dni, telefono_secundario, estado, id_etapa,
                              activo, fecha_registro, origen, medio_contacto)
        VALUES (:t, :nc, :n, :a, NULLIF(:dni, ''), NULLIF(:t2, ''), 'Venta', :et, TRUE, NOW(), 'Web kmlentes.pe', 'Web')
        RETURNING id_cliente"""), {"t": tel_db, "nc": (nombre or "Cliente web").split(" ")[0], "n": nombre, "a": apellido,
                                   "dni": dni, "t2": tel2, "et": id_venta_etapa}).scalar()
    conn.execute(text("""INSERT INTO telefonoscliente (id_cliente, telefono, es_principal, activo, fecha_registro)
                         VALUES (:id, :t, TRUE, TRUE, NOW())"""), {"id": idc, "t": tel_db})
    return idc, True


def descontar_stock(conn, sku, cantidad, id_pedido, idc):
    """Devuelve (filas de detalle [(cantidad, es_inventario, nota)], alerta o None)."""
    v = conn.execute(text("""
        SELECT v.sku, COALESCE(v.stock_interno, 0) AS interno, COALESCE(v.stock_externo, 0) AS externo,
               COALESCE(p.macro_categoria, 'Lentes') AS macro
        FROM Variantes v LEFT JOIN Productos p ON p.id_producto = v.id_producto WHERE v.sku = :s"""), {"s": sku}).fetchone()
    if not v:
        return [(cantidad, False, "")], f"🔴 {sku}: no existe en el panel (revisa el SKU en la web)", "Otros"
    de_interno = min(max(v.interno, 0), cantidad)
    falta = cantidad - de_interno
    filas, alerta = [], None
    if de_interno:
        nuevo = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno - :c WHERE sku = :s RETURNING stock_interno"),
                             {"c": de_interno, "s": sku}).scalar()
        if nuevo <= 0:
            conn.execute(text("UPDATE Variantes SET ubicacion = '' WHERE sku = :s"), {"s": sku})
        conn.execute(text("""
            INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota, id_cliente)
            VALUES (:s, 'VENTA', :c, :ant, :nue, :nota, :idc)"""),
            {"s": sku, "c": de_interno, "ant": nuevo + de_interno, "nue": nuevo, "nota": f"Venta web #{id_pedido}", "idc": idc})
        filas.append((de_interno, True, ""))
    if falta:
        del_prov = min(max(v.externo, 0), falta)
        if del_prov:
            conn.execute(text("UPDATE Variantes SET stock_externo = stock_externo - :c WHERE sku = :s"), {"c": del_prov, "s": sku})
        filas.append((falta, False, " (pedir al proveedor)"))
        if del_prov == falta:
            alerta = (f"🟠 {sku}: pedir {falta} al proveedor (tu stock: {max(v.interno, 0)}, proveedor tenía: {v.externo})")
        else:
            alerta = (f"🔴 {sku}: faltan {falta} y el proveedor solo tenía {max(v.externo, 0)} — avisa al cliente")
    return filas, alerta, v.macro


def registrar(pedido, w):
    idp = pedido["id"]
    b, s = pedido.get("billing", {}), pedido.get("shipping", {})
    tel_n = normalizar_telefono_maestro(b.get("phone") or s.get("phone") or "")
    if not tel_n:
        avisar_dueno(f"⚠️ Pedido web *#{idp}* sin celular válido ({b.get('phone')}). Revísalo en la web.")
        return None
    tel2 = corto(meta(pedido, "_wc_other/km/telefono2"))
    dni = re.sub(r"[^0-9A-Za-z]", "", meta(pedido, "_wc_other/km/dni"))
    gps = meta(pedido, "_wc_other/km/gps")
    agencia = meta(pedido, "_wc_other/km/agencia")
    condicion = meta(pedido, "_km_condicion_pago")
    nombre, apellido = (b.get("first_name") or "").strip(), (b.get("last_name") or "").strip()
    clave, titulo_envio = clave_envio(pedido)
    tipo_panel, envio_corto = ENVIOS.get(clave, ("Otros", titulo_envio or "Envío"))
    pago = pedido.get("payment_method", "")
    pagado = bool(pedido.get("date_paid")) and pago not in ("bacs", "cod")
    total = float(pedido.get("total") or 0)
    costo_envio = float(pedido.get("shipping_total") or 0)
    direccion = ", ".join(x for x in [s.get("address_1") or b.get("address_1"), s.get("address_2") or b.get("address_2")] if x)
    distrito = s.get("city") or b.get("city") or ""
    lineas, alertas, skus = [], [], []

    with engine.begin() as conn:
        if conn.execute(text("SELECT 1 FROM pedidos_web WHERE id_pedido = :i"), {"i": idp}).scalar():
            return None  # otro proceso ya lo registró
        idc, nuevo_cli = buscar_o_crear_cliente(conn, tel_n["db"], f"{nombre}".strip(), apellido, dni, tel2)
        lista = en_lista_adelanto(conn, [tel_n["corto"], tel2], dni)

        es_agencia = tipo_panel.startswith("Agencia")
        conn.execute(text("UPDATE Direcciones SET es_principal = FALSE WHERE id_cliente = :id"), {"id": idc})
        conn.execute(text("""
            INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, direccion_texto, distrito,
                                     referencia, gps_link, dni_receptor, agencia_nombre, sede_entrega, observacion, activo, es_principal)
            VALUES (:id, :tipo, :nom, :tel, :dir, :dist, :ref, :gps, :dni, :age, :sede, :obs, TRUE, TRUE)"""), {
            "id": idc, "tipo": "AGENCIA" if es_agencia else "MOTO", "nom": f"{nombre} {apellido}".strip(),
            "tel": tel_n["corto"], "dir": None if es_agencia else direccion, "dist": distrito,
            "ref": pedido.get("customer_note") or None, "gps": gps or None, "dni": dni or None,
            "age": (envio_corto.split(" ")[0] if es_agencia else None), "sede": agencia or None,
            "obs": f"Web #{idp}" + (f" | Otro celular: {tel2}" if tel2 else "")})

        destino = (f"{agencia} ({distrito})" if es_agencia else f"{direccion} - {distrito}")
        nota = f"Web #{idp} | {titulo_envio} | Envío: {destino}" + (f" | Otro cel: {tel2}" if tel2 else "")
        clave_seg = str(random.randint(1000, 9999)) if es_agencia else None
        idv = conn.execute(text("""
            INSERT INTO Ventas (id_cliente, tipo_envio, costo_envio, total_venta, nota, clave_seguridad, pendiente_pago)
            VALUES (:idc, :tipo, :costo, :total, :nota, :clave, :pend) RETURNING id_venta"""), {
            "idc": idc, "tipo": tipo_panel, "costo": costo_envio, "total": total, "nota": nota,
            "clave": clave_seg, "pend": 0 if pagado else total}).scalar()

        for it in pedido.get("line_items", []):
            sku = (it.get("sku") or "").strip()
            cant = int(it.get("quantity") or 0)
            sub = float(it.get("total") or 0) + float(it.get("total_tax") or 0)  # los precios de la web incluyen IGV
            pu = sub / cant if cant else 0
            lineas.append(f"• {cant} x {it.get('name')}")
            if not sku or not cant:
                alertas.append(f"🔴 «{it.get('name')}» no tiene SKU: descuenta el stock a mano")
                filas, macro = [(cant, False, "")], "Otros"
            else:
                filas, alerta, macro = descontar_stock(conn, sku, cant, idp, idc)
                if alerta:
                    alertas.append(alerta)
                skus.append(sku)
            for c_fila, inv, extra in filas:
                conn.execute(text("""
                    INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario, macro_categoria)
                    VALUES (:v, :s, :d, :c, :pu, :sub, :inv, :m)"""), {
                    "v": idv, "s": sku or None, "d": f"{it.get('name')}{extra}", "c": c_fila, "pu": pu,
                    "sub": round(pu * c_fila, 2), "inv": inv, "m": macro})

        sin_adelanto = clave == "lima_dia" and not pagado
        confirmacion = "pendiente" if sin_adelanto and not lista else ("pagado" if pagado else "espera_pago")
        conn.execute(text("""
            INSERT INTO pedidos_web (id_pedido, id_venta, id_cliente, telefono, telefono2, dni, nombre, envio, pago, total,
                                     confirmacion, alertas, fecha_pedido)
            VALUES (:p, :v, :c, :t, :t2, :dni, :n, :e, :pg, :tot, :conf, :al, :f)"""), {
            "p": idp, "v": idv, "c": idc, "t": tel_n["db"], "t2": tel2 or None, "dni": dni or None,
            "n": f"{nombre} {apellido}".strip(), "e": clave or titulo_envio, "pg": pedido.get("payment_method_title") or pago,
            "tot": total, "conf": confirmacion, "al": "\n".join(alertas) or None, "f": pedido.get("date_created")})

    if skus:
        sync_woo_background(skus)
    try:
        w.post(f"orders/{idp}/notes", {"note": f"Registrado en el panel: venta #{idv}." + (" " + " · ".join(alertas) if alertas else "")})
    except Exception as e:
        log(f"No se pudo dejar la nota en el pedido #{idp}: {e}")

    # ---- Mensaje al cliente ----
    saludo = f"¡Hola {nombre.split(' ')[0].title()}! 👋" if nombre else "¡Hola! 👋"
    msg = (f"{saludo} Gracias por tu pedido *#{idp}* en kmlentes.pe 💙\n\n🛍️ *Tu pedido:*\n" + "\n".join(lineas) +
           f"\n\n🚚 {titulo_envio}\n📍 {destino}\n")
    if sin_adelanto and not lista:
        msg += (f"💰 *Monto a pagar al recibir:* {soles(total)} (efectivo o Yape)\n\n"
                "👉 *Para programar tu envío, respóndenos SÍ a este mensaje.* Si algún dato está mal, escríbenos aquí mismo.\n\n"
                "El motorizado te escribirá y llamará antes de llegar (entre 12:30 pm y 8:00 pm). "
                "Ten habilitadas las llamadas de números desconocidos 📲")
    elif sin_adelanto and lista:
        msg += (f"💰 Total: {soles(total)}\n\nPara programar tu envío necesitamos un adelanto de *S/ 10.00* por Yape al "
                f"{YAPE}; el resto ({soles(max(total - 10, 0))}) lo pagas al motorizado al recibir. "
                "Envíanos la captura por aquí y lo dejamos programado 🙌")
    elif pagado:
        msg += f"✅ Tu pago de {soles(total)} ya está confirmado. Te avisaremos por aquí cuando salga tu pedido."
    else:
        msg += (f"💰 {condicion or 'Total: ' + soles(total)}\n\nPuedes yapear al {YAPE} o usar las cuentas que "
                "aparecen en tu correo de confirmación. *Envíanos la captura del pago por aquí* para programar tu envío 📸")
    enviado = escribir_cliente(tel_n["db"], msg)
    if enviado:
        with engine.begin() as conn:
            conn.execute(text("UPDATE pedidos_web SET fecha_aviso = NOW() WHERE id_pedido = :p"), {"p": idp})

    # ---- Aviso al dueño ----
    aviso = (f"🛒 *Nuevo pedido web #{idp}* — {soles(total)}\n"
             f"👤 {nombre} {apellido} ({tel_n['corto']}{' / ' + tel2 if tel2 else ''}){' 🆕' if nuevo_cli else ''}\n"
             f"🚚 {titulo_envio} · {distrito}\n💳 {pedido.get('payment_method_title') or pago}"
             f"{' (pagado)' if pagado else ''}\n" + "\n".join(lineas) + f"\n🧾 Registrado en el panel: venta #{idv}")
    if alertas:
        aviso += "\n\n🚨 *STOCK*\n" + "\n".join(alertas)
    if lista:
        aviso += f"\n\n⚠️ *Cliente en la lista de adelanto* ({lista}): se le pidió S/ 10 de adelanto."
    if sin_adelanto and not lista:
        aviso += "\n\n📲 Se le pidió confirmar por WhatsApp. Te aviso cuando responda."
    if not enviado:
        aviso += "\n\n❌ No se pudo enviar el WhatsApp al cliente: escríbele tú."
    avisar_dueno(aviso)
    log(f"Pedido #{idp} → venta #{idv} (cliente {idc}{', nuevo' if nuevo_cli else ''}); alertas: {len(alertas)}")
    return idv


def ruta_courier(conn, idp):
    """Texto listo para el courier (plantilla RUTA COURIER de los diálogos)."""
    f = conn.execute(text("""
        SELECT pw.nombre, pw.telefono, pw.telefono2, pw.total, v.pendiente_pago, d.distrito, d.direccion_texto,
               d.referencia, d.gps_link
        FROM pedidos_web pw JOIN Ventas v ON v.id_venta = pw.id_venta
        LEFT JOIN Direcciones d ON d.id_cliente = pw.id_cliente AND d.es_principal
        WHERE pw.id_pedido = :p LIMIT 1"""), {"p": idp}).fetchone()
    if not f:
        return ""
    return (f"*Nombres:* {f.nombre}\n*Celular:* {corto(f.telefono)}{' / ' + f.telefono2 if f.telefono2 else ''}\n"
            f"*Distrito:* {f.distrito or ''}\n*Dirección:* {f.direccion_texto or ''}\n*Referencia:* {f.referencia or ''}\n"
            f"*Monto a cobrar:* {soles(f.pendiente_pago or 0)}\n*Ubicación GPS:* {f.gps_link or ''}\n*Observación:* Web #{idp}")


def revisar_confirmaciones(w):
    with engine.connect() as conn:
        pendientes = conn.execute(text("""
            SELECT id_pedido, id_cliente, telefono, telefono2, nombre, confirmacion, fecha_aviso,
                   (NOW() - fecha_aviso) > make_interval(hours => :h) AS vencido
            FROM pedidos_web
            WHERE confirmacion IN ('pendiente', 'sin_respuesta') AND fecha_aviso IS NOT NULL
              AND fecha_aviso > NOW() - INTERVAL '3 days'"""), {"h": HORAS_SIN_RESPUESTA}).fetchall()
    hora = datetime.now(TZ).hour
    for p in pendientes:
        with engine.connect() as conn:
            msgs = conn.execute(text("""
                SELECT contenido FROM mensajes
                WHERE tipo = 'ENTRANTE' AND fecha > :f
                  AND (id_cliente = :c OR telefono = :t)
                ORDER BY fecha"""), {"f": p.fecha_aviso, "c": p.id_cliente, "t": p.telefono}).fetchall()
        textos = [sin_tildes(m.contenido) for m in msgs if m.contenido]
        if textos:
            junto = " | ".join(m.contenido for m in msgs if m.contenido)[:500]
            if any(NO.search(t) for t in textos):
                nuevo, aviso = "revisar", f"❌ Pedido web *#{p.id_pedido}* ({p.nombre}) parece que quiere *cancelar*:\n«{junto}»\nRevisa el chat."
            elif any(SI.match(t) for t in textos):
                nuevo = "confirmado"
                with engine.connect() as conn:
                    ruta = ruta_courier(conn, p.id_pedido)
                aviso = f"✅ Pedido web *#{p.id_pedido}* ({p.nombre}) *confirmado* por el cliente. Datos para el courier:\n\n{ruta}"
                try:
                    w.put(f"orders/{p.id_pedido}", {"status": "processing"})
                    w.post(f"orders/{p.id_pedido}/notes", {"note": f"El cliente confirmó por WhatsApp: «{junto[:200]}»"})
                except Exception as e:
                    log(f"No se pudo actualizar el pedido #{p.id_pedido} en la web: {e}")
            elif p.confirmacion == "pendiente":
                nuevo, aviso = "revisar", f"💬 El cliente del pedido web *#{p.id_pedido}* ({p.nombre}) respondió algo distinto a SÍ:\n«{junto}»\nRevisa el chat."
            else:
                continue
            with engine.begin() as conn:
                conn.execute(text("""UPDATE pedidos_web SET confirmacion = :e, respuesta = :r,
                                     fecha_confirmacion = CASE WHEN :e = 'confirmado' THEN NOW() ELSE fecha_confirmacion END
                                     WHERE id_pedido = :p"""), {"e": nuevo, "r": junto, "p": p.id_pedido})
            avisar_dueno(aviso)
            log(f"Pedido #{p.id_pedido}: {nuevo}")
        elif p.confirmacion == "pendiente" and p.vencido and 9 <= hora < 21:
            with engine.begin() as conn:
                conn.execute(text("UPDATE pedidos_web SET confirmacion = 'sin_respuesta' WHERE id_pedido = :p"), {"p": p.id_pedido})
            avisar_dueno(f"⏰ El cliente del pedido web *#{p.id_pedido}* ({p.nombre}, {corto(p.telefono)}"
                         f"{' / ' + p.telefono2 if p.telefono2 else ''}) no confirma hace más de {HORAS_SIN_RESPUESTA} h. "
                         "Llámalo antes de programarlo.")
            log(f"Pedido #{p.id_pedido}: sin respuesta")


def sincronizar_lista_web():
    with engine.connect() as conn:
        filas = conn.execute(text("SELECT telefono, dni FROM lista_adelanto WHERE activo")).fetchall()
    tels = sorted({corto(f.telefono) for f in filas if f.telefono and corto(f.telefono)})
    dnis = sorted({re.sub(r"[^0-9A-Za-z]", "", f.dni) for f in filas if f.dni})
    datos = {"telefonos": tels, "dnis": dnis}
    huella = hashlib.md5(json.dumps(datos).encode()).hexdigest()
    if huella == ajuste("lista_adelanto_huella"):
        return
    r = requests.post(f"{os.getenv('WOO_URL').rstrip('/')}/wp-json/km/v1/lista-adelanto", json=datos,
                      auth=(os.getenv("WP_USER"), os.getenv("WP_APP_PASSWORD")), timeout=60)
    if r.status_code == 200:
        guardar_ajuste("lista_adelanto_huella", huella)
        log(f"Lista de adelanto enviada a la web: {r.json()}")
    else:
        log(f"No se pudo enviar la lista de adelanto a la web: {r.status_code} {r.text[:200]}")


def main():
    crear_tablas()
    desde = ajuste("pedidos_web_desde")
    if not desde:
        desde = datetime.now(TZ).strftime("%Y-%m-%dT%H:%M:%S")
        guardar_ajuste("pedidos_web_desde", desde)
        log(f"Primera ejecución: se registran los pedidos desde {desde}")
    w = woo()
    r = w.get("orders", params={"after": desde, "per_page": 50, "orderby": "date", "order": "asc"})
    if r.status_code != 200:
        log(f"Error leyendo pedidos de la web: {r.status_code} {r.text[:200]}")
        return
    with engine.connect() as conn:
        hechos = {f[0] for f in conn.execute(text("SELECT id_pedido FROM pedidos_web"))}
    for pedido in r.json():
        if pedido["id"] in hechos or pedido.get("status") in ESTADOS_IGNORAR:
            continue
        try:
            registrar(pedido, w)
        except Exception as e:
            log(f"🔥 Error registrando el pedido #{pedido['id']}: {e}")
            avisar_dueno(f"🔥 No pude registrar el pedido web *#{pedido['id']}* en el panel ({e}). Regístralo a mano.")
            with engine.begin() as conn:  # se marca para no reintentar (y no repetir el aviso) cada 5 minutos
                conn.execute(text("""INSERT INTO pedidos_web (id_pedido, confirmacion, alertas, fecha_pedido)
                                     VALUES (:p, 'error', :e, :f) ON CONFLICT DO NOTHING"""),
                             {"p": pedido["id"], "e": str(e)[:500], "f": pedido.get("date_created")})
    try:
        revisar_confirmaciones(w)
    except Exception as e:
        log(f"🔥 Error revisando confirmaciones: {e}")
    try:
        sincronizar_lista_web()
    except Exception as e:
        log(f"🔥 Error sincronizando la lista de adelanto: {e}")


if __name__ == "__main__":
    main()
