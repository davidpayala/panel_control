"""
🧾 Boletas automáticas en SUNAT (SEE - SOL) para las ventas del panel que todavía no tienen boleta.

Toma las ventas con facturado = FALSE (no anuladas, sin pago pendiente, de días anteriores a hoy),
de la más antigua a la más nueva, y emite una boleta electrónica por cada una en el portal SOL,
respetando los límites que se editan en el panel (Ventas → Facturación Individual → Boletas automáticas):

  · monto máximo por venta (S/ 400)            → las ventas mayores se saltan
  · no dos boletas seguidas con el mismo total en el mismo día (al día siguiente sí se puede)
  · máximo de boletas por día (10)
  · máximo emitido en el mes (S/ 5000), contando por la fecha en que se emitió en SUNAT

Por cada venta:
  1. Cliente: si el panel tiene DNI (datos del cliente o datos de envío) se consulta en SUNAT; si el nombre
     que devuelve SUNAT se parece al del panel se emite con DNI (y el DNI se guarda en el cliente).
     Si no hay DNI o el nombre no se parece → "SIN DOCUMENTO" con el nombre del cliente escrito a mano.
  2. Ítems: un "Bien" por producto (los de precio 0 no van) y un "Servicio" ENVIO si hubo costo de envío.
  3. Sin observaciones → boleta preliminar. Si su total no coincide con el del panel, se cancela.
  4. Emitir → Aceptar → se copia el número (EB01-…) al panel → Cerrar → Aceptar, y sigue con la próxima.

El portal de SUNAT a veces se cae o tarda mucho: ante cualquier falla se detiene y lo intenta al día siguiente.
Si la falla ocurre justo después de pulsar "Emitir" (no se sabe si la boleta salió) la venta queda
"POR REVISAR" en el panel y no se vuelve a intentar sola hasta que alguien la revise.

Todo queda registrado en la tabla sunat_boletas_log (se ve en el panel).

Uso:
    .venv/bin/python recurrente/boletas_sunat.py                  # simulación: qué ventas emitiría (no abre SUNAT)
    .venv/bin/python recurrente/boletas_sunat.py --aplicar        # emite de verdad
    .venv/bin/python recurrente/boletas_sunat.py --preliminar --venta=429   # llega a la preliminar y NO emite
    .venv/bin/python recurrente/boletas_sunat.py --aplicar --venta=429      # emite solo esa venta (respeta límites)
    Opciones: --max=N (emitir como mucho N en esta corrida), --origen=cron|panel|manual
    Cron: todos los días 05:00.
"""
import argparse
import difflib
import fcntl
import json
import os
import re
import sys
import time
import traceback
import unicodedata
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from dotenv import dotenv_values, load_dotenv
load_dotenv(os.path.join(_raiz, ".env"))

from sqlalchemy import text
from database import engine

LIMA = ZoneInfo("America/Lima")
CAPTURAS = os.path.expanduser("~/sunat_capturas")
LOCK = os.path.expanduser("~/.boletas_sunat.lock")
# El Chromium de Playwright necesita libasound, que no está instalada en el sistema: va extraída aquí (sin root)
LIBS_EXTRA = os.path.expanduser("~/libs_extra/x/usr/lib/x86_64-linux-gnu")
# El portal SOL corta la conexión al navegador "headless" por su identificador; se presenta como Chrome normal
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
URL_LOGIN = "https://e-menu.sunat.gob.pe/cl-ti-itmenu/MenuInternet.htm"
ALERTA_TEL = "51986203398"
ALERTA_SESION = "principal"

CONFIG_DEFECTO = {
    "activo": "1",            # 0 = el cron no emite nada
    "monto_max": "400",       # S/ por venta
    "max_dia": "10",          # boletas por día
    "max_mes": "5000",        # S/ emitidos en el mes
    "espera_seg": "30",       # SUNAT no deja emitir boletas con menos de 20 s entre una y otra
}


class PortalCaido(Exception):
    """SUNAT no respondió o cambió algo: se detiene la corrida (se reintenta al día siguiente)."""


class EstadoIncierto(Exception):
    """Falló después de pulsar Emitir: no se sabe si la boleta salió."""


# ─────────────────────────── base de datos ───────────────────────────

def preparar_tablas(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sunat_config (
            clave VARCHAR(40) PRIMARY KEY, valor TEXT, actualizado TIMESTAMP DEFAULT NOW())"""))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sunat_boletas_log (
            id SERIAL PRIMARY KEY,
            fecha TIMESTAMP DEFAULT (NOW() - INTERVAL '5 hours'),
            corrida VARCHAR(40), origen VARCHAR(20),
            id_venta INTEGER, monto NUMERIC(10,2),
            estado VARCHAR(20),         -- EMITIDA, SALTADA, ERROR, POR_REVISAR, PRELIMINAR, INFO, REVISADA
            boleta VARCHAR(30), documento VARCHAR(40), detalle TEXT)"""))
    for k, v in CONFIG_DEFECTO.items():
        conn.execute(text("INSERT INTO sunat_config (clave, valor) VALUES (:k, :v) ON CONFLICT (clave) DO NOTHING"),
                     {"k": k, "v": v})


def leer_config(conn):
    cfg = dict(CONFIG_DEFECTO)
    cfg.update({k: v for k, v in conn.execute(text("SELECT clave, valor FROM sunat_config")).fetchall()})
    return {
        "activo": str(cfg["activo"]) == "1",
        "monto_max": Decimal(str(cfg["monto_max"])),
        "max_dia": int(cfg["max_dia"]),
        "max_mes": Decimal(str(cfg["max_mes"])),
        "espera_seg": max(int(float(cfg["espera_seg"])), 20),
    }


def registrar(corrida, origen, id_venta, monto, estado, detalle="", boleta=None, documento=None):
    print(f"   [{estado}] venta {id_venta or '-'} S/{monto or '-'} {boleta or ''} {detalle}")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sunat_boletas_log (corrida, origen, id_venta, monto, estado, boleta, documento, detalle)
                VALUES (:c, :o, :v, :m, :e, :b, :d, :t)"""),
                {"c": corrida, "o": origen, "v": id_venta, "m": monto, "e": estado, "b": boleta,
                 "d": documento, "t": (detalle or "")[:1000]})
    except Exception as e:
        print(f"   ⚠️ no se pudo guardar en el log: {e}")


def alertar(mensaje, clave):
    """Aviso en la bandeja de alertas del panel + WhatsApp (no repite la misma clave en 20 h)."""
    print(f"🚨 {mensaje}")
    texto = f"🧾 *Boletas SUNAT*\n{mensaje}"
    try:
        with engine.begin() as conn:
            ya = conn.execute(text("""
                SELECT 1 FROM webhook_logs WHERE session_name = 'sunat' AND payload LIKE :c
                  AND fecha > NOW() - INTERVAL '20 hours' LIMIT 1"""), {"c": f'%"clave": "{clave}"%'}).fetchone()
            if ya:
                return
            conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) "
                              "VALUES ('sunat', 'ALERTA_CRITICA', :p)"),
                         {"p": json.dumps({"mensaje": texto.replace("*", ""), "clave": clave}, ensure_ascii=False)})
        from utils import enviar_mensaje_whatsapp
        enviar_mensaje_whatsapp(ALERTA_TEL, texto, session=ALERTA_SESION)
    except Exception as e:
        print(f"   ⚠️ no se pudo avisar: {e}")


def hoy_lima():
    return datetime.now(LIMA).date()


def estado_boletas(conn, hoy):
    """Lo ya emitido (a mano o automático): boletas de hoy, último monto de hoy y total del mes."""
    filas = conn.execute(text("""
        SELECT numero_boleta, total_venta FROM ventas
        WHERE fecha_facturacion = :h AND numero_boleta LIKE 'EB%%' AND anulado = FALSE"""), {"h": hoy}).fetchall()
    filas = sorted(filas, key=lambda f: int(re.sub(r"\D", "", f[0].split("-")[-1]) or 0))
    mes = conn.execute(text("""
        SELECT COALESCE(SUM(total_venta), 0) FROM ventas
        WHERE numero_boleta LIKE 'EB%%' AND anulado = FALSE
          AND fecha_facturacion >= date_trunc('month', CAST(:h AS date))
          AND fecha_facturacion <= :h"""), {"h": hoy}).scalar()
    return {"hoy": len(filas), "ultimo_monto": Decimal(filas[-1][1]) if filas else None,
            "mes": Decimal(mes or 0)}


def ventas_pendientes(conn, hoy, solo_venta=None):
    filtro = "AND v.id_venta = :vid" if solo_venta else "AND CAST(v.fecha_venta AS date) < :hoy"
    filas = conn.execute(text(f"""
        SELECT v.id_venta, v.fecha_venta, v.total_venta, COALESCE(v.costo_envio, 0) AS costo_envio, v.pendiente_pago,
               c.id_cliente, c.nombre, c.apellido, c.nombre_corto, c.dni
        FROM ventas v JOIN clientes c ON c.id_cliente = v.id_cliente
        WHERE v.facturado = FALSE AND COALESCE(v.anulado, FALSE) = FALSE {filtro}
          AND v.id_venta NOT IN (SELECT id_venta FROM sunat_boletas_log l WHERE l.estado = 'POR_REVISAR'
                                   AND l.id_venta IS NOT NULL
                                   AND NOT EXISTS (SELECT 1 FROM sunat_boletas_log r WHERE r.estado = 'REVISADA'
                                                   AND r.id_venta = l.id_venta AND r.id > l.id))
        ORDER BY v.fecha_venta, v.id_venta"""), {"hoy": hoy, "vid": solo_venta}).mappings().all()
    ventas = []
    for f in filas:
        v = dict(f)
        v["items"] = [dict(i) for i in conn.execute(text("""
            SELECT sku, descripcion, cantidad, precio_unitario, subtotal, macro_categoria
            FROM detalleventa WHERE id_venta = :v ORDER BY id_detalle"""), {"v": v["id_venta"]}).mappings().all()]
        v["direccion"] = conn.execute(text("""
            SELECT nombre_receptor, dni_receptor FROM direcciones WHERE id_cliente = :c
            ORDER BY id_direccion DESC LIMIT 1"""), {"c": v["id_cliente"]}).mappings().first()
        ventas.append(v)
    return ventas


def segundos_desde_ultima():
    """Segundos desde la última boleta emitida por este proceso (de cualquier corrida); None si no hay."""
    with engine.connect() as conn:
        seg = conn.execute(text("""
            SELECT EXTRACT(EPOCH FROM ((NOW() - INTERVAL '5 hours') - MAX(fecha)))
            FROM sunat_boletas_log WHERE estado IN ('EMITIDA', 'POR_REVISAR')""")).scalar()
    return float(seg) if seg is not None else None


def esperar_turno(espera):
    """SUNAT rechaza boletas emitidas con menos de 20 s de diferencia: espera hasta cumplir `espera` segundos
    desde la última emitida antes de empezar la siguiente."""
    try:
        pasados = segundos_desde_ultima()
    except Exception:
        pasados = 0  # si no se puede leer, se espera el tiempo completo
    if pasados is not None and pasados < espera:
        falta = espera - max(pasados, 0)
        print(f"   esperando {falta:.0f} s (SUNAT no permite boletas muy seguidas)")
        time.sleep(falta)


def guardar_boleta(id_venta, id_cliente, boleta, dni_usado):
    """Igual que 'Guardar y Archivar' de Facturación Individual."""
    with engine.begin() as conn:
        otra = conn.execute(text("SELECT id_venta FROM ventas WHERE numero_boleta = :b AND id_venta <> :v"),
                            {"b": boleta, "v": id_venta}).fetchone()
        if otra:
            raise RuntimeError(f"la boleta {boleta} ya figura en la venta {otra[0]}")
        conn.execute(text("""
            UPDATE ventas SET facturado = TRUE, fecha_facturacion = (NOW() - INTERVAL '5 hours'),
                   numero_boleta = :b WHERE id_venta = :v"""), {"b": boleta, "v": id_venta})
        if dni_usado:
            conn.execute(text("UPDATE clientes SET dni = :d WHERE id_cliente = :c AND COALESCE(TRIM(dni), '') = ''"),
                         {"d": dni_usado, "c": id_cliente})


# ─────────────────────────── preparar la boleta ───────────────────────────

def sin_tildes(s):
    s = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def limpiar(s, permitidos=r"A-Za-z0-9 .,()/\-"):
    s = sin_tildes(s).replace("Ñ", "N").replace("ñ", "n")
    s = re.sub(rf"[^{permitidos}]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def palabras(s):
    return [p for p in limpiar(s).lower().split() if len(p) >= 2 and not re.fullmatch(r"[\d.]+", p)]


def nombre_parecido(nombre_panel, nombre_sunat):
    """Se parece si al menos 2 palabras del panel están en el nombre de SUNAT
    (o la única palabra del panel, cuando el panel solo tiene un nombre). Tolera errores de tipeo."""
    pp, ps = palabras(nombre_panel), palabras(nombre_sunat)
    if not pp or not ps:
        return False
    coinciden = sum(1 for p in pp if any(p == s or difflib.SequenceMatcher(None, p, s).ratio() >= 0.85 for s in ps))
    return coinciden >= 2 or (len(pp) == 1 and coinciden == 1)


def nombre_cliente(v):
    n = f"{v['nombre'] or ''} {v['apellido'] or ''}".strip() or (v["nombre_corto"] or "").strip()
    return limpiar(n, r"A-Za-z0-9 .&\-").upper()[:100] or "CLIENTE"


def candidatos_dni(v):
    """[(dni, nombre con el que comparar)] — primero el del cliente, luego el de los datos de envío."""
    out = []
    nombre_cli = f"{v['nombre'] or ''} {v['apellido'] or ''}".strip() or (v["nombre_corto"] or "")
    dni = re.sub(r"\D", "", str(v["dni"] or ""))
    if len(dni) == 8:
        out.append((dni, [nombre_cli]))
    d = v["direccion"]
    if d:
        dni2 = re.sub(r"\D", "", str(d["dni_receptor"] or ""))
        if len(dni2) == 8 and dni2 not in [x[0] for x in out]:
            out.append((dni2, [nombre_cli, d["nombre_receptor"] or ""]))
    return out


def items_boleta(v):
    items = []
    for i in v["items"]:
        precio = Decimal(i["precio_unitario"] or 0)
        cant = Decimal(i["cantidad"] or 0)
        if precio <= 0 or cant <= 0:
            continue  # regalos (precio 0) no van en la boleta
        codigo = limpiar(i["sku"], r"A-Za-z0-9\-").replace(" ", "")
        if not codigo:
            codigo = limpiar(i["macro_categoria"] or "PRODUCTO", r"A-Za-z0-9").upper()
        desc = re.sub(r"\(\s*Ubi:[^)]*\)", "", str(i["descripcion"] or ""))
        desc = limpiar(desc)[:250] or codigo
        items.append({"tipo": "B", "cant": cant, "codigo": codigo[:30], "desc": desc, "precio": precio})
    envio = Decimal(v["costo_envio"] or 0)
    if envio > 0:
        items.append({"tipo": "S", "cant": Decimal(1), "codigo": "ENVIO", "desc": "Servicio de envio",
                      "precio": envio})
    return items


def plan(ventas, cfg, est, max_corrida=None):
    """Decide qué ventas se emiten hoy (en orden) y por qué se saltan las demás."""
    emitir, saltar = [], []
    hoy, ultimo, mes = est["hoy"], est["ultimo_monto"], est["mes"]
    for v in ventas:
        total = Decimal(v["total_venta"] or 0)
        items = items_boleta(v)
        suma = sum((i["cant"] * i["precio"] for i in items), Decimal(0))
        pend = a_decimal(v["pendiente_pago"] or 0)
        motivo = None
        if pend > 0:
            motivo = f"pago pendiente S/{pend:.2f}"
        elif total <= 0 or not items:
            motivo = "sin productos con precio"
        elif suma != total:
            motivo = f"el detalle suma S/{suma:.2f} y el total es S/{total:.2f}"
        elif total > cfg["monto_max"]:
            motivo = f"supera el máximo por venta (S/{cfg['monto_max']:.0f})"
        elif ultimo is not None and total == ultimo:
            motivo = "mismo monto que la boleta anterior de hoy"
        elif mes + total > cfg["max_mes"]:
            motivo = f"pasaría el máximo del mes (S/{mes:.2f} + S/{total:.2f} > S/{cfg['max_mes']:.0f})"
        if motivo:
            saltar.append((v, motivo))
            continue
        if hoy >= cfg["max_dia"] or (max_corrida is not None and len(emitir) >= max_corrida):
            break
        v["_items"] = items
        emitir.append(v)
        hoy += 1
        ultimo = total
        mes += total
    return emitir, saltar


# ─────────────────────────── navegador ───────────────────────────

def esperar(fr, selector, timeout=60000):
    try:
        fr.locator(selector).first.wait_for(state="visible", timeout=timeout)
    except Exception as e:
        raise PortalCaido(f"no apareció {selector}: {str(e).splitlines()[0]}")


def dialogo(fr):
    """Texto de un mensaje de error/aviso de la aplicación, si hay uno abierto."""
    for d in fr.locator(".dijitDialog:visible").all():
        t = d.inner_text().strip()
        t = re.sub(r"\s+", " ", t)
        # "Procesando...", "Adicionando...", "Cargando..." son avisos de espera, no errores
        if t and not (len(t) < 60 and t.endswith("...")):
            return t
    return None


class Sunat:
    def __init__(self, pw, capturas=True):
        env = dotenv_values(os.path.join(_raiz, ".env"))
        self.ruc = (env.get("SUNAT.RUC") or "").strip()
        self.usuario = (env.get("SUNAT.DNI") or "").strip()
        self.clave = (env.get("SUNAT.CLAVE") or "").strip()
        if not (self.ruc and self.usuario and self.clave):
            raise RuntimeError("faltan SUNAT.RUC / SUNAT.DNI / SUNAT.CLAVE en el .env")
        self.capturas = capturas
        self.browser = pw.chromium.launch()
        self.ctx = self.browser.new_context(viewport={"width": 1300, "height": 900}, locale="es-PE",
                                            user_agent=USER_AGENT)
        self.ctx.set_default_timeout(60000)
        self.page = self.ctx.new_page()
        self.fr = None

    def foto(self, nombre):
        if self.capturas:
            try:
                os.makedirs(CAPTURAS, exist_ok=True)
                self.page.screenshot(path=os.path.join(CAPTURAS, f"{nombre}.png"), full_page=True)
            except Exception:
                pass

    def cerrar(self):
        try:
            self.browser.close()
        except Exception:
            pass

    def entrar(self):
        p = self.page
        try:
            p.goto(URL_LOGIN, timeout=90000)
            p.wait_for_selector("#txtRuc", timeout=90000)
            p.fill("#txtRuc", self.ruc)
            p.fill("#txtUsuario", self.usuario)
            p.fill("#txtContrasena", self.clave)
            p.click("#btnAceptar")
            p.wait_for_selector("text=Empresas", timeout=90000)
        except Exception as e:
            self.foto("error_login")
            if p.locator("#txtRuc:visible").count():
                msg = p.locator("body").inner_text()[:300]
                raise RuntimeError(f"SUNAT no aceptó el inicio de sesión: {re.sub(chr(10), ' ', msg)}")
            raise PortalCaido(f"no cargó el inicio de sesión: {str(e).splitlines()[0]}")
        try:
            p.click("text=Empresas")
            p.wait_for_timeout(1500)
            p.locator("span:visible, a:visible").filter(has_text="Comprobantes de pago").first.click()
            p.wait_for_timeout(1500)
            p.locator("#nivel2_11_5").first.click()        # SEE - SOL
            p.wait_for_timeout(1200)
            p.locator("#nivel3_11_5_4").first.click()      # Boleta de Venta Electrónica
            p.wait_for_timeout(1200)
            p.locator("#nivel4_11_5_4_1_1").first.click()  # Emitir Boleta de Venta
        except Exception as e:
            self.foto("error_menu")
            raise PortalCaido(f"no se pudo abrir el menú de boletas: {str(e).splitlines()[0]}")
        self._formulario()

    def _formulario(self):
        for _ in range(60):
            self.fr = self.page.frame(name="iframeApplication")
            if self.fr and self.fr.locator("#inicio\\.numeroDocumento:visible").count():
                return
            self.page.wait_for_timeout(1000)
        self.foto("error_formulario")
        raise PortalCaido("no cargó el formulario de Emitir Boleta")

    def _escribir(self, selector, valor):
        l = self.fr.locator(selector)
        l.click()
        l.fill("")
        l.type(str(valor), delay=25)
        l.press("Tab")
        self.fr.wait_for_timeout(400)

    def _tipo_doc(self, valor):
        self.fr.evaluate("v => dijit.byId('inicio.tipoDocumento').set('value', v)", valor)
        self.fr.wait_for_timeout(1200)

    def consultar_dni(self, dni):
        """Pone el DNI y devuelve el nombre que trae SUNAT ('' si no lo encontró)."""
        self._tipo_doc("1")
        self._escribir("#inicio\\.numeroDocumento", dni)
        nombre = ""
        for _ in range(20):
            nombre = self.fr.locator("#inicio\\.razonSocial").input_value().strip()
            if nombre:
                break
            self.fr.wait_for_timeout(500)
        msg = dialogo(self.fr)
        if msg:
            self._cerrar_dialogo()
        return nombre

    def _cerrar_dialogo(self):
        for sel in ("#dlgBtnAceptar", "#dlgBtnAceptarConfirm", ".dijitDialog:visible span[role=button]:visible"):
            l = self.fr.locator(sel)
            if l.count() and l.first.is_visible():
                l.first.click()
                self.fr.wait_for_timeout(800)
                return

    def llenar_cliente(self, v):
        """Devuelve (documento para el log, dni usado o None)."""
        for dni, nombres in candidatos_dni(v):
            nombre_sunat = self.consultar_dni(dni)
            if nombre_sunat and any(nombre_parecido(n, nombre_sunat) for n in nombres):
                return f"DNI {dni} · {nombre_sunat}", dni
            print(f"   DNI {dni}: SUNAT dice '{nombre_sunat or '—'}' y el panel '{' / '.join(nombres)}' → no coincide")
        nombre = nombre_cliente(v)
        self._tipo_doc("-")
        rs = self.fr.locator("#inicio\\.razonSocial")
        if not rs.is_enabled():
            raise PortalCaido("con SIN DOCUMENTO no se habilitó el campo del nombre")
        self._escribir("#inicio\\.razonSocial", nombre)
        return f"SIN DOCUMENTO · {nombre}", None

    def continuar(self, boton, siguiente):
        self.fr.locator(boton).click()
        for _ in range(60):
            if self.fr.locator(f"{siguiente}:visible").count():
                return
            msg = dialogo(self.fr)
            if msg:
                self.foto("error_paso")
                raise PortalCaido(f"SUNAT respondió: {msg[:300]}")
            self.fr.wait_for_timeout(1000)
        self.foto("error_paso")
        raise PortalCaido(f"no avanzó de {boton}")

    def agregar_item(self, it):
        fr = self.fr
        fr.locator("#boleta\\.addItemButton").click()
        esperar(fr, "#item\\.cantidad", 30000)
        fr.wait_for_timeout(600)
        fr.locator("#item\\.subTipoTI01" if it["tipo"] == "B" else "#item\\.subTipoTI02").click()
        fr.wait_for_timeout(600)
        cant = it["cant"]
        self._escribir("#item\\.cantidad", f"{cant:f}".rstrip("0").rstrip(".") if "." in f"{cant:f}" else f"{cant}")
        self._escribir("#item\\.codigoItem", it["codigo"])
        self._escribir("#item\\.descripcion", it["desc"])
        self._escribir("#item\\.precioUnitario", f"{it['precio']:.2f}")
        fr.wait_for_timeout(600)
        importe = fr.locator("#item\\.importeVenta").input_value()
        esperado = it["cant"] * it["precio"]
        if a_decimal(importe) != esperado:
            raise PortalCaido(f"el ítem {it['codigo']} quedó con importe {importe} (esperado {esperado:.2f})")
        fr.locator("#item\\.botonAceptar").click()
        for _ in range(60):
            fr.wait_for_timeout(1000)
            if not fr.locator("#item\\.botonAceptar:visible").count():
                return
            msg = dialogo(fr)
            if msg and not msg.startswith("Nuevo Item"):
                break
        self.foto("error_item")
        raise PortalCaido(f"SUNAT no aceptó el ítem {it['codigo']}: {(dialogo(fr) or '')[:300]}")

    def preparar(self, v):
        """Llena todo hasta la boleta preliminar. Devuelve el documento usado y el dni a guardar."""
        documento, dni = self.llenar_cliente(v)
        self.continuar("#inicio\\.botonGrabarDocumento", "#boleta\\.addItemButton")
        for it in v["_items"]:
            self.agregar_item(it)
        total = a_decimal(self.fr.locator("#boleta\\.totalGeneral").input_value())
        if total != Decimal(v["total_venta"]):
            raise ValueError(f"en SUNAT suma S/{total:.2f} y en el panel S/{Decimal(v['total_venta']):.2f}")
        self.continuar("#boleta\\.botonGrabarDocumento", "#docsrel\\.botonGrabarDocumento")
        self.continuar("#docsrel\\.botonGrabarDocumento", "#boleta-preliminar\\.botonGrabarDocumento")
        texto = self.fr.locator("body").inner_text()
        m = re.search(r"Importe Total\s*:\s*S/\s*([\d,.]+)", texto)
        if m and a_decimal(m.group(1)) != Decimal(v["total_venta"]):
            raise ValueError(f"la preliminar dice S/{m.group(1)} y el panel S/{v['total_venta']}")
        return documento, dni

    def emitir(self):
        """Emitir → Aceptar → leer número → Cerrar → Aceptar (formulario listo para la siguiente)."""
        fr = self.fr
        for intento in range(4):
            fr.locator("#boleta-preliminar\\.botonGrabarDocumento").click()
            esperar(fr, "#dlgBtnAceptarConfirm", 30000)
            fr.locator("#dlgBtnAceptarConfirm").click()
            # Desde aquí la boleta pudo haberse emitido: cualquier falla es "incierta"
            try:
                numero, reintentar_en = None, None
                for _ in range(90):
                    fr.wait_for_timeout(1000)
                    m = re.search(r"\b(EB\d{2}\s*-\s*\d+)\b", fr.locator("body").inner_text())
                    if m and fr.locator("span[role=button]:visible", has_text="Descargar PDF").count():
                        numero = re.sub(r"\s", "", m.group(1))
                        break
                    msg = dialogo(fr)
                    if msg and "seguro de emitir" not in msg:
                        # "Solo se pueden emitir boletas de venta cada 20 segundos, por favor volver a intentar
                        #  en 4 segundo(s)": SUNAT la rechazó (no se emitió) → esperar y volver a pulsar Emitir
                        r = re.search(r"cada \d+ segundos.*?intentar en (\d+) segundo", msg, re.I)
                        if r and intento < 3:
                            reintentar_en = int(r.group(1)) + 5
                            break
                        raise EstadoIncierto(f"SUNAT respondió: {msg[:300]}")
                if reintentar_en:
                    print(f"   SUNAT pide esperar {reintentar_en - 5} s entre boletas: se reintenta en {reintentar_en} s")
                    self._cerrar_dialogo()
                    time.sleep(reintentar_en)
                    continue
                if not numero:
                    raise EstadoIncierto("no apareció el número de boleta")
            except EstadoIncierto:
                self.foto("incierto")
                raise
            except Exception as e:
                self.foto("incierto")
                raise EstadoIncierto(str(e).splitlines()[0])
            self.foto("ultima_emitida")
            return numero
        raise EstadoIncierto("SUNAT siguió pidiendo esperar entre boletas")

    def siguiente(self):
        """Cerrar → '¿Desea emitir otra Boleta?' Aceptar → formulario nuevo."""
        fr = self.fr
        fr.locator("span[role=button]:visible", has_text="Cerrar").last.click()
        esperar(fr, "#dlgBtnAceptarConfirm", 30000)
        fr.locator("#dlgBtnAceptarConfirm").click()
        self._formulario()

    def cancelar(self):
        """Abandona la boleta en curso (sin emitir) y vuelve a un formulario limpio."""
        self.page.locator("#nivel4_11_5_4_1_1").first.click()
        self.page.wait_for_timeout(2000)
        self._formulario()


def a_decimal(s):
    s = re.sub(r"[^\d.]", "", str(s).replace(",", ""))
    return Decimal(s or "0").quantize(Decimal("0.01"))


# ─────────────────────────── corrida ───────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aplicar", action="store_true", help="emite de verdad")
    ap.add_argument("--preliminar", action="store_true", help="llega a la preliminar y no emite")
    ap.add_argument("--venta", type=int)
    ap.add_argument("--max", type=int)
    ap.add_argument("--origen", default="manual")
    a = ap.parse_args()

    lock = open(LOCK, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("Ya hay otra corrida de boletas en marcha.")
        return 1

    corrida = datetime.now(LIMA).strftime("%Y%m%d-%H%M%S")
    hoy = hoy_lima()
    with engine.begin() as conn:
        preparar_tablas(conn)
        cfg = leer_config(conn)
        est = estado_boletas(conn, hoy)
        ventas = ventas_pendientes(conn, hoy, a.venta)

    print(f"=== Boletas SUNAT {corrida} ({a.origen}) — hoy {est['hoy']} emitidas, último monto "
          f"{est['ultimo_monto']}, mes S/{est['mes']:.2f} — límites {cfg}")
    if a.aplicar and not cfg["activo"] and a.origen == "cron":
        print("Emisión automática desactivada en el panel.")
        registrar(corrida, a.origen, None, None, "INFO", "emisión automática desactivada en el panel")
        return 0

    emitir, saltar = plan(ventas, cfg, est, a.max)
    if a.preliminar:
        emitir = emitir[:1] if emitir else []
    print(f"Se emitirían {len(emitir)}; se saltan {len(saltar)}.")
    for v, motivo in saltar:
        print(f"   saltar venta {v['id_venta']} S/{v['total_venta']}: {motivo}")
    for v in emitir:
        print(f"   emitir venta {v['id_venta']} S/{v['total_venta']} {nombre_cliente(v)} "
              f"· ítems: {[(i['codigo'], str(i['cant']), str(i['precio'])) for i in v['_items']]}")

    if not (a.aplicar or a.preliminar):
        print("(simulación: no se abrió SUNAT — usa --aplicar para emitir o --preliminar para probar)")
        return 0
    if a.aplicar and not a.preliminar:
        for v, motivo in saltar:
            registrar(corrida, a.origen, v["id_venta"], v["total_venta"], "SALTADA", motivo)
    if not emitir:
        registrar(corrida, a.origen, None, None, "INFO", "no hay ventas para emitir hoy (límites o sin pendientes)")
        return 0

    if os.path.isdir(LIBS_EXTRA):
        os.environ["LD_LIBRARY_PATH"] = LIBS_EXTRA + (":" + os.environ["LD_LIBRARY_PATH"]
                                                      if os.environ.get("LD_LIBRARY_PATH") else "")
    from playwright.sync_api import sync_playwright

    emitidas = 0
    with sync_playwright() as pw:
        s = Sunat(pw)
        try:
            s.entrar()
            for v in emitir:
                vid, total = v["id_venta"], v["total_venta"]
                if not a.preliminar:
                    esperar_turno(cfg["espera_seg"])
                print(f"→ venta {vid} S/{total}")
                try:
                    documento, dni = s.preparar(v)
                except ValueError as e:      # no cuadra el total: se cancela esta y se sigue con la próxima
                    registrar(corrida, a.origen, vid, total, "ERROR", str(e))
                    s.cancelar()
                    continue
                if a.preliminar:
                    s.foto(f"preliminar_venta_{vid}")
                    registrar(corrida, a.origen, vid, total, "PRELIMINAR", "prueba: llegó a la preliminar, no se emitió",
                              documento=documento)
                    break
                try:
                    numero = s.emitir()
                except EstadoIncierto as e:
                    registrar(corrida, a.origen, vid, total, "POR_REVISAR",
                              f"falló justo después de pulsar Emitir ({e}). Revisa en SUNAT (Consultar Boleta) si "
                              "salió; si salió, pon el número en Facturación Individual.", documento=documento)
                    alertar(f"⚠️ No sé si se emitió la boleta de la venta {vid} (S/{total}): {e}. "
                            "Revísalo en SUNAT → Consultar Boleta de Venta. Se detuvo la emisión de hoy.",
                            f"incierta-{vid}")
                    break
                try:
                    guardar_boleta(vid, v["id_cliente"], numero, dni)
                    registrar(corrida, a.origen, vid, total, "EMITIDA", "", boleta=numero, documento=documento)
                except Exception as e:
                    registrar(corrida, a.origen, vid, total, "POR_REVISAR",
                              f"la boleta {numero} SÍ se emitió pero no se pudo guardar en el panel: {e}",
                              boleta=numero, documento=documento)
                    alertar(f"⚠️ La boleta {numero} (venta {vid}) se emitió pero no se guardó en el panel: {e}",
                            f"noguardada-{vid}")
                    break
                emitidas += 1
                s.siguiente()
        except PortalCaido as e:
            registrar(corrida, a.origen, None, None, "ERROR",
                      f"el portal de SUNAT falló, se intenta de nuevo mañana: {e}")
            if a.origen == "cron":
                alertar(f"El portal de SUNAT falló y hoy se emitieron {emitidas} boletas: {e}", f"portal-{hoy}")
        except Exception as e:
            traceback.print_exc()
            registrar(corrida, a.origen, None, None, "ERROR", f"{type(e).__name__}: {e}")
            alertar(f"La emisión de boletas se detuvo por un error: {e}", f"error-{hoy}")
        finally:
            s.cerrar()
    print(f"=== fin: {emitidas} boletas emitidas")
    registrar(corrida, a.origen, None, None, "INFO", f"fin de la corrida: {emitidas} boletas emitidas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
