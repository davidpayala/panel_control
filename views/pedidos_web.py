"""Ventas → 🌐 Pedidos web: pedidos de kmlentes.pe registrados por recurrente/pedidos_web.py y lista interna de
clientes que deben adelantar (no recibieron un pedido). La lista se sube sola a la web en la siguiente pasada (≤ 5 min)."""
import re

import pandas as pd
import streamlit as st
from sqlalchemy import text

from database import engine

ESTADOS = {
    "pendiente": "⏳ Esperando que confirme",
    "sin_respuesta": "⏰ No responde",
    "revisar": "💬 Revisar chat",
    "confirmado": "✅ Confirmado",
    "espera_pago": "💳 Esperando pago",
    "pagado": "💰 Pagado en la web",
    "entregado": "📦 Entregado",
    "no_recibio": "🚫 No recibió",
    "error": "🔥 Error al registrar",
}


def _tablas_listas():
    try:
        with engine.connect() as c:
            c.execute(text("SELECT 1 FROM pedidos_web LIMIT 1"))
            c.execute(text("SELECT 1 FROM lista_adelanto LIMIT 1"))
        return True
    except Exception:
        return False


def _ajuste(clave, defecto=""):
    with engine.connect() as c:
        v = c.execute(text("SELECT valor FROM ajustes WHERE clave = :k"), {"k": clave}).scalar()
    return v or defecto


def _celular(t):
    d = re.sub(r"\D", "", str(t or ""))
    return d[2:] if len(d) == 11 and d.startswith("51") else d


def render_pedidos_web():
    if not _tablas_listas():
        st.info("Todavía no se ha ejecutado la revisión de pedidos web (recurrente/pedidos_web.py).")
        return

    st.caption("Cada 5 minutos se revisan los pedidos nuevos de kmlentes.pe: se registran como venta, se descuenta el stock, "
               "se le escribe al cliente y te llega un aviso por WhatsApp.")

    with engine.connect() as c:
        df = pd.read_sql(text("""
            SELECT id_pedido AS "Pedido", fecha_pedido AS "Fecha", nombre AS "Cliente", telefono AS "Celular",
                   telefono2 AS "Otro celular", envio AS "Envío", pago AS "Pago", total AS "Total",
                   confirmacion, id_venta AS "Venta", alertas AS "Alertas", respuesta AS "Respuesta"
            FROM pedidos_web ORDER BY id_pedido DESC LIMIT 100"""), c)

    if df.empty:
        st.info("Aún no llegan pedidos de la web desde que se activó el registro automático.")
    else:
        df["Estado"] = df["confirmacion"].map(lambda e: ESTADOS.get(e, e))
        st.dataframe(df.drop(columns=["confirmacion"]), use_container_width=True, hide_index=True)

        st.markdown("#### Actualizar un pedido")
        opciones = {f"#{r.Pedido} · {r.Cliente} · {r.Estado}": int(r.Pedido) for r in df.itertuples()}
        sel = st.selectbox("Pedido:", list(opciones.keys()))
        idp = opciones[sel]
        c1, c2, c3 = st.columns(3)
        if c1.button("✅ Confirmado", use_container_width=True):
            _cambiar_estado(idp, "confirmado")
        if c2.button("📦 Entregado", use_container_width=True):
            _cambiar_estado(idp, "entregado")
        if c3.button("🚫 No recibió → lista de adelanto", use_container_width=True,
                     help="Agrega sus celulares y DNI a la lista: la próxima vez la web le pedirá S/ 10 de adelanto. "
                          "Recuerda anular la venta en «Historial y Anulaciones» para devolver el stock."):
            _no_recibio(idp)

    st.divider()
    st.markdown("#### 🚫 Lista de clientes que deben adelantar")
    st.caption("Uso interno: no se comparte con nadie. A estos clientes la web les pide S/ 10 de adelanto en el delivery de Lima.")
    with engine.connect() as c:
        lista = pd.read_sql(text("""SELECT id, telefono AS "Celular", dni AS "DNI", nombre AS "Nombre", motivo AS "Motivo",
                                           id_pedido AS "Pedido", fecha AS "Fecha"
                                    FROM lista_adelanto WHERE activo ORDER BY fecha DESC"""), c)
    if lista.empty:
        st.info("La lista está vacía.")
    else:
        st.dataframe(lista.drop(columns=["id"]), use_container_width=True, hide_index=True)
        quitar = st.selectbox("Quitar de la lista:", [""] + [f"{r.id} · {r.Celular or ''} {r.DNI or ''} · {r.Nombre or ''}" for r in lista.itertuples()])
        if quitar and st.button("Quitar"):
            with engine.begin() as c:
                c.execute(text("UPDATE lista_adelanto SET activo = FALSE WHERE id = :i"), {"i": int(quitar.split(" · ")[0])})
            st.success("Quitado. La web se actualiza en unos minutos.")
            st.rerun()

    with st.form("agregar_lista", clear_on_submit=True):
        a1, a2, a3 = st.columns(3)
        tel = a1.text_input("Celular")
        dni = a2.text_input("DNI")
        nom = a3.text_input("Nombre")
        motivo = st.text_input("Motivo", placeholder="Ej: confirmó y no recibió al motorizado")
        if st.form_submit_button("Agregar a la lista"):
            tel_c, dni_c = _celular(tel), re.sub(r"[^0-9A-Za-z]", "", dni or "")
            if not tel_c and not dni_c:
                st.error("Pon al menos un celular o un DNI.")
            else:
                with engine.begin() as c:
                    c.execute(text("INSERT INTO lista_adelanto (telefono, dni, nombre, motivo) VALUES (:t, :d, :n, :m)"),
                              {"t": tel_c or None, "d": dni_c or None, "n": nom or None, "m": motivo or None})
                st.success("Agregado. La web se actualiza en unos minutos.")

    with st.expander("⚙️ ¿A qué número te llegan los avisos?"):
        actual = _ajuste("pedidos_web_alerta_tel", "51941380271")
        nuevo = st.text_input("Número para los avisos (con 51)", value=actual)
        st.caption("Los avisos y los mensajes a los clientes salen desde el WhatsApp de KM Lentes (936 041 531).")
        if st.button("Guardar número") and _celular(nuevo):
            with engine.begin() as c:
                c.execute(text("""INSERT INTO ajustes (clave, valor) VALUES ('pedidos_web_alerta_tel', :v)
                                  ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor"""), {"v": "51" + _celular(nuevo)})
            st.success("Guardado.")


def _cambiar_estado(idp, estado):
    with engine.begin() as c:
        c.execute(text("UPDATE pedidos_web SET confirmacion = :e WHERE id_pedido = :p"), {"e": estado, "p": idp})
    st.success("Actualizado.")
    st.rerun()


def _no_recibio(idp):
    with engine.begin() as c:
        p = c.execute(text("SELECT telefono, telefono2, dni, nombre FROM pedidos_web WHERE id_pedido = :p"), {"p": idp}).fetchone()
        for tel in {_celular(p.telefono), _celular(p.telefono2)} - {""}:
            c.execute(text("""INSERT INTO lista_adelanto (telefono, dni, nombre, motivo, id_pedido)
                              VALUES (:t, :d, :n, 'No recibió el pedido web', :p)"""),
                      {"t": tel, "d": p.dni, "n": p.nombre, "p": idp})
        c.execute(text("UPDATE pedidos_web SET confirmacion = 'no_recibio' WHERE id_pedido = :p"), {"p": idp})
    st.success("Agregado a la lista de adelanto. Anula la venta en «Historial y Anulaciones» para devolver el stock.")
    st.rerun()
