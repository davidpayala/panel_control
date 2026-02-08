import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
import traceback # Para ver el detalle exacto del error
from database import engine 

# --- IMPORTACIÓN SEGURA DE UTILS ---
try:
    from utils import (
        enviar_mensaje_media, 
        enviar_mensaje_whatsapp, 
        normalizar_telefono_maestro, 
        sincronizar_historial,
        marcar_chat_como_leido_waha as marcar_leido_waha
    )
except ImportError as e:
    st.error(f"⚠️ Error importando utils: {e}")
    # Dummies para evitar crash
    def enviar_mensaje_whatsapp(*args): return False, "Error import"
    def marcar_leido_waha(*args): pass
    def sincronizar_historial(*args): return "Error import"

def get_table_name(conn):
    """Detecta nombre de tabla (clientes vs Clientes)"""
    try:
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        return "\"Clientes\""

def render_chat():
    st.title("💬 Chat Center")

    # --- 1. PANEL DE CONTROL EN PANTALLA PRINCIPAL (PARA ASEGURAR VISIBILIDAD) ---
    # Lo ponemos aquí arriba para que siempre puedas acceder a él, falle o no la sidebar.
    with st.expander("🛠️ Herramientas de Recuperación (Usar si la lista falla)", expanded=False):
        col_fix, col_sync = st.columns(2)
        
        # BOTÓN FIX (Recuperar chats perdidos)
        if col_fix.button("🚑 REPARAR CHATS (FIX)", use_container_width=True):
            try:
                with st.spinner("Reparando base de datos..."):
                    with engine.connect() as conn:
                        tabla = get_table_name(conn)
                        # Insertar clientes que existen en mensajes pero no en la tabla clientes
                        sql_fix = f"""
                            INSERT INTO {tabla} (telefono, nombre_corto, estado, activo, fecha_registro)
                            SELECT DISTINCT m.telefono, 'Recuperado', 'Sin empezar', TRUE, NOW()
                            FROM mensajes m
                            WHERE m.telefono NOT IN (SELECT c.telefono FROM {tabla} c)
                        """
                        res = conn.execute(text(sql_fix))
                        conn.commit()
                        if res.rowcount > 0:
                            st.success(f"✅ ¡Recuperados {res.rowcount} chats! Recarga la página.")
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.info("La base de datos parece correcta (0 recuperados).")
            except Exception as e:
                st.error(f"Error al reparar: {e}")
                st.code(traceback.format_exc())

        # BOTÓN SYNC
        if col_sync.button("🔄 SINCRONIZAR WAHA", use_container_width=True):
            with st.spinner("Sincronizando..."):
                res = sincronizar_historial()
                st.info(res)

    # --- 2. INTENTO DE CARGAR LA BARRA LATERAL ---
    telefono_actual = st.session_state.get('chat_actual_telefono', None)
    
    try:
        # Usamos st.sidebar explícitamente sin 'with' para probar otra forma si el bloque falla
        st.sidebar.title("📂 Chats")
        
        # Cargar datos
        with engine.connect() as conn:
            tabla = get_table_name(conn)
            
            # Buscador en sidebar
            busqueda = st.sidebar.text_input("🔍 Buscar:", key="search_chat")
            
            # Query segura
            query = f"""
                SELECT 
                    c.telefono, 
                    COALESCE(c.nombre_corto, c.telefono) as nombre,
                    (SELECT COUNT(*) FROM mensajes m 
                     WHERE m.telefono = c.telefono AND m.leido = FALSE AND m.tipo = 'ENTRANTE') as unread
                FROM {tabla} c
                WHERE c.activo = TRUE
            """
            
            if busqueda:
                query += f" AND (c.telefono ILIKE '%{busqueda}%' OR c.nombre_corto ILIKE '%{busqueda}%')"
            
            query += " ORDER BY unread DESC, c.id_cliente DESC LIMIT 50"
            
            df = pd.read_sql(text(query), conn)

        # Renderizar botones en sidebar
        if df.empty:
            st.sidebar.warning("No hay chats.")
        else:
            st.sidebar.caption(f"Total: {len(df)}")
            for _, row in df.iterrows():
                t = row['telefono']
                n = row['nombre']
                u = row['unread']
                
                label = f"{'🔴' if u > 0 else '👤'} {n}"
                if u > 0: label += f" ({u})"
                
                # Botón de selección
                b_type = "primary" if telefono_actual == t else "secondary"
                if st.sidebar.button(label, key=f"chat_{t}", use_container_width=True, type=b_type):
                    st.session_state['chat_actual_telefono'] = t
                    st.rerun()

    except Exception as e:
        # Si falla la sidebar, mostramos el error EN GRANDE en la pantalla principal
        st.error("❌ ERROR CRÍTICO CARGANDO LA LISTA LATERAL")
        st.error(str(e))
        st.code(traceback.format_exc())

    # --- 3. ÁREA DE CHAT (Solo si hay seleccionado) ---
    if not telefono_actual:
        st.info("👈 Selecciona un chat (o usa el botón 'REPARAR' arriba si no ves la lista).")
        return

    try:
        # Cargar conversación
        with engine.connect() as conn:
            # Marcar leído
            conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
            conn.commit()
            
            # Leer mensajes
            msgs = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha ASC"), conn, params={"t": telefono_actual})
        
        st.markdown(f"### 👤 Chat: {telefono_actual}")
        
        # Render simple de mensajes
        with st.container(height=500):
            for _, m in msgs.iterrows():
                tipo = m['tipo']
                align = "text-align: right;" if tipo == 'SALIENTE' else "text-align: left;"
                color = "#dcf8c6" if tipo == 'SALIENTE' else "#ffffff"
                st.markdown(
                    f"<div style='{align} margin: 5px;'><span style='background:{color}; padding:8px; border-radius:10px; display:inline-block; color:black;'>{m['contenido']}</span></div>", 
                    unsafe_allow_html=True
                )
        
        # Input enviar
        with st.form("send_msg", clear_on_submit=True):
            col_in, col_btn = st.columns([0.85, 0.15])
            txt = col_in.text_input("Mensaje", label_visibility="collapsed")
            if col_btn.form_submit_button("➤"):
                if txt:
                    ok, r = enviar_mensaje_whatsapp(telefono_actual, txt)
                    if ok:
                        with engine.connect() as conn:
                            conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido) VALUES (:t, 'SALIENTE', :c, NOW(), TRUE)"), {"t": telefono_actual, "c": txt})
                            conn.commit()
                        st.rerun()
                    else:
                        st.error(f"Error envío: {r}")

    except Exception as e:
        st.error(f"Error en chat: {e}")