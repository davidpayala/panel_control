import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
import os
import threading
from datetime import datetime, timedelta, date
from database import engine 

# --- CONFIGURACIÓN ---
WAHA_URL = os.getenv("WAHA_URL")
WAHA_KEY = os.getenv("WAHA_KEY")

# --- GESTIÓN DE IMPORTACIONES ROBUSTA ---
try:
    from utils import (
        enviar_mensaje_whatsapp, 
        marcar_chat_como_leido_waha as marcar_leido_waha 
    )
except ImportError:
    def enviar_mensaje_whatsapp(*args): return False, "Error import"
    def marcar_leido_waha(*args): pass

def get_table_name(conn):
    try:
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        return "\"Clientes\""

# ==============================================================================
# RENDERIZADO DE LA VISTA
# ==============================================================================
def render_chat():
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    telefono_actual = st.session_state['chat_actual_telefono']
    col_lista, col_chat = st.columns([35, 65])

    # ==========================================
    # COLUMNA IZQUIERDA: LISTA DE CHATS
    # ==========================================
    with col_lista:
        st.subheader("Bandeja")
        st.divider()

        try:
            with engine.connect() as conn:
                tabla = get_table_name(conn)
                busqueda = st.text_input("🔍 Buscar:", placeholder="Nombre o teléfono...")
                
                query = f"""
                    SELECT c.telefono, COALESCE(c.nombre_corto, c.telefono) as nombre, c.whatsapp_internal_id,
                           (SELECT COUNT(*) FROM mensajes m WHERE m.telefono = c.telefono AND m.leido = FALSE AND m.tipo = 'ENTRANTE') as no_leidos,
                           (SELECT MAX(fecha) FROM mensajes m WHERE m.telefono = c.telefono) as ultima_interaccion
                    FROM {tabla} c
                    WHERE c.activo = TRUE
                """
                
                if busqueda:
                    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
                    filtro = f" AND (COALESCE(c.nombre_corto,'') ILIKE '%{busqueda}%'"
                    if busqueda_limpia: filtro += f" OR c.telefono ILIKE '%{busqueda_limpia}%')"
                    else: filtro += f" OR c.telefono ILIKE '%{busqueda}%')"
                    query += filtro
                
                query += " ORDER BY no_leidos DESC, ultima_interaccion DESC NULLS LAST, c.fecha_registro DESC LIMIT 50"
                df_clientes = pd.read_sql(text(query), conn)

            with st.container(height=600):
                if df_clientes.empty:
                    st.info("No se encontraron chats.")
                else:
                    for _, row in df_clientes.iterrows():
                        t_row = row['telefono']
                        c_leidos = row['no_leidos']
                        icono = "🔴" if c_leidos > 0 else "👤"
                        label = f"{icono} {row['nombre']}" + (f" ({c_leidos})" if c_leidos > 0 else "")
                        tipo = "primary" if telefono_actual == t_row else "secondary"
                        
                        if st.button(label, key=f"c_{t_row}", use_container_width=True, type=tipo):
                            st.session_state['chat_actual_telefono'] = t_row
                            st.rerun()
        except Exception as e:
            st.error("Error cargando lista")
            st.code(e)

    # ==========================================
    # COLUMNA DERECHA: CONVERSACIÓN
    # ==========================================
    with col_chat:
        if not telefono_actual:
            st.info("👈 Selecciona un chat de la izquierda.")
        else:
            try:
                # 🚀 SOLUCIÓN LENTITUD: Hilo en segundo plano (Dispara y olvida)
                # Esto evita que Streamlit se congele esperando a WAHA
                try: 
                    threading.Thread(target=marcar_leido_waha, args=(f"{telefono_actual}@c.us",)).start()
                except: pass

                with engine.connect() as conn:
                    tabla = get_table_name(conn)
                    conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
                    conn.commit()
                    info = conn.execute(text(f"SELECT * FROM {tabla} WHERE telefono=:t"), {"t": telefono_actual}).fetchone()
                    nombre = info.nombre_corto if info and info.nombre_corto else telefono_actual
                    wsp_id = info.whatsapp_internal_id if hasattr(info, 'whatsapp_internal_id') else "Desconocido"
                    msgs = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha ASC"), conn, params={"t": telefono_actual})

                st.subheader(f"👤 {nombre}")
                st.caption(f"📱 {telefono_actual} | 🆔 {wsp_id}")
                
                # 🚀 SOLUCIÓN AUTO-SCROLL Y RENDIMIENTO VISUAL
                if msgs.empty: 
                    st.caption("Inicio de la conversación.")
                else:
                    html_blocks = []
                    ultima_fecha = None
                    hoy = datetime.now().date()
                    ayer = hoy - timedelta(days=1)

                    # 1. Construimos los mensajes cronológicamente
                    for _, m in msgs.iterrows():
                        try: fecha_msg = m['fecha'].date() if pd.notna(m['fecha']) else None
                        except: fecha_msg = None

                        if fecha_msg and fecha_msg != ultima_fecha:
                            if fecha_msg == hoy: texto_fecha = "Hoy"
                            elif fecha_msg == ayer: texto_fecha = "Ayer"
                            else: texto_fecha = fecha_msg.strftime("%d/%m/%Y")
                            
                            html_blocks.append(f"<div class='date-separator'><span>{texto_fecha}</span></div>")
                            ultima_fecha = fecha_msg

                        es_mio = (m['tipo'] == 'SALIENTE')
                        clase_row = "msg-mio" if es_mio else "msg-otro"
                        clase_bub = "b-mio" if es_mio else "b-otro"
                        hora = m['fecha'].strftime("%H:%M") if pd.notna(m['fecha']) else ""
                        
                        icono_estado = ""
                        if es_mio:
                            estado = m.get('estado_waha', 'pendiente')
                            if estado == 'leido': icono_estado = "<span class='check-read'>✓✓</span>"
                            elif estado == 'recibido': icono_estado = "<span class='check-sent'>✓✓</span>"
                            elif estado == 'enviado': icono_estado = "<span class='check-sent'>✓</span>"
                            else: icono_estado = "🕒"

                        reply_html = ""
                        if pd.notna(m.get('reply_content')) and str(m['reply_content']).strip() != "":
                            reply_html = f"<div class='reply-box'>↪️ {str(m['reply_content'])}</div>"

                        html_blocks.append(f"""
                        <div class='msg-row {clase_row}'>
                            <div class='bubble {clase_bub}'>
                                {reply_html}
                                <div style='white-space: pre-wrap;'>{m['contenido']}</div>
                                <div class='meta'>{hora} {icono_estado}</div>
                            </div>
                        </div>
                        """)

                    # 2. Invertimos la lista para usar el truco de gravedad de CSS
                    html_blocks.reverse()

                    # 3. Renderizamos TODO de un solo golpe (Extremadamente rápido)
                    st.markdown(f"""
                    <style>
                        .chat-container {{
                            display: flex;
                            flex-direction: column-reverse; /* ¡Aquí ocurre la magia del Scroll! */
                            height: 550px;
                            overflow-y: auto;
                            padding: 10px;
                            border: 1px solid rgba(128, 128, 128, 0.2);
                            border-radius: 10px;
                            background-image: url('https://user-images.githubusercontent.com/15075759/28719144-86dc0f70-73b1-11e7-911d-60d70fcded21.png'); /* Fondo WhatsApp */
                            background-color: transparent;
                        }}
                        .msg-row {{ display: flex; margin-bottom: 5px; }}
                        .msg-mio {{ justify-content: flex-end; }}
                        .msg-otro {{ justify-content: flex-start; }}
                        .bubble {{ padding: 8px 12px; border-radius: 10px; font-size: 15px; max-width: 80%; display: flex; flex-direction: column; box-shadow: 0 1px 0.5px rgba(0,0,0,0.13); }}
                        .b-mio {{ background-color: #dcf8c6; color: black; border-top-right-radius: 0; }}
                        .b-otro {{ background-color: #ffffff; color: black; border-top-left-radius: 0; }}
                        .meta {{ font-size: 10px; color: #777; text-align: right; margin-top: 3px; display: inline-block; }}
                        .check-read {{ color: #34B7F1; font-weight: bold; font-size: 12px; }}
                        .check-sent {{ color: #999; font-size: 12px; }}
                        .reply-box {{ background-color: rgba(0, 0, 0, 0.05); border-left: 4px solid #34B7F1; padding: 6px 8px; border-radius: 4px; font-size: 13px; margin-bottom: 6px; color: #555; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; text-overflow: ellipsis; }}
                        .b-mio .reply-box {{ border-left-color: #075E54; background-color: rgba(0, 0, 0, 0.08); }}
                        .date-separator {{ display: flex; justify-content: center; margin: 15px 0; }}
                        .date-separator span {{ background-color: #e1f3fb; color: #555; padding: 4px 12px; border-radius: 10px; font-size: 12px; font-weight: bold; box-shadow: 0 1px 0.5px rgba(0,0,0,0.13); }}
                    </style>
                    <div class='chat-container'>
                        {''.join(html_blocks)}
                    </div>
                    """, unsafe_allow_html=True)

                # --- INPUT ---
                with st.form("chat_form", clear_on_submit=True):
                    c_in, c_btn = st.columns([85, 15])
                    txt = c_in.text_input("Mensaje", label_visibility="collapsed", placeholder="Escribe un mensaje...")
                    btn = c_btn.form_submit_button("➤")
                    
                    if btn and txt:
                        ok, res = enviar_mensaje_whatsapp(telefono_actual, txt)
                        if ok:
                            with st.spinner("Enviando..."):
                                time.sleep(1.5) 
                            st.rerun()
                        else:
                            st.error(f"Error: {res}")

            except Exception as e:
                st.error("Error cargando chat")
                st.caption(str(e))