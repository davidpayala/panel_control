import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
import requests
import os
from datetime import datetime, timedelta
from database import engine 

# --- CONFIGURACIÓN ---
WAHA_URL = os.getenv("WAHA_URL")
WAHA_KEY = os.getenv("WAHA_KEY")
SESIONES = ["default", "principal"] # Tus sesiones activas

# --- GESTIÓN DE IMPORTACIONES ROBUSTA ---
try:
    from utils import (
        enviar_mensaje_media, 
        enviar_mensaje_whatsapp, 
        marcar_chat_como_leido_waha as marcar_leido_waha 
    )
except ImportError:
    def enviar_mensaje_media(*args): return False, "Error import"
    def enviar_mensaje_whatsapp(*args): return False, "Error import"
    def marcar_leido_waha(*args): pass

def get_table_name(conn):
    try:
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        return "\"Clientes\""

# ==============================================================================
# 🧠 LÓGICA DE SINCRONIZACIÓN TOTAL (MIGRACIÓN ID)
# ==============================================================================
def ejecutar_sync_masiva():
    log_msgs = []
    total_chats = 0
    total_msgs = 0
    
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY

    with st.status("🔄 Ejecutando Migración y Sincronización...", expanded=True) as status:
        
        for session in SESIONES:
            st.write(f"📡 Conectando con sesión: **{session}**...")
            
            try:
                # 1. Obtener todos los chats de la sesión
                url_chats = f"{WAHA_URL}/api/{session}/chats?limit=1000" # Traemos todos los posibles
                r = requests.get(url_chats, headers=headers, timeout=15)
                
                if r.status_code != 200:
                    st.error(f"Error conectando a {session}: {r.status_code}")
                    continue
                
                chats_data = r.json()
                st.write(f"📂 Procesando {len(chats_data)} chats en {session}...")

                with engine.connect() as conn:
                    for chat in chats_data:
                        # --- A) REGISTRO DE CONTACTOS (ID CENTRIC) ---
                        waha_id = chat.get('id') # Este es el ID Único (ej: 5199...@s.whatsapp.net)
                        name = chat.get('name') or chat.get('pushName') or "Sin Nombre"
                        
                        # Limpieza de teléfono
                        telefono_limpio = "".join(filter(str.isdigit, waha_id.split('@')[0]))
                        
                        if 'status' in waha_id: continue # Ignorar estados

                        # Upsert inteligente: Busca por ID Interno, si no, busca por teléfono
                        # Si encuentra, actualiza el ID Interno. Si no, crea.
                        
                        # 1. Intentar actualizar por ID
                        res = conn.execute(text("""
                            UPDATE Clientes 
                            SET whatsapp_internal_id = :wid, activo = TRUE 
                            WHERE whatsapp_internal_id = :wid
                        """), {"wid": waha_id})
                        
                        if res.rowcount == 0:
                            # 2. Si no existe por ID, buscar por teléfono (Migración Legacy)
                            res = conn.execute(text("""
                                UPDATE Clientes 
                                SET whatsapp_internal_id = :wid, activo = TRUE
                                WHERE telefono = :tel AND whatsapp_internal_id IS NULL
                            """), {"wid": waha_id, "tel": telefono_limpio})
                            
                            if res.rowcount == 0:
                                # 3. Si no existe, crear nuevo
                                es_grupo = '@g.us' in waha_id
                                nombre_final = f"Grupo {telefono_limpio[-4:]}" if es_grupo else name
                                
                                conn.execute(text("""
                                    INSERT INTO Clientes (telefono, whatsapp_internal_id, nombre_corto, estado, activo, fecha_registro)
                                    VALUES (:t, :wid, :n, 'Sin empezar', TRUE, NOW())
                                    ON CONFLICT (telefono) DO UPDATE SET whatsapp_internal_id = :wid
                                """), {"t": telefono_limpio, "wid": waha_id, "n": nombre_final})
                        
                        total_chats += 1

                        # --- B) RECUPERACIÓN DE MENSAJES ---
                        # Descargamos los últimos 50 mensajes de cada chat para llenar huecos
                        try:
                            url_msgs = f"{WAHA_URL}/api/{session}/chats/{waha_id}/messages?limit=50"
                            r_m = requests.get(url_msgs, headers=headers, timeout=5)
                            if r_m.status_code == 200:
                                msgs_data = r_m.json()
                                
                                for m in msgs_data:
                                    msg_id = m.get('id')
                                    from_me = m.get('fromMe', False)
                                    body = m.get('body', '')
                                    has_media = m.get('hasMedia', False)
                                    timestamp = m.get('timestamp')
                                    
                                    # Verificar si ya existe
                                    existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id = :wid"), {"wid": msg_id}).scalar()
                                    
                                    if not existe:
                                        # Calcular fecha (Timestamp Unix -> Datetime - 5h)
                                        try:
                                            dt_object = datetime.fromtimestamp(timestamp)
                                            fecha_peru = dt_object - timedelta(hours=5)
                                        except:
                                            fecha_peru = datetime.now() - timedelta(hours=5)

                                        tipo = 'SALIENTE' if from_me else 'ENTRANTE'
                                        contenido = body
                                        if has_media and not body: contenido = "📷 Archivo (Recuperado)"
                                        
                                        conn.execute(text("""
                                            INSERT INTO mensajes (
                                                telefono, tipo, contenido, fecha, leido, whatsapp_id, estado_waha
                                            ) VALUES (
                                                :tel, :tipo, :cont, :fecha, :leido, :wid, 'recuperado'
                                            )
                                        """), {
                                            "tel": telefono_limpio,
                                            "tipo": tipo,
                                            "cont": contenido,
                                            "fecha": fecha_peru,
                                            "leido": True, # Si es recuperado, lo marcamos como leído para no spamear
                                            "wid": msg_id
                                        })
                                        total_msgs += 1
                                        
                                conn.commit()
                        except Exception as e:
                            print(f"Error msg fetch: {e}")

            except Exception as e:
                st.error(f"Error crítico en sesión {session}: {e}")
        
        status.update(label="✅ ¡Sincronización Completa!", state="complete", expanded=False)
    
    return f"Procesados: {total_chats} chats y recuperados {total_msgs} mensajes."

# ==============================================================================
# RENDERIZADO DE LA VISTA
# ==============================================================================
def render_chat():
    # Título principal
    st.title("💬 Chat Center")

    # Inicializar estado
    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    telefono_actual = st.session_state['chat_actual_telefono']

    # --- LAYOUT DE DOS COLUMNAS ---
    col_lista, col_chat = st.columns([35, 65])

    # ==========================================
    # COLUMNA IZQUIERDA: LISTA DE CHATS
    # ==========================================
    with col_lista:
        st.subheader("Bandeja")
        
        # --- Botón de Acción Único (SYNC TOTAL) ---
        if st.button("🔄 Sync Total (Migración)", use_container_width=True, type="primary", help="Registra IDs y recupera historial faltante"):
            resultado = ejecutar_sync_masiva()
            st.success(resultado)
            time.sleep(2)
            st.rerun()

        st.divider()

        # --- Buscador y Lista ---
        try:
            with engine.connect() as conn:
                tabla = get_table_name(conn)
                
                # Input de búsqueda
                busqueda = st.text_input("🔍 Buscar:", placeholder="Nombre o teléfono...")
                
                # Consulta base (Optimizada)
                query = f"""
                    SELECT 
                        c.telefono, 
                        COALESCE(c.nombre_corto, c.telefono) as nombre,
                        c.whatsapp_internal_id,
                        (SELECT COUNT(*) FROM mensajes m 
                         WHERE m.telefono = c.telefono AND m.leido = FALSE AND m.tipo = 'ENTRANTE') as no_leidos
                    FROM {tabla} c
                    WHERE c.activo = TRUE
                """
                
                # --- LÓGICA DE BÚSQUEDA ---
                if busqueda:
                    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
                    filtro = " AND ("
                    filtro += f"COALESCE(c.nombre_corto,'') ILIKE '%{busqueda}%'"
                    if busqueda_limpia:
                        filtro += f" OR c.telefono ILIKE '%{busqueda_limpia}%'"
                    else:
                        filtro += f" OR c.telefono ILIKE '%{busqueda}%'"
                    filtro += ")"
                    query += filtro
                
                query += " ORDER BY no_leidos DESC, c.fecha_registro DESC LIMIT 50"
                df_clientes = pd.read_sql(text(query), conn)

            # Contenedor con scroll para la lista
            with st.container(height=600):
                if df_clientes.empty:
                    st.info("No se encontraron chats.")
                else:
                    for _, row in df_clientes.iterrows():
                        t_row = row['telefono']
                        n_row = row['nombre']
                        c_leidos = row['no_leidos']
                        
                        icono = "🔴" if c_leidos > 0 else "👤"
                        label = f"{icono} {n_row}"
                        if c_leidos > 0: label += f" ({c_leidos})"
                        
                        # Estilo del botón
                        tipo = "primary" if telefono_actual == t_row else "secondary"
                        
                        # Usamos index puro para evitar reinicios
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
            st.markdown(
                """
                <div style="text-align: center; margin-top: 50px; opacity: 0.5;">
                    <img src="https://cdn-icons-png.flaticon.com/512/1041/1041916.png" width="150">
                    <p>Selecciona un chat para comenzar</p>
                </div>
                """, 
                unsafe_allow_html=True
            )
        else:
            try:
                # Marcar leído Waha (silencioso)
                try: marcar_leido_waha(f"{telefono_actual}@c.us")
                except: pass

                with engine.connect() as conn:
                    tabla = get_table_name(conn)
                    # Marcar leído local
                    conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
                    conn.commit()
                    
                    # Info cliente
                    info = conn.execute(text(f"SELECT * FROM {tabla} WHERE telefono=:t"), {"t": telefono_actual}).fetchone()
                    nombre = info.nombre_corto if info and info.nombre_corto else telefono_actual
                    wsp_id = info.whatsapp_internal_id if hasattr(info, 'whatsapp_internal_id') else "Desconocido"
                    
                    # Mensajes
                    msgs = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha ASC"), conn, params={"t": telefono_actual})

                # Cabecera Chat
                st.subheader(f"👤 {nombre}")
                st.caption(f"📱 {telefono_actual} | 🆔 {wsp_id}")
                
                # --- AREA DE MENSAJES ---
                with st.container(height=550):
                    if msgs.empty:
                        st.caption("Inicio de la conversación.")
                    
                    st.markdown("""
                    <style>
                        .msg-row { display: flex; margin-bottom: 5px; }
                        .msg-mio { justify-content: flex-end; }
                        .msg-otro { justify-content: flex-start; }
                        .bubble { padding: 10px 14px; border-radius: 15px; font-size: 15px; max-width: 80%; }
                        .b-mio { background-color: #dcf8c6; color: black; border-top-right-radius: 0; }
                        .b-otro { background-color: #f2f2f2; color: black; border-top-left-radius: 0; border: 1px solid #ddd; }
                        .meta { font-size: 10px; color: #666; text-align: right; margin-top: 3px; }
                        .check-read { color: #34B7F1; font-weight: bold; }
                        .check-sent { color: #999; }
                    </style>
                    """, unsafe_allow_html=True)

                    for _, m in msgs.iterrows():
                        es_mio = (m['tipo'] == 'SALIENTE')
                        clase_row = "msg-mio" if es_mio else "msg-otro"
                        clase_bub = "b-mio" if es_mio else "b-otro"
                        hora = m['fecha'].strftime("%H:%M") if m['fecha'] else ""
                        
                        icono_estado = ""
                        if es_mio:
                            estado = m.get('estado_waha', 'pendiente')
                            if estado == 'leido': icono_estado = "<span class='check-read'>✓✓</span>"
                            elif estado == 'recibido': icono_estado = "<span class='check-sent'>✓✓</span>"
                            elif estado == 'enviado': icono_estado = "<span class='check-sent'>✓</span>"
                            else: icono_estado = "🕒"

                        st.markdown(f"""
                        <div class='msg-row {clase_row}'>
                            <div class='bubble {clase_bub}'>
                                {m['contenido']}
                                <div class='meta'>{hora} {icono_estado}</div>
                            </div>
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
                            with engine.connect() as conn:
                                # Corrección horaria (-5 horas)
                                conn.execute(text("""
                                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, estado_waha) 
                                    VALUES (:t, 'SALIENTE', :c, (NOW() - INTERVAL '5 hours'), TRUE, 'enviado')
                                """), {"t": telefono_actual, "c": txt})
                                conn.commit()
                            st.rerun()
                        else:
                            st.error(f"Error: {res}")

            except Exception as e:
                st.error("Error cargando chat")
                st.caption(str(e))