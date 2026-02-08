import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
from database import engine 

# --- GESTIÓN DE IMPORTACIONES ROBUSTA ---
try:
    from utils import (
        enviar_mensaje_media, 
        enviar_mensaje_whatsapp, 
        normalizar_telefono_maestro, 
        sincronizar_historial,
        marcar_chat_como_leido_waha as marcar_leido_waha 
    )
except ImportError:
    def enviar_mensaje_media(*args): return False, "Error import"
    def enviar_mensaje_whatsapp(*args): return False, "Error import"
    def normalizar_telefono_maestro(*args): return None
    def sincronizar_historial(*args): return "Error import"
    def marcar_leido_waha(*args): pass

def get_table_name(conn):
    try:
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        return "\"Clientes\""

def render_chat():
    # Título principal (ocupa todo el ancho)
    st.title("💬 Chat Center")

    # Inicializar estado
    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    telefono_actual = st.session_state['chat_actual_telefono']

    # --- LAYOUT DE DOS COLUMNAS ---
    # col_lista (izquierda): 35% del ancho
    # col_chat (derecha): 65% del ancho
    col_lista, col_chat = st.columns([35, 65])

    # ==========================================
    # COLUMNA IZQUIERDA: LISTA DE CHATS
    # ==========================================
    with col_lista:
        st.subheader("Bandeja")
        
        # --- Botones de Acción (Sync y Fix) ---
        c1, c2 = st.columns(2)
        if c1.button("🔄 Sync", use_container_width=True, help="Descargar mensajes"):
            with st.spinner("Sincronizando..."):
                try:
                    res = sincronizar_historial()
                    st.toast(res, icon="✅")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

        if c2.button("🛠️ Fix", use_container_width=True, help="Reparar lista"):
            try:
                with engine.connect() as conn:
                    tabla = get_table_name(conn)
                    sql_fix = f"""
                        INSERT INTO {tabla} (telefono, nombre_corto, estado, activo, fecha_registro)
                        SELECT DISTINCT m.telefono, 'Recuperado', 'Sin empezar', TRUE, NOW()
                        FROM mensajes m
                        WHERE m.telefono NOT IN (SELECT c.telefono FROM {tabla} c)
                    """
                    res = conn.execute(text(sql_fix))
                    conn.commit()
                    st.success(f"Recuperados: {res.rowcount}")
                    time.sleep(1)
                    st.rerun()
            except Exception as e:
                st.error(f"Error Fix: {e}")

        st.divider()

        # --- Buscador y Lista ---
        try:
            with engine.connect() as conn:
                tabla = get_table_name(conn)
                
                # Input de búsqueda
                busqueda = st.text_input("🔍 Buscar:", placeholder="Nombre o teléfono...")
                
                # Consulta base
                query = f"""
                    SELECT 
                        c.telefono, 
                        COALESCE(c.nombre_corto, c.telefono) as nombre,
                        c.estado,
                        (SELECT COUNT(*) FROM mensajes m 
                         WHERE m.telefono = c.telefono AND m.leido = FALSE AND m.tipo = 'ENTRANTE') as no_leidos
                    FROM {tabla} c
                    WHERE c.activo = TRUE
                """
                
            # --- LÓGICA DE BÚSQUEDA MEJORADA ---
                if busqueda:
                    # 1. Limpiamos el input para dejar solo números (para buscar por teléfono)
                    # Ejemplo: "+51 981 101" -> "51981101"
                    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
                    
                    filtro = " AND ("
                    # Siempre buscamos por nombre con el texto original
                    filtro += f"COALESCE(c.nombre_corto,'') ILIKE '%{busqueda}%'"
                    
                    # Si logramos extraer números, buscamos también en el teléfono
                    if busqueda_limpia:
                        filtro += f" OR c.telefono ILIKE '%{busqueda_limpia}%'"
                    # Si no hay números (solo texto), buscamos en teléfono tal cual por si acaso
                    else:
                        filtro += f" OR c.telefono ILIKE '%{busqueda}%'"
                    
                    filtro += ")"
                    query += filtro
                
                query += " ORDER BY no_leidos DESC, c.id_cliente DESC LIMIT 50"
                df_clientes = pd.read_sql(text(query), conn)
                
# Contenedor con scroll para la lista de chats (altura fija)
            with st.container(height=600):
                if df_clientes.empty:
                    st.info("No hay chats.")
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
                        
                        if st.button(label, key=f"c_{t_row}", use_container_width=True, type=tipo):
                            st.session_state['chat_actual_telefono'] = t_row
                            st.rerun()

        except Exception as e:
            st.error("Error lista")
            st.code(e)

    # ==========================================
    # COLUMNA DERECHA: CONVERSACIÓN
    # ==========================================
    with col_chat:
        if not telefono_actual:
            st.info("👈 Selecciona un chat de la izquierda.")
            st.image("https://cdn-icons-png.flaticon.com/512/1041/1041916.png", width=150) # Icono placeholder
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
                    
                    # Mensajes
                    msgs = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha ASC"), conn, params={"t": telefono_actual})

                # Cabecera Chat
                st.subheader(f"👤 {nombre}")
                st.caption(f"📱 {telefono_actual}")
                
                # --- AREA DE MENSAJES ---
                with st.container(height=550):
                    if msgs.empty:
                        st.caption("Inicio de la conversación.")
                    
                    # CSS Burbujas
                    st.markdown("""
                    <style>
                        .msg-row { display: flex; margin-bottom: 5px; }
                        .msg-mio { justify-content: flex-end; }
                        .msg-otro { justify-content: flex-start; }
                        .bubble { padding: 10px 14px; border-radius: 15px; font-size: 15px; max-width: 80%; }
                        .b-mio { background-color: #dcf8c6; color: black; border-top-right-radius: 0; }
                        .b-otro { background-color: #f2f2f2; color: black; border-top-left-radius: 0; border: 1px solid #ddd; }
                        .meta { font-size: 10px; color: #666; text-align: right; margin-top: 3px; }
                    </style>
                    """, unsafe_allow_html=True)

                    for _, m in msgs.iterrows():
                        es_mio = (m['tipo'] == 'SALIENTE')
                        clase_row = "msg-mio" if es_mio else "msg-otro"
                        clase_bub = "b-mio" if es_mio else "b-otro"
                        hora = m['fecha'].strftime("%H:%M") if m['fecha'] else ""
                        
                        st.markdown(f"""
                        <div class='msg-row {clase_row}'>
                            <div class='bubble {clase_bub}'>
                                {m['contenido']}
                                <div class='meta'>{hora}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                # --- INPUT ---
                # Usamos un formulario para que se envíe con Enter
                with st.form("chat_form", clear_on_submit=True):
                    c_in, c_btn = st.columns([85, 15])
                    txt = c_in.text_input("Mensaje", label_visibility="collapsed", placeholder="Escribe un mensaje...")
                    btn = c_btn.form_submit_button("➤")
                    
                    if btn and txt:
                        ok, res = enviar_mensaje_whatsapp(telefono_actual, txt)
                        if ok:
                            with engine.connect() as conn:
                                conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido) VALUES (:t, 'SALIENTE', :c, NOW(), TRUE)"), {"t": telefono_actual, "c": txt})
                                conn.commit()
                            st.rerun()
                        else:
                            st.error(f"Error: {res}")

            except Exception as e:
                st.error("Error cargando chat")
                st.caption(str(e))