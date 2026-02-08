import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
from database import engine 
# Intentamos importar con manejo de errores
try:
    from utils import (
        enviar_mensaje_media, enviar_mensaje_whatsapp, 
        normalizar_telefono_maestro, sincronizar_historial,
        marcar_leido_waha
    )
except ImportError:
    from utils import (
        enviar_mensaje_media, enviar_mensaje_whatsapp, 
        normalizar_telefono_maestro, sincronizar_historial
    )
    def marcar_leido_waha(t): pass

# --- FUNCIÓN PARA DETECTAR EL NOMBRE REAL DE LA TABLA ---
def get_table_name(conn):
    """Detecta si la tabla se llama 'clientes' o 'Clientes'"""
    try:
        # Probamos minúsculas
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        # Si falla, probamos con comillas (Mayúsculas)
        return "\"Clientes\""

def render_chat():
    st.title("💬 Chat Center")

    # --- 1. BARRA LATERAL (LISTA DE CHATS) ---
    with st.sidebar:
        st.header("Clientes")
        
        # Botones de acción
        col1, col2 = st.columns(2)
        if col1.button("🔄 Sync"):
            with st.spinner("Sincronizando..."):
                try:
                    res = sincronizar_historial()
                    st.toast(res)
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error Sync: {e}")
            
        if col2.button("🛠️ Fix"):
            # BOTÓN DE REPARACIÓN MANUAL
            try:
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
                    st.success(f"Reparado. Creados: {res.rowcount}")
                    time.sleep(1)
                    st.rerun()
            except Exception as e:
                st.error(f"Error Fix: {e}")

        st.markdown("---")
        
        # --- CARGA DE LISTA DE CHATS ---
        try:
            with engine.connect() as conn:
                tabla = get_table_name(conn)
                
                # Buscador
                busqueda = st.text_input("🔍 Buscar:", "")
                
                # Query Dinámica usando el nombre de tabla correcto
                query = f"""
                    SELECT 
                        c.telefono, 
                        c.nombre_corto, 
                        c.estado,
                        (SELECT COUNT(*) FROM mensajes m 
                         WHERE m.telefono = c.telefono AND m.leido = FALSE AND m.tipo = 'ENTRANTE') as no_leidos
                    FROM {tabla} c
                    WHERE c.activo = TRUE
                """
                
                if busqueda:
                    query += f" AND (c.nombre_corto ILIKE '%{busqueda}%' OR c.telefono ILIKE '%{busqueda}%')"
                
                query += " ORDER BY no_leidos DESC, c.fecha_registro DESC"
                
                df_clientes = pd.read_sql(text(query), conn)

                if df_clientes.empty:
                    st.info("📭 Lista vacía.")
                    st.caption("Si tienes mensajes, pulsa '🛠️ Fix' arriba.")
                
                # Renderizar cada chat en la lista
                for i, row in df_clientes.iterrows():
                    telefono = row['telefono']
                    nombre = row['nombre_corto'] or telefono
                    no_leidos = row['no_leidos']
                    estado = row['estado']
                    
                    # Diseño del botón
                    label = f"{nombre}"
                    if no_leidos > 0:
                        label = f"🔴 {nombre} ({no_leidos})"
                    
                    # Al hacer clic, guardamos en session_state y recargamos
                    if st.button(f"{label}\n📌 {estado}", key=f"chat_{telefono}", use_container_width=True):
                        st.session_state['chat_actual_telefono'] = telefono
                        st.rerun()
                    
        except Exception as e:
            st.error("Error cargando lista.")
            st.code(e)

    # --- 2. AREA DE CHAT PRINCIPAL ---
    if 'chat_actual_telefono' not in st.session_state:
        st.info("👈 Selecciona un chat del menú lateral para comenzar.")
        return

    telefono_actual = st.session_state['chat_actual_telefono']
    
    # Intentar marcar leido en WhatsApp (Blue Ticks)
    try: marcar_leido_waha(f"{telefono_actual}@c.us")
    except: pass

    # Obtener mensajes y actualizar leídos localmente
    with engine.connect() as conn:
        tabla = get_table_name(conn)
        
        # Marcar leídos en DB local
        conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
        conn.commit()
        
        # Info del cliente
        info_cliente = conn.execute(text(f"SELECT * FROM {tabla} WHERE telefono=:t"), {"t": telefono_actual}).fetchone()
        
        # Cargar historial
        df_msgs = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono = :t ORDER BY fecha ASC"), conn, params={"t": telefono_actual})

    # Cabecera del chat
    nombre_mostrar = info_cliente.nombre_corto if info_cliente else telefono_actual
    st.subheader(f"Conversación con {nombre_mostrar}")
    
    # Contenedor de mensajes (Scrollable)
    contenedor = st.container(height=500)
    with contenedor:
        if df_msgs.empty:
            st.caption("No hay mensajes previos.")
        
        # Estilos CSS para las burbujas
        st.markdown("""
        <style>
            .msg-container { display: flex; margin-bottom: 8px; }
            .msg-mio { justify-content: flex-end; }
            .msg-otro { justify-content: flex-start; }
            .bubble { max-width: 75%; padding: 8px 12px; border-radius: 12px; font-size: 15px; position: relative; box-shadow: 0 1px 1px rgba(0,0,0,0.1); }
            .bubble-mio { background-color: #dcf8c6; color: black; border-top-right-radius: 0; }
            .bubble-otro { background-color: white; color: black; border-top-left-radius: 0; }
            .meta { font-size: 11px; color: #888; text-align: right; margin-top: 4px; }
        </style>
        """, unsafe_allow_html=True)

        for _, msg in df_msgs.iterrows():
            es_mio = (msg['tipo'] == 'SALIENTE')
            contenido = msg['contenido']
            hora = msg['fecha'].strftime("%H:%M") if msg['fecha'] else ""
            
            # Iconos de estado
            icono = ""
            if es_mio:
                est = msg.get('estado_waha', '')
                if est == 'leido': icono = "🔵"     # Azul
                elif est == 'recibido': icono = "☑️" # Gris doble
                elif est == 'enviado': icono = "✔️"  # Gris simple
                else: icono = "🕒"                   # Reloj

            clase_align = "msg-mio" if es_mio else "msg-otro"
            clase_bubble = "bubble-mio" if es_mio else "bubble-otro"
            
            st.markdown(f"""
            <div class='msg-container {clase_align}'>
                <div class='bubble {clase_bubble}'>
                    <div>{contenido}</div>
                    <div class='meta'>{hora} {icono}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # Input para enviar
    with st.form("chat_input", clear_on_submit=True):
        col_txt, col_btn = st.columns([85, 15])
        texto = col_txt.text_input("Escribe...", label_visibility="collapsed")
        enviar = col_btn.form_submit_button("➤")
        
        if enviar and texto:
            ok, res = enviar_mensaje_whatsapp(telefono_actual, texto)
            if ok:
                with engine.connect() as conn:
                    conn.execute(text("""
                        INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, estado_waha)
                        VALUES (:t, 'SALIENTE', :c, NOW(), TRUE, 'enviado')
                    """), {"t": telefono_actual, "c": texto})
                    conn.commit()
                st.rerun()
            else:
                st.error(f"Error: {res}")