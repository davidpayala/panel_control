import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
from database import engine 

# --- GESTIÓN DE IMPORTACIONES ROBUSTA ---
# Mantenemos este bloque de seguridad para evitar que la app se rompa si falla utils
try:
    from utils import (
        enviar_mensaje_media, 
        enviar_mensaje_whatsapp, 
        normalizar_telefono_maestro, 
        sincronizar_historial,
        marcar_chat_como_leido_waha as marcar_leido_waha 
    )
except ImportError:
    # Si falla la importación, usamos funciones vacías para que la interfaz cargue igual
    def enviar_mensaje_media(*args): return False, "Error import"
    def enviar_mensaje_whatsapp(*args): return False, "Error import"
    def normalizar_telefono_maestro(*args): return None
    def sincronizar_historial(*args): return "Error import"
    def marcar_leido_waha(*args): pass

def get_table_name(conn):
    """Detecta el nombre correcto de la tabla (clientes vs Clientes)"""
    try:
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        return "\"Clientes\""

def render_chat():
    st.title("💬 Chat Center")

    # Inicializar variable de sesión para el chat seleccionado
    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    telefono_actual = st.session_state['chat_actual_telefono']

    # ==========================================
    # 1. BARRA LATERAL (Bandeja de Entrada)
    # ==========================================
    with st.sidebar:
        st.header("Bandeja de Entrada")
        
        # --- Botones de Acción (Sync y Fix) ---
        col1, col2 = st.columns(2)
        
        # Botón Sincronizar
        if col1.button("🔄 Sync", use_container_width=True, help="Descargar mensajes nuevos de WAHA"):
            with st.spinner("Sincronizando..."):
                try:
                    res = sincronizar_historial()
                    st.toast(res, icon="✅")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

        # Botón Reparar (Fix)
        if col2.button("🛠️ Fix", use_container_width=True, help="Si un chat no aparece, usa este botón"):
            try:
                with engine.connect() as conn:
                    tabla = get_table_name(conn)
                    # Inserta en clientes los teléfonos que existen en mensajes pero no en clientes
                    sql_fix = f"""
                        INSERT INTO {tabla} (telefono, nombre_corto, estado, activo, fecha_registro)
                        SELECT DISTINCT m.telefono, 'Recuperado', 'Sin empezar', TRUE, NOW()
                        FROM mensajes m
                        WHERE m.telefono NOT IN (SELECT c.telefono FROM {tabla} c)
                    """
                    res = conn.execute(text(sql_fix))
                    conn.commit()
                    if res.rowcount > 0:
                        st.success(f"✅ {res.rowcount} chats recuperados")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.toast("Base de datos en orden", icon="👍")
            except Exception as e:
                st.error(f"Error Fix: {e}")

        st.divider()

        # --- Lista de Chats ---
        try:
            with engine.connect() as conn:
                tabla = get_table_name(conn)
                
                # Buscador
                busqueda = st.text_input("🔍 Buscar chat:", placeholder="Nombre o teléfono...")
                
                # Query Segura (usa COALESCE para evitar errores con nulos)
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
                
                if busqueda:
                    query += f" AND (COALESCE(c.telefono,'') ILIKE '%{busqueda}%' OR COALESCE(c.nombre_corto,'') ILIKE '%{busqueda}%')"
                
                # Ordenar: Primero con mensajes no leídos, luego por ID reciente
                query += " ORDER BY no_leidos DESC, c.id_cliente DESC LIMIT 50"
                
                df_clientes = pd.read_sql(text(query), conn)

            if df_clientes.empty:
                st.info("No se encontraron chats.")
            else:
                st.caption(f"Mostrando {len(df_clientes)} chats")
                
                for _, row in df_clientes.iterrows():
                    t_row = row['telefono']
                    n_row = row['nombre']
                    c_leidos = row['no_leidos']
                    
                    # Diseño del botón
                    icono = "🔴" if c_leidos > 0 else "👤"
                    label = f"{icono} {n_row}"
                    if c_leidos > 0:
                        label += f" ({c_leidos})"
                    
                    # Estilo visual: Primary si es el actual, Secondary si no
                    tipo_btn = "primary" if telefono_actual == t_row else "secondary"
                    
                    if st.button(label, key=f"chat_list_{t_row}", use_container_width=True, type=tipo_btn):
                        st.session_state['chat_actual_telefono'] = t_row
                        st.rerun()

        except Exception as e:
            st.error("⚠️ Error cargando la lista")
            st.caption(str(e)) # Muestra el error pequeño por si acaso

    # ==========================================
    # 2. ÁREA PRINCIPAL DE CHAT
    # ==========================================
    if not telefono_actual:
        st.info("👈 Selecciona un chat del menú lateral para ver la conversación.")
        return

    # Lógica del Chat Activo
    try:
        # Intentar marcar tick azul en WhatsApp (si falla no importa)
        try: marcar_leido_waha(f"{telefono_actual}@c.us")
        except: pass

        with engine.connect() as conn:
            tabla = get_table_name(conn)
            
            # 1. Marcar como leídos en local
            conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
            conn.commit()
            
            # 2. Obtener info del cliente
            info_cli = conn.execute(text(f"SELECT * FROM {tabla} WHERE telefono=:t"), {"t": telefono_actual}).fetchone()
            nombre_chat = info_cli.nombre_corto if info_cli and info_cli.nombre_corto else telefono_actual
            
            # 3. Cargar historial de mensajes
            df_msgs = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono = :t ORDER BY fecha ASC"), conn, params={"t": telefono_actual})

        # Cabecera
        st.subheader(f"Conversación con {nombre_chat}")
        st.caption(f"📱 {telefono_actual}")
        
        # Contenedor de mensajes con scroll
        chat_container = st.container(height=500)
        with chat_container:
            if df_msgs.empty:
                st.caption("No hay historial de mensajes.")
            
            # Estilos CSS para burbujas de chat
            st.markdown("""
            <style>
                .msg-row { display: flex; margin-bottom: 5px; }
                .msg-row-mio { justify-content: flex-end; }
                .msg-row-otro { justify-content: flex-start; }
                .msg-bubble { max-width: 80%; padding: 8px 12px; border-radius: 12px; font-size: 15px; position: relative; }
                .bubble-mio { background-color: #dcf8c6; color: black; border-top-right-radius: 0; }
                .bubble-otro { background-color: #f0f0f0; color: black; border-top-left-radius: 0; border: 1px solid #ddd; }
                .msg-meta { font-size: 10px; color: #666; text-align: right; margin-top: 2px; }
            </style>
            """, unsafe_allow_html=True)

            for _, msg in df_msgs.iterrows():
                es_mio = (msg['tipo'] == 'SALIENTE')
                txt = msg['contenido']
                hora = msg['fecha'].strftime("%H:%M") if msg['fecha'] else ""
                
                clase_row = "msg-row-mio" if es_mio else "msg-row-otro"
                clase_bbl = "bubble-mio" if es_mio else "bubble-otro"
                
                st.markdown(f"""
                <div class='msg-row {clase_row}'>
                    <div class='msg-bubble {clase_bbl}'>
                        <div>{txt}</div>
                        <div class='msg-meta'>{hora}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        # Zona de escritura
        with st.form("form_enviar", clear_on_submit=True):
            col_in, col_btn = st.columns([0.85, 0.15])
            texto_enviar = col_in.text_input("Escribe tu mensaje...", label_visibility="collapsed")
            btn_enviar = col_btn.form_submit_button("➤")
            
            if btn_enviar and texto_enviar:
                ok, res = enviar_mensaje_whatsapp(telefono_actual, texto_enviar)
                if ok:
                    # Guardar en DB
                    with engine.connect() as conn:
                        conn.execute(text("""
                            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, estado_waha)
                            VALUES (:t, 'SALIENTE', :c, NOW(), TRUE, 'enviado')
                        """), {"t": telefono_actual, "c": texto_enviar})
                        conn.commit()
                    st.rerun()
                else:
                    st.error(f"No se pudo enviar: {res}")

    except Exception as e:
        st.error("Error al cargar la conversación.")
        st.caption(str(e))