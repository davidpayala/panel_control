import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
from database import engine 

# --- GESTIÓN DE IMPORTACIONES ROBUSTA ---
# Corregimos el nombre de la función: en utils se llama 'marcar_chat_como_leido_waha'
try:
    from utils import (
        enviar_mensaje_media, 
        enviar_mensaje_whatsapp, 
        normalizar_telefono_maestro, 
        sincronizar_historial,
        marcar_chat_como_leido_waha as marcar_leido_waha # Alias para que funcione tu código
    )
except ImportError as e:
    st.error(f"Error de importación en chats: {e}")
    # Definimos funciones dummy para que no rompa la app
    def enviar_mensaje_media(*args): return False, "Error import"
    def enviar_mensaje_whatsapp(*args): return False, "Error import"
    def normalizar_telefono_maestro(*args): return None
    def sincronizar_historial(*args): return "Error import"
    def marcar_leido_waha(*args): pass

# --- FUNCIÓN PARA DETECTAR EL NOMBRE REAL DE LA TABLA ---
def get_table_name(conn):
    """Detecta de forma segura si la tabla es 'clientes' o 'Clientes'"""
    try:
        # Intentamos consulta simple
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        # Si falla, probamos con comillas (para PostgreSQL con mayúsculas)
        return "\"Clientes\""

def render_chat():
    st.title("💬 Chat Center")

    # Guardamos el teléfono actual en una variable local para usarla fácil
    telefono_actual = st.session_state.get('chat_actual_telefono', None)

    # --- 1. BARRA LATERAL (LISTA DE CHATS) ---
    with st.sidebar:
        st.header("Clientes / Chats")
        
        # --- BOTONES DE ACCIÓN ---
        # Los ponemos dentro de un contenedor para asegurar que se rendericen
        col1, col2 = st.columns(2)
        
        if col1.button("🔄 Sync", use_container_width=True):
            with st.spinner("Sincronizando WAHA..."):
                try:
                    res = sincronizar_historial()
                    st.toast(res, icon="✅")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Falló Sync: {e}")
            
        if col2.button("🛠️ Fix", use_container_width=True, help="Repara chats que no aparecen en la lista"):
            try:
                with engine.connect() as conn:
                    tabla = get_table_name(conn)
                    # SQL corrección: Inserta en clientes los teléfonos que existen en mensajes pero no en clientes
                    sql_fix = f"""
                        INSERT INTO {tabla} (telefono, nombre_corto, estado, activo, fecha_registro)
                        SELECT DISTINCT m.telefono, 'Recuperado', 'Sin empezar', TRUE, NOW()
                        FROM mensajes m
                        WHERE m.telefono NOT IN (SELECT c.telefono FROM {tabla} c)
                    """
                    res = conn.execute(text(sql_fix))
                    conn.commit()
                    if res.rowcount > 0:
                        st.success(f"✅ Se recuperaron {res.rowcount} chats.")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.info("Todo parece estar en orden.")
            except Exception as e:
                st.error(f"Error Fix: {e}")

        st.markdown("---")
        
        # --- CARGA DE LISTA DE CHATS ---
        # Envolvemos TODO esto en try/except para que la barra no desaparezca si falla la DB
        try:
            with engine.connect() as conn:
                tabla = get_table_name(conn)
                
                # Buscador
                busqueda = st.text_input("🔍 Buscar chat:", "", placeholder="Nombre o teléfono...")
                
                # Query Dinámica optimizada con COALESCE para evitar errores con NULLs
                base_query = f"""
                    SELECT 
                        c.telefono, 
                        COALESCE(c.nombre_corto, c.telefono) as nombre_mostrar, 
                        c.estado,
                        (SELECT COUNT(*) FROM mensajes m 
                         WHERE m.telefono = c.telefono AND m.leido = FALSE AND m.tipo = 'ENTRANTE') as no_leidos
                    FROM {tabla} c
                    WHERE c.activo = TRUE
                """
                
                if busqueda:
                    # Usamos COALESCE también en el filtro para que no falle si hay nulos
                    base_query += f""" 
                        AND (
                            COALESCE(c.nombre_corto, '') ILIKE '%{busqueda}%' 
                            OR 
                            COALESCE(c.telefono, '') ILIKE '%{busqueda}%'
                        )
                    """
                
                # Ordenar: Primero los que tienen no leídos, luego los más recientes (por ID o fecha si existiera)
                base_query += " ORDER BY no_leidos DESC, c.id_cliente DESC LIMIT 50"
                
                df_clientes = pd.read_sql(text(base_query), conn)

                if df_clientes.empty:
                    st.info("📭 No hay chats activos.")
                    st.caption("Prueba el botón '🛠️ Fix' o '🔄 Sync'.")
                
                # Renderizar lista
                st.write(f"Mostrando: {len(df_clientes)}")
                
                for i, row in df_clientes.iterrows():
                    t_row = row['telefono']
                    n_row = row['nombre_mostrar']
                    c_leidos = row['no_leidos']
                    e_row = row['estado'] or "Sin estado"
                    
                    # Estilo del botón según si está seleccionado o tiene mensajes nuevos
                    icono_alerta = "🔴" if c_leidos > 0 else "👤"
                    texto_btn = f"{icono_alerta} {n_row}"
                    if c_leidos > 0:
                        texto_btn += f" ({c_leidos})"
                    
                    # Destacar el seleccionado
                    tipo_btn = "secondary"
                    if telefono_actual == t_row:
                        tipo_btn = "primary" # Destaca el chat activo visualmente
                        texto_btn = f"👉 {texto_btn}"

                    # Usamos un key único combinando teléfono
                    if st.button(f"{texto_btn}\n📌 {e_row}", key=f"btn_chat_{t_row}", use_container_width=True, type=tipo_btn):
                        st.session_state['chat_actual_telefono'] = t_row
                        st.rerun()
                    
        except Exception as e:
            st.error("⚠️ Error cargando lista")
            st.exception(e) # Esto mostrará el error exacto en pantalla para poder depurar

    # --- 2. AREA DE CHAT PRINCIPAL ---
    if not telefono_actual:
        st.info("👈 Selecciona un chat del menú lateral para comenzar o pulsa 'Sync' para descargar mensajes nuevos.")
        return

    # Intentar marcar leido en WhatsApp (Blue Ticks) de forma segura
    try: 
        if telefono_actual:
            marcar_leido_waha(f"{telefono_actual}@c.us")
    except: 
        pass

    # Obtener mensajes y actualizar leídos localmente
    try:
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
        nombre_mostrar = info_cliente.nombre_corto if info_cliente and info_cliente.nombre_corto else telefono_actual
        st.subheader(f"Conversación con {nombre_mostrar}")
        st.caption(f"Teléfono: {telefono_actual}")
        
        # Contenedor de mensajes (Scrollable)
        contenedor = st.container(height=500)
        with contenedor:
            if df_msgs.empty:
                st.caption("No hay mensajes previos en la base de datos.")
            
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
                # Manejo seguro de fecha
                hora = ""
                if msg['fecha']:
                    hora = msg['fecha'].strftime("%H:%M")
                
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
            enviar = col_btn.form_submit_button("➤ Enviar")
            
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
                    st.error(f"Error al enviar: {res}")

    except Exception as e:
        st.error("Error cargando el chat seleccionado.")
        st.code(e)