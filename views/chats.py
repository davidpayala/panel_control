import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
from database import engine 
from utils import (
    enviar_mensaje_whatsapp, 
    sincronizar_historial,
    marcar_chat_como_leido_waha
)

def get_table_name(conn):
    """Detecta si la tabla se llama 'clientes' o 'Clientes'"""
    try:
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        return "\"Clientes\""

def render_chat():
    st.title("💬 Chat Center")

    # --- 1. INTENTO DE REPARACIÓN SILENCIOSA ---
    # No detenemos el código si esto falla
    try:
        with engine.connect() as conn:
            tabla = get_table_name(conn)
            
            # Verificar si hay mensajes huérfanos
            sql_check = f"""
                INSERT INTO {tabla} (telefono, nombre_corto, estado, activo, fecha_registro)
                SELECT DISTINCT m.telefono, 'Recuperado', 'Sin empezar', TRUE, NOW()
                FROM mensajes m
                WHERE m.telefono NOT IN (SELECT c.telefono FROM {tabla} c)
            """
            conn.execute(text(sql_check))
            conn.commit()
    except Exception as e:
        # Solo imprimimos en consola, no rompemos la UI
        print(f"Error menor en auto-reparación: {e}")

    # --- 2. BARRA LATERAL (SIDEBAR) ---
    with st.sidebar:
        st.header("Clientes")
        
        col1, col2 = st.columns(2)
        if col1.button("🔄 Sync"):
            with st.spinner("Sincronizando..."):
                res = sincronizar_historial()
            st.toast(res)
            time.sleep(1)
            st.rerun()
            
        if col2.button("🚑 Fix"):
            st.toast("Intentando reparación forzada...")
            try:
                with engine.connect() as conn:
                    tabla = get_table_name(conn)
                    conn.execute(text(f"""
                        INSERT INTO {tabla} (telefono, nombre_corto, estado, activo, fecha_registro)
                        SELECT DISTINCT m.telefono, 'Recuperado', 'Sin empezar', TRUE, NOW()
                        FROM mensajes m
                        WHERE m.telefono NOT IN (SELECT c.telefono FROM {tabla} c)
                    """))
                    conn.commit()
                st.success("Reparación ejecutada")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

        st.markdown("---")
        
        busqueda = st.text_input("🔍 Buscar:", "")

        # --- CARGA DE LA LISTA DE CHATS ---
        try:
            with engine.connect() as conn:
                tabla = get_table_name(conn)
                
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
                st.info("📭 No se encontraron chats.")
                st.caption("Pulsa '🔄 Sync' o envía un mensaje al bot para empezar.")
            
            # Renderizar botones
            for i, row in df_clientes.iterrows():
                telefono = row['telefono']
                nombre = row['nombre_corto'] or telefono
                no_leidos = row['no_leidos']
                estado_negocio = row['estado']
                
                # Diseño del botón
                label = f"{nombre}"
                if no_leidos > 0:
                    label += f" ({no_leidos} 📩)"
                
                if st.button(f"{label}\n📌 {estado_negocio}", key=f"chat_{telefono}", use_container_width=True):
                    st.session_state['chat_actual_telefono'] = telefono
                    st.rerun()
                
        except Exception as e:
            st.error(f"Error cargando lista: {e}")

    # --- 3. AREA DE CHAT (DERECHA) ---
    if 'chat_actual_telefono' in st.session_state:
        telefono_actual = st.session_state['chat_actual_telefono']
        
        # Intentar marcar leido en WAHA (silencioso)
        try: marcar_chat_como_leido_waha(telefono_actual)
        except: pass

        with engine.connect() as conn:
            tabla = get_table_name(conn)
            # Marcar leídos en DB local
            conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
            conn.commit()
            
            info_cliente = conn.execute(text(f"SELECT * FROM {tabla} WHERE telefono=:t"), {"t": telefono_actual}).fetchone()

        nombre_mostrar = info_cliente.nombre_corto if info_cliente else telefono_actual
        st.subheader(f"Conversación con {nombre_mostrar}")
        
        # Cargar mensajes
        with engine.connect() as conn:
            df_msgs = pd.read_sql(text("""
                SELECT * FROM mensajes 
                WHERE telefono = :t 
                ORDER BY fecha ASC
            """), conn, params={"t": telefono_actual})

        # Mostrar mensajes
        contenedor_chat = st.container(height=500)
        with contenedor_chat:
            if df_msgs.empty:
                st.info("No hay mensajes previos.")
            
            for _, msg in df_msgs.iterrows():
                es_mio = (msg['tipo'] == 'SALIENTE')
                contenido = msg['contenido']
                hora = msg['fecha'].strftime("%H:%M") if msg['fecha'] else ""
                
                # Iconos
                icono = ""
                if es_mio:
                    est = msg.get('estado_waha', '')
                    icono = "🔵" if est == 'leido' else "☑️" if est == 'recibido' else "✔️"

                # Burbujas
                align = "flex-end" if es_mio else "flex-start"
                bg = "#dcf8c6" if es_mio else "#ffffff"
                
                st.markdown(f"""
                <div style='display: flex; justify-content: {align}; margin-bottom: 5px;'>
                    <div style='background: {bg}; padding: 8px 12px; border-radius: 10px; max-width: 75%; box-shadow: 1px 1px 2px rgba(0,0,0,0.1); color: black;'>
                        <div style='font-size: 15px;'>{contenido}</div>
                        <div style='font-size: 11px; color: gray; text-align: right; margin-top: 4px;'>{hora} {icono}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        # Input de respuesta
        with st.form("form_chat", clear_on_submit=True):
            col_txt, col_send = st.columns([85, 15])
            texto = col_txt.text_input("Mensaje", label_visibility="collapsed")
            if col_send.form_submit_button("➤"):
                if texto:
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
    else:
        st.info("👈 Selecciona un chat del menú lateral.")