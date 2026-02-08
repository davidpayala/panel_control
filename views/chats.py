import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
from database import engine 
from utils import (
    enviar_mensaje_media, enviar_mensaje_whatsapp, 
    normalizar_telefono_maestro, sincronizar_historial,
    marcar_chat_como_leido_waha
)

def render_chat():
    st.title("💬 Chat Center")

    # --- 1. DIAGNÓSTICO AUTOMÁTICO DE TABLAS ---
    # Esto revisa si tus tablas tienen datos y si coinciden los nombres
    try:
        with engine.connect() as conn:
            # Verificamos cantidad de datos
            cant_mensajes = conn.execute(text("SELECT COUNT(*) FROM mensajes")).scalar()
            cant_clientes = conn.execute(text("SELECT COUNT(*) FROM clientes")).scalar()
            
            # AUTO-REPARACIÓN: Si hay mensajes pero no clientes, los creamos
            if cant_mensajes > 0 and cant_clientes == 0:
                st.toast("⚠️ Detectados mensajes sin cliente. Reparando...")
                conn.execute(text("""
                    INSERT INTO clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                    SELECT DISTINCT m.telefono, 'Recuperado', 'Sin empezar', TRUE, NOW()
                    FROM mensajes m
                    WHERE m.telefono NOT IN (SELECT c.telefono FROM clientes c)
                """))
                conn.commit()
                st.success("✅ Base de datos reparada automáticamente. Recargando...")
                time.sleep(1.5)
                st.rerun()
                
    except Exception as e:
        st.error(f"🔥 Error de Base de Datos: {e}")
        st.info("Posible causa: La tabla 'clientes' no existe o tiene otro nombre.")
        return # Detenemos la ejecución si hay error grave

    # --- 2. BARRA LATERAL (SIDEBAR) ---
    with st.sidebar:
        st.header("Clientes")
        
        # Muestra estado del sistema (Solo para debugging, puedes borrarlo luego)
        st.caption(f"📊 Estado: {cant_mensajes} msgs | {cant_clientes} clientes")

        col1, col2 = st.columns(2)
        if col1.button("🔄 Sync"):
            with st.spinner("Sincronizando..."):
                res = sincronizar_historial()
            st.toast(res)
            time.sleep(1)
            st.rerun()
            
        if col2.button("🛠️ Fix"):
            # Botón manual de reparación por si acaso
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                    SELECT DISTINCT m.telefono, 'Recuperado', 'Sin empezar', TRUE, NOW()
                    FROM mensajes m
                    WHERE m.telefono NOT IN (SELECT c.telefono FROM clientes c)
                """))
                conn.commit()
            st.toast("Reparación forzada completada")
            st.rerun()

        st.markdown("---")
        
        busqueda = st.text_input("🔍 Buscar:", "")

        # Consulta principal para la lista
        try:
            # Nota: Usamos 'clientes' en minúsculas. Si falla, el try/except lo dirá.
            query = """
                SELECT 
                    c.telefono, 
                    c.nombre_corto, 
                    c.estado,
                    (SELECT COUNT(*) FROM mensajes m 
                     WHERE m.telefono = c.telefono AND m.leido = FALSE AND m.tipo = 'ENTRANTE') as no_leidos
                FROM clientes c
                WHERE c.activo = TRUE
            """
            
            if busqueda:
                query += f" AND (c.nombre_corto ILIKE '%{busqueda}%' OR c.telefono ILIKE '%{busqueda}%')"
            
            query += " ORDER BY no_leidos DESC, c.fecha_registro DESC"
            
            with engine.connect() as conn:
                df_clientes = pd.read_sql(text(query), conn)

            if df_clientes.empty:
                st.info("📭 No hay chats activos.")
                st.markdown("Pueba el botón **🔄 Sync** para descargar chats.")
            
            # --- RENDERIZADO DE LA LISTA ---
            for i, row in df_clientes.iterrows():
                telefono = row['telefono']
                nombre = row['nombre_corto'] or "Desconocido"
                no_leidos = row['no_leidos']
                estado_negocio = row['estado']
                
                # Estilo visual
                clase_extra = "unread" if no_leidos > 0 else ""
                alert_html = f"<span class='badge'>{no_leidos}</span>" if no_leidos > 0 else ""
                
                # Botón HTML simulado
                btn_label = f"{nombre} {alert_html}"
                if st.button(f"{nombre}\n{estado_negocio}", key=f"chat_{telefono}", use_container_width=True):
                    st.session_state['chat_actual_telefono'] = telefono
                    st.rerun()
                
        except Exception as e:
            st.error(f"Error cargando lista: {e}")

    # --- 3. AREA DE CHAT (DERECHA) ---
    if 'chat_actual_telefono' in st.session_state:
        telefono_actual = st.session_state['chat_actual_telefono']
        
        # Marcar leídos visualmente en WhatsApp (Blue Ticks)
        try:
            marcar_chat_como_leido_waha(telefono_actual)
        except: pass

        # Consultar info del cliente
        with engine.connect() as conn:
            # Marcar leídos en DB local
            conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
            conn.commit()
            
            info_cliente = conn.execute(text("SELECT * FROM clientes WHERE telefono=:t"), {"t": telefono_actual}).fetchone()

        if info_cliente:
            st.subheader(f"Conversación con {info_cliente.nombre_corto or telefono_actual}")
            
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
                    
                    # Iconos de estado (Ticks)
                    icono_estado = ""
                    if es_mio:
                        estado = msg.get('estado_waha', '') # .get por si la columna no existe aun
                        if estado == 'leido': icono_estado = "🔵"
                        elif estado == 'recibido': icono_estado = "☑️"
                        elif estado == 'enviado': icono_estado = "✔️"
                        else: icono_estado = "🕒"

                    # Burbujas
                    alineacion = "flex-end" if es_mio else "flex-start"
                    bg_color = "#dcf8c6" if es_mio else "#ffffff"
                    
                    st.markdown(f"""
                    <div style='display: flex; justify-content: {alineacion}; margin-bottom: 10px;'>
                        <div style='background: {bg_color}; padding: 10px; border-radius: 10px; max-width: 70%; box-shadow: 1px 1px 2px rgba(0,0,0,0.1); color: black;'>
                            <div>{contenido}</div>
                            <div style='font-size: 0.7em; color: gray; text-align: right; margin-top: 5px;'>
                                {hora} {icono_estado}
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Input de respuesta
            with st.form("form_chat", clear_on_submit=True):
                col_txt, col_send = st.columns([5, 1])
                texto = col_txt.text_input("Escribe un mensaje...", label_visibility="collapsed")
                enviar = col_send.form_submit_button("➤")
                
                if enviar and texto:
                    ok, res = enviar_mensaje_whatsapp(telefono_actual, texto)
                    if ok:
                        # Guardar localmente
                        with engine.connect() as conn:
                             conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, estado_waha)
                                VALUES (:t, 'SALIENTE', :c, NOW(), TRUE, 'enviado')
                             """), {"t": telefono_actual, "c": texto})
                             conn.commit()
                        st.rerun()
                    else:
                        st.error(f"Error enviando: {res}")
    else:
        st.info("👈 Selecciona un chat del menú lateral para comenzar.")