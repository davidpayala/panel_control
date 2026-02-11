import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import json
import requests
import os
from datetime import datetime

# Configuración API
WAHA_URL = os.getenv("WAHA_URL")
WAHA_KEY = os.getenv("WAHA_KEY")

def get_headers():
    h = {"Content-Type": "application/json"}
    if WAHA_KEY: h["X-Api-Key"] = WAHA_KEY
    return h

def render_diagnostico():
    st.title("🛠️ Centro de Diagnóstico")
    
    tab_logs, tab_inspector = st.tabs(["📡 Logs Webhook (Recibidos)", "🕵️ Inspector API (Consulta)"])

    # ==========================================================================
    # PESTAÑA 1: LOGS DE BASE DE DATOS
    # ==========================================================================
    with tab_logs:
        c_btn, c_info = st.columns([20, 80])
        if c_btn.button("🔄 Actualizar", key="btn_logs"):
            st.rerun()
        
        c_info.info("Muestra los últimos 20 eventos crudos recibidos por el servidor.")

        try:
            with engine.connect() as conn:
                # 🚀 CORRECCIÓN CRÍTICA: Commit para leer datos frescos
                conn.commit()
                
                try:
                    query = """
                        SELECT id, fecha, session_name, event_type, payload 
                        FROM webhook_logs 
                        ORDER BY id DESC LIMIT 20
                    """
                    df = pd.read_sql(text(query), conn)
                except Exception as e:
                    st.warning(f"Esperando datos... ({str(e)})")
                    df = pd.DataFrame()

            if df.empty:
                st.caption("No hay registros recientes.")
            else:
                for index, row in df.iterrows():
                    fecha_str = row['fecha'].strftime("%H:%M:%S")
                    evento = row['event_type']
                    
                    # Intentamos parsear el JSON para mostrar un título bonito
                    try:
                        payload_json = json.loads(row['payload'])
                        p = payload_json.get('payload', {})
                        resumen = "Datos..."
                        
                        if evento == 'message':
                            body = p.get('body', '') or (p.get('media', {}).get('url', '')) 
                            if not body: body = "[Multimedia/Sticker]"
                            origen = "📤 YO" if p.get('fromMe') else f"📥 {p.get('from', '?').split('@')[0]}"
                            resumen = f"{origen} | {str(body)[:40]}"
                        elif evento == 'message.ack':
                            est_map = {1:'Enviado', 2:'Recibido', 3:'Leído', 4:'Play'}
                            status = est_map.get(p.get('ack'), str(p.get('ack')))
                            resumen = f"Estado: {status}"
                    except:
                        payload_json = {"error": "No JSON", "raw": str(row['payload'])}
                        resumen = "Log sin formato"

                    # Color del log según evento
                    icono = "📩"
                    if evento == 'message.ack': icono = "✔️"
                    
                    with st.expander(f"{icono} {fecha_str} | {evento} | {resumen}"):
                        st.text(f"ID Log: {row['id']} | Sesión: {row['session_name']}")
                        st.json(payload_json)

        except Exception as e:
            st.error(f"Error leyendo logs: {e}")

    # ==========================================================================
    # PESTAÑA 2: INSPECTOR API
    # ==========================================================================
    with tab_inspector:
        st.info("Consulta directa a WAHA para verificar cómo ve el sistema un chat.")

        c1, c2 = st.columns(2)
        session = c1.selectbox("Sesión", ["principal", "default"], key="sel_session")
        chat_id_manual = c2.text_input("Teléfono (519...)", placeholder="Ej: 51999888777", key="txt_chat_id")
        
        if st.button("🔍 Analizar Chat", type="primary"):
            if not WAHA_URL:
                st.error("Falta WAHA_URL")
                return

            chat_target = chat_id_manual.strip()
            if chat_target and "@" not in chat_target:
                chat_target = f"{chat_target}@c.us"

            # Si no puso nada, buscamos uno activo
            if not chat_target:
                with st.spinner("Buscando último chat activo..."):
                    try:
                        r = requests.get(f"{WAHA_URL}/api/{session}/chats?limit=1", headers=get_headers())
                        data = r.json()
                        if data: chat_target = data[0]['id']
                    except: pass

            if chat_target:
                st.write(f"📂 Analizando: `{chat_target}`")
                try:
                    url_msgs = f"{WAHA_URL}/api/{session}/chats/{chat_target}/messages?limit=5"
                    r_msg = requests.get(url_msgs, headers=get_headers())
                    mensajes = list(reversed(r_msg.json()))

                    for msg in mensajes:
                        ts = msg.get('timestamp', 0)
                        fecha = datetime.fromtimestamp(int(str(ts)[:10])).strftime('%H:%M:%S')
                        cuerpo = msg.get('body') or "[Media]"
                        origen = "📤" if msg.get('fromMe') else "📥"
                        
                        with st.expander(f"{origen} {fecha} - {cuerpo[:50]}"):
                            st.json(msg)
                except Exception as e:
                    st.error(f"Error API: {e}")
            else:
                st.warning("No se especificó chat ni se encontró uno reciente.")