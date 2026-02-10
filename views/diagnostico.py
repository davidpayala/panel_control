import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import json
import requests
import os
from datetime import datetime

# Configuración API (Para el Inspector)
WAHA_URL = os.getenv("WAHA_URL")
WAHA_KEY = os.getenv("WAHA_KEY")

def get_headers():
    h = {"Content-Type": "application/json"}
    if WAHA_KEY: h["X-Api-Key"] = WAHA_KEY
    return h

def render_diagnostico():
    st.title("🛠️ Centro de Diagnóstico")
    
    # Crear dos pestañas
    tab_logs, tab_inspector = st.tabs(["📡 Logs Webhook (Recibidos)", "🕵️ Inspector API (Consulta)"])

    # ==========================================================================
    # PESTAÑA 1: LOGS DE BASE DE DATOS (Lo Nuevo - Caja Negra)
    # ==========================================================================
    with tab_logs:
        st.info("Aquí se muestran los últimos 20 eventos tal cual llegaron al servidor (JSON Crudo).")
        
        if st.button("🔄 Actualizar Logs", key="btn_logs"):
            st.rerun()

        try:
            with engine.connect() as conn:
                # Verificar si la tabla existe antes de consultar
                try:
                    query = """
                        SELECT id, fecha, session_name, event_type, payload 
                        FROM webhook_logs 
                        ORDER BY id DESC
                    """
                    df = pd.read_sql(text(query), conn)
                except:
                    st.warning("La tabla 'webhook_logs' aún no existe. Espera a recibir el primer mensaje.")
                    df = pd.DataFrame()

            if df.empty:
                st.caption("No hay registros recientes.")
            else:
                for index, row in df.iterrows():
                    fecha_str = row['fecha'].strftime("%H:%M:%S")
                    evento = row['event_type']
                    sesion = row['session_name']
                    
                    try:
                        payload_json = json.loads(row['payload'])
                        # Resumen inteligente para el título
                        p = payload_json.get('payload', {})
                        resumen = "Datos..."
                        
                        if evento == 'message':
                            body = p.get('body', '[Media]') or '[Sin texto]'
                            origen = "📤 YO" if p.get('fromMe') else f"📥 {p.get('from', '?')}"
                            resumen = f"{origen} | {body[:40]}"
                        elif evento == 'message.ack':
                            status = p.get('ack')
                            resumen = f"ACK Estado: {status}"
                            
                    except:
                        payload_json = row['payload']
                        resumen = "Error Parseo JSON"

                    with st.expander(f"#{row['id']} | {fecha_str} | {evento} | {resumen}"):
                        c1, c2 = st.columns(2)
                        c1.text(f"Sesión: {sesion}")
                        c2.text(f"Evento: {evento}")
                        st.json(payload_json)

        except Exception as e:
            st.error(f"Error leyendo logs: {e}")

    # ==========================================================================
    # PESTAÑA 2: INSPECTOR API (Tu herramienta anterior)
    # ==========================================================================
    with tab_inspector:
        st.info("Esta herramienta consulta directamente a WAHA para ver cómo ve el sistema un chat específico.")

        c1, c2 = st.columns(2)
        session = c1.selectbox("Sesión WAHA", ["principal", "default"], key="sel_session")
        chat_id_manual = c2.text_input("ID de Chat (Opcional)", placeholder="Dejar vacío para automático", key="txt_chat_id")
        
        if st.button("🔍 Analizar Mensajes Reales", type="primary", key="btn_inspect"):
            if not WAHA_URL:
                st.error("Falta WAHA_URL")
                return

            chat_a_analizar = chat_id_manual

            # 1. Buscar chat activo si no hay ID
            if not chat_a_analizar:
                with st.spinner(f"Buscando chats activos en '{session}'..."):
                    try:
                        url_chats = f"{WAHA_URL}/api/{session}/chats?limit=15"
                        r = requests.get(url_chats, headers=get_headers(), timeout=10)
                        chats_data = r.json()
                        
                        for c in chats_data:
                            cid = c.get('id', '')
                            if 'status@broadcast' not in cid:
                                chat_a_analizar = cid
                                st.toast(f"Analizando chat automático: {cid}", icon="🤖")
                                break
                    except Exception as e:
                        st.error(f"Error buscando chats: {e}")
                        return

            if not chat_a_analizar:
                st.warning("No se encontró ningún chat para analizar.")
            else:
                # 2. Descargar mensajes
                st.write(f"📂 **Extrayendo mensajes de:** `{chat_a_analizar}`")
                try:
                    url_msgs = f"{WAHA_URL}/api/{session}/chats/{chat_a_analizar}/messages?limit=10"
                    r_msg = requests.get(url_msgs, headers=get_headers(), timeout=10)
                    mensajes = list(reversed(r_msg.json()))

                    for i, msg in enumerate(mensajes):
                        cuerpo = msg.get('body', '[Sin texto]') or '[Multimedia]'
                        ts = msg.get('timestamp', 0)
                        try: fecha = datetime.fromtimestamp(int(str(ts)[:10])).strftime('%H:%M:%S')
                        except: fecha = "S/F"
                        
                        es_mio = "📤 YO" if msg.get('fromMe') else "📥 CLIENTE"
                        
                        with st.expander(f"{es_mio} | {fecha} | {cuerpo[:40]}...", expanded=(i==0)):
                            st.markdown("### 🎯 Análisis de IDs")
                            c_from = msg.get('from')
                            c_part = msg.get('participant')
                            _data = msg.get('_data', {})
                            key = _data.get('key', {})
                            
                            c_remote = key.get('remoteJid')
                            c_remote_alt = key.get('remoteJidAlt')
                            c_part_alt = key.get('participantAlt')
                            
                            # Tabla comparativa extendida
                            cols = st.columns(3)
                            cols[0].metric("1. FROM (Origen)", c_from if c_from else "---")
                            cols[1].metric("2. PARTICIPANT", c_part if c_part else "---")
                            cols[2].metric("3. REMOTE JID", c_remote if c_remote else "---")
                            
                            st.markdown("---")
                            st.markdown("#### 💎 Tesoros Escondidos (LID Decoders)")
                            cols2 = st.columns(2)
                            cols2[0].metric("🔑 remoteJidAlt", c_remote_alt if c_remote_alt else "No hay")
                            cols2[1].metric("🔑 participantAlt", c_part_alt if c_part_alt else "No hay")

                            st.caption("JSON Completo:")
                            st.json(msg)

                except Exception as e:
                    st.error(f"Error al analizar: {e}")