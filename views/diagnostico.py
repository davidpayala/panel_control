import streamlit as st
import requests
import os
import json
from datetime import datetime

# Configuración
WAHA_URL = os.getenv("WAHA_URL")
WAHA_KEY = os.getenv("WAHA_KEY")

def get_headers():
    h = {"Content-Type": "application/json"}
    if WAHA_KEY: h["X-Api-Key"] = WAHA_KEY
    return h

def render_diagnostico():
    st.title("🕵️ Inspector de Mensajes (V2)")
    st.info("Esta herramienta descarga los mensajes reales del chat más reciente para verificar el formato del número.")

    # 1. Configuración
    c1, c2 = st.columns(2)
    session = c1.selectbox("Sesión WAHA", ["principal", "default"])
    chat_id_manual = c2.text_input("ID de Chat (Opcional)", placeholder="Dejar vacío para automático")
    
    if st.button("🔍 Analizar Mensajes Reales", type="primary"):
        if not WAHA_URL:
            st.error("Falta WAHA_URL")
            return

        chat_a_analizar = chat_id_manual

        # PASO 1: Si no hay ID manual, buscamos el chat más reciente
        if not chat_a_analizar:
            with st.spinner(f"Buscando chats activos en '{session}'..."):
                try:
                    # Pedimos chats sin ordenar para evitar errores 400, WAHA suele traer los recientes
                    url_chats = f"{WAHA_URL}/api/{session}/chats?limit=15"
                    r = requests.get(url_chats, headers=get_headers(), timeout=10)
                    
                    if r.status_code != 200:
                        st.error(f"Error obteniendo chats: {r.status_code} - {r.text}")
                        return
                    
                    chats_data = r.json()
                    
                    if not chats_data:
                        st.warning(f"La sesión '{session}' no devolvió ningún chat. Prueba cambiar de sesión.")
                        return

                    # Tomamos el primer chat válido que no sea de estado
                    for c in chats_data:
                        cid = c.get('id', '')
                        if 'status@broadcast' not in cid:
                            chat_a_analizar = cid
                            st.toast(f"Analizando chat automático: {cid}", icon="🤖")
                            break
                    
                    if not chat_a_analizar:
                        st.warning("No se encontraron chats válidos (solo estados).")
                        return

                except Exception as e:
                    st.error(f"Error buscando chats: {e}")
                    return

        # PASO 2: Descargar mensajes de ese chat específico
        st.write(f"📂 **Extrayendo mensajes de:** `{chat_a_analizar}`")
        
        try:
            url_msgs = f"{WAHA_URL}/api/{session}/chats/{chat_a_analizar}/messages?limit=10"
            r_msg = requests.get(url_msgs, headers=get_headers(), timeout=10)
            
            if r_msg.status_code != 200:
                st.error(f"Error descargando mensajes: {r_msg.text}")
                return
            
            mensajes = r_msg.json()
            
            # Ordenamos para ver el más reciente arriba
            mensajes = list(reversed(mensajes))

            if not mensajes:
                st.warning("El chat existe pero no tiene mensajes recientes.")
                return

            # PASO 3: Mostrar Análisis
            st.success("¡Mensajes encontrados! Revisa los candidatos abajo 👇")

            for i, msg in enumerate(mensajes):
                cuerpo = msg.get('body', '[Sin texto]') or '[Multimedia]'
                ts = msg.get('timestamp', 0)
                try:
                    fecha = datetime.fromtimestamp(int(str(ts)[:10])).strftime('%H:%M:%S')
                except: fecha = "S/F"
                
                es_mio = "📤 YO" if msg.get('fromMe') else "📥 CLIENTE"
                
                with st.expander(f"{es_mio} | {fecha} | {cuerpo[:40]}...", expanded=(i==0)):
                    
                    st.markdown("### 🎯 ¿Cuál es el número real?")
                    
                    c_from = msg.get('from')
                    c_part = msg.get('participant')
                    c_remote = msg.get('_data', {}).get('id', {}).get('remote')
                    
                    # Tabla comparativa
                    cols = st.columns(3)
                    cols[0].metric("1. FROM", c_from if c_from else "---")
                    cols[1].metric("2. PARTICIPANT", c_part if c_part else "---")
                    cols[2].metric("3. REMOTE", c_remote if c_remote else "---")

                    if c_from and '@g.us' in c_from:
                        st.info("💡 Es un GRUPO: El número del cliente suele ser el **PARTICIPANT**.")
                    elif '@lid' in str(c_from) or '@lid' in str(c_remote):
                        st.warning("⚠️ Detectado ID Privado (LID). WAHA necesita resolverlo.")

                    st.caption("JSON Completo:")
                    st.json(msg)

        except Exception as e:
            st.error(f"Error crítico: {e}")