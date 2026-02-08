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
    st.title("🕵️ Inspector de Mensajes (Diagnóstico)")
    st.warning("Herramienta para detectar el formato correcto del número de teléfono.")

    # 1. Configuración de búsqueda
    c1, c2 = st.columns(2)
    session = c1.selectbox("Sesión WAHA", ["principal", "default"])
    chat_id = c2.text_input("ID de Chat específico (Opcional)", placeholder="Ej: 51999...@c.us")
    
    limit = st.slider("Cantidad de mensajes a analizar", 1, 20, 5)

    if st.button("🔍 Analizar Últimos Mensajes", type="primary"):
        if not WAHA_URL:
            st.error("No se detectó WAHA_URL en las variables de entorno.")
            return

        with st.spinner(f"Consultando sesión '{session}'..."):
            try:
                # Construir URL
                if chat_id:
                    # Si buscamos un chat específico
                    url = f"{WAHA_URL}/api/{session}/chats/{chat_id}/messages?limit={limit}"
                else:
                    # CORRECCIÓN AQUÍ: Usamos 'conversationTimestamp' que es el válido
                    url = f"{WAHA_URL}/api/{session}/chats?limit={limit}&sortBy=conversationTimestamp"

                r = requests.get(url, headers=get_headers(), timeout=10)
                
                if r.status_code != 200:
                    st.error(f"Error WAHA: {r.status_code} - {r.text}")
                    return

                data = r.json()
                
                # Preparar lista de mensajes
                mensajes = []
                if chat_id:
                    mensajes = list(reversed(data)) # En chat específico vienen array directo
                else:
                    # En lista de chats, sacamos el 'lastMessage'
                    for chat in data:
                        if chat.get('lastMessage'):
                            mensajes.append(chat.get('lastMessage'))

                # MOSTRAR RESULTADOS
                if not mensajes:
                    st.info("No se encontraron mensajes recientes.")
                    return

                st.success(f"Se analizaron {len(mensajes)} mensajes. Revisa los candidatos abajo 👇")
                
                for i, msg in enumerate(mensajes):
                    cuerpo = msg.get('body', '[Sin texto]') or '[Multimedia]'
                    fecha = msg.get('timestamp', '---')
                    
                    with st.expander(f"Mensaje #{i+1} | {fecha} | {cuerpo[:40]}...", expanded=True):
                        
                        # CANDIDATOS DE NÚMERO
                        st.markdown("### 🎯 Candidatos de Número")
                        c_from = msg.get('from')
                        c_part = msg.get('participant')
                        c_remo = msg.get('_data', {}).get('id', {}).get('remote')
                        
                        col1, col2, col3 = st.columns(3)
                        col1.metric("1. FROM", c_from if c_from else "Vacío")
                        col2.metric("2. PARTICIPANT", c_part if c_part else "Vacío")
                        col3.metric("3. REMOTE", c_remo if c_remo else "Vacío")

                        # Alerta de Grupo
                        if c_from and '@g.us' in c_from:
                            st.warning("⚠️ Este mensaje viene de un GRUPO. El número real es el 'PARTICIPANT'.")

                        # RAW JSON (Para que yo pueda verlo)
                        st.caption("JSON Crudo (Copia esto si tienes dudas):")
                        st.json(msg)

            except Exception as e:
                st.error(f"Error ejecutando script: {e}")