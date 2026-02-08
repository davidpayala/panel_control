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
    st.warning("Esta es una herramienta técnica para ver cómo WAHA entrega los números.")

    # 1. Configuración de búsqueda
    c1, c2 = st.columns(2)
    session = c1.selectbox("Sesión WAHA", ["principal", "default"])
    chat_id = c2.text_input("ID de Chat específico (Opcional)", placeholder="Ej: 123456@g.us")
    
    limit = st.slider("Cantidad de mensajes a analizar", 1, 20, 5)

    if st.button("🔍 Analizar Últimos Mensajes", type="primary"):
        if not WAHA_URL:
            st.error("No se detectó WAHA_URL en las variables de entorno.")
            return

        with st.spinner(f"Consultando sesión '{session}'..."):
            try:
                # Construir URL
                if chat_id:
                    url = f"{WAHA_URL}/api/{session}/chats/{chat_id}/messages?limit={limit}"
                else:
                    # Traer chats recientes para sacar sus últimos mensajes
                    url = f"{WAHA_URL}/api/{session}/chats?limit={limit}&sortBy=messageTimestamp"

                r = requests.get(url, headers=get_headers(), timeout=10)
                
                if r.status_code != 200:
                    st.error(f"Error WAHA: {r.status_code} - {r.text}")
                    return

                data = r.json()
                
                # Preparar lista de mensajes
                mensajes = []
                if chat_id:
                    mensajes = reversed(data) # En chat específico vienen array directo
                else:
                    # En lista de chats, sacamos el 'lastMessage'
                    for chat in data:
                        if chat.get('lastMessage'):
                            mensajes.append(chat.get('lastMessage'))

                # MOSTRAR RESULTADOS
                st.success("Datos obtenidos. Revisa los candidatos abajo 👇")
                
                for i, msg in enumerate(mensajes):
                    with st.expander(f"Mensaje #{i+1}: {msg.get('body', '[Sin texto]')[:40]}...", expanded=True):
                        
                        # CANDIDATOS DE NÚMERO
                        st.markdown("### 🎯 Candidatos de Número")
                        c_from = msg.get('from')
                        c_part = msg.get('participant')
                        c_remo = msg.get('_data', {}).get('id', {}).get('remote')
                        
                        col1, col2, col3 = st.columns(3)
                        col1.metric("1. FROM", c_from if c_from else "Vacío")
                        col2.metric("2. PARTICIPANT", c_part if c_part else "Vacío")
                        col3.metric("3. REMOTE", c_remo if c_remo else "Vacío")

                        # RAW JSON (Para que yo pueda verlo)
                        st.caption("JSON Crudo (Copia esto si tienes dudas):")
                        st.json(msg)

            except Exception as e:
                st.error(f"Error ejecutando script: {e}")