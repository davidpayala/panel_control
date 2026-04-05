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
# Intentamos adivinar la URL local del webhook, o usa la variable si existe
WEBHOOK_INTERNAL_URL = os.getenv("WEBHOOK_URL", "https://kmlentes-webhook.up.railway.app/webhook")

def get_headers():
    h = {"Content-Type": "application/json"}
    if WAHA_KEY: h["X-Api-Key"] = WAHA_KEY
    return h

def render_diagnostico():
    st.title("🛠️ Centro de Diagnóstico")
    
    tab_logs, tab_inspector, tab_simulador, tab_respuestas = st.tabs(["📡 Logs Webhook", "🕵️ Inspector API", "🧪 Simulador (Test)", "🤖 Auto-Respuestas"])
    # ==========================================================================
    # PESTAÑA 1: LOGS DE BASE DE DATOS
    # ==========================================================================
    with tab_logs:
        c_btn, c_info = st.columns([20, 80])
        if c_btn.button("🔄 Actualizar", key="btn_logs"):
            st.rerun()
        
        c_info.info("Últimos eventos recibidos por el servidor.")

        try:
            with engine.connect() as conn:
                conn.commit()
                query = """
                    SELECT id, fecha, session_name, event_type, payload 
                    FROM webhook_logs 
                    ORDER BY id DESC LIMIT 20
                """
                df = pd.read_sql(text(query), conn)

            if df.empty:
                st.caption("No hay registros recientes.")
            else:
                for index, row in df.iterrows():
                    fecha_str = row['fecha'].strftime("%H:%M:%S")
                    evento = row['event_type']
                    
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

                    icono = "📩" if evento != 'message.ack' else "✔️"
                    
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
                    if r_msg.status_code == 200:
                        mensajes = list(reversed(r_msg.json()))
                        for msg in mensajes:
                            ts = msg.get('timestamp', 0)
                            fecha = datetime.fromtimestamp(int(str(ts)[:10])).strftime('%H:%M:%S')
                            cuerpo = msg.get('body') or "[Media]"
                            origen = "📤" if msg.get('fromMe') else "📥"
                            with st.expander(f"{origen} {fecha} - {cuerpo[:50]}"):
                                st.json(msg)
                    else:
                        st.error(f"Error API: {r_msg.text}")
                except Exception as e:
                    st.error(f"Error conexión: {e}")
            else:
                st.warning("No se especificó chat.")

    # ==========================================================================
    # PESTAÑA 3: SIMULADOR (TEST DE INYECCIÓN)
    # ==========================================================================
    with tab_simulador:
        st.markdown("### 🧪 Inyección de Eventos (Mock)")
        st.info("Envía un JSON directamente a tu Webhook local para probar si la lógica de `remoteJidAlt` y `LID` funciona correctamente.")

        # JSON de ejemplo conflictivo
        default_json = """{
  "id": "evt_simulado_001",
  "timestamp": 1770901602945,
  "event": "message",
  "session": "default",
  "payload": {
    "id": "false_214924743712877@lid_TEST_SIMULADO",
    "timestamp": 1770901602,
    "from": "214924743712877@lid",
    "fromMe": false,
    "body": "🔔 PRUEBA DE SIMULACIÓN: Este mensaje debe asignarse al 51963...",
    "_data": {
      "key": {
        "remoteJid": "214924743712877@lid",
        "remoteJidAlt": "51963168383@s.whatsapp.net",
        "fromMe": false
      },
      "notifyName": "Cliente Simulado"
    }
  }
}"""

        col_url, col_btn = st.columns([70, 30])
        target_url = col_url.text_input("URL del Webhook", value=WEBHOOK_INTERNAL_URL)
        
        json_input = st.text_area("JSON Payload", value=default_json, height=350)

        if col_btn.button("🚀 Disparar Webhook", type="primary", use_container_width=True):
            try:
                payload = json.loads(json_input)
                
                with st.spinner("Enviando petición POST..."):
                    # Enviamos el POST al webhook (flask)
                    response = requests.post(target_url, json=payload, timeout=10)
                
                if response.status_code == 200:
                    st.success(f"✅ Éxito: {response.status_code}")
                    st.json(response.json())
                    st.info("👉 Ahora ve a la pestaña 'Chats' y verifica si apareció el mensaje en el número `51963168383`.")
                else:
                    st.error(f"❌ Error del Servidor: {response.status_code}")
                    st.text(response.text)
                    
            except json.JSONDecodeError:
                st.error("❌ El texto no es un JSON válido.")
            except requests.exceptions.ConnectionError:
                st.error(f"❌ No se pudo conectar a `{target_url}`. ¿Está corriendo el servidor Flask?")
            except Exception as e:
                st.error(f"❌ Error inesperado: {str(e)}")

    # ==========================================================================
    # PESTAÑA 4: GESTIÓN DE RESPUESTAS AUTOMÁTICAS (ZOMBIES)
    # ==========================================================================
    with tab_respuestas:
        st.markdown("### 🤖 Gestión de Clientes Zombie")
        st.info("💡 **Tip:** Escribe la Frase Clave en minúsculas (ej: `precio`, `info`, `catalogo`). Si necesitas borrar una fila, selecciónala y presiona la tecla `Suprimir` (Delete).")
        
        try:
            with engine.connect() as conn:
                df_resp = pd.read_sql(text("SELECT id, frase_clave, respuesta_nivel_1, respuesta_nivel_2 FROM respuestas_automaticas ORDER BY id"), conn)
            
            # El data_editor permite modificar como si fuera Excel
            editado = st.data_editor(
                df_resp,
                num_rows="dynamic", # Permite añadir filas nuevas
                use_container_width=True,
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "frase_clave": st.column_config.TextColumn("Frase Clave (minúsculas)", required=True),
                    "respuesta_nivel_1": st.column_config.TextColumn("Nivel 1 (5 a 10 min)", required=True),
                    "respuesta_nivel_2": st.column_config.TextColumn("Nivel 2 (6 a 12 horas)", required=True)
                },
                key="editor_respuestas"
            )
            
            if st.button("💾 Guardar Cambios en Respuestas", type="primary"):
                with engine.begin() as conn:
                    # Limpiamos la tabla y la volvemos a llenar con lo que está en pantalla
                    conn.execute(text("DELETE FROM respuestas_automaticas"))
                    for _, row in editado.iterrows():
                        if pd.notna(row['frase_clave']) and str(row['frase_clave']).strip():
                            f_clave = str(row['frase_clave']).strip().lower()
                            r1 = str(row['respuesta_nivel_1']) if pd.notna(row['respuesta_nivel_1']) else ""
                            r2 = str(row['respuesta_nivel_2']) if pd.notna(row['respuesta_nivel_2']) else ""
                            
                            conn.execute(text("""
                                INSERT INTO respuestas_automaticas (frase_clave, respuesta_nivel_1, respuesta_nivel_2)
                                VALUES (:f, :r1, :r2)
                            """), {"f": f_clave, "r1": r1, "r2": r2})
                st.success("✅ Respuestas actualizadas correctamente en la Base de Datos.")
                st.rerun()
                
        except Exception as e:
            st.error(f"❌ Error al cargar/guardar la tabla: {e}")