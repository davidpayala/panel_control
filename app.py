from dotenv import load_dotenv
load_dotenv()
import streamlit as st
import extra_streamlit_components as stx
import os
import time
import pandas as pd
from datetime import datetime
from sqlalchemy import text

# Importar configuración y módulos
from database import engine
import utils 

# Importar las vistas (¡ELIMINAMOS FACTURACION!)
from views import ventas, compras, productos, clientes, seguimiento, catalogo, chats, campanas, diagnostico, opciones, estadisticas 

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="K&M Ventas", layout="wide", page_icon="🛍️")

def render_login():
    st.title("🔐 Acceso al Sistema")
    with st.form("login_form"):
        user = st.text_input("Usuario")
        pwd = st.text_input("Contraseña", type="password")
        submit = st.form_submit_button("Ingresar")

        if submit:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT rol, modulos FROM Usuarios WHERE usuario=:u AND password=:p"), 
                                   {"u": user, "p": pwd}).fetchone()
                if res:
                    st.session_state['usuario'] = user
                    st.session_state['rol'] = res.rol
                    st.session_state['modulos'] = res.modulos if isinstance(res.modulos, list) else []
                    st.rerun()
                else:
                    st.error("Credenciales incorrectas")

# --- FUNCIÓN PRINCIPAL ---
def main():
    # 1. VERIFICAR SESIÓN PRIMERO
    if 'usuario' not in st.session_state:
        render_login()
        return  

    # 2. INICIALIZAR CARRITO
    if 'carrito' not in st.session_state:
        st.session_state.carrito = []

    # 3. CONTADOR DE CHATS NO LEÍDOS
    try:
        with engine.connect() as conn:
            n_no_leidos = conn.execute(text(
                "SELECT COUNT(*) FROM mensajes WHERE leido = FALSE AND tipo = 'ENTRANTE'"
            )).scalar()
    except:
        n_no_leidos = 0

    texto_dinamico_chat = f"💬 Chat ({n_no_leidos})" if n_no_leidos > 0 else "💬 Chat"

# 4. BARRA LATERAL Y MENÚ
    with st.sidebar:
        st.write(f"👤 Bienvenido, **{st.session_state['usuario']}** ({st.session_state['rol']})")
        if st.button("🚪 Cerrar Sesión"):
            st.session_state.clear()
            st.rerun()
            
        st.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=100)
        
        # ==========================================
        # 🚨 MONITOREO DE WHATSAPP EN TIEMPO REAL
        # ==========================================
        import requests
        
        # Definir ambas instancias de WAHA
        waha_url_3000 = os.getenv("WAHA_URL", "http://localhost:3000")
        waha_url_3001 = os.getenv("WAHA_URL_ESTADOS", "http://localhost:3001")
        waha_key = os.getenv("WAHA_KEY", "")
        
        headers = {"Accept": "application/json"}
        if waha_key:
            headers["X-Api-Key"] = waha_key

        instancias_waha = [
            {"nombre": "Principal (3000)", "url": waha_url_3000},
            {"nombre": "Estados (3001)", "url": waha_url_3001}
        ]

        # Monitorear cada instancia de forma independiente
        for instancia in instancias_waha:
            try:
                res = requests.get(f"{instancia['url']}/api/sessions?all=true", headers=headers, timeout=2)
                
                if res.status_code == 200:
                    sesiones = res.json()
                    for sesion in sesiones:
                        estado = sesion.get('status')
                        nombre_sesion = sesion.get('name')
                        
                        if estado == "SCAN_QR_CODE":
                            st.error(f"🚨 **QR REQUERIDO: {instancia['nombre']}**\n\nLa sesión **{nombre_sesion}** se desvinculó. Escanéalo en Opciones o WAHA para reconectar.")
                        elif estado in ["FAILED", "STOPPED"]:
                            st.error(f"⚠️ **FALLO DE SESIÓN: {instancia['nombre']}**\n\nLa sesión **{nombre_sesion}** está colapsada ({estado}).")
                else:
                    st.error(f"🚨 **ALERTA CRÍTICA: {instancia['nombre']}**\n\nLa API no responde (Status {res.status_code}).")
            except requests.exceptions.RequestException:
                # Si lanza excepción, el contenedor Docker de ese puerto está apagado
                puerto = instancia['url'].split(':')[-1]
                st.error(f"🚨 **CONTENEDOR CAÍDO: {instancia['nombre']}**\n\nEl contenedor WAHA del puerto {puerto} está apagado o inaccesible.")
            
        # ==========================================
        # 🔔 BANDEJA DE AVISOS DEL SERVIDOR (INBOX)
        # ==========================================
        try:
            with engine.connect() as conn:
                # Buscamos TODAS las alertas críticas de las últimas 24 horas
                alertas = conn.execute(text("""
                    SELECT id, fecha, payload FROM webhook_logs 
                    WHERE event_type = 'ALERTA_CRITICA' 
                    AND fecha > (NOW() - INTERVAL '24 hours') 
                    ORDER BY id DESC
                """)).fetchall()
                
                if alertas:
                    # Usamos un expander para no saturar visualmente si hay muchas
                    with st.expander(f"🚨 Alertas del Servidor ({len(alertas)})", expanded=True):
                        import json
                        for alerta in alertas:
                            try:
                                data_alerta = json.loads(alerta.payload)
                                mensaje = data_alerta.get('mensaje', 'Aviso del sistema')
                            except:
                                mensaje = "Fallo reportado"
                                
                            # Formatear la hora (Ej: 24/10 - 03:15 AM)
                            fecha_f = alerta.fecha.strftime("%d/%m - %I:%M %p") if alerta.fecha else ""
                            
                            st.markdown(f"**🗓️ {fecha_f}**")
                            st.warning(mensaje)
                            
                            # Botón de descarte (con Key única usando el ID de la base de datos)
                            if st.button("✔️ Descartar", key=f"ok_{alerta.id}", use_container_width=True):
                                with engine.begin() as tx:
                                    # Al actualizar a 'ALERTA_RESUELTA', desaparece automáticamente del SELECT de arriba
                                    tx.execute(text(
                                        "UPDATE webhook_logs SET event_type = 'ALERTA_RESUELTA' WHERE id = :id"
                                    ), {"id": alerta.id})
                                st.rerun()
                                
                            st.divider() # Línea separadora entre alertas
        except Exception:
            pass

        # ==========================================
        # 🔄 RADAR DE ACTUALIZACIONES DE WAHA
        # ==========================================
        @st.cache_data(ttl=43200) # Revisa silenciosamente solo 1 vez cada 12 horas
        def verificar_actualizacion_waha(url, key):
            import requests
            try:
                # 1. Consultar la última versión oficial en GitHub
                gh_res = requests.get("https://api.github.com/repos/devlikeapro/waha/releases/latest", timeout=3)
                if gh_res.status_code != 200:
                    return None
                ultima_version = gh_res.json().get("tag_name", "").replace("v", "")
                
                # 2. Consultar la versión instalada en tu servidor
                headers = {"Accept": "application/json"}
                if key: headers["X-Api-Key"] = key
                
                loc_res = requests.get(f"{url}/api/environment", headers=headers, timeout=2)
                if loc_res.status_code == 200:
                    version_local = loc_res.json().get("version", "").replace("v", "")
                    
                    # 3. Comparar versiones
                    if version_local and ultima_version and version_local != ultima_version:
                        return f"⚠️ **Actualización Disponible**\n\nTienes WAHA `{version_local}` y la última es `{ultima_version}`.\n\nPara actualizar, lanza en tu terminal SSH:\n`sudo docker-compose pull`\n`sudo docker-compose up -d`"
                
                return None
            except:
                return None # Si no hay internet o falla, ignorar silenciosamente

        alerta_update = verificar_actualizacion_waha(waha_url, waha_key)
        if alerta_update:
            st.warning(alerta_update)
        # ==========================================

        st.title("Menú K&M")
        
        # --- ORDEN ACTUALIZADO Y SIN FACTURACIÓN ---
        OPCIONES_BASE = [
            "PRODUCTOS", "VENTA", "COMPRAS", "CLIENTES",
            "SEGUIMIENTO", "CATALOGO", "CHAT", "CAMPANAS", "DIAGNOSTICO", "ESTADISTICAS"
        ]

        # Lógica de Roles
        if st.session_state['rol'] == 'Admin':
            OPCIONES_MENU = OPCIONES_BASE + ["OPCIONES"] 
        else:
            OPCIONES_MENU = [opc for opc in OPCIONES_BASE if opc in st.session_state['modulos']]

        if "indice_menu" not in st.session_state:
            st.session_state.indice_menu = 0

        # Función de formato ajustada
        def formatear_menu(opcion):
            mapeo = {
                "PRODUCTOS": "📦 Productos", "VENTA": "🛒 Venta (POS)", "COMPRAS": "📦 Compras", 
                "CLIENTES": "👤 Clientes", "SEGUIMIENTO": "📆 Seguimiento", "CATALOGO": "🔧 Catálogo",
                "CHAT": texto_dinamico_chat, "CAMPANAS": "📢 Campañas", "DIAGNOSTICO": "🕵️ Diagnóstico",
                "ESTADISTICAS": "📊 Estadísticas", "OPCIONES": "⚙️ Opciones"
            }
            return mapeo.get(opcion, opcion)

        if st.session_state.indice_menu >= len(OPCIONES_MENU):
            st.session_state.indice_menu = 0

        seleccion_interna = st.radio(
            "Ir a:", 
            OPCIONES_MENU,
            index=st.session_state.indice_menu,
            format_func=formatear_menu
        )
        
        if seleccion_interna in OPCIONES_MENU:
            st.session_state.indice_menu = OPCIONES_MENU.index(seleccion_interna)

        st.divider()
        st.caption("Sistema v3.0 - Control de Acceso")

    # --- RENDERIZADO DE VISTAS ---
    st.title(f"{formatear_menu(seleccion_interna)}") 
    st.markdown("---")

    if seleccion_interna == "PRODUCTOS": productos.vista_productos()
    elif seleccion_interna == "VENTA": ventas.render_ventas()
    elif seleccion_interna == "COMPRAS": compras.render_compras()
    elif seleccion_interna == "CLIENTES": clientes.render_clientes()
    elif seleccion_interna == "SEGUIMIENTO": seguimiento.render_seguimiento()
    elif seleccion_interna == "CATALOGO": catalogo.render_catalogo()
    elif seleccion_interna == "CHAT": chats.render_chat()
    elif seleccion_interna == "CAMPANAS": campanas.render_campanas()
    elif seleccion_interna == "DIAGNOSTICO": diagnostico.render_diagnostico()
    elif seleccion_interna == "ESTADISTICAS": estadisticas.render_estadisticas()
    elif seleccion_interna == "OPCIONES": opciones.render_opciones()

if __name__ == "__main__":
    main()