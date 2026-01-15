import streamlit as st
import extra_streamlit_components as stx
import os
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text

# Importar configuración y módulos
from database import engine
import utils 

# Importar las vistas
from views import ventas, compras, inventario, clientes, seguimiento, catalogo, facturacion, chats

# Cargar variables
load_dotenv()

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="K&M Ventas", layout="wide", page_icon="🛍️")

# --- LOGIN (Marcador de posición) ---
def check_password():
    if st.session_state.get("password_correct", False):
        return True
    return False 

# --- INICIO DE LA APP ---
# if not check_password():
#    st.stop()

# Inicializar variables de sesión globales
if 'carrito' not in st.session_state:
    st.session_state.carrito = []

# --- CALCULAR NOTIFICACIONES (CHAT) ---
try:
    with engine.connect() as conn:
        n_no_leidos = conn.execute(text(
            "SELECT COUNT(*) FROM mensajes WHERE leido = FALSE AND tipo = 'ENTRANTE'"
        )).scalar()
except:
    n_no_leidos = 0

# Calculamos el texto bonito, PERO NO LO USAREMOS COMO CLAVE
texto_dinamico_chat = f"💬 Chat ({n_no_leidos})" if n_no_leidos > 0 else "💬 Chat"

# --- BARRA LATERAL (SIDEBAR) ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=100)
    st.title("Menú K&M")
    
    # 1. Definimos una lista de claves ESTÁTICAS (nunca cambian)
    OPCIONES_MENU = [
        "VENTA", 
        "COMPRAS", 
        "INVENTARIO", 
        "CLIENTES", 
        "SEGUIMIENTO", 
        "CATALOGO",
        "FACTURACION",
        "CHAT" # Esta clave interna nunca cambiará, aunque cambie el número de mensajes
    ]

    # 2. Función para "maquillar" las claves y que se vean bonitas
    def formatear_menu(opcion):
        mapeo = {
            "VENTA": "🛒 Venta (POS)",
            "COMPRAS": "📦 Compras",
            "INVENTARIO": "🔎 Inventario",
            "CLIENTES": "👤 Clientes",
            "SEGUIMIENTO": "📆 Seguimiento",
            "CATALOGO": "🔧 Catálogo",
            "FACTURACION": "💰 Facturación",
            "CHAT": texto_dinamico_chat # <--- AQUÍ USAMOS EL TEXTO DINÁMICO
        }
        return mapeo.get(opcion, opcion)

    # 3. El Radio Button usa las claves estáticas
    seleccion_interna = st.radio(
        "Ir a:", 
        OPCIONES_MENU,
        format_func=formatear_menu, # <--- ESTO ES LA MAGIA
        key="navegacion_principal" 
    )
    
    st.divider()
    st.caption("Sistema v2.0 - WAHA")

# --- RENDERIZADO DE PÁGINAS (Usamos las claves estáticas) ---

# Título dinámico en la parte superior (opcional)
titulo_visual = formatear_menu(seleccion_interna).split('(')[0]
st.title(f"🛒 KM - {titulo_visual}") 
st.markdown("---")

if seleccion_interna == "VENTA":
    ventas.render_ventas()

elif seleccion_interna == "COMPRAS":
    compras.render_compras()

elif seleccion_interna == "INVENTARIO":
    inventario.render_inventario()

elif seleccion_interna == "CLIENTES":
    clientes.render_clientes()

elif seleccion_interna == "SEGUIMIENTO":
    seguimiento.render_seguimiento()

elif seleccion_interna == "CATALOGO":
    catalogo.render_catalogo()

elif seleccion_interna == "FACTURACION":
    facturacion.render_facturacion()

elif seleccion_interna == "CHAT":
    chats.render_chat()