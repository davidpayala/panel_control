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
import utils # Para cargar variables si es necesario

# Importar las vistas (Pestañas)
from views import ventas, compras, inventario, clientes, seguimiento, catalogo, facturacion, chat

# Cargar variables
load_dotenv()

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="K&M Ventas", layout="wide", page_icon="🛍️")

# --- LOGIN (Mantenlo aquí o muévelo a un auth.py si quieres) ---
def check_password():
    # ... (Tu código de login original va aquí) ...
    # Resumido para el ejemplo:
    if st.session_state.get("password_correct", False):
        return True
    # ... lógica de cookies ...
    return False # Si falla

# --- INICIO DE LA APP ---
if not check_password():
    st.stop()

# Inicializar variables de sesión globales
if 'carrito' not in st.session_state:
    st.session_state.carrito = []

# --- CALCULAR NOTIFICACIONES (CHAT) ---
with engine.connect() as conn:
    n_no_leidos = conn.execute(text(
        "SELECT COUNT(*) FROM mensajes WHERE leido = FALSE AND tipo = 'ENTRANTE'"
    )).scalar()

titulo_chat = f"💬 Chat ({n_no_leidos})" if n_no_leidos > 0 else "💬 Chat"

# --- MENÚ PRINCIPAL ---
st.title("🛒 KM - Punto de Venta")
st.markdown("---")

# Definimos las pestañas
pestanas = st.tabs([
    "🛒 VENTA (POS)", 
    "📦 Compras", 
    "🔎 Inventario", 
    "👤 Clientes", 
    "📆 Seguimiento", 
    "🔧 Catálogo",
    "💰 Facturación",
    titulo_chat
])

# --- CARGAMOS CADA PESTAÑA DESDE SU ARCHIVO ---
with pestanas[0]:
    ventas.render_ventas()

with pestanas[1]:
    compras.render_compras()

with pestanas[2]:
    inventario.render_inventario()

with pestanas[3]:
    clientes.render_clientes()

with pestanas[4]:
    seguimiento.render_seguimiento()

with pestanas[5]:
    catalogo.render_catalogo()

with pestanas[6]:
    facturacion.render_facturacion()

with pestanas[7]:
    chat.render_chat()

# (Opcional) Guardar cambios globales o funciones de cierre