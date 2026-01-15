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


# ESTO ACTUALIZA TU BASE DE DATOS AUTOMÁTICAMENTE
# --- EN APP.PY (Bloque de Migración) ---
try:
    with engine.connect() as conn:
        # 1. Actualizar tabla Clientes con tus nuevos campos
        conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS nombre_corto TEXT;"))
        conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS medio_contacto TEXT DEFAULT 'WhatsApp';"))
        conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS codigo_contacto TEXT;"))
        conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS fecha_seguimiento DATE;"))
        conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS google_id TEXT;"))
        
        # 2. Crear tabla Direcciones (Si no existe)
        # Relacionada por 'telefono' del cliente
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Direcciones (
                id SERIAL PRIMARY KEY,
                telefono TEXT, 
                direccion TEXT,
                tipo_envio TEXT, -- 'AGENCIA' o 'MOTO'
                departamento TEXT,
                provincia TEXT,
                distrito TEXT,
                referencia TEXT
            );
        """))
        conn.commit()
    print("✅ Base de datos actualizada con nuevas reglas.")
except Exception as e:
    print(f"Nota DB: {e}")
# ------------------------------------------------

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

# --- CALCULAR NOTIFICACIONES (Igual que antes) ---
try:
    with engine.connect() as conn:
        n_no_leidos = conn.execute(text(
            "SELECT COUNT(*) FROM mensajes WHERE leido = FALSE AND tipo = 'ENTRANTE'"
        )).scalar()
except:
    n_no_leidos = 0

texto_dinamico_chat = f"💬 Chat ({n_no_leidos})" if n_no_leidos > 0 else "💬 Chat"

# --- BARRA LATERAL CON ÍNDICE FORZADO ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=100)
    st.title("Menú K&M")
    
    # 1. Lista ESTÁTICA de opciones (las llaves internas)
    OPCIONES_MENU = [
        "VENTA", 
        "COMPRAS", 
        "INVENTARIO", 
        "CLIENTES", 
        "SEGUIMIENTO", 
        "CATALOGO",
        "FACTURACION",
        "CHAT" 
    ]

    # 2. Inicializar la variable de control en Session State si no existe
    if "indice_menu" not in st.session_state:
        st.session_state.indice_menu = 0

    # 3. Función callback que se ejecuta AL HACER CLIC
    def actualizar_indice():
        # Buscamos qué opción seleccionó el usuario en el radio button
        opcion_elegida = st.session_state.radio_navegacion
        # Guardamos su número (índice) en la memoria segura
        st.session_state.indice_menu = OPCIONES_MENU.index(opcion_elegida)

    # 4. Función de formateo visual
    def formatear_menu(opcion):
        mapeo = {
            "VENTA": "🛒 Venta (POS)",
            "COMPRAS": "📦 Compras",
            "INVENTARIO": "🔎 Inventario",
            "CLIENTES": "👤 Clientes",
            "SEGUIMIENTO": "📆 Seguimiento",
            "CATALOGO": "🔧 Catálogo",
            "FACTURACION": "💰 Facturación",
            "CHAT": texto_dinamico_chat # <--- El texto cambia aquí
        }
        return mapeo.get(opcion, opcion)

    # 5. EL WIDGET (Aquí está el truco: index=...)
    seleccion_interna = st.radio(
        "Ir a:", 
        OPCIONES_MENU,
        index=st.session_state.indice_menu, # <--- OBLIGAMOS A MANTENER LA POSICIÓN
        format_func=formatear_menu,
        key="radio_navegacion",
        on_change=actualizar_indice # <--- Guardamos el cambio inmediatamente
    )
    
    st.divider()
    st.caption("Sistema v2.0 - WAHA")

# --- RENDERIZADO (Usamos la variable seleccion_interna) ---
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