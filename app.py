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
from views import ventas, compras, inventario, clientes, seguimiento, catalogo, facturacion, chats, campanas

# Cargar variables
load_dotenv()

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="K&M Ventas", layout="wide", page_icon="🛍️")

# ==========================================
# LIMPIEZA Y MIGRACIÓN BLINDADA
# ==========================================
def ejecutar_migraciones():
    try:
        with engine.connect() as conn:
            # --- PASO 0: ELIMINAR DUPLICADOS PREVIOS (LA CURA AL ERROR) ---
            # Esta consulta mágica borra los duplicados "sucios" antes de que causen error.
            # Mantiene el registro más antiguo (ORDER BY id_cliente ASC)
            print("🧹 Paso 0: Eliminando duplicados conflictivos...")
            conn.execute(text("""
                DELETE FROM Clientes
                WHERE id_cliente IN (
                    SELECT id_cliente
                    FROM (
                        SELECT id_cliente,
                               ROW_NUMBER() OVER (
                                   -- Agrupamos por cómo se vería el número limpio
                                   PARTITION BY REPLACE(REPLACE(REPLACE(telefono, ' ', ''), '-', ''), '+', '')
                                   ORDER BY id_cliente ASC
                               ) as rn
                        FROM Clientes
                    ) t
                    WHERE t.rn > 1
                );
            """))

            # --- FASE 1: LIMPIEZA DE CARACTERES ---
            print("🧹 Paso 1: Limpiando espacios y símbolos...")
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, ' ', '')"))
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, '-', '')"))
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, '+', '')"))
            
            # Limpiar también mensajes
            conn.execute(text("UPDATE mensajes SET telefono = REPLACE(telefono, ' ', '')"))
            conn.execute(text("UPDATE mensajes SET telefono = REPLACE(telefono, '-', '')"))
            conn.execute(text("UPDATE mensajes SET telefono = REPLACE(telefono, '+', '')"))

            # --- FASE 2: FORMATO PERÚ (51) ---
            # Si después de limpiar quedan 9 dígitos, le pegamos el 51
            print("🇵🇪 Paso 2: Estandarizando formato Perú...")
            
            # Primero borramos posibles duplicados que se generarían al agregar 51
            # (Ej: Si tienes '999...' y '51999...', borramos el '999...' antes de actualizarlo)
            conn.execute(text("""
                DELETE FROM Clientes
                WHERE LENGTH(telefono) = 9 
                AND ('51' || telefono) IN (SELECT telefono FROM Clientes);
            """))
            
            # Ahora sí actualizamos seguros
            conn.execute(text("UPDATE Clientes SET telefono = '51' || telefono WHERE LENGTH(telefono) = 9"))
            conn.execute(text("UPDATE mensajes SET telefono = '51' || telefono WHERE LENGTH(telefono) = 9"))
            
            # --- FASE 3: ESTRUCTURA ---
            print("🏗️ Paso 3: Verificando columnas...")
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS nombre_corto TEXT;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS medio_contacto TEXT DEFAULT 'WhatsApp';"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS codigo_contacto TEXT;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS fecha_seguimiento DATE;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS google_id TEXT;"))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS Direcciones (
                    id_direccion SERIAL PRIMARY KEY,
                    id_cliente INTEGER,
                    telefono TEXT, 
                    direccion_texto TEXT,
                    tipo_envio TEXT,
                    departamento TEXT,
                    provincia TEXT,
                    distrito TEXT,
                    referencia TEXT,
                    nombre_receptor TEXT,
                    dni_receptor TEXT,
                    telefono_receptor TEXT,
                    agencia_nombre TEXT,
                    sede_entrega TEXT,
                    activo BOOLEAN DEFAULT TRUE
                );
            """))
            conn.commit()
            print("✅ ¡BASE DE DATOS OPTIMIZADA AL 100%!")
            
    except Exception as e:
        print(f"⚠️ Nota de Migración: {e}")

# Ejecutamos limpieza al inicio
ejecutar_migraciones()

# --- FUNCIÓN PRINCIPAL ---
def main():
    if 'carrito' not in st.session_state:
        st.session_state.carrito = []

    # Notificaciones
    try:
        with engine.connect() as conn:
            n_no_leidos = conn.execute(text(
                "SELECT COUNT(*) FROM mensajes WHERE leido = FALSE AND tipo = 'ENTRANTE'"
            )).scalar()
    except:
        n_no_leidos = 0

    texto_dinamico_chat = f"💬 Chat ({n_no_leidos})" if n_no_leidos > 0 else "💬 Chat"

    # --- BARRA LATERAL ---
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=100)
        st.title("Menú K&M")
        
        OPCIONES_MENU = [
            "VENTA", "COMPRAS", "INVENTARIO", "CLIENTES", 
            "SEGUIMIENTO", "CATALOGO", "FACTURACION", "CHAT", "CAMPANAS"
        ]

        if "indice_menu" not in st.session_state:
            st.session_state.indice_menu = 0

        def actualizar_indice():
            try:
                st.session_state.indice_menu = OPCIONES_MENU.index(st.session_state.radio_navegacion)
            except:
                st.session_state.indice_menu = 0

        def formatear_menu(opcion):
            mapeo = {
                "VENTA": "🛒 Venta (POS)", "COMPRAS": "📦 Compras", 
                "INVENTARIO": "🔎 Inventario", "CLIENTES": "👤 Clientes",
                "SEGUIMIENTO": "📆 Seguimiento", "CATALOGO": "🔧 Catálogo",
                "FACTURACION": "💰 Facturación", "CHAT": texto_dinamico_chat,
                "CAMPANAS": "📢 Campañas"
            }
            return mapeo.get(opcion, opcion)

        seleccion_interna = st.radio(
            "Ir a:", OPCIONES_MENU,
            index=st.session_state.indice_menu,
            format_func=formatear_menu,
            key="radio_navegacion",
            on_change=actualizar_indice
        )
        st.divider()
        st.caption("Sistema v2.3 - Clean DB")

    # --- RENDERIZADO ---
    st.title(f" {formatear_menu(seleccion_interna)}") 
    st.markdown("---")

    if seleccion_interna == "VENTA": ventas.render_ventas()
    elif seleccion_interna == "COMPRAS": compras.render_compras()
    elif seleccion_interna == "INVENTARIO": inventario.render_inventario()
    elif seleccion_interna == "CLIENTES": clientes.render_clientes()
    elif seleccion_interna == "SEGUIMIENTO": seguimiento.render_seguimiento()
    elif seleccion_interna == "CATALOGO": catalogo.render_catalogo()
    elif seleccion_interna == "FACTURACION": facturacion.render_facturacion()
    elif seleccion_interna == "CHAT": chats.render_chat()
    elif seleccion_interna == "CAMPANAS": campanas.render_campanas()

if __name__ == "__main__":
    main()