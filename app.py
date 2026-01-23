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
# LIMPIEZA Y MIGRACIÓN (CORREGIDA FK)
# ==========================================
def ejecutar_migraciones():
    try:
        with engine.connect() as conn:
            print("🧹 Iniciando Mantenimiento Inteligente de DB...")

            # --- PASO 0: UNIFICACIÓN DE DATOS (SOLUCIÓN AL ERROR FK) ---
            # Creamos una tabla temporal para saber quién es el duplicado y quién el original
            conn.execute(text("""
                CREATE TEMP TABLE IF NOT EXISTS Temp_Unificacion AS
                SELECT 
                    id_cliente as id_eliminar,
                    FIRST_VALUE(id_cliente) OVER (
                        PARTITION BY REPLACE(REPLACE(REPLACE(telefono, ' ', ''), '-', ''), '+', '')
                        ORDER BY id_cliente ASC
                    ) as id_conservar
                FROM Clientes;
            """))

            # 1. MOVER DIRECCIONES (Esto arregla tu error actual)
            # Pasamos las direcciones del cliente que se va a borrar al que se queda
            conn.execute(text("""
                UPDATE Direcciones 
                SET id_cliente = t.id_conservar
                FROM Temp_Unificacion t
                WHERE Direcciones.id_cliente = t.id_eliminar 
                AND t.id_eliminar != t.id_conservar;
            """))

            # 2. MOVER MENSAJES (Para no perder historial)
            conn.execute(text("""
                UPDATE mensajes 
                SET id_cliente = t.id_conservar
                FROM Temp_Unificacion t
                WHERE mensajes.id_cliente = t.id_eliminar 
                AND t.id_eliminar != t.id_conservar;
            """))
            
            # 3. MOVER VENTAS (Si existen)
            # Nota: Usamos un bloque try/catch en SQL por si la tabla Ventas no existe aun
            try:
                conn.execute(text("""
                    UPDATE Ventas 
                    SET id_cliente = t.id_conservar
                    FROM Temp_Unificacion t
                    WHERE Ventas.id_cliente = t.id_eliminar 
                    AND t.id_eliminar != t.id_conservar;
                """))
            except:
                pass 

            # 4. AHORA SÍ: BORRAR DUPLICADOS
            # Como ya no tienen hijos (direcciones/mensajes), se pueden borrar sin error
            print("🗑️ Eliminando duplicados vacíos...")
            conn.execute(text("""
                DELETE FROM Clientes
                WHERE id_cliente IN (
                    SELECT id_eliminar FROM Temp_Unificacion WHERE id_eliminar != id_conservar
                );
            """))
            
            # Limpiamos la tabla temporal
            conn.execute(text("DROP TABLE IF EXISTS Temp_Unificacion;"))

            # --- PASO 1: LIMPIEZA DE CARACTERES ---
            print("✨ Limpiando formatos...")
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, ' ', '')"))
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, '-', '')"))
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, '+', '')"))
            
            conn.execute(text("UPDATE mensajes SET telefono = REPLACE(telefono, ' ', '')"))
            conn.execute(text("UPDATE mensajes SET telefono = REPLACE(telefono, '-', '')"))
            conn.execute(text("UPDATE mensajes SET telefono = REPLACE(telefono, '+', '')"))

            # --- PASO 2: FORMATO PERÚ (51) ---
            # Prevenir duplicados que se generen al agregar 51
            conn.execute(text("""
                DELETE FROM Clientes 
                WHERE LENGTH(telefono) = 9 
                AND ('51' || telefono) IN (SELECT telefono FROM Clientes);
            """))
            
            conn.execute(text("UPDATE Clientes SET telefono = '51' || telefono WHERE LENGTH(telefono) = 9"))
            conn.execute(text("UPDATE mensajes SET telefono = '51' || telefono WHERE LENGTH(telefono) = 9"))
            
            # --- PASO 3: ESTRUCTURA ---
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS nombre_corto TEXT;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS medio_contacto TEXT DEFAULT 'WhatsApp';"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS codigo_contacto TEXT;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS fecha_seguimiento DATE;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS google_id TEXT;"))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS Direcciones (
                    id_direccion SERIAL PRIMARY KEY,
                    id_cliente INTEGER REFERENCES Clientes(id_cliente) ON DELETE CASCADE,
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
            print("✅ BASE DE DATOS OPTIMIZADA Y LISTA.")
            
    except Exception as e:
        print(f"⚠️ Nota DB (No crítico si la app inicia): {e}")

# ENVOLVER LA FUNCIÓN EN CACHÉ
@st.cache_resource
def iniciar_sistema_db():
    print("🚀 Iniciando sistema y validando base de datos...")
    ejecutar_migraciones() # Tu función original
    return True

# LLAMAR A LA FUNCIÓN CON CACHÉ
iniciar_sistema_db()

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
                opcion = st.session_state.radio_navegacion
                st.session_state.indice_menu = OPCIONES_MENU.index(opcion)
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
        st.caption("Sistema v2.4 - FK Fix")

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