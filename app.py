from dotenv import load_dotenv
load_dotenv()
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
from views import ventas, compras, inventario, clientes, seguimiento, catalogo, facturacion, chats, campanas, diagnostico

# Cargar variables
load_dotenv()

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="K&M Ventas", layout="wide", page_icon="🛍️")

# ==========================================
# LIMPIEZA Y MIGRACIÓN (Mantenimiento DB)
# ==========================================
def ejecutar_migraciones():
    try:
        with engine.connect() as conn:
            # --- VALIDACIONES BÁSICAS Y COLUMNAS ---
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS etiquetas TEXT DEFAULT '';")) 
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS nombre_ia TEXT DEFAULT '';"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS codigo_contacto TEXT;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS nombre_corto TEXT;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS medio_contacto TEXT DEFAULT 'WhatsApp';"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS fecha_seguimiento DATE;"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS google_id TEXT;"))
            
            # --- LOGICA DE UNIFICACIÓN (Tu código nuevo para arreglar FK) ---
            # Crear tabla temporal para detectar duplicados
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

            # 1. Mover Direcciones
            conn.execute(text("""
                UPDATE Direcciones 
                SET id_cliente = t.id_conservar
                FROM Temp_Unificacion t
                WHERE Direcciones.id_cliente = t.id_eliminar 
                AND t.id_eliminar != t.id_conservar;
            """))

            # 2. Mover Mensajes
            conn.execute(text("""
                UPDATE mensajes 
                SET id_cliente = t.id_conservar
                FROM Temp_Unificacion t
                WHERE mensajes.id_cliente = t.id_eliminar 
                AND t.id_eliminar != t.id_conservar;
            """))
            
            # 3. Mover Ventas (si existe la tabla)
            try:
                conn.execute(text("""
                    UPDATE Ventas 
                    SET id_cliente = t.id_conservar
                    FROM Temp_Unificacion t
                    WHERE Ventas.id_cliente = t.id_eliminar 
                    AND t.id_eliminar != t.id_conservar;
                """))
            except: pass 

            # 4. Eliminar duplicados vacíos
            conn.execute(text("""
                DELETE FROM Clientes
                WHERE id_cliente IN (
                    SELECT id_eliminar FROM Temp_Unificacion WHERE id_eliminar != id_conservar
                );
            """))
            conn.execute(text("DROP TABLE IF EXISTS Temp_Unificacion;"))

            # --- LIMPIEZA DE FORMATOS ---
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, ' ', '')"))
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, '-', '')"))
            conn.execute(text("UPDATE Clientes SET telefono = REPLACE(telefono, '+', '')"))
            
            # Crear tabla Direcciones si no existe
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
            
    except Exception as e:
        print(f"⚠️ Nota Mantenimiento DB: {e}")

@st.cache_resource
def iniciar_sistema_db():
    print("🚀 Iniciando sistema...")
    ejecutar_migraciones()
    return True

# LLAMAR A LA FUNCIÓN CON CACHÉ
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
        return  # Detiene la ejecución aquí si no hay sesión

    # 2. INICIALIZAR CARRITO
    if 'carrito' not in st.session_state:
        st.session_state.carrito = []

    # 3. CONTADOR DE CHATS NO LEÍDOS (Debe ir aquí arriba)
    try:
        with engine.connect() as conn:
            n_no_leidos = conn.execute(text(
                "SELECT COUNT(*) FROM mensajes WHERE leido = FALSE AND tipo = 'ENTRANTE'"
            )).scalar()
    except:
        n_no_leidos = 0

    # Creamos la variable que luego usará el menú
    texto_dinamico_chat = f"💬 Chat ({n_no_leidos})" if n_no_leidos > 0 else "💬 Chat"

    # 4. BARRA LATERAL Y MENÚ
    with st.sidebar:
        st.write(f"👤 Bienvenido, **{st.session_state['usuario']}** ({st.session_state['rol']})")
        if st.button("🚪 Cerrar Sesión"):
            st.session_state.clear()
            st.rerun()
            
        st.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=100)
        st.title("Menú K&M")
        
        OPCIONES_BASE = [
            "VENTA", "COMPRAS", "INVENTARIO", "CLIENTES", 
            "SEGUIMIENTO", "CATALOGO", "FACTURACION", "CHAT", "CAMPANAS", "DIAGNOSTICO"
        ]

        # Lógica de Roles
        if st.session_state['rol'] == 'Admin':
            OPCIONES_MENU = OPCIONES_BASE + ["USUARIOS"] # El admin ve la pestaña extra
        else:
            OPCIONES_MENU = [opc for opc in OPCIONES_BASE if opc in st.session_state['modulos']]

        if "indice_menu" not in st.session_state:
            st.session_state.indice_menu = 0

        # Función de formato (AQUÍ reconoce a texto_dinamico_chat correctamente)
        def formatear_menu(opcion):
            mapeo = {
                "VENTA": "🛒 Venta (POS)", "COMPRAS": "📦 Compras", 
                "INVENTARIO": "🔎 Inventario", "CLIENTES": "👤 Clientes",
                "SEGUIMIENTO": "📆 Seguimiento", "CATALOGO": "🔧 Catálogo",
                "FACTURACION": "💰 Facturación", "CHAT": texto_dinamico_chat,
                "CAMPANAS": "📢 Campañas", "DIAGNOSTICO": "🕵️ Diagnóstico",
                "USUARIOS": "👥 Usuarios"
            }
            return mapeo.get(opcion, opcion)

        # Evitar error de índice si el rol cambia a uno con menos permisos
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

    if seleccion_interna == "VENTA": ventas.render_ventas()
    elif seleccion_interna == "COMPRAS": compras.render_compras()
    elif seleccion_interna == "INVENTARIO": inventario.render_inventario()
    elif seleccion_interna == "CLIENTES": clientes.render_clientes()
    elif seleccion_interna == "SEGUIMIENTO": seguimiento.render_seguimiento()
    elif seleccion_interna == "CATALOGO": catalogo.render_catalogo()
    elif seleccion_interna == "FACTURACION":
        tab1, tab2 = st.tabs(["📝 Registro de Boletas", "📊 Reporte Mensual"])
        with tab1: facturacion.render_facturacion() 
        with tab2: facturacion.render_reporte_mensual()
    elif seleccion_interna == "CHAT": chats.render_chat()
    elif seleccion_interna == "CAMPANAS": campanas.render_campanas()
    elif seleccion_interna == "DIAGNOSTICO": diagnostico.render_diagnostico()
    elif seleccion_interna == "USUARIOS": 
        # Aquí puedes llamar a una nueva vista usuarios.render_usuarios() 
        # o colocar el código del formulario directamente:
        st.warning("Módulo de Gestión de Usuarios en construcción...")

if __name__ == "__main__":
    ejecutar_migraciones()  # Agrega esta línea
    main()