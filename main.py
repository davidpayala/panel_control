import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, datetime, timedelta # <--- Agrega ", timedelta"
import time
import os
import urllib.parse 
import extra_streamlit_components as stx
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import requests # Necesario para hablar con la API de Meta
import random

# Cargar variables de entorno (Local y Nube)
load_dotenv()

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="K&M Ventas", layout="wide", page_icon="🛍️")

# --- SISTEMA DE LOGIN CON COOKIES (RECORDAR SESIÓN) ---
def check_password():
    """Maneja el login con persistencia de Cookies."""
    
    # 1. Configuramos el Gestor de Cookies
    cookie_manager = stx.CookieManager()
    
    # Intentamos leer la cookie 'kmlentes_auth_token'
    # NOTA: A veces requiere recargar la página una vez para leerla
    cookie_val = cookie_manager.get(cookie="kmlentes_auth_token")

    # A) Si la cookie existe y es correcta -> PASE DIRECTO
    if cookie_val == os.getenv("ADMIN_PASS"):
        st.session_state["password_correct"] = True
        return True

    # B) Si ya validamos en esta sesión -> PASE
    if st.session_state.get("password_correct", False):
        return True

    # C) Si no hay cookie ni sesión, MOSTRAR LOGIN
    st.markdown(
        """
        <style>
        .stTextInput {max-width: 400px; margin: auto;}
        .stForm {max-width: 400px; margin: auto;}
        </style>
        """, unsafe_allow_html=True
    )
    
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.title("🔒 Acceso Restringido")
        st.caption("Sistema de Gestión K&M Ventas Virtuales")
        
        with st.form("login_form"):
            st.text_input("Usuario", key="username")
            password = st.text_input("Contraseña", type="password", key="password")
            recordarme = st.checkbox("💾 Mantener sesión iniciada (30 días)")
            
            submit_btn = st.form_submit_button("Ingresar", type="primary", width='stretch')
            
            if submit_btn:
                user_env = os.getenv("ADMIN_USER")
                pass_env = os.getenv("ADMIN_PASS")
                
                if st.session_state["username"] == user_env and password == pass_env:
                    st.session_state["password_correct"] = True
                    del st.session_state["password"]
                    del st.session_state["username"]
                    
                    # SI MARCÓ "RECORDARME", GUARDAMOS LA COOKIE
                    if recordarme:
                        cookie_manager.set("kmlentes_auth_token", pass_env, expires_at=datetime.now() + pd.Timedelta(days=30))
                        # ⚠️ TRUCO CLAVE: Damos 1 segundo al navegador para que guarde la cookie
                        time.sleep(1)
                    
                    st.rerun()
                else:
                    st.error("😕 Usuario o contraseña incorrectos")
    return False

# --- BLOQUEO DE LA APLICACIÓN ---
if not check_password():
    st.stop()  # 🛑 AQUÍ SE DETIENE TODO SI NO HAY LOGIN 🛑

# ==============================================================================
# 🚀 A PARTIR DE AQUÍ VA TU CÓDIGO DEL SISTEMA (BASE DE DATOS Y PESTAÑAS)
# ==============================================================================

# --- CONEXIÓN BASE DE DATOS ---
def get_connection():
    try:
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASS')
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        dbname = os.getenv('DB_NAME')
        password_encoded = urllib.parse.quote_plus(password)
        return create_engine(f'postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{dbname}')
    except Exception as e:
        st.error(f"Error BD: {e}")
        return None

engine = get_connection()

# --- INICIALIZAR CARRITO DE COMPRAS (Memoria Temporal) ---
if 'carrito' not in st.session_state:
    st.session_state.carrito = []



# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================

# --- FUNCIÓN 1 (MODIFICADA PARA VER EL ID) ---
def subir_archivo_meta(archivo_bytes, mime_type):
    token = os.getenv("WHATSAPP_TOKEN")
    phone_id = os.getenv("WHATSAPP_PHONE_ID")
    
    # 1. Validación Previa
    if not token:
        return None, "❌ Error: Variable WHATSAPP_TOKEN está vacía o no existe."
    if not phone_id:
        return None, "❌ Error: Variable WHATSAPP_PHONE_ID está vacía o no existe."

    # Limpiamos el ID por si tiene espacios accidentales
    phone_id = str(phone_id).strip()

    url = f"https://graph.facebook.com/v17.0/{phone_id}/media"
    headers = {"Authorization": f"Bearer {token}"}
    
    files = {
        'file': ('archivo', archivo_bytes, mime_type),
        'messaging_product': (None, 'whatsapp')
    }
    
    try:
        # Imprimimos en la consola de Railway para tener registro
        print(f"📡 Subiendo archivo a URL: {url}")
        
        r = requests.post(url, headers=headers, files=files)
        
        if r.status_code == 200:
            return r.json().get("id"), None
        else:
            # AQUÍ ESTÁ LA CLAVE: Devolvemos el ID usado en el mensaje de error
            return None, f"⚠️ Falló usando ID: '{phone_id}'. Meta dice: {r.text}"
            
    except Exception as e:
        return None, f"Excepción crítica usando ID '{phone_id}': {str(e)}"
    

# --- FUNCIÓN 2: ENVIAR EL MENSAJE CON EL ARCHIVO ---
def enviar_mensaje_media(telefono, media_id, tipo_archivo, caption="", filename="archivo"):
    token = os.getenv("WHATSAPP_TOKEN")
    phone_id = os.getenv("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Determinar si es imagen o documento
    tipo_payload = "image" if "image" in tipo_archivo else "document"
    
    data = {
        "messaging_product": "whatsapp",
        "to": telefono,
        "type": tipo_payload,
        tipo_payload: {
            "id": media_id,
            "caption": caption
        }
    }
    
    # Si es documento, agregamos el nombre del archivo para que se vea bonito
    if tipo_payload == "document":
        data["document"]["filename"] = filename

    try:
        r = requests.post(url, headers=headers, json=data)
        if r.status_code == 200:
            return True, r.json()
        else:
            return False, r.text
    except Exception as e:
        return False, str(e)
    
def agregar_al_carrito(sku, nombre, cantidad, precio, es_inventario, stock_max=None):
    # Validar stock si es de inventario
    if es_inventario:
        # Verificar si ya está en el carrito para sumar
        cant_en_carrito = sum(item['cantidad'] for item in st.session_state.carrito if item['sku'] == sku)
        if (cant_en_carrito + cantidad) > stock_max:
            st.error(f"❌ No hay suficiente stock. Disponibles: {stock_max}, En carrito: {cant_en_carrito}")
            return

    st.session_state.carrito.append({
        "sku": sku,
        "descripcion": nombre,
        "cantidad": int(cantidad),
        "precio": float(precio),
        "subtotal": float(precio * cantidad),
        "es_inventario": es_inventario
    })
    st.success(f"Añadido: {nombre}")

# Función para descargar fotos de WhatsApp (Con caché para velocidad)
@st.cache_data(show_spinner=False)
def obtener_imagen_whatsapp(media_id):
    token = os.getenv("WHATSAPP_TOKEN")
    if not token: return None
    
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        # 1. Pedimos a Facebook la URL real de la imagen
        url_info = f"https://graph.facebook.com/v17.0/{media_id}"
        r_info = requests.get(url_info, headers=headers)
        
        if r_info.status_code == 200:
            url_descarga = r_info.json().get("url")
            
            # 2. Descargamos los bytes de la imagen
            r_img = requests.get(url_descarga, headers=headers)
            if r_img.status_code == 200:
                return r_img.content
    except:
        pass
    return None


# Función para generar el feed de facebook
def generar_feed_facebook():
    with engine.connect() as conn:
        # He cambiado p.url_compra por un link construido hacia TU web
        query = text("""
            SELECT 
                v.sku as id, 
                
                -- TITULO: Marca + Modelo + Color + (Variante si existe)
                p.marca || ' ' || p.modelo || ' ' || p.nombre || ' ' || COALESCE(v.nombre_variante, '') as title,
                
                -- DESCRIPCION:
                'Lentes de contacto ' || p.marca || ' color ' || p.nombre || '. Disponibles en kmlentes.pe' as description,
                
                -- DISPONIBILIDAD:
                CASE WHEN (v.stock_interno + v.stock_externo) > 0 THEN 'in_stock' ELSE 'out_of_stock' END as availability,
                
                'new' as condition,
                v.precio || ' PEN' as price,
                
                -- CORRECCIÓN AQUÍ: ENLACE A TU WEB (Búsqueda por SKU)
                'https://kmlentes.pe/?s=' || v.sku || '&post_type=product' as link,
                
                p.url_imagen as image_link,
                p.marca as brand,
                v.sku as mpn
                
            FROM Variantes v
            JOIN Productos p ON v.id_producto = p.id_producto
            WHERE p.url_imagen IS NOT NULL 
              AND p.url_imagen != ''
        """)
        df_feed = pd.read_sql(query, conn)

    # Guardar en carpeta estática
    if not os.path.exists('static'):
        os.makedirs('static')
        
    ruta_archivo = "static/feed_facebook.csv" 
    df_feed.to_csv(ruta_archivo, index=False)
    
    return len(df_feed)


# --- FUNCIÓN AUXILIAR PARA GUARDAR CAMBIOS (Pégalo al final del archivo, sin sangría) ---
def actualizar_estados(df_modificado):
    """
    Recorre el DataFrame modificado por el usuario y actualiza 
    el estado y fecha de cada cliente en la base de datos.
    """
    if df_modificado.empty:
        return

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            for idx, row in df_modificado.iterrows():
                conn.execute(
                    text("UPDATE Clientes SET estado=:e, fecha_seguimiento=:f WHERE id_cliente=:id"),
                    {
                        "e": row['estado'], 
                        "f": row['fecha_seguimiento'], 
                        "id": row['id_cliente']
                    }
                )
            trans.commit()
            st.success("✅ Estados actualizados correctamente.")
            time.sleep(0.5)
            st.rerun()
        except Exception as e:
            trans.rollback()
            st.error(f"Error al actualizar: {e}")

# ==============================================================================
# FUNCIÓN DE ENVÍO A WHATSAPP (MODO SEGURO / RAILWAY)
# ==============================================================================
def enviar_mensaje_whatsapp(numero, texto):
    # 1. Credenciales (Asegúrate de que estas sean las correctas y SIN comillas en el .env)
    TOKEN = os.getenv("WHATSAPP_TOKEN")
    PHONE_ID = os.getenv("WHATSAPP_PHONE_ID")

    if not TOKEN or not PHONE_ID:
        return False, "⚠️ Faltan credenciales en .env"

    url = f"https://graph.facebook.com/v17.0/{PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    # --- 2. LIMPIEZA Y FORMATO DEL NÚMERO (AQUÍ ESTÁ LA SOLUCIÓN) ---
    # Paso A: Quitar espacios, guiones y símbolos '+'
    numero_limpio = str(numero).replace("+", "").replace(" ", "").replace("-", "").strip()
    
    # Paso B: Lógica para Perú (Si tiene 9 dígitos, le falta el 51)
    if len(numero_limpio) == 9:
        numero_final = f"51{numero_limpio}"
    else:
        # Si ya tiene 11 dígitos (519...) o es otro caso, lo dejamos tal cual
        numero_final = numero_limpio
    # ----------------------------------------------------------------
    
    data = {
        "messaging_product": "whatsapp",
        "to": numero_final,
        "type": "text",
        "text": {"body": texto}
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, response.text
    except Exception as e:
        return False, str(e)
    
    
def sincronizar_desde_google_batch():
    """Recorre clientes con nombre vacío, los busca en Google y actualiza sus datos reales."""
    service = get_google_service()
    if not service:
        st.error("No hay conexión con Google (token.json).")
        return

    with engine.connect() as conn:
        # 1. Buscamos clientes pendientes
        # (Esta lectura inicia la transacción automática)
        df_pendientes = pd.read_sql("""
            SELECT id_cliente, telefono, nombre_corto, google_id 
            FROM Clientes 
            WHERE (nombre IS NULL OR nombre = '') AND activo = TRUE
        """, conn)
        
        if df_pendientes.empty:
            st.success("¡Todos tus clientes ya tienen nombre y apellido!")
            return

        st.info(f"Sincronizando {len(df_pendientes)} contactos con Google...")
        progress_bar = st.progress(0)
        total = len(df_pendientes)
        actualizados = 0

        # 2. Descargamos agenda de Google
        agenda_google = {}
        try:
            page_token = None
            while True:
                results = service.people().connections().list(
                    resourceName='people/me',
                    pageSize=1000, 
                    personFields='names,phoneNumbers',
                    pageToken=page_token
                ).execute()
                
                for person in results.get('connections', []):
                    phones = person.get('phoneNumbers', [])
                    names = person.get('names', [])
                    
                    if phones and names:
                        g_nombre = names[0].get('givenName', '')
                        g_apellido = names[0].get('familyName', '')
                        g_id = person.get('resourceName')
                        
                        for phone in phones:
                            clean_num = normalizar_celular(phone.get('value'))
                            if clean_num:
                                agenda_google[clean_num] = {
                                    'nombre': g_nombre,
                                    'apellido': g_apellido,
                                    'google_id': g_id
                                }
                
                page_token = results.get('nextPageToken')
                if not page_token: break
        except Exception as e:
            st.error(f"Error descargando agenda de Google: {e}")
            return

        # 3. Cruzamos la información
        # --- ELIMINADO: trans = conn.begin() ---
        try:
            for idx, row in df_pendientes.iterrows():
                telefono_db = normalizar_celular(row['telefono'])
                
                if telefono_db in agenda_google:
                    datos_google = agenda_google[telefono_db]
                    
                    conn.execute(text("""
                        UPDATE Clientes 
                        SET nombre=:n, apellido=:a, google_id=:gid 
                        WHERE id_cliente=:id
                    """), {
                        "n": datos_google['nombre'],
                        "a": datos_google['apellido'],
                        "gid": datos_google['google_id'],
                        "id": row['id_cliente']
                    })
                    actualizados += 1
                
                progress_bar.progress((idx + 1) / total)
            
            # --- AGREGADO: Confirmamos todo al final ---
            conn.commit() 
            
            st.success(f"✅ Éxito: Se actualizaron {actualizados} clientes desde Google.")
            time.sleep(2)
            st.rerun()
            
        except Exception as e:
            st.error(f"Error actualizando DB: {e}")

# --- AGREGAR ESTO AL INICIO DEL ARCHIVO (DESPUÉS DE LOS IMPORTS) ---

def normalizar_celular(numero):
    """
    Convierte cualquier formato (+51 999..., 999-999, etc) 
    a un formato limpio de 9 dígitos para comparar.
    """
    if not numero: return ""
    
    # 1. Dejar solo números (borrar +, espacios, guiones)
    solo_numeros = "".join(filter(str.isdigit, str(numero)))
    
    # 2. Si tiene 11 dígitos y empieza con 51 (ej: 51999888777), quitar el 51
    if len(solo_numeros) == 11 and solo_numeros.startswith("51"):
        return solo_numeros[2:]
        
    # 3. Si tiene más dígitos, intentamos tomar los últimos 9
    if len(solo_numeros) > 9:
        return solo_numeros[-9:]
        
    return solo_numeros

# ==============================================================================
# LÓGICA GOOGLE: SINCRONIZACIÓN REAL-TIME
# ==============================================================================
# --- BUSCA ESTA PARTE EN TU CÓDIGO Y ACTUALÍZALA ---

def get_google_service():
    # 1. SI ESTAMOS EN LA NUBE Y NO EXISTE EL ARCHIVO, LO CREAMOS DESDE LA VARIABLE SECRETA
    if not os.path.exists('token.json'):
        token_content = os.getenv("GOOGLE_TOKEN_JSON")
        if token_content:
            with open("token.json", "w") as f:
                f.write(token_content)
    
    # 2. AHORA SÍ, LEEMOS EL ARCHIVO COMO SIEMPRE
    if os.path.exists('token.json'):
        try:
            creds = Credentials.from_authorized_user_file('token.json', ['https://www.googleapis.com/auth/contacts'])
            return build('people', 'v1', credentials=creds)
        except Exception as e:
            # Si el token falló, mejor lo borramos para no causar errores infinitos
            # os.remove('token.json') 
            return None
    return None

def buscar_contacto_google(telefono):
    """Busca si existe un contacto en Google por teléfono y devuelve su ID y datos."""
    service = get_google_service()
    if not service: return None
    
    # Normalizamos para buscar
    tel_busqueda = normalizar_celular(telefono)
    if len(tel_busqueda) < 7: return None

    # Descargamos agenda (paginada) para buscar
    # NOTA: La API de People no permite buscar directo por teléfono fácilmente, 
    # así que iteramos (es rápido para <5000 contactos)
    page_token = None
    while True:
        results = service.people().connections().list(
            resourceName='people/me',
            pageSize=1000, 
            personFields='names,phoneNumbers',
            pageToken=page_token
        ).execute()
        
        for person in results.get('connections', []):
            for phone in person.get('phoneNumbers', []):
                if normalizar_celular(phone.get('value', '')) == tel_busqueda:
                    return {
                        'resourceName': person.get('resourceName'), # El ID de Google
                        'etag': person.get('etag') # Necesario para editar
                    }
        
        page_token = results.get('nextPageToken')
        if not page_token: break
    return None

def crear_en_google(nombre, apellido, telefono):
    """Crea el contacto en Google y retorna su google_id."""
    service = get_google_service()
    if not service: return None
    
    try:
        resultado = service.people().createContact(body={
            "names": [{"givenName": nombre, "familyName": apellido}],
            "phoneNumbers": [{"value": telefono}]
        }).execute()
        return resultado.get('resourceName')
    except Exception as e:
        st.error(f"Error Google Create: {e}")
        return None

def actualizar_en_google(google_id, nombre, apellido, telefono):
    """Actualiza un contacto existente en Google."""
    service = get_google_service()
    if not service: return False

    try:
        # 1. Obtenemos el contacto actual para sacar su 'etag' (obligatorio para editar)
        contacto = service.people().get(
            resourceName=google_id,
            personFields='names,phoneNumbers'
        ).execute()
        
        # 2. Preparamos la actualización
        contacto['names'] = [{"givenName": nombre, "familyName": apellido}]
        contacto['phoneNumbers'] = [{"value": telefono}]
        
        # 3. Enviamos cambios
        service.people().updateContact(
            resourceName=google_id,
            updatePersonFields='names,phoneNumbers',
            body=contacto
        ).execute()
        return True
    except Exception as e:
        # Si falla (ej: fue borrado), intentamos crearlo de nuevo o buscarlo
        st.warning(f"No se pudo actualizar en Google (¿Quizás se borró?): {e}")
        return False
    

# --- INTERFAZ ---
st.title("🛒 KM - Punto de Venta")
st.markdown("---")

    # --- CONSTANTES (TUS ESTADOS) ---
ESTADOS_CLIENTE = [
    "Sin empezar", "Responder duda", "Interesado en venta", 
    "Proveedor nacional", "Proveedor internacional", 
    "Venta motorizado", "Venta agencia", "Venta express moto",
    "En camino moto", "En camino agencia", "Contraentrega agencia", 
    "Pendiente agradecer", "Problema post"
]
MEDIOS_CONTACTO = ["Wsp 941380271", "Wsp 936041531", "Facebook-Instagram", "TikTok", "Físico/Tienda"]

# --- 1. CALCULAR NOTIFICACIONES GLOBALES ---
# Hacemos esto ANTES de crear las tabs para poner el número en el título
with engine.connect() as conn:
    # Contamos cuántos mensajes ENTRANTES tienen leido = FALSE
    n_no_leidos = conn.execute(text(
        "SELECT COUNT(*) FROM mensajes WHERE leido = FALSE AND tipo = 'ENTRANTE'"
    )).scalar()

# --- 2. DEFINIR PESTAÑAS CON ICONO ---
titulo_chat = f"💬 Chat ({n_no_leidos})" if n_no_leidos > 0 else "💬 Chat"

# AHORA SON 7 PESTAÑAS
tabs = st.tabs(["🛒 VENTA (POS)", 
                "📦 Compras", 
                "🔎 Inventario", 
                "👤 Clientes", 
                "📆 Seguimiento", 
                "🔧 Catálogo",
                "💰 Facturación",
                titulo_chat])

# ==============================================================================
# PESTAÑA 1: VENTAS / SALIDAS (CORREGIDO)
# ==============================================================================
with tabs[0]:
    # Importamos random aquí por seguridad por si falta arriba
    import random 

    # --- CABECERA ---
    col_modo, col_titulo = st.columns([1, 3])
    with col_modo:
        modo_operacion = st.radio("Modo:", ["💰 Venta", "📉 Salida / Merma"], horizontal=True)
    with col_titulo:
        if modo_operacion == "💰 Venta":
            st.subheader("🛒 Punto de Venta (Ingresos)")
        else:
            st.subheader("📉 Registro de Salidas (Mermas / Uso Interno)")

    st.divider()

    col_izq, col_der = st.columns([1, 1])

    # ------------------------------------------------------------------
    # COLUMNA IZQUIERDA: BUSCADOR
    # ------------------------------------------------------------------
    with col_izq:
        st.caption("1. Buscar Productos")
        tipo_producto = st.radio("Origen:", ["Inventario (SQL)", "Manual/Extra"], horizontal=True, label_visibility="collapsed")
        
        if tipo_producto == "Inventario (SQL)":
            sku_input = st.text_input("Escanear/Escribir SKU:", placeholder="Ej: CL-01...", key="sku_pos")
            if sku_input:
                with engine.connect() as conn:
                    res = pd.read_sql(text("""
                        SELECT v.sku, p.modelo, p.nombre as color, v.medida, v.stock_interno, v.precio, v.ubicacion 
                        FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto
                        WHERE v.sku = :sku
                    """), conn, params={"sku": sku_input})
                
                if not res.empty:
                    prod = res.iloc[0]
                    nombre_full = f"{prod['modelo']} {prod['color']} ({prod['medida']})"
                    
                    if prod['stock_interno'] <= 0:
                        st.error(f"❌ Sin Stock ({prod['stock_interno']})")
                    else:
                        st.success(f"✅ Stock: {prod['stock_interno']} | 📍 {prod['ubicacion']}")

                    st.markdown(f"**{nombre_full}**")
                    
                    c1, c2 = st.columns(2)
                    cantidad = c1.number_input("Cant.", min_value=1, value=1)
                    precio_sugerido = float(prod['precio']) if modo_operacion == "💰 Venta" else 0.0
                    precio_final = c2.number_input("Precio Unit.", value=precio_sugerido, disabled=(modo_operacion != "💰 Venta"))
                    
                    if st.button("➕ Agregar"):
                        agregar_al_carrito(prod['sku'], nombre_full, cantidad, precio_final, True, prod['stock_interno'])
                else:
                    st.warning("SKU no encontrado.")
        
        else: 
            st.info("Item Manual (Servicios, etc.)")
            desc_manual = st.text_input("Descripción:")
            c1, c2 = st.columns(2)
            cant_manual = c1.number_input("Cant.", min_value=1, value=1, key="cm")
            precio_manual = c2.number_input("Precio", value=0.0, key="pm", disabled=(modo_operacion != "💰 Venta"))
            if st.button("➕ Agregar Manual"):
                if desc_manual: agregar_al_carrito(None, desc_manual, cant_manual, precio_manual, False)

    # ------------------------------------------------------------------
    # COLUMNA DERECHA: PROCESAR
    # ------------------------------------------------------------------
    with col_der:
        st.caption("2. Confirmación")
        
        if len(st.session_state.carrito) > 0:
            df_cart = pd.DataFrame(st.session_state.carrito)
            st.dataframe(df_cart[['descripcion', 'cantidad', 'subtotal']], hide_index=True, use_container_width=True)
            
            suma_subtotal = float(df_cart['subtotal'].sum())
            
            st.divider()

            # ==========================================================
            # MODO A: VENTA
            # ==========================================================
            if modo_operacion == "💰 Venta":
                st.markdown(f"**Subtotal Items:** S/ {suma_subtotal:.2f}")

                # 1. CLIENTE
                with engine.connect() as conn:
                    cli_df = pd.read_sql(text("SELECT id_cliente, nombre_corto FROM Clientes WHERE activo = TRUE ORDER BY nombre_corto"), conn)
                lista_cli = {row['nombre_corto']: row['id_cliente'] for i, row in cli_df.iterrows()}
                
                if not lista_cli:
                    st.error("No hay clientes. Crea uno en la pestaña Clientes.")
                    st.stop()

                nombre_cli = st.selectbox("Cliente:", options=list(lista_cli.keys()))
                id_cliente = lista_cli[nombre_cli]

                # 2. TIPO DE ENVÍO
                col_e1, col_e2 = st.columns(2)
                tipo_envio = col_e1.selectbox("Método Envío", ["Gratis", "🚚 Envío Lima", "Express (Moto)", "Agencia (Pago Destino)", "Agencia (Pagado)"])
                costo_envio = col_e2.number_input("Costo Envío", value=0.0)

                # 3. LÓGICA DE DIRECCIÓN
                es_agencia = "Agencia" in tipo_envio
                es_envio_lima = tipo_envio == "🚚 Envío Lima" or tipo_envio == "Express (Moto)"
                
                if es_agencia: cat_direccion = "AGENCIA"
                elif es_envio_lima: cat_direccion = "MOTO"
                else: cat_direccion = "OTROS"

                # Buscamos direcciones guardadas
                with engine.connect() as conn:
                    q_dir = text("""
                        SELECT * FROM Direcciones 
                        WHERE id_cliente = :id AND tipo_envio = :tipo AND activo = TRUE 
                        ORDER BY id_direccion DESC
                    """)
                    df_dirs = pd.read_sql(q_dir, conn, params={"id": id_cliente, "tipo": cat_direccion})

                usar_guardada = False
                datos_nuevos = {} 
                texto_direccion_final = ""
                
                opciones_visuales = {}
                if not df_dirs.empty:
                    for idx, row in df_dirs.iterrows():
                        if es_agencia:
                            lbl = f"🏢 {row['agencia_nombre']} - {row['sede_entrega']}"
                        else:
                            lbl = f"🏠 {row['direccion_texto']} ({row['distrito']})"
                        if row['observacion']: lbl += f" | 👁️ {row['observacion'][:20]}..."
                        opciones_visuales[lbl] = row

                KEY_NUEVA = "➕ Usar una Nueva Dirección..."
                lista_desplegable = list(opciones_visuales.keys()) + [KEY_NUEVA]
                
                st.markdown("📍 **Datos de Entrega:**")
                seleccion_dir = st.selectbox("Elige destino:", options=lista_desplegable, label_visibility="collapsed")
                
                if seleccion_dir != KEY_NUEVA:
                    usar_guardada = True
                    dir_data = opciones_visuales[seleccion_dir]
                    if es_agencia:
                        texto_direccion_final = f"{dir_data['agencia_nombre']} - {dir_data['sede_entrega']} [{dir_data['dni_receptor']}]"
                        st.info(f"📦 Destino: **{texto_direccion_final}**")
                    else:
                        texto_direccion_final = f"{dir_data['direccion_texto']} - {dir_data['distrito']}"
                        st.info(f"🏠 Destino: **{texto_direccion_final}**")
                        st.caption(f"📝 {dir_data['observacion']}")
                else:
                    st.warning("📝 Registro de Nuevos Datos:")
                    with st.container(border=True):
                        c_nom, c_tel = st.columns(2)
                        recibe = c_nom.text_input("Nombre Recibe:", value=nombre_cli)
                        telf = c_tel.text_input("Teléfono:", key="telf_new")
                        
                        if es_envio_lima:
                            direcc = st.text_input("Dirección Exacta:")
                            c_dist, c_ref = st.columns(2)
                            dist = c_dist.text_input("Distrito:")
                            ref = c_ref.text_input("Referencia:")
                            gps = st.text_input("📍 GPS (Link Google Maps):")
                            obs_extra = st.text_input("Observación:")
                            obs_full = f"REF: {ref} | GPS: {gps} | {obs_extra}"
                            datos_nuevos = {"tipo": "MOTO", "nom": recibe, "tel": telf, "dir": direcc, "dist": dist, "obs": obs_full, "dni": "", "age": "", "sede": ""}
                            texto_direccion_final = f"{direcc} - {dist} (Ref: {ref})"
                        
                        elif es_agencia:
                            c_dni, c_age = st.columns(2)
                            dni = c_dni.text_input("DNI:")
                            agencia = c_age.text_input("Agencia:", value="Shalom")
                            sede = st.text_input("Sede:")
                            obs_new = st.text_input("Obs:")
                            datos_nuevos = {"tipo": "AGENCIA", "nom": recibe, "tel": telf, "dni": dni, "age": agencia, "sede": sede, "obs": obs_new, "dir": "", "dist": ""}
                            texto_direccion_final = f"{agencia} - {sede}"
                        
                        else:
                            obs_new = st.text_input("Observación / Lugar:")
                            datos_nuevos = {"tipo": "OTROS", "nom": recibe, "tel": telf, "obs": obs_new, "dir": "", "dist": "", "dni": "", "age": "", "sede": ""}
                            texto_direccion_final = "Entrega Directa / Otro"

                # 4. CLAVE AGENCIA
                clave_agencia = None
                if es_agencia:
                    if 'clave_temp' not in st.session_state: 
                        st.session_state['clave_temp'] = str(random.randint(1000, 9999))
                    
                    col_k1, col_k2 = st.columns([1,2])
                    clave_agencia = col_k1.text_input("Clave", value=st.session_state['clave_temp'])
                    col_k2.info("🔐 Clave Entrega")

                total_final = suma_subtotal + costo_envio
                
                st.divider()
                c_tot1, c_tot2 = st.columns([2, 1])
                c_tot1.markdown(f"### 💰 Monto a Cobrar: S/ {total_final:.2f}")
                nota_venta = c_tot2.text_input("Nota Interna:", placeholder="Opcional")

                if st.button("✅ REGISTRAR VENTA", type="primary", use_container_width=True):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            if not usar_guardada and datos_nuevos:
                                conn.execute(text("""
                                    INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, 
                                    direccion_texto, distrito, dni_receptor, agencia_nombre, sede_entrega, observacion, activo)
                                    VALUES (:id, :tipo, :nom, :tel, :dir, :dist, :dni, :age, :sede, :obs, TRUE)
                                """), {"id": id_cliente, **datos_nuevos})

                            nota_full = f"{nota_venta} | Envío: {texto_direccion_final}"
                            res_v = conn.execute(text("""
                                INSERT INTO Ventas (id_cliente, tipo_envio, costo_envio, total_venta, nota, clave_seguridad)
                                VALUES (:idc, :tipo, :costo, :total, :nota, :clave) RETURNING id_venta
                            """), {"idc": id_cliente, "tipo": tipo_envio, "costo": costo_envio, "total": total_final, "nota": nota_full, "clave": clave_agencia})
                            id_venta = res_v.fetchone()[0]

                            for item in st.session_state.carrito:
                                conn.execute(text("""
                                    INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario)
                                    VALUES (:idv, :sku, :desc, :cant, :pu, :sub, :inv)
                                """), {"idv": id_venta, "sku": item['sku'], "desc": item['descripcion'], "cant": int(item['cantidad']), "pu": float(item['precio']), "sub": float(item['subtotal']), "inv": item['es_inventario']})
                                
                                if item['es_inventario']:
                                    res_s = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno - :c WHERE sku=:s RETURNING stock_interno"),
                                                     {"c": int(item['cantidad']), "s": item['sku']})
                                    nuevo_s = res_s.scalar()
                                    if nuevo_s <= 0: 
                                        conn.execute(text("UPDATE Variantes SET ubicacion = '' WHERE sku=:s"), {"s": item['sku']})
                                    
                                    conn.execute(text("""
                                        INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota, id_cliente) 
                                        VALUES (:sku, 'VENTA', :c, (SELECT stock_interno + :c FROM Variantes WHERE sku=:sku), :nue, :nota, :idc)
                                    """), {"sku": item['sku'], "c": int(item['cantidad']), "nue": nuevo_s, "nota": f"Venta #{id_venta}", "idc": id_cliente})
                            
                            trans.commit()
                        st.balloons()
                        st.success(f"¡Venta #{id_venta} registrada!")
                        st.session_state.carrito = []
                        if 'clave_temp' in st.session_state: del st.session_state['clave_temp']
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

            # ==========================================================
            # MODO B: SALIDA (Merma)
            # ==========================================================
            else:
                st.warning("⚠️ Estás registrando una salida de stock (Sin cobro).")
                motivo_salida = st.selectbox("Motivo:", ["Merma / Dañado", "Regalo / Marketing", "Uso Personal", "Ajuste Inventario"])
                detalle_motivo = st.text_input("Detalle (Opcional):", placeholder="Ej: Se rompió una luna...")
                
                if st.button("📉 CONFIRMAR SALIDA", type="primary"):
                     # ... (Tu lógica de salida) ...
                     pass 
        else:
            st.info("El carrito está vacío.")
            
        if st.button("🗑️ Limpiar Todo", key="btn_limpiar_carrito"):
            st.session_state.carrito = []
            st.rerun()
# ==============================================================================
# PESTAÑA 2: COMPRAS E IMPORTACIONES (FILTRO AVANZADO ALIEXPRESS)
# ==============================================================================
with tabs[1]:
    st.subheader("🚢 Gestión de Importaciones y Reposición")

    tab_asistente, tab_pedir, tab_recepcionar = st.tabs([
        "💡 Asistente de Compras (IA)", 
        "✈️ Registrar Compra (AliExpress)", 
        "📦 Recepcionar Mercadería (Llegó)"
    ])
    
    # -------------------------------------------------------------------------
    # A) ASISTENTE INTELIGENTE
    # -------------------------------------------------------------------------
    with tab_asistente:
        # 1. CONTROLES
        with st.container(border=True):
            c_filtros, c_acciones = st.columns([3, 1])
            with c_filtros:
                st.markdown("**Configuración del Reporte**")
                col_f1, col_f2, col_f3 = st.columns(3)
                
                umbral_stock = col_f1.slider("Alerta Stock bajo (<):", 0, 50, 5)
                solo_con_externo = col_f2.checkbox("Stock en Proveedor", value=True)
                
                # MEJORA: Usamos Radio para poder elegir entre TODOS, CON o SIN link
                filtro_ali = col_f3.radio(
                    "Filtro AliExpress:", 
                    ["Todos", "Con Link", "Sin Link"], 
                    index=0, # Por defecto "Todos"
                    horizontal=True
                )
            
            with c_acciones:
                st.write("")
                if st.button("🔄 Actualizar Tabla", type="primary", width='stretch'):
                    st.rerun()

        # 2. DEFINIR AÑOS
        year_actual = datetime.now().year 
        y1, y2, y3 = year_actual, year_actual - 1, year_actual - 2 

        def get_hist_sql(year):
            return f"COALESCE(h.v{year}, 0)" if year <= 2025 else "0"

        # 3. CONSULTA HÍBRIDA
        with engine.connect() as conn:
            try:
                hist_y3, hist_y2, hist_y1 = get_hist_sql(y3), get_hist_sql(y2), get_hist_sql(y1)

                query_hybrid = text(f"""
                    WITH VentasSQL AS (
                        SELECT 
                            d.sku,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y3 THEN d.cantidad ELSE 0 END) as sql_y3,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y2 THEN d.cantidad ELSE 0 END) as sql_y2,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y1 THEN d.cantidad ELSE 0 END) as sql_y1
                        FROM DetalleVenta d
                        JOIN Ventas v ON d.id_venta = v.id_venta
                        GROUP BY d.sku
                    )
                    SELECT 
                        v.sku, 
                        p.marca || ' ' || p.modelo || ' - ' || COALESCE(p.nombre, '') || ' (' || v.medida || ')' as nombre,
                        v.stock_interno,
                        v.stock_externo,
                        COALESCE(v.stock_transito, 0) as stock_transito,
                        p.importacion,
                        ({hist_y3} + COALESCE(live.sql_y3, 0)) as venta_year_3,
                        ({hist_y2} + COALESCE(live.sql_y2, 0)) as venta_year_2,
                        ({hist_y1} + COALESCE(live.sql_y1, 0)) as venta_year_1
                    FROM Variantes v
                    JOIN Productos p ON v.id_producto = p.id_producto
                    LEFT JOIN HistorialAnual h ON v.sku = h.sku
                    LEFT JOIN VentasSQL live ON v.sku = live.sku
                    WHERE (v.stock_interno + COALESCE(v.stock_transito, 0)) <= :umbral
                """)
                
                df_reco = pd.read_sql(query_hybrid, conn, params={"umbral": umbral_stock, "y1": y1, "y2": y2, "y3": y3})
                
                if not df_reco.empty:
                    df_reco['demanda_historica'] = df_reco['venta_year_1'] + df_reco['venta_year_2'] + df_reco['venta_year_3']
                    df_reco['sugerencia_compra'] = df_reco['demanda_historica'] - (df_reco['stock_interno'] + df_reco['stock_transito'])
                    df_reco['sugerencia_compra'] = df_reco['sugerencia_compra'].clip(lower=0)

            except Exception as e:
                st.error(f"⚠️ Error en consulta: {e}")
                df_reco = pd.DataFrame()

        # 4. FILTROS
        if not df_reco.empty:
            df_reco['sku'] = df_reco['sku'].astype(str).str.strip()
            
            # Filtro 1: Stock Externo
            if solo_con_externo:
                df_reco = df_reco[df_reco['stock_externo'] > 0]
            
            # Filtro 2: Lógica AliExpress (MEJORADA)
            if filtro_ali == "Con Link":
                df_reco = df_reco[df_reco['importacion'].notna() & (df_reco['importacion'] != '')]
            elif filtro_ali == "Sin Link":
                df_reco = df_reco[df_reco['importacion'].isna() | (df_reco['importacion'] == '')]
            # Si es "Todos", no hacemos nada, pasan todos.

            patron_medida = r'-\d{4}$'
            es_medida = df_reco['sku'].str.contains(patron_medida, regex=True, na=False)
            es_base = df_reco['sku'].str.endswith('-0000', na=False)
            df_reco = df_reco[~es_medida | es_base]
            
            df_reco = df_reco.sort_values(by='sugerencia_compra', ascending=False)

        # 5. VISUALIZACIÓN
        st.divider()
        col_res_txt, col_res_btn = st.columns([3, 1])
        with col_res_txt:
            st.markdown(f"### 📋 Sugerencias de Compra ({len(df_reco)} items)")
            if filtro_ali == "Sin Link":
                st.caption("Mostrando productos que **NO** tienen enlace de importación configurado.")

        with col_res_btn:
            if not df_reco.empty:
                import io
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_reco.to_excel(writer, index=False, sheet_name='SugerenciaCompra')
                st.download_button("📥 Descargar Excel", data=buffer.getvalue(), file_name=f"Compras_{date.today()}.xlsx", width='stretch')

        st.dataframe(
            df_reco,
            column_config={
                "sku": "SKU",
                "nombre": st.column_config.TextColumn("Producto", width="large"),
                "importacion": st.column_config.LinkColumn("Link Ali"),
                "stock_interno": st.column_config.NumberColumn("En Mano", format="%d"),
                "stock_transito": st.column_config.NumberColumn("En Camino", format="%d"),
                "sugerencia_compra": st.column_config.NumberColumn("⚠️ Sugerido", format="%d"),
                "demanda_historica": st.column_config.ProgressColumn("Demanda Hist.", format="%d", min_value=0, max_value=int(df_reco['demanda_historica'].max()) if not df_reco.empty else 10),
            },
            hide_index=True,
            width='stretch'
        )

    # -------------------------------------------------------------------------
    # B) REGISTRAR PEDIDO (FIXED: TRIM WHITESPACE)
    # -------------------------------------------------------------------------
    with tab_pedir:
        st.info("✈️ Usa esta pestaña cuando **PAGAS** un pedido. Se sumará a 'En Camino'.")
        # FIX: Agregamos .strip() al final para borrar espacios si copias mal
        sku_pedido_raw = st.text_input("SKU a Importar:", key="sku_pedir")
        sku_pedido = sku_pedido_raw.strip() if sku_pedido_raw else ""
        
        if sku_pedido:
            with engine.connect() as conn:
                res = pd.read_sql(text("SELECT sku, stock_transito FROM Variantes WHERE sku = :s"), conn, params={"s": sku_pedido})
            
            if not res.empty:
                curr_transito = int(res.iloc[0]['stock_transito'] or 0)
                st.success(f"Producto encontrado. En camino actual: **{curr_transito}**")
                
                with st.form("form_pedido_ali"):
                    cant_pedido = st.number_input("Cantidad Comprada:", min_value=1, step=1)
                    nota_pedido = st.text_input("Nota / ID Pedido:")
                    
                    if st.form_submit_button("✈️ Registrar 'En Camino'", width='stretch'):
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                conn.execute(text("UPDATE Variantes SET stock_transito = :nt WHERE sku=:s"), 
                                            {"nt": curr_transito + cant_pedido, "s": sku_pedido})
                                
                                conn.execute(text("""
                                    INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                    VALUES (:s, 'PEDIDO_IMPORT', :c, :ant, :nue, :nota)
                                """), {"s": sku_pedido, "c": cant_pedido, "ant": curr_transito, "nue": curr_transito + cant_pedido, "nota": nota_pedido})
                                
                                trans.commit()
                                st.success(f"✅ Registrado correctamente.")
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"Error: {e}")
            else:
                # MENSAJE DE AYUDA MEJORADO
                st.warning(f"⚠️ El SKU '{sku_pedido}' no existe en tu base de datos.")
                st.caption("💡 **Solución:** Si es un producto nuevo que nunca has vendido, primero ve a la pestaña **'Catálogo'** y créalo. Luego regresa aquí para comprarlo.")

# -------------------------------------------------------------------------
    # C) RECEPCIONAR MERCADERÍA (CORREGIDO: Columna 'fecha')
    # -------------------------------------------------------------------------
    with tab_recepcionar:
        st.write("📦 **Lista de productos en camino** (Selecciona los que llegaron)")

        # 1. CONSULTA DE PRODUCTOS EN TRÁNSITO
        with engine.connect() as conn:
            # CORRECCIÓN: Ahora usamos 'ORDER BY m.fecha' tal como indicaste
            query_transito = text("""
                SELECT 
                    v.sku,
                    p.modelo || ' - ' || COALESCE(p.nombre, '') as nombre,
                    v.stock_transito as pendiente,
                    v.stock_interno as stock_actual,
                    v.ubicacion,
                    -- Subconsulta para traer la nota del último pedido (ordenado por 'fecha')
                    (SELECT nota 
                     FROM Movimientos m 
                     WHERE m.sku = v.sku AND m.tipo_movimiento = 'PEDIDO_IMPORT' 
                     ORDER BY m.fecha DESC LIMIT 1) as ultima_nota
                FROM Variantes v
                JOIN Productos p ON v.id_producto = p.id_producto
                WHERE v.stock_transito > 0
                ORDER BY ultima_nota DESC, p.modelo ASC
            """)
            df_transito = pd.read_sql(query_transito, conn)

        if not df_transito.empty:
            # 2. PREPARAR DATOS
            df_transito["✅ Llegó?"] = False
            df_transito["Cant. Recibida"] = df_transito["pendiente"]
            
            # Ordenamos columnas
            df_editor = df_transito[[
                "✅ Llegó?", "sku", "ultima_nota", "nombre", "Cant. Recibida", "pendiente", "stock_actual", "ubicacion"
            ]]

            # 3. MOSTRAR TABLA EDITABLE
            cambios = st.data_editor(
                df_editor,
                column_config={
                    "✅ Llegó?": st.column_config.CheckboxColumn(help="Marca si ya tienes este producto"),
                    "sku": st.column_config.TextColumn("SKU", disabled=True),
                    "ultima_nota": st.column_config.TextColumn("Nota Pedido", disabled=True),
                    "nombre": st.column_config.TextColumn("Producto", disabled=True, width="large"),
                    "Cant. Recibida": st.column_config.NumberColumn("Ingresar (+)", min_value=1),
                    "pendiente": st.column_config.NumberColumn("Esperado", disabled=True),
                    "stock_actual": st.column_config.NumberColumn("Stock Hoy", disabled=True),
                    "ubicacion": st.column_config.TextColumn("Ubicación", disabled=False)
                },
                hide_index=True,
                use_container_width=True,
                key="editor_recepcion_final"
            )

            # 4. BOTÓN DE PROCESAMIENTO MASIVO
            filas_seleccionadas = cambios[cambios["✅ Llegó?"] == True]
            
            if not filas_seleccionadas.empty:
                st.write("") 
                if st.button(f"📥 Procesar Ingreso ({len(filas_seleccionadas)} productos)", type="primary", width='stretch'):
                    
                    with engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            contador = 0
                            for index, row in filas_seleccionadas.iterrows():
                                sku_proc = row['sku']
                                cant_real = int(row['Cant. Recibida'])
                                cant_pendiente = int(row['pendiente'])
                                stock_anterior = int(row['stock_actual'])
                                ubi_nueva = row['ubicacion']
                                nota_ref = row['ultima_nota']

                                # Cálculos
                                nuevo_stock_mano = stock_anterior + cant_real
                                nuevo_transito = max(0, cant_pendiente - cant_real) 

                                # UPDATE Variantes
                                conn.execute(text("""
                                    UPDATE Variantes 
                                    SET stock_interno = :nm, stock_transito = :nt, ubicacion = :u 
                                    WHERE sku = :s
                                """), {"nm": nuevo_stock_mano, "nt": nuevo_transito, "u": ubi_nueva, "s": sku_proc})

                                # INSERT Movimientos (sin especificar 'fecha' para que use el default actual)
                                conn.execute(text("""
                                    INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                    VALUES (:s, 'RECEPCION_IMPORT', :c, :ant, :nue, :nota)
                                """), {
                                    "s": sku_proc, 
                                    "c": cant_real, 
                                    "ant": stock_anterior, 
                                    "nue": nuevo_stock_mano, 
                                    "nota": f"Recepción - Ref: {nota_ref}"
                                })
                                contador += 1
                            
                            trans.commit()
                            st.balloons()
                            st.success(f"✅ ¡Excelente! Se ingresaron {contador} productos al stock.")
                            time.sleep(1.5)
                            st.rerun()
                            
                        except Exception as e:
                            trans.rollback()
                            st.error(f"❌ Error: {e}")

            elif not df_transito.empty:
                st.info("👆 Marca la casilla '✅ Llegó?' en los productos que recibiste.")

        else:
            st.info("🎉 Todo al día. No hay mercadería pendiente de llegada.")
# ==============================================================================
# PESTAÑA 3: INVENTARIO (VISTA DETALLADA, UBICACIONES E IMPORTACIÓN)
# ==============================================================================
with tabs[2]:
    st.subheader("🔎 Gestión de Inventario e Importación")

    # --- 1. BARRA DE HERRAMIENTAS ---
    col_search, col_btn = st.columns([4, 1])
    with col_search:
        filtro_inv = st.text_input("🔍 Buscar:", placeholder="Escribe SKU, Marca, Modelo o Ubicación...")
    with col_btn:
        st.write("") 
        if st.button("🔄 Recargar Tabla"):
            if 'df_inventario' in st.session_state: del st.session_state['df_inventario']
            st.rerun()

    # --- 2. CARGA DE DATOS ---
    if 'df_inventario' not in st.session_state:
        with engine.connect() as conn:
            # ACTUALIZACIÓN: Traemos p.url_imagen
            q_inv = """
                SELECT 
                    v.sku, 
                    v.id_producto,
                    p.categoria,
                    p.marca, 
                    p.modelo, 
                    v.nombre_variante,
                    p.color_principal, 
                    p.diametro, 
                    v.medida,
                    v.stock_interno,
                    v.stock_externo,
                    v.stock_transito,
                    v.ubicacion,
                    p.importacion,
                    p.url_compra,
                    p.url_imagen  /* ### <--- NUEVO: Traemos la foto */
                FROM Variantes v
                JOIN Productos p ON v.id_producto = p.id_producto
                ORDER BY p.marca, p.modelo, v.sku ASC
            """
            st.session_state.df_inventario = pd.read_sql(text(q_inv), conn)

    # Trabajamos con una copia
    df_calc = st.session_state.df_inventario.copy()

    # --- 3. CREACIÓN DE COLUMNAS COMBINADAS ---
    df_calc['nombre_completo'] = (
        df_calc['marca'].fillna('') + " " + 
        df_calc['modelo'].fillna('') + " - " + 
        df_calc['nombre_variante'].fillna('')
    ).str.strip()

    def formatear_detalles(row):
        partes = []
        if row['color_principal']: partes.append(str(row['color_principal']))
        if row['diametro']: partes.append(f"Dia:{row['diametro']}")
        if row['medida']: partes.append(f"Med:{row['medida']}")
        return " | ".join(partes)

    df_calc['detalles_info'] = df_calc.apply(formatear_detalles, axis=1)

    # --- 4. FILTRADO ---
    if filtro_inv:
        f = filtro_inv.lower()
        df_calc = df_calc[
            df_calc['nombre_completo'].str.lower().str.contains(f, na=False) |
            df_calc['sku'].str.lower().str.contains(f, na=False) |
            df_calc['ubicacion'].str.lower().str.contains(f, na=False) |
            df_calc['importacion'].str.lower().str.contains(f, na=False)
        ]

    # Seleccionamos columnas finales (INCLUYENDO LA FOTO)
    df_final = df_calc[[
        'url_imagen', # ### <--- NUEVO: Columna de imagen al principio
        'sku', 
        'id_producto', 
        'categoria', 
        'nombre_completo', 
        'detalles_info', 
        'stock_interno', 
        'stock_externo',
        'stock_transito',
        'ubicacion',
        'importacion',
        'url_compra'
    ]]

    # --- 5. TABLA EDITABLE ---
    st.caption("📝 Editables: **En Tránsito**, **Ubicación**, **Importación** y **URL**.")
    
    cambios_inv = st.data_editor(
        df_final,
        key="editor_inventario_v3",
        column_config={
            # ### <--- NUEVO: Configuración de la columna IMAGEN
            "url_imagen": st.column_config.ImageColumn(
                "Foto 📸", 
                width="small",
                help="Clic para ver en grande"
            ),
            
            "sku": st.column_config.TextColumn("SKU", disabled=True, width="small"),
            "id_producto": None, 
            "categoria": st.column_config.TextColumn("Cat.", disabled=True, width="small"),
            "nombre_completo": st.column_config.TextColumn("Producto", disabled=True, width="large"),
            "detalles_info": st.column_config.TextColumn("Detalles", disabled=True, width="medium"),
            
            "stock_interno": st.column_config.NumberColumn("S. Int.", disabled=True, format="%d"),
            "stock_externo": st.column_config.NumberColumn("S. Ext.", disabled=True, format="%d"),
            
            "stock_transito": st.column_config.NumberColumn(
                "En Camino 🚚", 
                help="Stock que ya se pidió al proveedor",
                min_value=0, step=1, format="%d", width="small"
            ),
            
            "ubicacion": st.column_config.TextColumn("Ubicación 📍", width="small"),
            
            "importacion": st.column_config.SelectboxColumn(
                "Importar De ✈️", width="small",
                options=["Aliexpress", "Alibaba", "Proveedor Nacional", "Otro"], 
                required=False
            ),
            
            "url_compra": st.column_config.LinkColumn(
                "Link Compra 🔗", width="medium", display_text="Ver Enlace", validate="^https://.*", required=False
            )
        },
        hide_index=True,
        width='stretch',
        num_rows="fixed" 
    )

    # --- 6. GUARDAR CAMBIOS (MISMO CÓDIGO) ---
    edited_rows = st.session_state["editor_inventario_v3"].get("edited_rows")

    if edited_rows:
        st.info(f"💾 Tienes cambios pendientes en {len(edited_rows)} filas...")
        
        if st.button("Confirmar Cambios"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    count_ubi = 0
                    count_prod = 0
                    count_transito = 0 
                    
                    for idx, updates in edited_rows.items():
                        row_original = df_final.iloc[idx]
                        sku_target = row_original['sku']
                        id_prod_target = int(row_original['id_producto']) 
                        
                        # A) CAMBIOS EN VARIANTES
                        if 'ubicacion' in updates:
                            conn.execute(text("UPDATE Variantes SET ubicacion = :u WHERE sku = :s"), {"u": updates['ubicacion'], "s": sku_target})
                            count_ubi += 1

                        if 'stock_transito' in updates:
                            conn.execute(text("UPDATE Variantes SET stock_transito = :st WHERE sku = :s"), {"st": updates['stock_transito'], "s": sku_target})
                            count_transito += 1
                        
                        # B) CAMBIOS EN PRODUCTOS
                        if 'importacion' in updates or 'url_compra' in updates:
                            nuevo_imp = updates.get('importacion', row_original['importacion'])
                            nueva_url = updates.get('url_compra', row_original['url_compra'])
                            conn.execute(text("UPDATE Productos SET importacion = :imp, url_compra = :url WHERE id_producto = :idp"), 
                                         {"imp": nuevo_imp, "url": nueva_url, "idp": id_prod_target})
                            count_prod += 1
                    
                    trans.commit()
                    st.success(f"✅ Guardado: {count_ubi} Ubicaciones, {count_transito} Stocks en tránsito y {count_prod} Datos de Importación.")
                    
                    del st.session_state['df_inventario'] 
                    time.sleep(1.5)
                    st.rerun()
                    
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error al guardar: {e}")

# ==============================================================================
# PESTAÑA 4: GESTIÓN DE CLIENTES (ACTUALIZADA Y EDITABLE)
# ==============================================================================
with tabs[3]:
    st.subheader("👥 Gestión de Clientes")

    # --- SECCIÓN 1: CREAR NUEVO CLIENTE (Se mantiene igual) ---
    with st.expander("➕ Nuevo Cliente (Sincronizado)", expanded=True):
        with st.form("form_nuevo_cliente"):
            col1, col2 = st.columns(2)
            with col1:
                # Campos Base + Google
                nombre_real = st.text_input("Nombre (Google y Base)")
                apellido_real = st.text_input("Apellido (Google y Base)")
                telefono = st.text_input("Teléfono Principal (Google y Base)")
            with col2:
                # Campos Solo Base
                nombre_corto = st.text_input("Nombre Corto (Alias/Rápido)")
                medio = st.selectbox("Medio de Contacto", ["WhatsApp", "Instagram", "Facebook", "TikTok", "Recomendado", "Web"])
                codigo = st.text_input("Código Principal (DNI/RUC/Otro)")
                estado_ini = st.selectbox("Estado Inicial", ["Interesado en venta", "Responder duda", "Proveedor nacional"])
            
            btn_crear = st.form_submit_button("💾 Guardar y Sincronizar", type="primary")

            if btn_crear:
                if not telefono or not nombre_corto:
                    st.error("El Teléfono y el Nombre Corto son obligatorios.")
                else:
                    # 1. VERIFICAR DUPLICADOS (Base de Datos)
                    with engine.connect() as conn:
                        existe_db = conn.execute(text("SELECT COUNT(*) FROM Clientes WHERE telefono = :t"), {"t": telefono}).scalar()
                    
                    # 2. VERIFICAR DUPLICADOS (Google)
                    existe_google = buscar_contacto_google(telefono)

                    if existe_db > 0:
                        st.error("⚠️ Este teléfono ya existe en la Base de Datos.")
                    elif existe_google:
                        st.error(f"⚠️ Este teléfono ya existe en Google Contacts (ID: {existe_google['resourceName']}).")
                    else:
                        # 3. CREAR EN GOOGLE
                        google_id = crear_en_google(nombre_real, apellido_real, telefono)
                        
                        # 4. CREAR EN BASE DE DATOS
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                conn.execute(text("""
                                    INSERT INTO Clientes (
                                        nombre_corto, nombre, apellido, telefono, medio_contacto, 
                                        codigo_contacto, estado, fecha_seguimiento, google_id, activo
                                    ) VALUES (
                                        :nc, :nom, :ape, :tel, :medio, :cod, :est, CURRENT_DATE, :gid, TRUE
                                    )
                                """), {
                                    "nc": nombre_corto, "nom": nombre_real, "ape": apellido_real,
                                    "tel": telefono, "medio": medio, "cod": codigo,
                                    "est": estado_ini, "gid": google_id
                                })
                                trans.commit()
                                st.success(f"✅ Cliente creado en Sistema y Google.")
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"Error DB: {e}")

    st.divider()

    # --- SECCIÓN 2: BUSCADOR Y EDICIÓN RÁPIDA (MODIFICADA ⭐) ---
    st.subheader("🔍 Buscar y Editar Clientes")
    
    col_search, col_btn = st.columns([3, 1])
    with col_search:
        busqueda = st.text_input("Escribe el nombre o teléfono del cliente:", placeholder="Ej: Maria, 999...")

    # Lista completa de estados para el desplegable
    OPCIONES_ESTADO = [
        "Sin empezar", "Responder duda", "Interesado en venta", 
        "Proveedor nacional", "Proveedor internacional", 
        "Venta motorizado", "Venta agencia", "Venta express moto",
        "En camino moto", "En camino agencia", "Contraentrega agencia",
        "Pendiente agradecer", "Problema post"
    ]

    df_resultados = pd.DataFrame()
    
    if busqueda:
        with engine.connect() as conn:
            # AHORA INCLUIMOS 'estado' EN LA CONSULTA
            query = text("""
                SELECT id_cliente, nombre_corto, estado, nombre, apellido, telefono, google_id 
                FROM Clientes 
                WHERE (nombre_corto ILIKE :b OR telefono ILIKE :b) AND activo = TRUE 
                ORDER BY nombre_corto ASC LIMIT 20
            """)
            df_resultados = pd.read_sql(query, conn, params={"b": f"%{busqueda}%"})
    else:
        st.info("👆 Escribe arriba para buscar.")

    if not df_resultados.empty:
        st.caption(f"Se encontraron {len(df_resultados)} resultados.")
        
        cambios = st.data_editor(
            df_resultados,
            key="editor_busqueda",
            column_config={
                "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "google_id": None, # Oculto
                
                # AHORA SON EDITABLES:
                "nombre_corto": st.column_config.TextColumn("Nombre Corto (Alias)", required=True),
                "estado": st.column_config.SelectboxColumn("Estado Actual", options=OPCIONES_ESTADO, width="medium", required=True),
                
                # Datos Personales
                "nombre": st.column_config.TextColumn("Nombre (Google)", required=True),
                "apellido": st.column_config.TextColumn("Apellido (Google)", required=True),
                "telefono": st.column_config.TextColumn("Teléfono", required=True)
            },
            hide_index=True,
            use_container_width=True
        )

        if st.button("💾 Guardar Cambios"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for idx, row in cambios.iterrows():
                        # 1. Actualizamos la Base de Datos (Ahora incluye nombre_corto y estado)
                        conn.execute(text("""
                            UPDATE Clientes 
                            SET nombre=:n, apellido=:a, telefono=:t, nombre_corto=:nc, estado=:est
                            WHERE id_cliente=:id
                        """), {
                            "n": row['nombre'], "a": row['apellido'], 
                            "t": row['telefono'], "nc": row['nombre_corto'], 
                            "est": row['estado'], "id": row['id_cliente']
                        })
                        
                        # 2. Sincronizamos con Google (Solo datos personales, Google no tiene "estado" ni "alias")
                        if row['google_id']:
                            actualizar_en_google(row['google_id'], row['nombre'], row['apellido'], row['telefono'])
                            
                    trans.commit()
                    st.success("✅ Datos actualizados correctamente.")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error al guardar: {e}")
    
    elif busqueda:
        st.warning("No se encontraron clientes con esos datos.")

# ==============================================================================
# HERRAMIENTA DE FUSIÓN DE CLIENTES
# ==============================================================================
st.divider()
st.subheader("🧬 Fusión de Clientes Duplicados")
st.info("Utiliza esta herramienta cuando una persona tenga dos registros (ej. dos números). Se moverá todo el historial al 'Cliente Principal' y se guardará el número antiguo.")

col_dup, col_orig = st.columns(2)

# --- 1. SELECCIONAR EL DUPLICADO (EL QUE SE VA A BORRAR) ---
with col_dup:
    st.markdown("### ❌ 1. Cliente a ELIMINAR")
    search_dup = st.text_input("Buscar duplicado (Nombre/Telf):", key="search_dup")
    
    id_duplicado = None
    info_duplicado = None
    
    if search_dup:
        with engine.connect() as conn:
            # Buscamos clientes activos
            res = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono, nombre, apellido FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo = TRUE LIMIT 5"), conn, params={"s": f"%{search_dup}%"})
        
        if not res.empty:
            # Usamos un selectbox para elegir el ID exacto
            opts_dup = res.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID: {x['id_cliente']}", axis=1).tolist()
            sel_dup_str = st.selectbox("Selecciona:", opts_dup, key="sel_dup")
            id_duplicado = int(sel_dup_str.split("ID: ")[1])
            
            # Guardamos datos para mostrar confirmación
            row_dup = res[res['id_cliente'] == id_duplicado].iloc[0]
            info_duplicado = f"**{row_dup['nombre_corto']}**\nTelf: {row_dup['telefono']}"
            st.warning(f"⚠️ Este cliente será DESACTIVADO.")
        else:
            st.caption("No encontrado.")

# --- 2. SELECCIONAR EL ORIGINAL (EL QUE SE QUEDA) ---
with col_orig:
    st.markdown("### ✅ 2. Cliente PRINCIPAL")
    search_orig = st.text_input("Buscar principal (Nombre/Telf):", key="search_orig")
    
    id_original = None
    
    if search_orig:
        with engine.connect() as conn:
            res2 = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo = TRUE LIMIT 5"), conn, params={"s": f"%{search_orig}%"})
        
        if not res2.empty:
            opts_orig = res2.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID: {x['id_cliente']}", axis=1).tolist()
            sel_orig_str = st.selectbox("Selecciona:", opts_orig, key="sel_orig")
            id_original = int(sel_orig_str.split("ID: ")[1])
            
            st.success(f"✅ Este cliente recibirá el historial.")
        else:
            st.caption("No encontrado.")

# --- 3. BOTÓN DE FUSIÓN (LÓGICA BLINDADA) ---
st.divider()

if id_duplicado and id_original:
    if id_duplicado == id_original:
        st.error("⛔ ¡No puedes fusionar al cliente consigo mismo! Selecciona dos distintos.")
    else:
        st.markdown(f"### 🔄 Confirmar Fusión")
        st.write(f"Vas a pasar todo de {info_duplicado} hacia el ID **{id_original}**.")
        
        if st.button("🚀 FUSIONAR AHORA", type="primary"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    # 1. Obtener el teléfono del duplicado para no perderlo
                    old_phone = conn.execute(text("SELECT telefono FROM Clientes WHERE id_cliente = :id"), {"id": id_duplicado}).scalar()
                    
                    # 2. Mover VENTAS
                    conn.execute(text("UPDATE Ventas SET id_cliente = :new_id WHERE id_cliente = :old_id"), {"new_id": id_original, "old_id": id_duplicado})
                    
                    # 3. Mover DIRECCIONES
                    conn.execute(text("UPDATE Direcciones SET id_cliente = :new_id WHERE id_cliente = :old_id"), {"new_id": id_original, "old_id": id_duplicado})
                    
                    # 4. Actualizar el Principal (Guardamos el teléfono viejo como secundario)
                    # Solo si el campo secundario está vacío, para no sobrescribir algo importante
                    conn.execute(text("""
                        UPDATE Clientes 
                        SET telefono_secundario = :old_tel 
                        WHERE id_cliente = :new_id AND (telefono_secundario IS NULL OR telefono_secundario = '')
                    """), {"old_tel": old_phone, "new_id": id_original})
                    
                    # 5. Desactivar el duplicado (Soft Delete)
                    conn.execute(text("UPDATE Clientes SET activo = FALSE, nombre_corto = nombre_corto || ' (FUSIONADO)' WHERE id_cliente = :old_id"), {"old_id": id_duplicado})
                    
                    trans.commit()
                    st.balloons()
                    st.success("✨ ¡Fusión Completada! Historial unificado.")
                    time.sleep(2)
                    st.rerun()
                    
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error en la fusión: {e}")
# ==============================================================================
# PESTAÑA 5: LOGÍSTICA PRO (CON FECHA DE SEGUIMIENTO EDITABLE)
# ==============================================================================
with tabs[4]:
    # CSS para ajustar altura de filas y ver los saltos de línea
    st.markdown("""
        <style>
            div[data-testid="stDataEditor"] td {
                white-space: pre-wrap !important;
                vertical-align: top !important;
            }
        </style>
    """, unsafe_allow_html=True)

    st.subheader("🎯 Tablero de Seguimiento Logístico")

    # --- 1. CONFIGURACIÓN ---
    ETAPAS = {
        "ETAPA_0": ["Sin empezar"],
        "ETAPA_1": ["Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional"],
        "ETAPA_2": ["Venta motorizado", "Venta agencia", "Venta express moto"],
        "ETAPA_3": ["En camino moto", "En camino agencia", "Contraentrega agencia"],
        "ETAPA_4": ["Pendiente agradecer", "Problema post"]
    }
    TODOS_LOS_ESTADOS = [e for lista in ETAPAS.values() for e in lista]

    # --- 2. CONSULTA SQL ---
    with engine.connect() as conn:
        query_seg = text("""
            SELECT 
                c.id_cliente, c.nombre_corto, c.telefono, c.estado, c.fecha_seguimiento, 
                
                -- Datos de Venta
                v.id_venta, v.total_venta, v.clave_seguridad, 
                v.fecha_venta, 
                v.pendiente_pago,
                (SELECT STRING_AGG(d.cantidad || 'x ' || d.descripcion, ', ') 
                 FROM DetalleVenta d WHERE d.id_venta = v.id_venta) as resumen_items,

                -- Datos de Dirección
                dir.id_direccion, dir.nombre_receptor, dir.telefono_receptor, 
                dir.direccion_texto, dir.distrito, 
                dir.referencia, dir.gps, dir.observacion,
                dir.dni_receptor, dir.agencia_nombre, dir.sede_entrega

            FROM Clientes c
            LEFT JOIN LATERAL (
                SELECT * FROM Ventas v2 WHERE v2.id_cliente = c.id_cliente ORDER BY v2.id_venta DESC LIMIT 1
            ) v ON TRUE
            LEFT JOIN LATERAL (
                SELECT * FROM Direcciones d2 WHERE d2.id_cliente = c.id_cliente ORDER BY d2.id_direccion DESC LIMIT 1
            ) dir ON TRUE
            WHERE c.activo = TRUE 
            ORDER BY c.fecha_seguimiento ASC
        """)
        df_seg = pd.read_sql(query_seg, conn)

    # --- 3. FUNCIÓN DE GUARDADO ---
    def guardar_edicion_rapida(df_editado, tipo_tabla):
        try:
            with engine.connect() as conn:
                for index, row in df_editado.iterrows():
                    # A) Actualizar Estado y FECHA DE SEGUIMIENTO
                    conn.execute(text("UPDATE Clientes SET estado = :est, fecha_seguimiento = :fec WHERE id_cliente = :id"), 
                                 {"est": row['estado'], "fec": row['fecha_seguimiento'], "id": row['id_cliente']})
                    
                    # B) Actualizar Pendiente de Pago (Si hay venta asociada)
                    if pd.notnull(row['id_venta']):
                        conn.execute(text("UPDATE Ventas SET pendiente_pago = :pen WHERE id_venta = :idv"),
                                     {"pen": row['pendiente_pago'], "idv": row['id_venta']})
                        
                    conn.commit()
            st.toast("✅ Cambios guardados correctamente", icon="💾")
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    # --- 4. RENDERIZADO ---
    if not df_seg.empty:
        # Filtros
        df_moto = df_seg[df_seg['estado'].isin(["Venta motorizado", "Venta express moto"])].copy()
        df_agencia = df_seg[df_seg['estado'] == "Venta agencia"].copy()
        df_ruta = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_3"])].copy()
        # Resto de etapas
        df_e1 = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_1"])].copy()
        df_e4 = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_4"])].copy()

        # Métricas
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🛵 Moto / Express", len(df_moto), border=True)
        c2.metric("🏢 Agencia", len(df_agencia), border=True)
        c3.metric("🚚 En Ruta", len(df_ruta))
        c4.metric("💬 Conversación", len(df_e1))
        
        st.divider()
        st.markdown("### 🔥 Zona Operativa: Por Despachar")
        st.info("💡 Marca la casilla '👉' para gestionar la dirección.")
        
        tab_moto, tab_agencia = st.tabs(["🛵 MOTORIZADO", "🏢 AGENCIA"])

        # --- FORMATOS VISUALES ---
        def formatear_entrega_moto(row):
            return (f"👤 {row['nombre_receptor']}\n"
                    f"📞 {row['telefono_receptor']}\n"
                    f"📍 {row['direccion_texto']} ({row['distrito']})\n"
                    f"🏠 Ref: {row['referencia']}\n"
                    f"🗺️ GPS: {row['gps']}\n"
                    f"📝 Obs: {row['observacion']}")

        def formatear_entrega_agencia(row):
            return (f"👤 {row['nombre_receptor']}\n"
                    f"🆔 DNI: {row['dni_receptor']}\n"
                    f"📞 {row['telefono_receptor']}\n"
                    f"🏢 {row['agencia_nombre']} - {row['sede_entrega']}\n"
                    f"🔐 Clave: {row['clave_seguridad']}")

        def formatear_venta_resumen(row):
            if pd.isnull(row['id_venta']): return ""
            fecha_str = row['fecha_venta'].strftime('%d/%m %H:%M') if pd.notnull(row['fecha_venta']) else "--"
            total = float(row['total_venta']) if pd.notnull(row['total_venta']) else 0.0
            return (f"📅 {fecha_str}\n"
                    f"🛒 {row['resumen_items']}\n"
                    f"💰 Total Venta: S/ {total:.2f}")

        # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA MOTO <<<<<<<<<<<<<<<<<<<<<<<<<
        with tab_moto:
            if not df_moto.empty:
                df_moto["datos_entrega"] = df_moto.apply(formatear_entrega_moto, axis=1)
                df_moto["resumen_venta"] = df_moto.apply(formatear_venta_resumen, axis=1)
                
                df_view = df_moto.copy()
                df_view.insert(0, "Seleccionar", False)

                cols_show = ["Seleccionar", "id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", 
                             "resumen_venta", "datos_entrega", "pendiente_pago"]
                
                cfg = {
                    "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                    "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS, width="medium"),
                    "fecha_seguimiento": st.column_config.DateColumn("📅 Fecha", format="DD/MM/YYYY", width="medium"),
                    "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                    "telefono": st.column_config.TextColumn("📞 Telf. Cliente", disabled=True),
                    "resumen_venta": st.column_config.TextColumn("🧾 Resumen Venta", width="medium", disabled=True),
                    "datos_entrega": st.column_config.TextColumn("📦 Datos de Entrega", width="large", disabled=True),
                    "pendiente_pago": st.column_config.NumberColumn("❗ A Cobrar", format="S/ %.2f"),
                    "id_cliente": None
                }

                event_moto = st.data_editor(
                    df_view[cols_show], 
                    key="ed_moto", column_config=cfg, 
                    hide_index=True, use_container_width=True
                )
                
                c_btn1, c_btn2 = st.columns([1, 1])
                
                if c_btn1.button("💾 Guardar Cambios", key="btn_save_moto"): 
                    df_save = df_moto.loc[event_moto.index].copy()
                    df_save['estado'] = event_moto['estado']
                    df_save['fecha_seguimiento'] = event_moto['fecha_seguimiento']
                    df_save['pendiente_pago'] = event_moto['pendiente_pago']
                    guardar_edicion_rapida(df_save, "MOTO")

                if c_btn2.button("📋 Generar Lista de Ruta (Texto)", key="btn_gen_ruta"):
                    texto_ruta = ""
                    count = 1
                    df_rut = df_moto.loc[event_moto.index]
                    for idx, row in df_rut.iterrows():
                        monto = float(row['pendiente_pago']) if pd.notnull(row['pendiente_pago']) else 0.0
                        texto_ruta += f"*Pedido {count}*\n"
                        texto_ruta += f"*Recibe:* {row['nombre_receptor'] or ''}\n"
                        texto_ruta += f"*Dirección:* {row['direccion_texto'] or ''}\n"
                        texto_ruta += f"*Referencia:* {row['referencia'] or ''}\n"
                        texto_ruta += f"*GPS:* {row['gps'] or ''}\n"
                        texto_ruta += f"*Distrito:* {row['distrito'] or ''}\n"
                        texto_ruta += f"*Teléfono:* {row['telefono_receptor'] or ''}\n"
                        texto_ruta += f"*Observación:* {row['observacion'] or ''}\n"
                        texto_ruta += f"*Monto a cobrar:* S/ {monto:.2f}\n"
                        texto_ruta += "----------------------------------\n"
                        count += 1
                    st.code(texto_ruta, language="text")
                    st.toast("Lista generada arriba.", icon="📋")

                # GESTIÓN DIRECCIÓN MOTO
                filas_sel = event_moto[event_moto["Seleccionar"] == True]
                if not filas_sel.empty:
                    row_full = df_moto.loc[filas_sel.index[0]]
                    st.divider()
                    st.markdown(f"#### 📍 Gestionar Dirección: **{row_full['nombre_corto']}**")
                    with st.container(border=True):
                        with engine.connect() as conn:
                            hist_dirs = pd.read_sql(text("SELECT id_direccion, direccion_texto, distrito, referencia FROM Direcciones WHERE id_cliente = :id AND tipo_envio = 'MOTO' ORDER BY id_direccion DESC"), conn, params={"id": int(row_full['id_cliente'])})
                        
                        opts = {"🆕 Nueva / Editar Actual...": -1}
                        for i, r in hist_dirs.iterrows(): opts[f"{r['direccion_texto']} ({r['distrito']})"] = r['id_direccion']
                        sel_id = st.selectbox("Cargar Datos:", list(opts.keys()))
                        
                        with st.form("form_moto"):
                            if opts[sel_id] == -1:
                                d_nom, d_tel, d_dir, d_dist, d_ref, d_gps, d_obs = row_full['nombre_receptor'], row_full['telefono_receptor'], row_full['direccion_texto'], row_full['distrito'], row_full['referencia'], row_full['gps'], row_full['observacion']
                            else:
                                with engine.connect() as conn:
                                    dd = conn.execute(text("SELECT * FROM Direcciones WHERE id_direccion=:id"), {"id": opts[sel_id]}).fetchone()
                                    d_nom, d_tel, d_dir, d_dist, d_ref, d_gps, d_obs = dd.nombre_receptor, dd.telefono_receptor, dd.direccion_texto, dd.distrito, dd.referencia, dd.gps, dd.observacion

                            c1, c2 = st.columns(2)
                            n_nom, n_tel = c1.text_input("Recibe", d_nom), c2.text_input("Teléfono", d_tel)
                            n_dir = st.text_input("Dirección", d_dir)
                            c3, c4 = st.columns(2)
                            n_dist, n_ref = c3.text_input("Distrito", d_dist), c4.text_input("Ref", d_ref)
                            n_gps, n_obs = st.text_input("GPS", d_gps), st.text_input("Obs", d_obs)

                            if st.form_submit_button("✅ Guardar Dirección"):
                                with engine.connect() as conn:
                                    conn.execute(text("INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, direccion_texto, distrito, referencia, gps, observacion, activo) VALUES (:id, 'MOTO', :n, :t, :d, :di, :r, :g, :o, TRUE)"), 
                                                 {"id": int(row_full['id_cliente']), "n": n_nom, "t": n_tel, "d": n_dir, "di": n_dist, "r": n_ref, "g": n_gps, "o": n_obs})
                                    conn.commit()
                                st.rerun()
            else:
                st.info("Nada en moto.")

        # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA AGENCIA <<<<<<<<<<<<<<<<<<<<<<<<<
        with tab_agencia:
            if not df_agencia.empty:
                df_agencia["datos_entrega"] = df_agencia.apply(formatear_entrega_agencia, axis=1)
                df_agencia["resumen_venta"] = df_agencia.apply(formatear_venta_resumen, axis=1)
                
                df_view_a = df_agencia.copy()
                df_view_a.insert(0, "Seleccionar", False)
                
                cols_show_a = ["Seleccionar", "id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", 
                               "resumen_venta", "datos_entrega", "pendiente_pago"]
                
                cfg_a = {
                    "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                    "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS, width="medium"),
                    "fecha_seguimiento": st.column_config.DateColumn("📅 Fecha", format="DD/MM/YYYY", width="medium"),
                    "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                    "telefono": st.column_config.TextColumn("📞 Telf. Cliente", disabled=True),
                    "resumen_venta": st.column_config.TextColumn("🧾 Resumen", width="medium", disabled=True),
                    "datos_entrega": st.column_config.TextColumn("📦 Datos Envío", width="large", disabled=True),
                    "pendiente_pago": st.column_config.NumberColumn("❗ A Cobrar", format="S/ %.2f"),
                    "id_cliente": None
                }

                event_agencia = st.data_editor(
                    df_view_a[cols_show_a], key="ed_age", column_config=cfg_a, 
                    hide_index=True, use_container_width=True
                )
                
                if st.button("💾 Guardar Cambios", key="btn_save_age"): 
                    df_save_a = df_agencia.loc[event_agencia.index].copy()
                    df_save_a['estado'] = event_agencia['estado']
                    df_save_a['fecha_seguimiento'] = event_agencia['fecha_seguimiento'] 
                    df_save_a['pendiente_pago'] = event_agencia['pendiente_pago']
                    guardar_edicion_rapida(df_save_a, "AGENCIA")

                # GESTIÓN AGENCIA
                filas_sel_a = event_agencia[event_agencia["Seleccionar"] == True]
                if not filas_sel_a.empty:
                    row_full_a = df_agencia.loc[filas_sel_a.index[0]]
                    st.divider()
                    st.markdown(f"#### 🏢 Gestionar Agencia: **{row_full_a['nombre_corto']}**")
                    with st.form("form_age"):
                        c1, c2, c3 = st.columns(3)
                        n_nom, n_dni, n_tel = c1.text_input("Recibe", row_full_a['nombre_receptor']), c2.text_input("DNI", row_full_a['dni_receptor']), c3.text_input("Telf", row_full_a['telefono_receptor'])
                        c4, c5 = st.columns(2)
                        n_age, n_sede = c4.selectbox("Agencia", ["Shalom", "Olva", "Marvisur"]), c5.text_input("Sede", row_full_a['sede_entrega'])
                        if st.form_submit_button("✅ Guardar Agencia"):
                             with engine.connect() as conn:
                                    conn.execute(text("INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, dni_receptor, telefono_receptor, agencia_nombre, sede_entrega, activo) VALUES (:id, 'AGENCIA', :n, :d, :t, :a, :s, TRUE)"),
                                                 {"id": int(row_full_a['id_cliente']), "n": n_nom, "d": n_dni, "t": n_tel, "a": n_age, "s": n_sede})
                                    conn.commit()
                             st.rerun()
            else:
                st.info("Nada en agencia.")

        st.divider()
        st.markdown("### 🚚 Zona Logística: En Ruta")
        
        if not df_ruta.empty:
            cols_ruta = ["id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", "resumen_items"]
            
            cfg_ruta = {
                "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                "fecha_seguimiento": st.column_config.DateColumn("Fecha Seg.", format="DD/MM/YYYY"),
                "id_cliente": None
            }
            
            edit_ruta = st.data_editor(
                df_ruta[cols_ruta], 
                key="ed_ruta", 
                column_config=cfg_ruta, 
                hide_index=True, 
                use_container_width=True
            )
            
            if st.button("💾 Actualizar Ruta", key="btn_save_ruta"):
                # --- CORRECCIÓN AQUÍ ---
                # 1. Recuperamos la data completa original (que sí tiene id_venta) usando el índice
                df_save_ruta = df_ruta.loc[edit_ruta.index].copy()
                
                # 2. Sobrescribimos solo las columnas que permitimos editar
                df_save_ruta['estado'] = edit_ruta['estado']
                df_save_ruta['fecha_seguimiento'] = edit_ruta['fecha_seguimiento']
                
                # 3. Ahora sí guardamos (df_save_ruta tiene id_venta oculto, así que no fallará)
                guardar_edicion_rapida(df_save_ruta, "RUTA")
        else:
            st.info("Nada en ruta.")

        # ==================================================================
        # 📂 BANDEJAS DE GESTIÓN
        # ==================================================================
        st.divider()
        st.markdown("### 📂 Bandejas de Gestión")

        # --- ETAPA 1 (Restaurada, aquí se había colado el duplicado) ---
        with st.expander(f"💬 Conversación / Cotizando ({len(df_e1)})"):
            if not df_e1.empty:
                cols_e1 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
                cfg_e1 = {
                    "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                    "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                    "resumen_items": st.column_config.TextColumn("Historial / Interés", width="large"),
                    "fecha_seguimiento": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                    "id_cliente": None
                }
                event_e1 = st.data_editor(df_e1[cols_e1], key="ed_e1", column_config=cfg_e1, hide_index=True, use_container_width=True)
                if st.button("💾 Guardar (Conversación)", key="btn_save_e1"):
                     df_save_e1 = df_e1.loc[event_e1.index].copy()
                     df_save_e1['estado'] = event_e1['estado']
                     df_save_e1['fecha_seguimiento'] = event_e1['fecha_seguimiento']
                     guardar_edicion_rapida(df_save_e1, "GENERICO")
            else:
                st.info("Bandeja vacía.")

        # --- ETAPA 4 ---
        with st.expander(f"✨ Post-Venta ({len(df_e4)})"):
             if not df_e4.empty:
                cols_e4 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
                cfg_e4 = {
                    "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                    "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                    "resumen_items": st.column_config.TextColumn("Compra Anterior", width="large"),
                    "fecha_seguimiento": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                    "id_cliente": None
                }
                event_e4 = st.data_editor(df_e4[cols_e4], key="ed_e4", column_config=cfg_e4, hide_index=True, use_container_width=True)
                if st.button("💾 Guardar (Post-Venta)", key="btn_save_e4"):
                     df_save_e4 = df_e4.loc[event_e4.index].copy()
                     df_save_e4['estado'] = event_e4['estado']
                     df_save_e4['fecha_seguimiento'] = event_e4['fecha_seguimiento']
                     guardar_edicion_rapida(df_save_e4, "GENERICO")
             else:
                st.info("Bandeja vacía.")
# ==============================================================================
# PESTAÑA 6: GESTIÓN DE CATÁLOGO (FINAL)
# ==============================================================================
with tabs[5]:
    st.subheader("🔧 Administración de Productos y Variantes")
    # --- BLOQUE DE REPARACIÓN DE BASE DE DATOS ---
with st.expander("🛠️ Reparar Error de ID Producto", expanded=True):
    st.warning("Usa esto solo una vez si te sale el error 'null value in column id_producto'.")
    
    if st.button("🔧 Reparar Tabla Productos"):
        try:
            with engine.connect() as conn:
                trans = conn.begin()
                
                # 1. Crear una secuencia si no existe
                conn.execute(text("CREATE SEQUENCE IF NOT EXISTS productos_id_seq;"))
                
                # 2. Sincronizar la secuencia con el ID más alto actual (para no repetir números)
                #    Si la tabla está vacía, empieza en 1.
                conn.execute(text("SELECT setval('productos_id_seq', COALESCE((SELECT MAX(id_producto) FROM Productos), 1));"))
                
                # 3. Vincular la secuencia a la columna id_producto
                conn.execute(text("ALTER TABLE Productos ALTER COLUMN id_producto SET DEFAULT nextval('productos_id_seq');"))
                
                trans.commit()
                st.success("✅ Tabla reparada. Ahora los IDs se generarán solos.")
        except Exception as e:
            st.error(f"Error al reparar: {e}")
# ---------------------------------------------
    # --- BARRA LATERAL: BUSCADOR RÁPIDO ---
    with st.expander("🔎 Verificador Rápido de SKU / Nombre", expanded=False):
        check_str = st.text_input("Escribe para buscar coincidencias:", placeholder="Ej: NL01")
        if check_str:
            with engine.connect() as conn:
                q_check = text("""
                    SELECT v.sku, p.modelo, p.nombre as color, v.medida 
                    FROM Variantes v 
                    JOIN Productos p ON v.id_producto = p.id_producto
                    WHERE v.sku ILIKE :s OR p.nombre ILIKE :s OR p.modelo ILIKE :s
                    LIMIT 10
                """)
                df_check = pd.read_sql(q_check, conn, params={"s": f"%{check_str}%"})
            if not df_check.empty:
                st.dataframe(df_check, hide_index=True)
            else:
                st.caption("✅ No se encontraron coincidencias.")

    st.divider()
    
    modo_catalogo = st.radio("Acción:", ["🌱 Crear Nuevo", "✏️ Editar / Renombrar"], horizontal=True)

    # LISTA OFICIAL DE COLORES
    COLORES_OFICIALES = ["", "Amarillo", "Azul", "Blanco", "Chocolate", "Dorado", "Gris", "Marrón", "Miel", "Morado", "Multicolor", "Naranja", "Negro", "Rojo", "Rosado", "Turquesa", "Verde"]

    # ------------------------------------------------------------------
    # MODO 1: CREAR NUEVO (ESTA PARTE SE MANTIENE IGUAL)
    # ------------------------------------------------------------------
    if modo_catalogo == "🌱 Crear Nuevo":
        tipo_creacion = st.selectbox("Tipo de Creación:", 
                                     ["Medida Nueva (Hijo) para Producto Existente", 
                                      "Producto Nuevo (Marca/Color Nuevo)"])
        
        # A) NUEVA MEDIDA
        if "Medida Nueva" in tipo_creacion:
            with engine.connect() as conn:
                df_prods = pd.read_sql(text("SELECT id_producto, marca, modelo, nombre FROM Productos ORDER BY marca, modelo, nombre"), conn)
            
            if not df_prods.empty:
                opciones_prod = df_prods.apply(lambda x: f"{x['marca']} {x['modelo']} - {x['nombre']} (ID: {x['id_producto']})", axis=1).to_dict()
                idx_prod = st.selectbox("Selecciona el Producto (Modelo y Color):", options=opciones_prod.keys(), format_func=lambda x: opciones_prod[x])
                id_producto_real = df_prods.iloc[idx_prod]['id_producto']
                
                with st.form("form_add_variante"):
                    st.caption(f"Agregando medida a: **{df_prods.iloc[idx_prod]['nombre']}**")
                    c1, c2 = st.columns(2)
                    sku_new = c1.text_input("Nuevo SKU (Único):").strip()
                    medida_new = c2.text_input("Medida / Graduación:", value="0.00")

                    c3, c4 = st.columns(2)
                    stock_ini = c3.number_input("Stock Inicial:", min_value=0)
                    precio_new = c4.number_input("Precio Venta:", min_value=0.0)
                    
                    ubi_new = st.text_input("Ubicación:")

                    if st.form_submit_button("Guardar Medida"):
                        try:
                            with engine.connect() as conn:
                                conn.execute(text("""
                                    INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, ubicacion)
                                    VALUES (:sku, :idp, '', :med, :si, :pre, :ubi)
                                """), {
                                    "sku": sku_new, "idp": int(id_producto_real), 
                                    "med": medida_new, "si": stock_ini, "pre": precio_new, "ubi": ubi_new
                                })
                                conn.commit()
                            st.success(f"SKU {sku_new} creado exitosamente.")
                        except Exception as e:
                            st.error(f"Error: {e}")

        # B) PRODUCTO NUEVO
        else:
            with st.form("form_new_full"):
                st.markdown("**1. Definir Producto (Visual)**")
                c1, c2, c3 = st.columns(3)
                marca = c1.text_input("Marca:")
                modelo = c2.text_input("Modelo:")
                nombre_prod = c3.text_input("Nombre (Color):", placeholder="Ej: Gris, Azul...")
                
                c_cat, c_col = st.columns(2)
                categ = c_cat.selectbox("Categoría:", ["Lentes Contacto", "Pelucas", "Accesorios", "Liquidos"])
                color_prin = c_col.selectbox("Color Filtro (Base):", COLORES_OFICIALES)

                c_dia, c_url1 = st.columns(2)
                diametro = c_dia.number_input("Diámetro (mm):", min_value=0.0, step=0.1, format="%.1f")
                url_img = c_url1.text_input("URL Imagen (Foto):")
                url_buy = st.text_input("URL Compra (Importación):")

                st.markdown("**2. Crear Primera Medida (Ej: Plano)**")
                c4, c5, c6 = st.columns(3)
                sku_1 = c4.text_input("SKU Variante:")
                medida_1 = c5.text_input("Medida:", value="0.00")
                prec_1 = c6.number_input("Precio Venta", 0.0)
                
                ubi_1 = st.text_input("Ubicación")

                if st.form_submit_button("Crear Producto Completo"):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            res_p = conn.execute(text("""
                                INSERT INTO Productos (marca, modelo, nombre, categoria, color_principal, diametro, url_imagen, url_compra) 
                                VALUES (:m, :mod, :nom, :cat, :col, :dia, :uimg, :ubuy) RETURNING id_producto
                            """), {
                                "m": marca, "mod": modelo, "nom": nombre_prod, "cat": categ, "col": color_prin, 
                                "dia": str(diametro), "uimg": url_img, "ubuy": url_buy
                            })
                            new_id = res_p.fetchone()[0]

                            conn.execute(text("""
                                INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, ubicacion)
                                VALUES (:sku, :idp, '', :med, 0, :pr, :ub)
                            """), {
                                "sku": sku_1, "idp": new_id, "med": medida_1,
                                "pr": prec_1, "ub": ubi_1
                            })
                            trans.commit()
                        st.success(f"Producto '{nombre_prod}' creado con éxito.")
                    except Exception as e:
                        st.error(f"Error: {e}")

    # ------------------------------------------------------------------
    # MODO 2: EDITAR / RENOMBRAR (AQUÍ ESTÁ EL CAMBIO)
    # ------------------------------------------------------------------
    else:
        st.markdown("#### ✏️ Modificar Producto")
        
        sku_edit = st.text_input("Ingresa SKU exacto para editar:", placeholder="Ej: NL152D-0000")
        
        if sku_edit:
            with engine.connect() as conn:
                query_full = text("""
                    SELECT v.*, p.marca, p.modelo, p.nombre as nombre_prod, p.categoria, p.diametro, p.color_principal, p.url_imagen, p.url_compra
                    FROM Variantes v 
                    JOIN Productos p ON v.id_producto = p.id_producto
                    WHERE v.sku = :sku
                """)
                df_data = pd.read_sql(query_full, conn, params={"sku": sku_edit})
            
            if not df_data.empty:
                curr = df_data.iloc[0]
                
                col_img, col_form = st.columns([1, 3])
                
                with col_img:
                    if curr['url_imagen']:
                        st.image(curr['url_imagen'], caption="Foto Actual", width='stretch')
                    else:
                        st.info("Sin imagen")

                with col_form:
                    st.info(f"Editando: **{curr['marca']} {curr['modelo']}** - Color: **{curr['nombre_prod']}**")
                    
                    with st.form("form_edit_sku"):
                        # 1. PRODUCTO
                        st.markdown("📦 **Datos Generales (Producto)**")
                        
                        c_p1, c_p2, c_p3 = st.columns(3)
                        new_marca = c_p1.text_input("Marca:", value=curr['marca'])
                        new_modelo = c_p2.text_input("Modelo:", value=curr['modelo'])
                        new_nombre_prod = c_p3.text_input("Nombre (Color):", value=curr['nombre_prod'])
                        
                        c_p4, c_p5 = st.columns(2)
                        idx_col = COLORES_OFICIALES.index(curr['color_principal']) if curr['color_principal'] in COLORES_OFICIALES else 0
                        new_color_prin = c_p4.selectbox("Color Filtro:", COLORES_OFICIALES, index=idx_col)
                        val_dia = float(curr['diametro']) if curr['diametro'] else 0.0
                        new_diametro = c_p5.number_input("Diámetro:", value=val_dia, step=0.1, format="%.1f")

                        new_url_img = st.text_input("URL Imagen:", value=curr['url_imagen'] if curr['url_imagen'] else "")
                        new_url_buy = st.text_input("URL Compra:", value=curr['url_compra'] if curr['url_compra'] else "")

                        st.divider()

                        # 2. VARIANTE (SKU y Medidas)
                        st.markdown(f"🏷️ **Datos de Variante ({curr['sku']})**")
                        col_a, col_b = st.columns(2)
                        new_sku_val = col_a.text_input("SKU:", value=curr['sku'])
                        new_medida = col_b.text_input("Medida:", value=curr['medida'] if curr['medida'] else "0.00")
                        
                        col_e, col_f = st.columns(2)
                        new_precio = col_e.number_input("Precio Normal:", value=float(curr['precio']))
                        
                        # --- CAMBIO AQUÍ: TEXT INPUT PARA PERMITIR VACÍO ---
                        # Si hay precio rebajado y es > 0, lo mostramos. Si es None o 0, mostramos vacío.
                        val_reb_str = str(curr['precio_rebajado']) if (curr['precio_rebajado'] and float(curr['precio_rebajado']) > 0) else ""
                        new_precio_reb_txt = col_f.text_input("Precio Rebajado (Vacío = Sin Oferta):", value=val_reb_str)

                        if st.form_submit_button("💾 Guardar Cambios"):
                            # Lógica para convertir el texto a Float o Null
                            final_rebajado = None
                            if new_precio_reb_txt.strip(): # Si no está vacío
                                try:
                                    final_rebajado = float(new_precio_reb_txt)
                                except:
                                    st.error("El precio rebajado debe ser un número (o dejarlo vacío).")
                                    st.stop()

                            try:
                                with engine.connect() as conn:
                                    trans = conn.begin()
                                    
                                    # A) Actualizar Variante
                                    conn.execute(text("""
                                        UPDATE Variantes 
                                        SET sku=:n_sku, medida=:n_med, precio=:n_pre, precio_rebajado=:n_prer
                                        WHERE sku=:old_sku
                                    """), {
                                        "n_sku": new_sku_val, "n_med": new_medida,
                                        "n_pre": new_precio, "n_prer": final_rebajado, # Pasamos el valor procesado
                                        "old_sku": curr['sku']
                                    })

                                    # B) Actualizar Producto
                                    conn.execute(text("""
                                        UPDATE Productos 
                                        SET marca=:mar, modelo=:mod, nombre=:nom, color_principal=:col, diametro=:dia,
                                            url_imagen=:uimg, url_compra=:ubuy
                                        WHERE id_producto=:idp
                                    """), {
                                        "mar": new_marca, "mod": new_modelo, "nom": new_nombre_prod,
                                        "col": new_color_prin, "dia": str(new_diametro), 
                                        "uimg": new_url_img, "ubuy": new_url_buy,
                                        "idp": int(curr['id_producto'])
                                    })
                                    
                                    trans.commit()
                                
                                st.success("✅ ¡Actualizado correctamente!")
                                time.sleep(1.5)
                                st.rerun()

                            except Exception as e:
                                st.error(f"Error: {e}")
        else:
            st.warning("SKU no encontrado.")
# --- Poner esto en un botón en tu App ---
if st.button("📢 Generar Feed para Facebook"):
    try:
        total = generar_feed_facebook()
        st.success(f"✅ Feed generado con {total} productos.")
        st.info("Tu URL para Facebook es: https://panelcontrol-production.up.railway.app/app/static/feed_facebook.csv") 
        # (Ojo: Tendrás que configurar tu servidor para que sirva este archivo)
    except Exception as e:
        st.error(f"Error: {e}")

from woocommerce import API

# ==============================================================================
# PESTAÑA 7: SINCRONIZACIÓN CON WORDPRESS
# ==============================================================================
# Agrega esto a tu lista de pestañas o usa una existente
with st.expander("🔄 Sincronizar Imágenes desde Web (WordPress)", expanded=False):
    st.info("Esta herramienta conecta con kmlentes.pe, descarga las fotos de los productos y las asocia a tu inventario usando el SKU.")
    
    # Formulario para las credenciales (para no dejarlas escritas en el código por seguridad)
    col_k1, col_k2 = st.columns(2)
    wc_key = col_k1.text_input("Consumer Key (ck_...)", type="password")
    wc_secret = col_k2.text_input("Consumer Secret (cs_...)", type="password")
    
    col_url = st.text_input("URL de tu tienda:", value="https://kmlentes.pe")

    if st.button("🚀 Iniciar Sincronización de Fotos"):
        if not wc_key or not wc_secret:
            st.error("Por favor ingresa las llaves de WooCommerce.")
        else:
            # 1. CONEXIÓN A WORDPRESS
            wcapi = API(
                url=col_url,
                consumer_key=wc_key,
                consumer_secret=wc_secret,
                version="wc/v3",
                timeout=30
            )
            
            st.caption("Conectando con la web... esto puede tardar unos minutos dependiendo de la cantidad de productos.")
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                # 2. DESCARGAR PRODUCTOS (PAGINACIÓN)
                # WooCommerce entrega los productos por páginas. Hay que recorrerlas todas.
                page = 1
                productos_web = []
                
                while True:
                    status_text.text(f"Descargando página {page} de la web...")
                    res = wcapi.get("products", params={"page": page, "per_page": 50})
                    
                    if res.status_code != 200:
                        st.error(f"Error al conectar: {res.status_code} - {res.text}")
                        break
                        
                    data = res.json()
                    if not data:
                        break # Se acabaron los productos
                    
                    productos_web.extend(data)
                    page += 1
                
                total_web = len(productos_web)
                status_text.text(f"✅ Se encontraron {total_web} productos en la web. Procesando imágenes...")
                progress_bar.progress(50)
                
                # 3. EXTRAER SKU Y FOTOS
                # Creamos un diccionario {SKU: URL_IMAGEN}
                mapa_imagenes = {}
                
                for p in productos_web:
                    # A) Productos Simples
                    if p['sku'] and p['images']:
                        mapa_imagenes[p['sku']] = p['images'][0]['src']
                    
                    # B) Productos Variables (si tienes variaciones con fotos propias)
                    # A veces WooCommerce manda las variaciones en un endpoint aparte, 
                    # pero intentaremos ver si el producto padre tiene la imagen principal correcta.
                    # (Si tus variaciones tienen fotos distintas, el código se complica un poco más,
                    #  pero por lo general la foto del padre sirve).
                
                st.write(f"📸 Se encontraron {len(mapa_imagenes)} productos con SKU y Foto en la web.")
                
                # 4. ACTUALIZAR BASE DE DATOS LOCAL
                count_updated = 0
                
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        # Recorremos el mapa y actualizamos
                        for sku_web, url_web in mapa_imagenes.items():
                            # Buscamos si existe ese SKU en Variantes y obtenemos su id_producto
                            # Luego actualizamos la tabla Productos
                            
                            # La lógica es: 
                            # 1. Buscar el id_producto asociado a ese SKU en Variantes
                            # 2. Actualizar url_imagen en Productos usando ese id_producto
                            
                            # Hacemos el UPDATE directo cruzando tablas (PostgreSQL permite esto)
                            # Ojo: Si varios SKU comparten el mismo id_producto (variantes), 
                            # se quedará con la foto del último SKU procesado.
                            
                            res_up = conn.execute(text("""
                                UPDATE Productos
                                SET url_imagen = :url
                                WHERE id_producto = (
                                    SELECT id_producto FROM Variantes WHERE sku = :sku LIMIT 1
                                )
                            """), {"url": url_web, "sku": sku_web})
                            
                            if res_up.rowcount > 0:
                                count_updated += 1
                        
                        trans.commit()
                        progress_bar.progress(100)
                        st.success(f"✨ ÉXITO: Se actualizaron las imágenes de {count_updated} productos en tu App.")
                        
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error en base de datos: {e}")

            except Exception as e:
                st.error(f"Error general: {e}")
# ==============================================================================
# PESTAÑA 7: FACTURACIÓN PENDIENTE 
# ==============================================================================
with tabs[6]: 
    st.subheader("🧾 Facturación Individual")
    st.info("Sistema protegido: No permite boletas duplicadas y formatea los nombres automáticamente.")

    # --- 1. CARGAR LISTA DE VENTAS PENDIENTES ---
    with engine.connect() as conn:
        query_pendientes = text("""
            SELECT 
                v.id_venta,
                c.nombre || ' ' || c.apellido as nombre_completo,
                v.fecha_venta,
                v.total_venta
            FROM Ventas v
            JOIN Clientes c ON v.id_cliente = c.id_cliente
            WHERE v.facturado = FALSE
            ORDER BY v.id_venta ASC
        """)
        df_pendientes = pd.read_sql(query_pendientes, conn)

    if df_pendientes.empty:
        st.success("🎉 ¡Felicidades! No hay facturas pendientes.")
    else:
        # --- 2. SELECTOR DE VENTA ---
        opciones_venta = df_pendientes['id_venta'].tolist()
        
        def formato_opcion(id_v):
            fila = df_pendientes[df_pendientes['id_venta'] == id_v]
            if not fila.empty:
                row = fila.iloc[0]
                return f"🆔 {row['id_venta']} | 📅 {row['fecha_venta']} | 👤 {row['nombre_completo']} | 💰 S/ {row['total_venta']}"
            return f"Venta {id_v}"

        seleccion_id = st.selectbox(
            "👇 Elige la venta a procesar:", 
            options=opciones_venta, 
            format_func=formato_opcion
        )

        st.divider()

        # --- 3. CARGAR DETALLES ---
        if seleccion_id:
            with engine.connect() as conn:
                # A) Datos Cliente
                query_cliente = text("""
                    SELECT c.id_cliente, c.nombre, c.apellido, c.dni, c.google_id, c.telefono 
                    FROM Ventas v JOIN Clientes c ON v.id_cliente = c.id_cliente 
                    WHERE v.id_venta = :id
                """)
                cliente_data = pd.read_sql(query_cliente, conn, params={"id": int(seleccion_id)}).iloc[0]

                # B) Ítems
                query_items = text("""
                    SELECT 
                        d.sku as "Código",
                        d.descripcion as "Descripción",
                        d.cantidad as "Cant.",
                        d.precio_unitario as "P.Unit",
                        (d.cantidad * d.precio_unitario) as "Total"
                    FROM DetalleVenta d
                    WHERE d.id_venta = :id
                    
                    UNION ALL
                    
                    SELECT 
                        'ENVIO' as "Código",
                        'Servicio de Envío' as "Descripción",
                        1 as "Cant.",
                        v.costo_envio as "P.Unit",
                        v.costo_envio as "Total"
                    FROM Ventas v
                    WHERE v.id_venta = :id AND v.costo_envio > 0
                """)
                df_items = pd.read_sql(query_items, conn, params={"id": int(seleccion_id)})

            # --- 4. INTERFAZ DE REGISTRO ---
            col_datos, col_tabla = st.columns([1, 2])
            
            with col_datos:
                st.markdown("#### 👤 Datos del Cliente")
                with st.form("form_facturacion"):
                    val_nombre = cliente_data['nombre'] if cliente_data['nombre'] else ""
                    val_apellido = cliente_data['apellido'] if cliente_data['apellido'] else ""
                    val_dni = cliente_data['dni'] if cliente_data['dni'] else ""

                    nuevo_nombre = st.text_input("Nombre", value=val_nombre)
                    nuevo_apellido = st.text_input("Apellido", value=val_apellido)
                    nuevo_dni = st.text_input("DNI / RUC", value=val_dni)
                    
                    st.markdown("---")
                    st.markdown("#### 🧾 Datos de Factura")
                    numero_boleta = st.text_input("N° Boleta (EB01...)", placeholder="Ingresa el número")
                    
                    btn_guardar = st.form_submit_button("✅ Guardar y Archivar", type="primary")
            
            with col_tabla:
                st.markdown(f"#### 🛒 Detalle de Items (Venta {seleccion_id})")
                st.dataframe(df_items, hide_index=True, width='stretch')
                st.caption("👆 Copia estas filas y pégalas en tu sistema contable.")

            # --- 5. LÓGICA DE GUARDADO (CORREGIDA) ---
            if btn_guardar:
                if not numero_boleta:
                    st.warning("⚠️ Debes ingresar el Número de Boleta para continuar.")
                else:
                    nombre_formateado = nuevo_nombre.strip().title() if nuevo_nombre else ""
                    apellido_formateado = nuevo_apellido.strip().title() if nuevo_apellido else ""
                    boleta_limpia = numero_boleta.strip().upper() 

                    with engine.connect() as conn:
                        # --- CAMBIO CLAVE: Abrimos transacción AL PRINCIPIO ---
                        trans = conn.begin() 
                        try:
                            # 1. VERIFICAR DUPLICADOS DENTRO DE LA TRANSACCIÓN
                            existe_boleta = conn.execute(
                                text("SELECT id_venta FROM Ventas WHERE numero_boleta = :b"),
                                {"b": boleta_limpia}
                            ).fetchone()

                            if existe_boleta:
                                # Si existe, no hacemos nada y mostramos error
                                st.error(f"⛔ ¡ERROR! La boleta '{boleta_limpia}' ya está registrada en la Venta #{existe_boleta[0]}.")
                                # No hace falta rollback porque solo leímos, pero salimos limpio.
                            else:
                                # 2. SI NO EXISTE, PROCEDEMOS A GUARDAR TODO
                                
                                # A. Actualizar Cliente
                                conn.execute(text("""
                                    UPDATE Clientes 
                                    SET nombre = :n, apellido = :a, dni = :d 
                                    WHERE id_cliente = :cid
                                """), {
                                    "n": nombre_formateado, 
                                    "a": apellido_formateado, 
                                    "d": nuevo_dni, 
                                    "cid": int(cliente_data['id_cliente'])
                                })

                                # B. Sincronizar Google
                                if cliente_data['google_id']:
                                    actualizar_en_google(
                                        cliente_data['google_id'], 
                                        nombre_formateado, 
                                        apellido_formateado, 
                                        cliente_data['telefono']
                                    )

                                # C. Actualizar Venta
                                conn.execute(text("""
                                    UPDATE Ventas 
                                    SET facturado = TRUE, 
                                        fecha_facturacion = CURRENT_DATE,
                                        numero_boleta = :bol
                                    WHERE id_venta = :vid
                                """), {
                                    "bol": boleta_limpia, 
                                    "vid": int(seleccion_id)
                                })
                                
                                # D. Confirmar todo
                                trans.commit()
                                st.balloons()
                                st.success(f"¡Correcto! Venta guardada con boleta {boleta_limpia}.")
                                time.sleep(1.5)
                                st.rerun()
                                
                        except Exception as e:
                            trans.rollback()
                            st.error(f"Error al guardar: {e}")
# ==============================================================================
# PESTAÑA CHAT CENTER (CORREGIDA Y ESTABLE)
# ==============================================================================
with tabs[7]: 
    st.subheader("💬 Chat Center")

    # 1. INICIALIZAR MEMORIA (Para que no se olvide a quién seleccionaste)
    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # Estilos CSS para que los botones parezcan tarjetas de chat
    st.markdown("""
    <style>
    div.stButton > button:first-child {
        text-align: left; 
        width: 100%;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2])

    # --- 1. IZQUIERDA: LISTA DE CONTACTOS ---
    with col_lista:
        st.markdown("#### 📩 Bandeja")
        
        # Traemos la lista de conversaciones agrupada por TELÉFONO
        # (Es lo más seguro para WhatsApp)
        with engine.connect() as conn:
            lista_chats = conn.execute(text("""
                SELECT 
                    m.telefono,
                    MAX(m.fecha) as ultima_fecha,
                    -- Intentamos buscar el nombre si existe en la tabla Clientes
                    COALESCE(MAX(c.nombre_corto), MAX(c.nombre) || ' ' || MAX(c.apellido), m.telefono) as nombre_mostrar,
                    SUM(CASE WHEN m.leido = FALSE AND m.tipo = 'ENTRANTE' THEN 1 ELSE 0 END) as no_leidos
                FROM mensajes m
                LEFT JOIN Clientes c ON m.telefono = c.telefono
                GROUP BY m.telefono
                ORDER BY ultima_fecha DESC
            """)).fetchall()

        if not lista_chats:
            st.info("📭 No hay mensajes.")

        # Generamos los BOTONES de la lista
        for chat in lista_chats:
            tel = chat.telefono
            nombre = chat.nombre_mostrar
            hora = chat.ultima_fecha.strftime('%d/%m %H:%M')
            notif = f"🔴 {chat.no_leidos}" if chat.no_leidos > 0 else ""
            icono = "👤"
            
            # Texto del botón
            label_btn = f"{icono} {nombre}\n⏱ {hora} {notif}"
            
            # Color del botón: Primary si está seleccionado, Secondary si no
            tipo = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"

            # SI SE PRESIONA EL BOTÓN:
            if st.button(label_btn, key=f"btn_{tel}", type=tipo):
                st.session_state['chat_actual_telefono'] = tel # Guardamos en memoria
                st.rerun() # Recargamos para mostrar el chat a la derecha


    # --- 2. DERECHA: VENTANA DE CHAT ---
    with col_chat:
        # Recuperamos el teléfono de la memoria
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            st.markdown(f"### 💬 Chat con: **{telefono_activo}**")
            st.divider()
            
            # Contenedor con scroll para mensajes
            contenedor_mensajes = st.container(height=500)
            
            # A. Obtener mensajes y Marcar como leídos
            with engine.connect() as conn:
                # 1. Marcar leídos
                conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE telefono = :t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                
                # 2. Leer historial
                historial = pd.read_sql(text("""
                    SELECT tipo, contenido, fecha 
                    FROM mensajes 
                    WHERE telefono = :t 
                    ORDER BY fecha ASC
                """), conn, params={"t": telefono_activo})

            # B. Dibujar mensajes
            with contenedor_mensajes:
                if historial.empty:
                    st.write("Inicia la conversación...")
                
                for _, row in historial.iterrows():
                    es_usuario = (row['tipo'] == 'ENTRANTE')
                    role = "user" if es_usuario else "assistant"
                    avatar = "👤" if es_usuario else "🛍️"
                    
                    with st.chat_message(role, avatar=avatar):
                        contenido = row['contenido']
                        
                        # --- Lógica Multimedia (Fotos/Audios) ---
                        if "|ID:" in contenido:
                            try:
                                partes = contenido.split("|ID:")
                                texto_visible = partes[0].strip()
                                media_id = partes[1].replace("|", "").strip()
                                
                                st.markdown(texto_visible) # Mostrar etiqueta
                                
                                # Descargar archivo real (Si tienes la función obtener_imagen_whatsapp)
                                archivo_bytes = obtener_imagen_whatsapp(media_id)
                                if archivo_bytes:
                                    if "[Audio]" in texto_visible:
                                        st.audio(archivo_bytes)
                                    elif "[Imagen]" in texto_visible:
                                        st.image(archivo_bytes, width=250)
                                    elif "[Documento]" in texto_visible:
                                        st.download_button("⬇️ Descargar", archivo_bytes, "archivo_whatsapp")
                            except:
                                st.error("Error cargando adjunto")
                        else:
                            st.markdown(contenido)
                        
                        st.caption(f"{row['fecha'].strftime('%H:%M')} - {row['tipo']}")

            # C. CAJA DE TEXTO (INPUT)
            if prompt := st.chat_input("Escribe tu respuesta..."):
                # 1. Enviar a Meta
                enviado_ok, resp = enviar_mensaje_whatsapp(telefono_activo, prompt)
                
                if enviado_ok:
                    # 2. Guardar en DB
                    # Asegurar formato 51 para guardar
                    tel_guardar = telefono_activo.replace("+", "").strip()
                    if len(tel_guardar) == 9: tel_guardar = f"51{tel_guardar}"

                    with engine.connect() as conn:
                        conn.execute(text("""
                            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido)
                            VALUES (:tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE)
                        """), {"tel": tel_guardar, "txt": prompt})
                        conn.commit()
                    st.rerun() # Recargar para ver el mensaje enviado
                else:
                    st.error(f"Error enviando: {resp}")

        else:
            # Pantalla de espera (Nadie seleccionado)
            st.markdown("<div style='text-align: center; margin-top: 50px; color: gray;'>", unsafe_allow_html=True)
            st.markdown("### 👈 Selecciona un cliente de la lista")
            st.markdown("Para ver el historial y responder.")
            st.markdown("</div>", unsafe_allow_html=True)

            