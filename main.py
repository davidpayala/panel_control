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
# PESTAÑA 1: VENTAS / SALIDAS (CON MULTI-DIRECCIÓN)
# ==============================================================================
with tabs[0]:
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
    # COLUMNA IZQUIERDA: BUSCADOR (Igual que antes)
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
                    # Nombre compuesto mejorado
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
            st.dataframe(df_cart[['descripcion', 'cantidad', 'subtotal']], width='stretch', hide_index=True)
            
            suma_subtotal = float(df_cart['subtotal'].sum())
            
            st.divider()

            # ==========================================================
            # MODO A: VENTA (Con Cliente y Selección de Dirección)
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
                tipo_envio = col_e1.selectbox("Método Envío", ["Gratis", "Express (Moto)", "Agencia (Pago Destino)", "Agencia (Pagado)"])
                costo_envio = col_e2.number_input("Costo Envío", value=0.0)

                # 3. SELECCIÓN DE DIRECCIÓN (Lógica Nueva)
                es_agencia = "Agencia" in tipo_envio
                cat_direccion = "AGENCIA" if es_agencia else "MOTO"
                
                # Buscamos TODAS las direcciones activas
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
                
                # Preparamos las opciones para el SelectBox
                opciones_visuales = {}
                if not df_dirs.empty:
                    for idx, row in df_dirs.iterrows():
                        # Creamos una etiqueta bonita para identificar la dirección
                        if es_agencia:
                            lbl = f"🏢 {row['agencia_nombre']} - {row['sede_entrega']} (Recibe: {row['nombre_receptor']})"
                        else:
                            lbl = f"🏠 {row['direccion_texto']} ({row['distrito']})"
                        
                        if row['observacion']:
                            lbl += f" | 👁️ {row['observacion']}"
                            
                        opciones_visuales[lbl] = row

                # Opción Especial para Nueva Dirección
                KEY_NUEVA = "➕ Usar una Nueva Dirección..."
                lista_desplegable = list(opciones_visuales.keys()) + [KEY_NUEVA]
                
                # WIDGET SELECTOR
                st.markdown("📍 **Destino del Pedido:**")
                seleccion_dir = st.selectbox("Elige la dirección:", options=lista_desplegable, label_visibility="collapsed")
                
                # --- LÓGICA DE SELECCIÓN ---
                if seleccion_dir != KEY_NUEVA:
                    # CASO: Dirección Guardada
                    usar_guardada = True
                    dir_data = opciones_visuales[seleccion_dir]
                    
                    if es_agencia:
                        texto_direccion_final = f"{dir_data['agencia_nombre']} - {dir_data['sede_entrega']} [{dir_data['dni_receptor']}]"
                        st.info(f"✅ Enviar a: **{texto_direccion_final}**")
                    else:
                        texto_direccion_final = f"{dir_data['direccion_texto']} - {dir_data['distrito']}"
                        st.info(f"✅ Enviar a: **{texto_direccion_final}**\n\nRef: {dir_data['referencia']}")
                    
                    if dir_data['observacion']:
                        st.caption(f"📝 Obs: {dir_data['observacion']}")

                else:
                    # CASO: Nueva Dirección (Formulario)
                    st.warning("📝 Ingresa los nuevos datos:")
                    with st.container(border=True):
                        recibe = st.text_input("Recibe:", value=nombre_cli)
                        telf = st.text_input("Teléfono:", key="telf_new")
                        
                        obs_new = st.text_input("Observaciones:", placeholder="Fachada, timbre, pago destino...")

                        if es_agencia:
                            dni = st.text_input("DNI:")
                            agencia = st.text_input("Agencia:", value="Shalom")
                            sede = st.text_input("Sede:")
                            datos_nuevos = {
                                "tipo": "AGENCIA", "nom": recibe, "tel": telf, "dni": dni, 
                                "age": agencia, "sede": sede, "obs": obs_new,
                                "dir": "", "dist": "", "ref": ""
                            }
                            texto_direccion_final = f"{agencia} - {sede}"
                        else:
                            direcc = st.text_input("Dirección:")
                            dist = st.text_input("Distrito:")
                            datos_nuevos = {
                                "tipo": "MOTO", "nom": recibe, "tel": telf, 
                                "dir": direcc, "dist": dist, "obs": obs_new,
                                "ref": "", "gps": "", "dni": "", "age": "", "sede": ""
                            }
                            texto_direccion_final = f"{direcc} - {dist}"

                # 4. CLAVE DE AGENCIA
                clave_agencia = None
                if es_agencia:
                    if 'clave_temp' not in st.session_state: st.session_state['clave_temp'] = str(random.randint(1000, 9999))
                    col_k1, col_k2 = st.columns([1,2])
                    clave_agencia = col_k1.text_input("Clave", value=st.session_state['clave_temp'])
                    col_k2.info("🔐 Clave Entrega")

                # TOTALES
                total_final = suma_subtotal + costo_envio
                st.markdown(f"### Total: S/ {total_final:.2f}")
                nota_venta = st.text_input("Nota Venta:")

                if st.button("✅ FINALIZAR VENTA", type="primary"):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            
                            # A) Guardar Dirección Nueva si aplica
                            if not usar_guardada and datos_nuevos:
                                conn.execute(text("""
                                    INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, 
                                    direccion_texto, distrito, dni_receptor, agencia_nombre, sede_entrega, observacion, activo)
                                    VALUES (:id, :tipo, :nom, :tel, :dir, :dist, :dni, :age, :sede, :obs, TRUE)
                                """), {"id": id_cliente, **datos_nuevos})

                            # B) Registrar Venta
                            nota_full = f"{nota_venta} | Envío: {texto_direccion_final}"
                            res_v = conn.execute(text("""
                                INSERT INTO Ventas (id_cliente, tipo_envio, costo_envio, total_venta, nota, clave_seguridad)
                                VALUES (:idc, :tipo, :costo, :total, :nota, :clave) RETURNING id_venta
                            """), {"idc": id_cliente, "tipo": tipo_envio, "costo": costo_envio, "total": total_final, "nota": nota_full, "clave": clave_agencia})
                            id_venta = res_v.fetchone()[0]

                            # C) Detalles y Stock
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
                        st.success("¡Venta Exitosa!")
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
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            res_v = conn.execute(text("""
                                INSERT INTO Ventas (tipo_envio, costo_envio, total_venta, nota, id_cliente)
                                VALUES ('SALIDA', 0, 0, :nota, NULL) RETURNING id_venta
                            """), {"nota": f"[{motivo_salida}] {detalle_motivo}"})
                            id_salida = res_v.fetchone()[0]

                            for item in st.session_state.carrito:
                                conn.execute(text("""
                                    INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario)
                                    VALUES (:idv, :sku, :desc, :cant, 0, 0, :inv)
                                """), {"idv": id_salida, "sku": item['sku'], "desc": item['descripcion'], "cant": int(item['cantidad']), "inv": item['es_inventario']})
                                
                                if item['es_inventario']:
                                    res_s = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno - :c WHERE sku=:s RETURNING stock_interno"),
                                                 {"c": int(item['cantidad']), "s": item['sku']})
                                    nuevo_s = res_s.scalar()
                                    if nuevo_s <= 0: conn.execute(text("UPDATE Variantes SET ubicacion = '' WHERE sku=:s"), {"s": item['sku']})
                                    
                                    conn.execute(text("""
                                        INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                        VALUES (:sku, 'SALIDA', :c, (SELECT stock_interno + :c FROM Variantes WHERE sku=:sku), :nue, :nota)
                                    """), {"sku": item['sku'], "c": int(item['cantidad']), "nue": nuevo_s, "nota": f"Salida #{id_salida} | {motivo_salida}"})
                            
                            trans.commit()
                        st.success(f"✅ Salida #{id_salida} registrada correctamente.")
                        st.session_state.carrito = []
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                         # Si falla por NULL en id_cliente (depende config DB), manejarlo
                        if "null value in column" in str(e).lower() and "id_cliente" in str(e).lower():
                            st.error("Error: La base de datos requiere Cliente. Crea un cliente 'Interno' y modifica el código para usar su ID.")
                        else:
                            st.error(f"Error al registrar salida: {e}")

        else:
            st.info("El carrito está vacío.")
            
        if st.button("🗑️ Limpiar Todo"):
            st.session_state.carrito = []
            st.rerun()

# ==============================================================================
# PESTAÑA 2: COMPRAS (CORREGIDO: 2026 + NUMPY + WIDTH STRETCH)
# ==============================================================================
with tabs[1]:
    st.subheader("📦 Gestión de Compras y Reposición")
    
    tab_asistente, tab_registro = st.tabs(["💡 Asistente de Reposición (IA)", "📝 Registrar Ingreso Manual"])
    
    # --- A) ASISTENTE INTELIGENTE ---
    with tab_asistente:
        # 1. CONTROLES
        with st.container(border=True):
            c_filtros, c_acciones = st.columns([3, 1])
            with c_filtros:
                st.markdown("**Configuración del Reporte**")
                col_f1, col_f2 = st.columns(2)
                umbral_stock = col_f1.slider("Mostrar productos con Stock menor a:", 0, 20, 1)
                solo_con_externo = col_f2.checkbox("Solo lo que tiene el Proveedor (Stock Ext. > 0)", value=True)
            
            with c_acciones:
                st.write("")
                if st.button("🔄 Actualizar Datos", type="primary", width='stretch'):
                    st.rerun()

        # 2. DEFINIR AÑOS DINÁMICAMENTE
        year_actual = datetime.now().year 
        y1, y2, y3 = year_actual, year_actual - 1, year_actual - 2 

        # Función auxiliar para evitar error de columna inexistente (2026, 2027...)
        def get_hist_sql(year):
            if year <= 2025: 
                return f"COALESCE(h.v{year}, 0)"
            else:
                return "0" 

        # 3. CONSULTA HÍBRIDA
        with engine.connect() as conn:
            try:
                hist_y3 = get_hist_sql(y3)
                hist_y2 = get_hist_sql(y2)
                hist_y1 = get_hist_sql(y1)

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
                        ({hist_y3} + COALESCE(live.sql_y3, 0)) as venta_year_3,
                        ({hist_y2} + COALESCE(live.sql_y2, 0)) as venta_year_2,
                        ({hist_y1} + COALESCE(live.sql_y1, 0)) as venta_year_1
                    FROM Variantes v
                    JOIN Productos p ON v.id_producto = p.id_producto
                    LEFT JOIN HistorialAnual h ON v.sku = h.sku
                    LEFT JOIN VentasSQL live ON v.sku = live.sku
                    WHERE v.stock_interno <= :umbral
                """)
                
                df_reco = pd.read_sql(query_hybrid, conn, params={
                    "umbral": umbral_stock,
                    "y1": y1, "y2": y2, "y3": y3
                })
                
                if not df_reco.empty:
                    df_reco['demanda_historica'] = (
                        df_reco['venta_year_1'] + df_reco['venta_year_2'] + df_reco['venta_year_3']
                    )

            except Exception as e:
                st.error(f"⚠️ Error en consulta: {e}")
                df_reco = pd.DataFrame()

        # 4. FILTROS
        if not df_reco.empty:
            df_reco['sku'] = df_reco['sku'].astype(str).str.strip()
            if solo_con_externo:
                df_reco = df_reco[df_reco['stock_externo'] > 0]

            patron_medida = r'-\d{4}$'
            es_medida = df_reco['sku'].str.contains(patron_medida, regex=True, na=False)
            es_base = df_reco['sku'].str.endswith('-0000', na=False)
            df_reco = df_reco[~es_medida | es_base]

            df_reco = df_reco.sort_values(by='demanda_historica', ascending=False)

        # 5. VISUALIZACIÓN
        val_max = int(df_reco['demanda_historica'].max()) if not df_reco.empty else 10
        if val_max == 0: val_max = 10

        st.divider()
        col_res_txt, col_res_btn = st.columns([3, 1])
        with col_res_txt:
            st.markdown(f"### 📋 Lista Sugerida ({len(df_reco)} modelos)")
            if not df_reco.empty:
                st.caption(f"🔥 Top #1: **{df_reco.iloc[0]['nombre']}**")

        with col_res_btn:
            if not df_reco.empty:
                import io
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_reco.to_excel(writer, index=False, sheet_name='Reposicion')
                
                st.download_button(
                    label="📥 Descargar Excel",
                    data=buffer.getvalue(),
                    file_name=f"Reposicion_{date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width='stretch' # En botones sí se usa este nombre
                )

        st.dataframe(
            df_reco,
            column_config={
                "sku": "SKU",
                "nombre": st.column_config.TextColumn("Producto", width="large"),
                "stock_interno": st.column_config.NumberColumn("Mi Stock", format="%d"),
                "stock_externo": st.column_config.NumberColumn("Prov.", format="%d"),
                "demanda_historica": st.column_config.ProgressColumn(
                    "Demanda 3 Años", format="%d", min_value=0, max_value=val_max
                ),
                "venta_year_3": st.column_config.NumberColumn(f"{y3}", width="small"),
                "venta_year_2": st.column_config.NumberColumn(f"{y2}", width="small"),
                "venta_year_1": st.column_config.NumberColumn(f"{y1}", width="small"), 
            },
            hide_index=True,
            width='stretch' # <--- CORREGIDO
        )

# --- B) REGISTRO MANUAL (CON EDICIÓN DE UBICACIÓN) ---
    with tab_registro:
        st.subheader("📦 Ingreso de Mercadería")
        sku_compra = st.text_input("SKU Producto a ingresar:", key="sku_compra_tab2")
        
        if sku_compra:
            with engine.connect() as conn:
                # CAMBIO 1: Traemos también la ubicación
                res = pd.read_sql(text("SELECT sku, stock_interno, ubicacion FROM Variantes WHERE sku = :s"), conn, params={"s": sku_compra})
            
            if not res.empty:
                # Convertimos a int/str nativos para evitar problemas
                curr_stock = int(res.iloc[0]['stock_interno'])
                curr_ubi = str(res.iloc[0]['ubicacion']) if res.iloc[0]['ubicacion'] else ""
                
                st.info(f"📊 Stock Actual: **{curr_stock}**")
                
                with st.form("form_ingreso_manual"):
                    c1, c2 = st.columns(2)
                    cant_ingreso = c1.number_input("Cantidad a sumar (+):", min_value=1, step=1)
                    # CAMBIO 2: Campo para ver y editar la ubicación
                    ubi_ingreso = c2.text_input("Ubicación (Editar si es necesario):", value=curr_ubi)
                    
                    nota_ingreso = st.text_input("Nota / Proveedor:")
                    
                    if st.form_submit_button("💾 Registrar Entrada y Actualizar", width='stretch'):
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                nuevo_st = int(curr_stock + cant_ingreso)
                                
                                # CAMBIO 3: El UPDATE ahora actualiza stock Y ubicación
                                conn.execute(text("UPDATE Variantes SET stock_interno = :n, ubicacion = :u WHERE sku=:s"), 
                                            {"n": nuevo_st, "u": ubi_ingreso, "s": sku_compra})
                                
                                conn.execute(text("""
                                    INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                    VALUES (:s, 'COMPRA', :c, :ant, :nue, :nota)
                                """), {
                                    "s": sku_compra, 
                                    "c": int(cant_ingreso), 
                                    "ant": curr_stock, 
                                    "nue": nuevo_st, 
                                    "nota": nota_ingreso
                                })
                                
                                trans.commit()
                                st.success(f"✅ Stock actualizado a {nuevo_st}. Ubicación: {ubi_ingreso}")
                                time.sleep(1.5)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"Error: {e}")
            else:
                st.warning("⚠️ SKU no encontrado. Ve a la pestaña 'Catálogo' para crearlo primero.")

# ==============================================================================
# PESTAÑA 3: INVENTARIO (VISTA DETALLADA Y UBICACIONES)
# ==============================================================================
with tabs[2]:
    st.subheader("🔎 Gestión de Inventario Detallado")

    # --- 1. BARRA DE HERRAMIENTAS ---
    col_search, col_btn = st.columns([4, 1])
    with col_search:
        filtro_inv = st.text_input("🔍 Buscar:", placeholder="Escribe SKU, Marca, Modelo o Ubicación...")
    with col_btn:
        st.write("") # Espaciador
        if st.button("🔄 Recargar Tabla"):
            if 'df_inventario' in st.session_state: del st.session_state['df_inventario']
            st.rerun()

    # --- 2. CARGA DE DATOS ---
    if 'df_inventario' not in st.session_state:
        with engine.connect() as conn:
            # Traemos las columnas RAW de ambas tablas
            # Usamos COALESCE para que si algún campo está vacío no salga 'None'
            q_inv = """
                SELECT 
                    v.sku, 
                    p.categoria,
                    p.marca, 
                    p.modelo, 
                    v.nombre_variante,
                    p.color_principal, 
                    p.diametro, 
                    v.medida,
                    v.stock_interno,
                    v.stock_externo,
                    v.ubicacion
                FROM Variantes v
                JOIN Productos p ON v.id_producto = p.id_producto
                ORDER BY p.marca, p.modelo, v.sku ASC
            """
            st.session_state.df_inventario = pd.read_sql(text(q_inv), conn)

    # Trabajamos con una copia
    df_calc = st.session_state.df_inventario.copy()

    # --- 3. CREACIÓN DE COLUMNAS COMBINADAS (Python) ---
    # Esto es más seguro hacerlo en Python para manejar formatos y nulos fácilmente
    
    # A) Columna NOMBRE: Marca + Modelo + Variante
    df_calc['nombre_completo'] = (
        df_calc['marca'].fillna('') + " " + 
        df_calc['modelo'].fillna('') + " - " + 
        df_calc['nombre_variante'].fillna('')
    ).str.strip()

    # B) Columna DETALLES: ColorPrin + Diametro + Medida
    # Función auxiliar para formatear bonito
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
            df_calc['ubicacion'].str.lower().str.contains(f, na=False)
        ]

    # Seleccionamos y ordenamos SOLO las columnas que pediste ver
    df_final = df_calc[[
        'sku', 
        'categoria', 
        'nombre_completo', 
        'detalles_info', 
        'stock_interno', 
        'stock_externo', 
        'ubicacion'
    ]]

    # --- 5. TABLA EDITABLE ---
    st.caption("📝 Solo la columna **'Ubicación'** es editable.")
    
    cambios_inv = st.data_editor(
        df_final,
        key="editor_inventario_v2",
        column_config={
            "sku": st.column_config.TextColumn("SKU", disabled=True, width="small"),
            "categoria": st.column_config.TextColumn("Cat.", disabled=True, width="small"),
            "nombre_completo": st.column_config.TextColumn("Nombre del Producto", disabled=True, width="large"),
            "detalles_info": st.column_config.TextColumn("Detalles Técnicos", disabled=True, width="medium"),
            "stock_interno": st.column_config.NumberColumn("S. Int.", disabled=True, format="%d"),
            "stock_externo": st.column_config.NumberColumn("S. Ext.", disabled=True, format="%d"),
            "ubicacion": st.column_config.TextColumn("Ubicación 📍", required=False, width="small")
        },
        hide_index=True,
        width='stretch',
        num_rows="fixed"
    )

    # --- 6. GUARDAR CAMBIOS ---
    edited_rows = st.session_state["editor_inventario_v2"].get("edited_rows")

    if edited_rows:
        st.info(f"💾 Tienes {len(edited_rows)} cambios de ubicación pendientes...")
        
        if st.button("Confirmar Cambios"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    count = 0
                    for idx, updates in edited_rows.items():
                        # Recuperamos el SKU usando el índice del dataframe visual
                        sku_target = df_final.iloc[idx]['sku']
                        nueva_ubi = updates.get('ubicacion')
                        
                        if nueva_ubi is not None:
                            conn.execute(
                                text("UPDATE Variantes SET ubicacion = :u WHERE sku = :s"),
                                {"u": nueva_ubi, "s": sku_target}
                            )
                            count += 1
                    
                    trans.commit()
                    st.success(f"✅ ¡Se actualizaron {count} ubicaciones!")
                    del st.session_state['df_inventario'] # Limpiar caché
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error: {e}")

# ==============================================================================
# PESTAÑA 4: GESTIÓN DE CLIENTES (COMPLETA)
# ==============================================================================
with tabs[3]:
    st.subheader("👥 Gestión de Clientes")

    # --- SECCIÓN 1: CREAR NUEVO CLIENTE ---
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

# --- SECCIÓN 2: BUSCADOR Y EDICIÓN RÁPIDA ---
    st.divider()
    st.subheader("🔍 Buscar y Editar Clientes")
    
    col_search, col_btn = st.columns([3, 1])
    with col_search:
        # Buscador: Permite buscar por nombre o teléfono
        busqueda = st.text_input("Escribe el nombre o teléfono del cliente:", placeholder="Ej: Maria, 999...")

    # LOGICA DE BÚSQUEDA
    df_resultados = pd.DataFrame()
    
    if busqueda:
        with engine.connect() as conn:
            # Usamos ILIKE para que no importen mayúsculas/minúsculas
            query = text("""
                SELECT id_cliente, nombre_corto, nombre, apellido, telefono, google_id 
                FROM Clientes 
                WHERE (nombre_corto ILIKE :b OR telefono ILIKE :b) AND activo = TRUE 
                ORDER BY nombre_corto ASC LIMIT 20
            """)
            df_resultados = pd.read_sql(query, conn, params={"b": f"%{busqueda}%"})
    else:
        st.info("👆 Escribe arriba para buscar. (La lista completa está oculta para mayor velocidad)")

    # MOSTRAR RESULTADOS SI HAY
    if not df_resultados.empty:
        st.caption(f"Se encontraron {len(df_resultados)} resultados.")
        
        cambios = st.data_editor(
            df_resultados,
            key="editor_busqueda",
            column_config={
                "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "google_id": None, # Oculto
                "nombre_corto": st.column_config.TextColumn("Nombre Completo (Base)", disabled=True),
                "nombre": st.column_config.TextColumn("Nombre (Google)", required=True),
                "apellido": st.column_config.TextColumn("Apellido (Google)", required=True),
                "telefono": st.column_config.TextColumn("Teléfono", required=True)
            },
            hide_index=True,
            width='stretch'
        )

        if st.button("💾 Guardar Cambios"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for idx, row in cambios.iterrows():
                        # 1. Actualizamos la Base de Datos
                        conn.execute(text("""
                            UPDATE Clientes 
                            SET nombre=:n, apellido=:a, telefono=:t
                            WHERE id_cliente=:id
                        """), {
                            "n": row['nombre'], "a": row['apellido'], 
                            "t": row['telefono'], "id": row['id_cliente']
                        })
                        
                        # 2. Sincronizamos con Google (Si tiene ID)
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
# PESTAÑA 5: SEGUIMIENTO (LIMPIO Y ZONIFICADO)
# ==============================================================================
with tabs[4]:
    st.subheader("🎯 Tablero de Seguimiento de Pedidos")

    # --- 1. DEFINICIÓN DE ETAPAS ---
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
                c.medio_contacto, c.codigo_contacto,
                v.clave_seguridad as ultima_clave,
                v.total_venta as ultimo_total,
                (
                    SELECT STRING_AGG(d.cantidad || 'x ' || d.descripcion, ', ')
                    FROM DetalleVenta d
                    WHERE d.id_venta = v.id_venta
                ) as resumen_items
            FROM Clientes c
            LEFT JOIN LATERAL (
                SELECT * FROM Ventas v2
                WHERE v2.id_cliente = c.id_cliente
                ORDER BY v2.id_venta DESC
                LIMIT 1
            ) v ON TRUE
            WHERE c.activo = TRUE 
            ORDER BY c.fecha_seguimiento ASC
        """)
        df_seg = pd.read_sql(query_seg, conn)

    # --- 3. PROCESAMIENTO ---
    if not df_seg.empty:
        # Filtramos DataFrames (Ignoramos Etapa 0 completamente)
        df_e1 = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_1"])].copy()
        df_e2 = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_2"])].copy()
        df_e3 = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_3"])].copy()
        df_e4 = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_4"])].copy()

        # --- MÉTRICAS (SOLO LO IMPORTANTE) ---
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🔥 Por Despachar", len(df_e2), border=True)
        c2.metric("🚚 En Ruta", len(df_e3), border=True)
        c3.metric("💬 Cotizando", len(df_e1))
        c4.metric("✨ Post-Venta", len(df_e4))
        
        st.divider()

        # --- CONFIGURACIÓN DE COLUMNAS ---
        cfg_cols = {
            "id_cliente": None, "telefono": None,
            "nombre_corto": st.column_config.TextColumn("Cliente", width="medium"),
            "medio_contacto": st.column_config.TextColumn("Medio", width="small"),
            "codigo_contacto": st.column_config.TextColumn("Link/Código", width="small"),
            "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS, width="medium", required=True),
            "fecha_seguimiento": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
            "ultima_clave": st.column_config.TextColumn("🔐 Clave", disabled=True, width="small"),
            "ultimo_total": st.column_config.NumberColumn("💰 Total", format="S/ %.2f", disabled=True),
            "resumen_items": st.column_config.TextColumn("🛒 Historial / Últ. Compra", width="large", disabled=True)
        }

        # ==================================================================
        # 🚨 ZONA ROJA: OPERACIONES (VISIBLES SIEMPRE)
        # ==================================================================
        
        # --- ZONA 2: POR DESPACHAR ---
        st.markdown("### 🔥 Zona Operativa: Por Despachar")
        if not df_e2.empty:
            edit_e2 = st.data_editor(df_e2, key="ed_e2", column_config=cfg_cols, hide_index=True, width='stretch')
            if st.button("💾 Guardar Cambios (Despacho)"): actualizar_estados(edit_e2)
        else:
            st.info("✅ Bandeja de despacho vacía.")

        st.divider()

        # --- ZONA 3: EN RUTA ---
        st.markdown("### 🚚 Zona Logística: En Ruta")
        if not df_e3.empty:
            edit_e3 = st.data_editor(df_e3, key="ed_e3", column_config=cfg_cols, hide_index=True, width='stretch')
            if st.button("💾 Guardar Cambios (Ruta)"): actualizar_estados(edit_e3)
        else:
            st.info("✅ No hay pedidos en tránsito.")

        st.divider()

        # ==================================================================
        # 📂 ZONA DE GESTIÓN (OCULTAS EN ACORDEÓN)
        # ==================================================================
        st.markdown("### 📂 Bandejas de Gestión")

        # --- ZONA 1: CONVERSACIÓN ---
        with st.expander(f"💬 Etapa 1: En Conversación / Cotizando ({len(df_e1)})", expanded=False):
            if not df_e1.empty:
                st.caption("Prospectos interesados o proveedores.")
                edit_e1 = st.data_editor(df_e1, key="ed_e1", column_config=cfg_cols, hide_index=True, width='stretch')
                if st.button("💾 Guardar (Conversación)"): actualizar_estados(edit_e1)
            else:
                st.info("No hay clientes en esta etapa.")

        # --- ZONA 4: POST-VENTA ---
        with st.expander(f"✨ Etapa 4: Post-Venta y Fidelización ({len(df_e4)})", expanded=False):
            if not df_e4.empty:
                st.caption("Clientes que ya recibieron.")
                edit_e4 = st.data_editor(df_e4, key="ed_e4", column_config=cfg_cols, hide_index=True, width='stretch')
                if st.button("💾 Guardar (Post-Venta)"): actualizar_estados(edit_e4)
            else:
                st.info("No hay pendientes de post-venta.")

        # --- AQUÍ TERMINA EL CÓDIGO (Ya no hay calendario ni etapa 0) ---

    else:
        st.info("No hay clientes activos en la base de datos.")

# ==============================================================================
# PESTAÑA 6: GESTIÓN DE CATÁLOGO (FINAL)
# ==============================================================================
with tabs[5]:
    st.subheader("🔧 Administración de Productos y Variantes")
    
    # --- BARRA LATERAL: BUSCADOR RÁPIDO DE SKU (Para verificar duplicados) ---
    with st.expander("🔎 Verificador Rápido de SKU / Nombre", expanded=False):
        check_str = st.text_input("Escribe para buscar coincidencias:", placeholder="Ej: NL01")
        if check_str:
            with engine.connect() as conn:
                # Busca coincidencias en SKU o en el Nombre del Producto
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
    # MODO 1: CREAR NUEVO
    # ------------------------------------------------------------------
    if modo_catalogo == "🌱 Crear Nuevo":
        tipo_creacion = st.selectbox("Tipo de Creación:", 
                                     ["Medida Nueva (Hijo) para Producto Existente", 
                                      "Producto Nuevo (Marca/Color Nuevo)"])
        
        # A) NUEVA MEDIDA (Variante)
        if "Medida Nueva" in tipo_creacion:
            with engine.connect() as conn:
                # Ahora mostramos Marca - Modelo - NOMBRE (Color)
                df_prods = pd.read_sql(text("SELECT id_producto, marca, modelo, nombre FROM Productos ORDER BY marca, modelo, nombre"), conn)
            
            if not df_prods.empty:
                # Helper para el dropdown
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
                                # Ya no pedimos nombre_variante ni stock_externo aquí
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

        # B) PRODUCTO NUEVO (Marca + Modelo + Color)
        else:
            with st.form("form_new_full"):
                st.markdown("**1. Definir Producto (Visual)**")
                c1, c2, c3 = st.columns(3)
                marca = c1.text_input("Marca:")
                modelo = c2.text_input("Modelo:")
                # AQUI va el Nombre (Color) ahora
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
                            # Insertamos 'nombre' en Productos
                            res_p = conn.execute(text("""
                                INSERT INTO Productos (marca, modelo, nombre, categoria, color_principal, diametro, url_imagen, url_compra) 
                                VALUES (:m, :mod, :nom, :cat, :col, :dia, :uimg, :ubuy) RETURNING id_producto
                            """), {
                                "m": marca, "mod": modelo, "nom": nombre_prod, "cat": categ, "col": color_prin, 
                                "dia": str(diametro), "uimg": url_img, "ubuy": url_buy
                            })
                            new_id = res_p.fetchone()[0]

                            # Crear Variante (Medida)
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
    # MODO 2: EDITAR / RENOMBRAR
    # ------------------------------------------------------------------
    else:
        st.markdown("#### ✏️ Modificar Producto")
        
        sku_edit = st.text_input("Ingresa SKU exacto para editar:", placeholder="Ej: NL152D-0000")
        
        if sku_edit:
            # Traemos 'nombre' de Productos
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
                
                # --- VISUALIZACIÓN IMAGEN ---
                col_img, col_form = st.columns([1, 3])
                
                with col_img:
                    if curr['url_imagen']:
                        st.image(curr['url_imagen'], caption="Foto Actual", width='stretch')
                    else:
                        st.info("Sin imagen")

                with col_form:
                    st.info(f"Editando: **{curr['marca']} {curr['modelo']}** - Color: **{curr['nombre_prod']}**")
                    
                    with st.form("form_edit_sku"):
                        # 1. PRODUCTO (Ahora incluye el Nombre/Color)
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

                        # 2. VARIANTE (SKU y Medidas) - STOCK PROV OCULTO
                        st.markdown(f"🏷️ **Datos de Variante ({curr['sku']})**")
                        col_a, col_b = st.columns(2)
                        new_sku_val = col_a.text_input("SKU:", value=curr['sku'])
                        new_medida = col_b.text_input("Medida:", value=curr['medida'] if curr['medida'] else "0.00")
                        
                        col_e, col_f = st.columns(2)
                        new_precio = col_e.number_input("Precio:", value=float(curr['precio']))
                        new_precio_reb = col_f.number_input("Precio Rebajado:", value=float(curr['precio_rebajado'] if curr['precio_rebajado'] else 0.0))

                        if st.form_submit_button("💾 Guardar Cambios"):
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
                                        "n_pre": new_precio, "n_prer": new_precio_reb,
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
                                
                                st.success("✅ ¡Actualizado!")
                                time.sleep(1.5)
                                st.rerun()

                            except Exception as e:
                                st.error(f"Error: {e}")
            else:
                st.warning("SKU no encontrado.")

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
# PESTAÑA 8: CHAT CRM (BETA)
# ==============================================================================
with tabs[7]: 
    st.subheader("💬 Chat Center")
    
    # Preparamos las columnas
    col_lista, col_chat = st.columns([1, 2])

    # --- 1. IZQUIERDA: LISTA DE CONTACTOS (FRAGMENTO AUTÓNOMO) ---
    with col_lista:
        st.markdown("#### 📩 Bandeja")
        
        # Esta función se ejecuta sola cada 10 segundos SIN recargar la página entera
        @st.fragment(run_every=10)
        def mostrar_bandeja():
            with engine.connect() as conn:
                # Consulta para traer contactos y contar no leídos
                query_chats = text("""
                    SELECT 
                        COALESCE(m.id_cliente, -1) as id_cliente_raw,
                        m.telefono,
                        COALESCE(c.nombre || ' ' || c.apellido, m.cliente_nombre, m.telefono) as nombre_completo,
                        MAX(m.fecha) as ultima_fecha,
                        SUM(CASE WHEN m.leido = FALSE AND m.tipo = 'ENTRANTE' THEN 1 ELSE 0 END) as no_leidos
                    FROM mensajes m
                    LEFT JOIN Clientes c ON m.id_cliente = c.id_cliente
                    GROUP BY COALESCE(m.id_cliente, -1), m.telefono, COALESCE(c.nombre || ' ' || c.apellido, m.cliente_nombre, m.telefono)
                    ORDER BY ultima_fecha DESC
                """)
                df_chats = pd.read_sql(query_chats, conn)

            if not df_chats.empty:
                # Crear ID único
                df_chats['id_unico'] = df_chats.apply(
                    lambda x: f"ID-{int(x['id_cliente_raw'])}" if x['id_cliente_raw'] != -1 else f"TEL-{x['telefono']}", 
                    axis=1
                )
                
                # Formateador visual (Rojo si hay mensajes nuevos)
                def formatear_opcion(id_u):
                    row = df_chats[df_chats['id_unico'] == id_u].iloc[0]
                    notif = f"🔴 ({row['no_leidos']})" if row['no_leidos'] > 0 else ""
                    icono = "🔔" if row['no_leidos'] > 0 else "👤"
                    hora = row['ultima_fecha'].strftime('%d/%m %H:%M')
                    return f"{icono} {row['nombre_completo']} {notif} | {hora}"

                # SELECTOR: Al cambiar, Streamlit actualiza el session_state automáticamente
                # Usamos 'key' para guardar la selección en la memoria global
                st.radio(
                    "Selecciona:",
                    options=df_chats['id_unico'],
                    format_func=formatear_opcion,
                    label_visibility="collapsed",
                    key="chat_selector_key" 
                )
            else:
                st.info("📭 Vacío")

        # Llamamos a la función para que se renderice
        mostrar_bandeja()

# --- 2. DERECHA: VENTANA DE CHAT ---
    with col_chat:
        if "chat_selector_key" in st.session_state and st.session_state.chat_selector_key:
            
            id_seleccionado = st.session_state.chat_selector_key
            
            # --- Lógica para obtener datos del cliente ---
            with engine.connect() as conn:
                es_id_cliente = id_seleccionado.startswith("ID-")
                valor_id = id_seleccionado.split("-")[1]

                if es_id_cliente:
                    target_id = int(valor_id)
                    meta_chat = conn.execute(text("SELECT telefono, nombre || ' ' || apellido FROM Clientes WHERE id_cliente = :id"), {"id": target_id}).fetchone()
                    target_tel = meta_chat[0] if meta_chat else "Desconocido"
                    nombre_show = meta_chat[1] if meta_chat else "Cliente"
                    
                    # Marcar como leído
                    conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE id_cliente = :id AND tipo='ENTRANTE'"), {"id": target_id})
                    conn.commit()
                else:
                    target_id = -1
                    target_tel = valor_id 
                    nombre_show = target_tel
                    conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE telefono = :tel AND tipo='ENTRANTE'"), {"tel": target_tel})
                    conn.commit()

            # Cabecera del chat
            st.markdown(f"### 💬 **{nombre_show}**")
            st.caption(f"📱 {target_tel}")
            st.divider()

            # --- AQUÍ EMPIEZA LA FUNCIÓN ÚNICA Y CORRECTA ---
            @st.fragment(run_every=3)
            def renderizar_historial(t_id, t_tel):
                contenedor = st.container(height=400)
                
                # 1. Obtenemos los mensajes
                with engine.connect() as conn:
                    if t_id != -1:
                        historial = pd.read_sql(text("SELECT tipo, contenido, fecha FROM mensajes WHERE id_cliente = :id ORDER BY fecha ASC"), conn, params={"id": t_id})
                    else:
                        historial = pd.read_sql(text("SELECT tipo, contenido, fecha FROM mensajes WHERE telefono = :tel ORDER BY fecha ASC"), conn, params={"tel": t_tel})
                
                # 2. Dibujamos los mensajes
                with contenedor:
                    if historial.empty:
                        st.write("Inicia la conversación...")
                    
                    for _, row in historial.iterrows():
                        role = "user" if row['tipo'] == 'ENTRANTE' else "assistant"
                        avatar = "👤" if row['tipo'] == 'ENTRANTE' else "🛍️"
                        
                        with st.chat_message(role, avatar=avatar):
                            contenido_msg = row['contenido']
                            
                            # --- DETECTOR DE FOTOS (Lógica Visual) ---
                            if "|ID:" in contenido_msg:
                                try:
                                    partes = contenido_msg.split("|ID:")
                                    texto_visible = partes[0]
                                    media_id_oculto = partes[1].replace("|", "").strip()
                                    
                                    st.markdown(texto_visible)
                                    
                                    # Llamamos a la función global de descarga (definida al inicio del archivo)
                                    imagen_bytes = obtener_imagen_whatsapp(media_id_oculto)
                                    if imagen_bytes:
                                        st.image(imagen_bytes, width=250)
                                    else:
                                        st.caption("🚫 Imagen no disponible")
                                except:
                                    st.markdown(contenido_msg)
                            else:
                                st.markdown(contenido_msg)
                            
                            st.caption(f"_{row['fecha'].strftime('%H:%M')}_")

            # Llamamos a la función para que se ejecute
            renderizar_historial(target_id, target_tel)

            # --- INPUT DE RESPUESTA ---
            if prompt := st.chat_input("Escribe tu respuesta..."):
                enviado_ok, resp = enviar_mensaje_whatsapp(target_tel, prompt)
                
                if enviado_ok:
                    # Limpieza del número para guardar (agregando 51)
                    telefono_para_db = str(target_tel).replace(" ", "").replace("+", "").strip()
                    if len(telefono_para_db) == 9:
                        telefono_para_db = f"51{telefono_para_db}"
                    
                    with engine.connect() as conn:
                        conn.execute(text("""
                            INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido)
                            VALUES (:id, :tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE)
                        """), {
                            "id": int(target_id) if target_id != -1 else None,
                            "tel": telefono_para_db,
                            "txt": prompt
                        })
                        conn.commit()
                    st.rerun()
                else:
                    st.error(f"❌ Error WhatsApp: {resp}")

        else:
            st.markdown("<div style='text-align: center; color: gray; margin-top: 50px;'>👈 Selecciona un chat para comenzar</div>", unsafe_allow_html=True)