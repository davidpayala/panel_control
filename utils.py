import os
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import streamlit as st

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
    