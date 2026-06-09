import streamlit as st
import os
import requests
import pandas as pd
from sqlalchemy import text
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from database import engine
import json
import base64
from woocommerce import API
import threading
import random

# ==============================================================================
# CONFIGURACIÓN GENERAL
# ==============================================================================
WAHA_URL = os.getenv("WAHA_URL") 
WAHA_KEY = os.getenv("WAHA_KEY") 

# ==============================================================================
# 🏆 1. NORMALIZACIÓN Y FORMATO (CRÍTICO)
# ==============================================================================
def normalizar_telefono_maestro(entrada):
    """
    Convierte cualquier formato de teléfono a un estándar DB (solo números).
    Devuelve un diccionario con formatos útiles: db, waha, google, corto.
    """
    if not entrada: return None
    raw_id = ""

    # 1. Extraer el ID si es un objeto
    if isinstance(entrada, dict):
        if 'db' in entrada: return entrada # Ya estaba normalizado
        
        if entrada.get('fromMe', False):
            raw_id = entrada.get('to', '')
        else:
            raw_id = entrada.get('from', '')
            
        if not raw_id:
            raw_id = entrada.get('id', {}).get('remote', '') or entrada.get('participant', '')
        if not raw_id:
            raw_id = str(entrada.get('user', ''))
    else:
        raw_id = str(entrada)

    # 2. Filtros de seguridad
    if 'status@broadcast' in raw_id: return None
    if '@g.us' in raw_id: return None
    
    # 3. Limpieza
    cadena_limpia = raw_id.split('@')[0] if '@' in raw_id else raw_id
    solo_numeros = "".join(filter(str.isdigit, cadena_limpia))
    
    if not solo_numeros: return None
    
    # 4. Validaciones
    if len(solo_numeros) > 15 or len(solo_numeros) < 7: return None

    full = solo_numeros
    local = solo_numeros
    
    # 5. Lógica Perú (51)
    if len(solo_numeros) == 9:
        full = f"51{solo_numeros}"
        local = solo_numeros
    elif len(solo_numeros) == 11 and solo_numeros.startswith("51"):
        full = solo_numeros
        local = solo_numeros[2:]
    
    return {
        "db": full,           # 51999888777
        "waha": f"{full}@c.us", # 51999888777@c.us
        "google": f"+51 {local[:3]} {local[3:6]} {local[6:]}" if len(local)==9 else f"+{full}",
        "corto": local        # 999888777
    }

# ==============================================================================
# 🔍 2. FUNCIONES DE GOOGLE CONTACTS
# ==============================================================================
def get_google_service():
    """Autenticación silenciosa con Google con impresión de errores"""
    try:
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not creds_json: 
            print("⚠️ Variable GOOGLE_CREDENTIALS_JSON no encontrada en el .env")
            return None
        info = json.loads(creds_json)
        creds = Credentials.from_authorized_user_info(info, ['https://www.googleapis.com/auth/contacts'])
        return build('people', 'v1', credentials=creds)
    except Exception as e:
        print(f"❌ Error real en get_google_service: {e}") # <-- Esto nos dará la pista exacta
        return None

def buscar_contacto_google(telefono):
    """Busca un contacto en Google por número probando múltiples formatos."""
    datos = normalizar_telefono_maestro(telefono)
    if not datos: return {'encontrado': False}
    
    service = get_google_service()
    if not service: return {'encontrado': False, 'error': 'No auth'}

    try:
        # Se añade datos['google'] (con espacios) y el texto original de la búsqueda
        queries = [datos['corto'], datos['db'], datos['google'], str(telefono)]
        
        for q in queries:
            results = service.people().searchContacts(
                query=q, readMask='names,phoneNumbers,emailAddresses'
            ).execute()
            
            if results.get('results'):
                person = results['results'][0]['person']
                names = person.get('names', [])
                nombre_completo = names[0].get('displayName', 'Sin Nombre') if names else "Sin Nombre"
                partes = nombre_completo.split()
                nombre = partes[0] if partes else ""
                apellido = " ".join(partes[1:]) if len(partes) > 1 else ""

                return {
                    'encontrado': True,
                    'nombre_completo': nombre_completo,
                    'nombre': nombre,
                    'apellido': apellido,
                    'google_id': person.get('resourceName'),
                    'telefono_google': q
                }
    except Exception as e:
        print(f"Error Google Search: {e}")
    return {'encontrado': False}


def crear_en_google(nombre, apellido, telefono, email=None):
    """Crea un contacto en Google Contacts"""
    service = get_google_service()
    if not service: return False
    try:
        body = {
            "names": [{"givenName": nombre, "familyName": apellido}],
            "phoneNumbers": [{"value": telefono}],
        }
        if email: body["emailAddresses"] = [{"value": email}]
        service.people().createContact(body=body).execute()
        return True
    except: return False

def actualizar_en_google(google_id, nombre, apellido, telefono):
    if not google_id:
        return False
    try:
        # Obtener el servicio de autenticación correcto
        service = get_google_service()
        if not service:
            print("⚠️ No se pudo inicializar el servicio de Google.")
            return False

        # Forzar formato correcto del resourceName
        resource_name = google_id if google_id.startswith('people/') else f"people/{google_id}"
        
        # 1. Obtener el 'etag' actual del contacto
        contacto = service.people().get(
            resourceName=resource_name,
            personFields='metadata'
        ).execute()
        etag = contacto.get('etag')

        # 2. Construir el cuerpo con los datos modificados
        body = {
            "etag": etag,
            "names": [{"givenName": nombre, "familyName": apellido}],
            "phoneNumbers": [{"value": telefono, "type": "mobile"}]
        }

        # 3. Enviar la actualización a la API
        service.people().updateContact(
            resourceName=resource_name,
            updatePersonFields="names,phoneNumbers",
            body=body
        ).execute()
        
        return True
    except Exception as e:
        print(f"❌ Error real en actualizar_en_google: {e}")
        return False
    
# ==============================================================================
# 💬 3. FUNCIONES WAHA (API) - CHATS, CAMPAÑAS Y VERIFICACIÓN
# ==============================================================================
def marcar_chat_como_leido_waha(chat_id):
    if not WAHA_URL: return
    try:
        if "@" not in chat_id: chat_id = f"{chat_id}@c.us"
        url = f"{WAHA_URL.rstrip('/')}/api/sendSeen"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        requests.post(url, json={"session": "default", "chatId": chat_id}, headers=headers, timeout=3)
    except: pass

def obtener_perfil_waha(telefono):
    if not WAHA_URL: return None
    try:
        norm = normalizar_telefono_maestro(telefono)
        if not norm: return None
        url = f"{WAHA_URL.rstrip('/')}/api/contacts/{norm['waha']}"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200: return r.json()
    except: pass
    return None

def enviar_mensaje_whatsapp(telefono, mensaje, url_imagen=None, session="default"):
    """Envía texto simple o imagen con texto (Campañas y Notificaciones)"""
    if not WAHA_URL: return False
    try:
        norm = normalizar_telefono_maestro(telefono)
        if not norm: return False
        
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        
        # Lógica inteligente: ¿Tiene imagen o es solo texto?
        if url_imagen:
            url = f"{WAHA_URL.rstrip('/')}/api/sendImage"
            payload = {"session": session, "chatId": norm['waha'], "file": {"url": url_imagen}, "caption": mensaje}
        else:
            url = f"{WAHA_URL.rstrip('/')}/api/sendText"
            payload = {"session": session, "chatId": norm['waha'], "text": mensaje}
            
        r = requests.post(url, json=payload, headers=headers, timeout=15)
        return r.status_code in [200, 201]
    except Exception as e:
        print(f"⚠️ Error al enviar WSP (utils): {e}")
        return False

def enviar_mensaje_media(telefono, caption, archivo_bytes, nombre_archivo, mime_type, session="default"):
    """Envía archivos adjuntos (Campañas)"""
    if not WAHA_URL: return False
    try:
        norm = normalizar_telefono_maestro(telefono)
        if not norm: return False

        b64_data = base64.b64encode(archivo_bytes).decode('utf-8')
        data_uri = f"data:{mime_type};base64,{b64_data}"

        url = f"{WAHA_URL.rstrip('/')}/api/sendImage"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY

        payload = {
            "session": session,
            "chatId": norm['waha'],
            "file": {
                "mimetype": mime_type,
                "filename": nombre_archivo,
                "url": data_uri
            },
            "caption": caption
        }

        r = requests.post(url, json=payload, headers=headers, timeout=30)
        return r.status_code in [200, 201]
    except Exception as e:
        print(f"Error media: {e}")
        return False

# 🚀 FUNCION PARA VERIFICAR EL NUMERO WSP SI EXISTE
def verificar_numero_waha(telefono):
    """
    Verifica si el número tiene WhatsApp activo usando WAHA.
    Retorna True (existe), False (no existe), o None (error de conexión).
    """
    if not WAHA_URL: return None
    
    try:
        norm = normalizar_telefono_maestro(telefono)
        if not norm: return False
        
        url = f"{WAHA_URL.rstrip('/')}/api/contacts/check-exists"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        
        # Probamos en ambas sesiones por si acaso
        for sesion in ['default', 'principal']:
            try:
                # Usamos GET y params (como aprendimos del bot de marketing)
                params = {"session": sesion, "phone": norm['db']}
                r = requests.get(url, params=params, headers=headers, timeout=15)
                
                if r.status_code == 200:
                    data = r.json()
                    # Retornamos directamente lo que diga WAHA (True o False)
                    return data.get('numberExists', False)
            except Exception as e:
                print(f"⚠️ WAHA Error en sesión '{sesion}': {e}")
                continue # Si esta sesión falla, intenta con la siguiente
                
        # Si el bucle termina y fallaron todas las sesiones, hay error de conexión
        return None
        
    except Exception as e:
        print(f"🔥 Error general en verificar_numero_waha: {e}")
        return None
# ==============================================================================
# 🤖 4. FUNCIONES DE INTELIGENCIA ARTIFICIAL
# ==============================================================================
def generar_nombre_ia(nombre_corto, nombre_real):
    """
    Genera un nombre amigable (solo el primer nombre) para que el bot de IA 
    salude al cliente, priorizando el nombre real de Google si existe.
    """
    try:
        # Prioridad 1: El primer nombre real registrado en Google Contacts
        if nombre_real and isinstance(nombre_real, str) and nombre_real.strip():
            primer_nombre = nombre_real.strip().split()[0]
            return primer_nombre.capitalize()
        
        # Prioridad 2: El primer nombre del Alias / Nombre Corto
        if nombre_corto and isinstance(nombre_corto, str) and nombre_corto.strip():
            # Limpiamos cosas como "VIP", "Nuevo", etc. si estuvieran en el alias
            primer_nombre = nombre_corto.strip().split()[0]
            return primer_nombre.capitalize()
            
    except Exception as e:
        print(f"Error al generar nombre IA: {e}")
        
    # Default si no hay datos
    return "Amigo"

# ==============================================================================
# 🛒 5. FUNCIONES DE WOOCOMMERCE
# ==============================================================================
def sync_woo_background(skus_a_sincronizar):
    """Actualiza en 2do plano solo los productos especificados en WooCommerce."""
    if not skus_a_sincronizar:
        return

    try:
        wcapi = API(
            url=os.getenv("WOO_URL"),
            consumer_key=os.getenv("WOO_KEY"),
            consumer_secret=os.getenv("WOO_SECRET"),
            version="wc/v3",
            timeout=15
        )

        from database import engine
        from sqlalchemy import text
        
        with engine.connect() as conn:
            # Generar los marcadores seguros para la consulta SQL
            placeholders = ", ".join([f":sku_{i}" for i in range(len(skus_a_sincronizar))])
            query = text(f"""
                SELECT sku, (COALESCE(stock_interno, 0) + COALESCE(stock_externo, 0)) AS stock_total, COALESCE(stock_transito, 0) AS stock_transito
                FROM Variantes 
                WHERE sku IN ({placeholders})
            """)
            params = {f"sku_{i}": sku for i, sku in enumerate(skus_a_sincronizar)}
            resultados = conn.execute(query, params).fetchall()

        paquete_actualizacion = []
        
        # Buscar el ID de WooCommerce de los productos modificados y preparar actualización
        for row in resultados:
            sku = row.sku
            stock = row.stock_total
            visibilidad = "visible" if (stock > 0 or row.stock_transito > 0) else "hidden"

            resp = wcapi.get("products", params={"sku": sku})
            if resp.status_code == 200:
                woo_data = resp.json()
                if woo_data:
                    paquete_actualizacion.append({
                        "id": woo_data[0]["id"],
                        "manage_stock": True,
                        "stock_quantity": stock,
                        "catalog_visibility": visibilidad
                    })

        # Enviar la actualización directa de este pequeño lote
        if paquete_actualizacion:
            wcapi.post("products/batch", {"update": paquete_actualizacion})
            print(f"⚡ Sync en tiempo real completada silenciosamente para: {skus_a_sincronizar}")

    except Exception as e:
        print(f"🔥 Error en sync de WooCommerce en tiempo real: {e}")

def _tarea_sync_woo(skus):
    """Tarea interna que se ejecuta en segundo plano para no congelar la pantalla de ventas"""
    try:
        woo_url = os.getenv("WOO_URL")
        woo_key = os.getenv("WOO_KEY")
        woo_secret = os.getenv("WOO_SECRET")
        
        if not woo_url or not woo_key or not woo_secret:
            return

        wcapi = API(
            url=woo_url,
            consumer_key=woo_key,
            consumer_secret=woo_secret,
            version="wc/v3",
            timeout=20
        )

        with engine.connect() as conn:
            for sku in skus:
                # Obtenemos stock físico y en tránsito
                query = text("""
                    SELECT stock_interno, stock_externo, COALESCE(stock_transito, 0) as stock_transito
                    FROM Variantes WHERE sku = :sku
                """)
                row = conn.execute(query, {"sku": sku}).fetchone()
                
                if row:
                    stock_fisico = row.stock_interno + row.stock_externo
                    
                    # Regla de negocio: visible si hay físico o tránsito
                    visibilidad = "visible" if (stock_fisico > 0 or row.stock_transito > 0) else "hidden"
                    stock_enviar = stock_fisico if stock_fisico > 0 else 0

                    # Buscar producto/variante en WooCommerce por SKU
                    res_woo = wcapi.get("products", params={"sku": sku}).json()
                    
                    if res_woo and isinstance(res_woo, list) and len(res_woo) > 0:
                        woo_id = res_woo[0]["id"]
                        data = {
                            "manage_stock": True,
                            "stock_quantity": stock_enviar,
                            "catalog_visibility": visibilidad
                        }
                        # Actualizar producto en WooCommerce
                        wcapi.put(f"products/{woo_id}", data)
                        print(f"✅ Woo Sync [Venta]: SKU {sku} -> Stock: {stock_enviar} | Visibilidad: {visibilidad}")

    except Exception as e:
        print(f"❌ Error en Woo Sync [Fondo]: {e}")

def sync_woo_background(skus):
    """
    Función principal que llama ventas.py. 
    Lanza un hilo para no hacer esperar al vendedor mientras se actualiza WooCommerce.
    """
    if isinstance(skus, str):
        skus = [skus]
    hilo = threading.Thread(target=_tarea_sync_woo, args=(skus,))
    hilo.start()

def subir_estado_whatsapp(session_name, texto, media_url=None):
    """
    Sube un estado a WhatsApp (Texto, Imagen o Video).
    """
    try:
        if media_url:
            # Detectamos si es un video por la extensión
            if media_url.lower().endswith('.mp4'):
                endpoint = f"{WAHA_URL}/api/{session_name}/status/video"
            else:
                endpoint = f"{WAHA_URL}/api/{session_name}/status/image"
                
            payload = {
                "file": {
                    "url": media_url 
                },
                "caption": texto if texto else ""
            }
        else:
            endpoint = f"{WAHA_URL}/api/{session_name}/status/text"
            payload = {
                "text": texto
            }

        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "X-Api-Key": WAHA_KEY  
        }

        response = requests.post(endpoint, json=payload, headers=headers)
        
        if response.status_code in [200, 201]:
            return True, "Estado subido correctamente a WhatsApp."
        else:
            return False, f"Error de WAHA: {response.text} (Código: {response.status_code})"

    except Exception as e:
        return False, f"Error interno al conectar con WAHA: {str(e)}"

# CREACION DEL NOMBRE COMPLETO DEL PRODUCTO
def seleccionar_producto_para_estado(prob_natural, prob_fantasia, prob_accesorios):
    """
    Selecciona un producto aplicando las reglas de negocio, incluyendo
    color principal y detalles de su grupo promocional.
    """
    import random
    from sqlalchemy import text
    from database import engine

    categorias = ["Estilo Natural", "Estilo Fantasía", "Accesorio"] 
    pesos_categoria = [prob_natural, prob_fantasia, prob_accesorios]
    
    if sum(pesos_categoria) == 0:
        pesos_categoria = [33, 33, 34]
        
    categoria_elegida = random.choices(categorias, weights=pesos_categoria, k=1)[0]
    
    with engine.connect() as conn:
        sql = text("""
            SELECT 
                v.sku, 
                p.nombre, 
                p.marca,
                p.modelo,
                p.categoria,
                p.color_principal,
                g.nombre_grupo,
                g.descripcion AS descripcion_grupo,
                v.stock_interno, 
                p.url_imagen 
            FROM productos p
            JOIN variantes v ON p.id_producto = v.id_producto
            LEFT JOIN grupos_productos g ON v.id_grupo = g.id_grupo
            WHERE p.categoria = :cat 
              AND v.stock_interno > 0 
              AND v.sku NOT IN (
                  SELECT sku FROM Historial_Estados 
                  WHERE fecha_publicacion > NOW() - INTERVAL '14 days'
              )
        """)
        
        resultados = conn.execute(sql, {"cat": categoria_elegida}).fetchall()
        
    if not resultados:
        return None 

    opciones = []
    pesos_stock = []
    for row in resultados:
        opciones.append(row)
        pesos_stock.append(row.stock_interno) 
        
    producto_elegido = random.choices(opciones, weights=pesos_stock, k=1)[0]
    
    datos_producto = {
        'categoria': producto_elegido.categoria,
        'marca': producto_elegido.marca,
        'modelo': producto_elegido.modelo,
        'nombre': producto_elegido.nombre,
        'sku': producto_elegido.sku,
        'color_principal': producto_elegido.color_principal,
        'grupo': producto_elegido.nombre_grupo,
        'descripcion_grupo': producto_elegido.descripcion_grupo
    }
    
    nombre_final = generar_nombre_inteligente(datos_producto)

    return {
        "sku": producto_elegido.sku,
        "nombre": nombre_final,
        "color_principal": producto_elegido.color_principal,
        "grupo": producto_elegido.nombre_grupo,
        "descripcion_grupo": producto_elegido.descripcion_grupo,
        "stock": producto_elegido.stock_interno,
        "imagen": producto_elegido.url_imagen
    }


# ==============================================================================
# FUNCIÓN INDEPENDIENTE PARA EL NOMBRE INTELIGENTE (USADA EN EL PANEL)
# ==============================================================================
def generar_nombre_inteligente(row):
    """
    Recibe una fila de base de datos o Pandas y devuelve el nombre formateado.
    """
    # Obtenemos los datos limpiando los nulos
    categoria = str(row.get('categoria', ''))
    marca = str(row.get('marca', '')) if pd.notna(row.get('marca', None)) else ''
    modelo = str(row.get('modelo', '')) if pd.notna(row.get('modelo', None)) else ''
    nombre = str(row.get('nombre', '')) if pd.notna(row.get('nombre', None)) else ''
    sku = str(row.get('sku', '')) if pd.notna(row.get('sku', None)) else ''

    # Aplicamos la regla del prefijo
    if categoria == 'Estilo Natural':
        prefijo = marca
    elif categoria == 'Estilo Fantasía':
        prefijo = 'Lente'
    else: # Accesorio u otros
        prefijo = ''

    # Armamos la primera parte (Prefijo + Modelo)
    partes_inicio = []
    if prefijo: 
        partes_inicio.append(prefijo)
    if modelo: 
        partes_inicio.append(modelo)
    
    inicio = " ".join(partes_inicio).strip()

    # Armamos el texto final
    if inicio:
        resultado = f"{inicio} - {nombre} ({sku})"
    else:
        resultado = f"{nombre} ({sku})"
        
    return resultado.strip()

def determinar_sesiones_para_estado(prob_lentes, prob_principal):
    """
    Determina qué sesiones de WhatsApp deben publicar el estado basándose
    en su porcentaje de probabilidad individual (0 a 100).
    Devuelve una lista con las sesiones elegidas ('default', 'principal' o ambas).
    """
    import random
    sesiones_elegidas = []

    # Evaluación independiente para la sesión Lentes (default)
    if random.randint(1, 100) <= prob_lentes:
        sesiones_elegidas.append("default")

    # Evaluación independiente para la sesión Principal (principal)
    if random.randint(1, 100) <= prob_principal:
        sesiones_elegidas.append("principal")

    return sesiones_elegidas

import requests

def generar_texto_estado_ia(producto):
    """
    Conecta con Ollama local. Python procesa la categoría primero 
    y le da una orden estricta e inequívoca a la IA.
    """
    url_ia = "http://localhost:11434/api/generate"
    
    # Usamos 'or' para asegurarnos de que si viene vacío (None), tenga un valor por defecto
    nombre = producto.get('nombre') or 'Producto'
    categoria = producto.get('categoria') or 'Accesorio'
    color = producto.get('color_principal') or 'No especificado'
    grupo = producto.get('grupo') or 'No especificado'
    desc_grupo = producto.get('descripcion_grupo') or ''
    
    # 🧠 LÓGICA EN PYTHON: Evaluamos la categoría sin importar mayúsculas o si le faltan palabras
    cat_lower = str(categoria).lower()
    
    if 'natural' in cat_lower:
        enfoque_estricto = "ENFOQUE OBLIGATORIO: Habla sobre resaltar la belleza natural, el uso diario, cosmética y una mirada sutil pero impactante. NO hables de cosplay, ni disfraces."
    elif 'fantas' in cat_lower or 'cosplay' in cat_lower or 'anime' in cat_lower:
        enfoque_estricto = "ENFOQUE OBLIGATORIO: Habla sobre cosplay, disfraces, anime, eventos de cultura pop y transformaciones extremas. Usa un tono muy llamativo y atrevido."
    elif 'lentes' in cat_lower:
        enfoque_estricto = "ENFOQUE OBLIGATORIO: Habla sobre resaltar la belleza natural, el uso diario, cosmética y una mirada sutil pero impactante. NO hables de cosplay, ni disfraces."
    else:
        enfoque_estricto = "ENFOQUE OBLIGATORIO: Destaca su utilidad, diseño exclusivo y lo práctico que es como accesorio para complementar el estilo."

    # Armamos el prompt con el enfoque inyectado
    prompt = f"""Eres un copywriter experto en marketing digital para KMLentes.pe, tienda virtual de lentes de contacto en Perú. 
    Escribe un mensaje corto para un estado de WhatsApp ofreciendo este producto.
    
    DATOS DEL PRODUCTO:
    - Nombre: {nombre}
    - Color destacado: {color}
    - Colección: {grupo}
    - Detalles: {desc_grupo}
    
    {enfoque_estricto}
    
    REGLAS DE FORMATO:
    1. Sigue al pie de la letra el ENFOQUE OBLIGATORIO.
    2. Usa emojis adecuados al tema. 
    3. Invita a que te envíen un mensaje al final al Whatsapp, no es necesario decir el numero telefonico ya que es un estado.
    4. Máximo 35 palabras.
    5. Devuelve ÚNICAMENTE el texto final para copiar y pegar (sin comillas, sin decir 'Aquí tienes', sin saludos iniciales).
    """

    payload = {
        "model": "llama3.1",
        "prompt": prompt,
        "stream": False
    }

    try:
        response = requests.post(url_ia, json=payload, timeout=30)
        if response.status_code == 200:
            return response.json().get("response", "").strip()
    except Exception as e:
        print(f"⚠️ Error en IA local: {e}")

    return f"✨ ¡{nombre} disponible! Ideal para tu estilo. Escríbenos para asegurar el tuyo. 📲"

# ==============================================================================
# HERRAMIENTA DE SINCRONIZACIÓN MASIVA GOOGLE
# ==============================================================================
def render_sincronizacion_masiva():
    import time
    with st.expander("🔄 Sincronización Masiva con Google Contacts", expanded=False):
        st.info("💡 **Sincronización de Historial:** Esta herramienta busca todos los clientes activos que no están vinculados con Google Contacts para registrarlos y asociar su ID permanentemente.")
        if st.button("🚀 Vincular Clientes Antiguos"):
            with st.spinner("Sincronizando historial..."):
                try:
                    with engine.connect() as conn:
                        df_sin_sync = pd.read_sql(text("""
                            SELECT c.id_cliente, c.nombre_corto, 
                                   (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1) as tel_prin
                            FROM clientes c 
                            WHERE c.activo=TRUE AND (c.google_id IS NULL OR TRIM(c.google_id) = '')
                        """), conn)
                    
                    if df_sin_sync.empty:
                        st.success("¡Todos los clientes ya se encuentran vinculados o no hay clientes activos sin ID de Google!")
                    else:
                        cont = 0
                        detalles_omisiones = []
                        
                        with engine.begin() as conn_tx:
                            for idx, row in df_sin_sync.iterrows():
                                id_cli = row['id_cliente']
                                nombre = row['nombre_corto']
                                tel = row['tel_prin']
                                
                                # Validación corregida para detectar valores nulos (NaN o vacíos)
                                if pd.isna(tel) or str(tel).strip().lower() in ['nan', '']:
                                    detalles_omisiones.append(f"⚠️ ID {id_cli} ({nombre}): No tiene teléfono principal asignado en el sistema.")
                                    continue
                                    
                                norm = normalizar_telefono_maestro(tel)
                                if not norm:
                                    detalles_omisiones.append(f"⚠️ ID {id_cli} ({nombre}): Falló la normalización para el número '{tel}'.")
                                    continue
                                    
                                tel_db = norm['db']
                                tel_google = norm.get('google', tel_db)
                                
                                res_g = buscar_contacto_google(tel_db)
                                g_id = None
                                if res_g and res_g.get('encontrado'):
                                    g_id = res_g['google_id']
                                else:
                                    if crear_en_google(nombre, "", tel_google):
                                        res_g2 = buscar_contacto_google(tel_db)
                                        if res_g2 and res_g2.get('encontrado'):
                                            g_id = res_g2['google_id']
                                        else:
                                            detalles_omisiones.append(f"❌ ID {id_cli} ({nombre}): Se creó en Google pero buscar_contacto_google no recuperó el ID.")
                                    else:
                                        detalles_omisiones.append(f"❌ ID {id_cli} ({nombre}): La función crear_en_google devolvió False.")
                                
                                if g_id:
                                    conn_tx.execute(text("UPDATE clientes SET google_id = :gid WHERE id_cliente = :id"), {"gid": g_id, "id": id_cli})
                                    cont += 1
                        
                        if detalles_omisiones:
                            st.markdown("##### 🔍 Detalles del proceso:")
                            for msg in detalles_omisiones:
                                st.warning(msg)
                                
                        st.success(f"¡Sincronización completada! Se vincularon {cont} clientes con éxito.")
                        if cont > 0:
                            time.sleep(2)
                            st.rerun()
                except Exception as e:
                    st.error(f"Error en la sincronización masiva: {e}")

def obtener_historial_compras(telefono):
    """
    Busca el historial de compras de un cliente por su número de teléfono.
    Retorna un DataFrame formateado o None si no hay compras.
    """
    query_compras = text("""
        SELECT 
            v.fecha_venta AS "Fecha", 
            COALESCE(prod.categoria, 'Otros') AS "Categoría",
            d.cantidad AS "Cant.", 
            COALESCE(
                (prod.nombre || ' - ' || var.nombre_variante), 
                d.descripcion, 
                d.sku, 
                'Artículo sin registrar'
            ) AS "Producto"
        FROM clientes c
        JOIN ventas v ON c.id_cliente = v.id_cliente
        JOIN detalleventa d ON v.id_venta = d.id_venta
        LEFT JOIN variantes var ON d.sku = var.sku
        LEFT JOIN productos prod ON var.id_producto = prod.id_producto
        WHERE c.telefono = :telefono 
        ORDER BY v.fecha_facturacion DESC NULLS LAST;
    """)
    
    with engine.connect() as conn:
        resultados = conn.execute(query_compras, {"telefono": telefono}).fetchall()
        
    if resultados:
        df_compras = pd.DataFrame(resultados)
        # Formateamos la fecha directamente aquí
        df_compras['Fecha'] = pd.to_datetime(df_compras['Fecha']).dt.strftime('%d/%m/%y').fillna('---')
        return df_compras
    
    return None