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
from datetime import datetime

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
    """Busca un contacto en Google por número probando múltiples formatos y búsqueda exhaustiva."""
    datos = normalizar_telefono_maestro(telefono)
    if not datos: return {'encontrado': False}
    
    service = get_google_service()
    if not service: return {'encontrado': False, 'error': 'No auth'}

    # 1. BÚSQUEDA RÁPIDA (API nativa de Google)
    try:
        queries = [datos['google'], f"+{datos['db']}", datos['db'], datos['corto'], str(telefono)]
        for q in queries:
            results = service.people().searchContacts(
                query=q, readMask='names,phoneNumbers,emailAddresses'
            ).execute()
            
            if results.get('results'):
                person = results['results'][0]['person']
                names = person.get('names', [])
                nombre_completo = names[0].get('displayName', 'Sin Nombre') if names else "Sin Nombre"
                partes = nombre_completo.split()
                return {
                    'encontrado': True,
                    'nombre_completo': nombre_completo,
                    'nombre': partes[0] if partes else "",
                    'apellido': " ".join(partes[1:]) if len(partes) > 1 else "",
                    'google_id': person.get('resourceName'),
                    'telefono_google': q
                }
    except Exception as e:
        print(f"Error Google Search Rápido: {e}")

    # 2. BÚSQUEDA EXHAUSTIVA (El arma secreta contra los duplicados)
    print(f"Búsqueda rápida falló para {telefono}. Iniciando búsqueda exhaustiva...")
    try:
        request = service.people().connections().list(
            resourceName='people/me',
            pageSize=1000,
            personFields='names,phoneNumbers'
        )
        
        while request is not None:
            response = request.execute()
            for person in response.get('connections', []):
                for phone in person.get('phoneNumbers', []):
                    val = phone.get('value', '')
                    # Quitamos todos los espacios y símbolos del número que nos da Google
                    val_norm = "".join(filter(str.isdigit, val))
                    
                    # Si los 9 dígitos locales coinciden (sin importar el +51), es la misma persona!
                    if datos['corto'] in val_norm or datos['db'] in val_norm:
                        names = person.get('names', [])
                        nombre_completo = names[0].get('displayName', 'Sin Nombre') if names else "Sin Nombre"
                        partes = nombre_completo.split()
                        print(f"✅ ¡Encontrado exhaustivamente!: {nombre_completo}")
                        return {
                            'encontrado': True,
                            'nombre_completo': nombre_completo,
                            'nombre': partes[0] if partes else "",
                            'apellido': " ".join(partes[1:]) if len(partes) > 1 else "",
                            'google_id': person.get('resourceName'),
                            'telefono_google': val
                        }
            request = service.people().connections().list_next(request, response)
    except Exception as e:
        print(f"Error en búsqueda exhaustiva: {e}")

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

        r = requests.post(url, json=payload, headers=headers, timeout=15)
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
    """Actualiza en 2do plano productos simples y variaciones en WooCommerce."""
    if not skus_a_sincronizar:
        return

    try:
        from woocommerce import API
        import os
        from database import engine
        from sqlalchemy import text

        wcapi = API(
            url=os.getenv("WOO_URL"),
            consumer_key=os.getenv("WOO_KEY"),
            consumer_secret=os.getenv("WOO_SECRET"),
            version="wc/v3",
            timeout=15
        )
        
        with engine.connect() as conn:
            placeholders = ", ".join([f":sku_{i}" for i in range(len(skus_a_sincronizar))])
            query = text(f"""
                SELECT sku, 
                       (COALESCE(stock_interno, 0) + COALESCE(stock_externo, 0)) AS stock_total,
                       COALESCE(stock_transito, 0) AS stock_transito
                FROM Variantes 
                WHERE sku IN ({placeholders})
            """)
            params = {f"sku_{i}": sku for i, sku in enumerate(skus_a_sincronizar)}
            resultados = conn.execute(query, params).fetchall()

        for row in resultados:
            sku = row.sku
            stock_real = row.stock_total
            stock_camino = row.stock_transito

            resp = wcapi.get("products", params={"sku": sku})
            if resp.status_code == 200:
                woo_data = resp.json()
                if woo_data:
                    prod = woo_data[0]
                    woo_id = prod["id"]
                    
                    data_update = {
                        "manage_stock": True,
                        "stock_quantity": stock_real
                    }

                    # Detectar si es variación o producto simple
                    if prod.get("type") == "variation" or prod.get("parent_id", 0) != 0:
                        parent_id = prod.get("parent_id")
                        wcapi.put(f"products/{parent_id}/variations/{woo_id}", data_update)
                    else:
                        # Solo los productos simples/padres soportan cambio de visibilidad
                        visibilidad = "visible" if (stock_real > 0 or stock_camino > 0) else "hidden"
                        data_update["catalog_visibility"] = visibilidad
                        wcapi.put(f"products/{woo_id}", data_update)

        print(f"⚡ Sync en tiempo real completada para: {skus_a_sincronizar}")

    except Exception as e:
        print(f"🔥 Error en sync de WooCommerce: {e}")

def _tarea_sync_woo(skus):
    """Tarea interna que se ejecuta en segundo plano para no congelar la pantalla de ventas"""
    try:
        woo_url = os.getenv("WOO_URL")
        woo_key = os.getenv("WOO_KEY")
        woo_secret = os.getenv("WOO_SECRET")
        
        if not woo_url or not woo_key or not woo_secret:
            return
        # Lógica dentro de _tarea_sync_woo:
        query_cat = text("SELECT p.categoria FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto WHERE v.sku = :sku")
        categoria = conn.execute(query_cat, {"sku": sku}).scalar()

        if categoria == 'Pelucas':
            url, key, secret = os.getenv("WOO_PELUCAS_URL"), os.getenv("WOO_PELUCAS_KEY"), os.getenv("WOO_PELUCAS_SECRET")
        else:
            url, key, secret = os.getenv("WOO_LENTES_URL"), os.getenv("WOO_LENTES_KEY"), os.getenv("WOO_LENTES_SECRET")

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


# CREACION DEL NOMBRE COMPLETO DEL PRODUCTO
def seleccionar_producto_para_estado(prob_natural, prob_fantasia, prob_accesorios, macro_objetivo='Lentes'):
    """
    Selecciona un producto con stock aplicando probabilidades de subcategoría, 
    filtrado estrictamente por macro_categoria y capturando la foto específica del SKU.
    """
    import random
    from sqlalchemy import text
    from database import engine

    # 1. ARMONIZACIÓN: Matcheamos con los nombres reales que guardó el migrador de Excel
    if macro_objetivo == 'Pelucas':
        categorias = ["Estilo Natural", "Estilo Fantasía", "Accesorios Pelucas"]
    else:
        categorias = ["Estilo Natural", "Estilo Fantasía", "Accesorios"] 

    pesos_categoria = [prob_natural, prob_fantasia, prob_accesorios]
    if sum(pesos_categoria) == 0:
        pesos_categoria = [34, 33, 33]
        
    categoria_elegida = random.choices(categorias, weights=pesos_categoria, k=1)[0]
    
    with engine.connect() as conn:
        # 2. INYECCIÓN DEL COALESCE: Prioriza foto del hijo (Variantes), si no hay, toma la del padre (Productos)
        sql = text("""
            SELECT 
                v.sku, p.nombre, p.marca, p.modelo, p.categoria, 
                COALESCE(p.macro_categoria, 'Lentes') AS macro_categoria, 
                p.color_principal, g.nombre_grupo, g.descripcion AS descripcion_grupo, 
                v.stock_interno, 
                COALESCE(NULLIF(TRIM(v.url_imagen), ''), NULLIF(TRIM(p.url_imagen), '')) AS imagen
            FROM Productos p
            JOIN Variantes v ON p.id_producto = v.id_producto
            LEFT JOIN Grupos_Productos g ON v.id_grupo = g.id_grupo
            WHERE p.categoria = :cat AND COALESCE(p.macro_categoria, 'Lentes') = :macro
              AND v.stock_interno > 0 
              AND v.sku NOT IN (
                  SELECT sku FROM Historial_Estados 
                  WHERE fecha_publicacion > NOW() - INTERVAL '14 days'
              )
        """)
        resultados = conn.execute(sql, {"cat": categoria_elegida, "macro": macro_objetivo}).fetchall()
        
    # Fallback de seguridad blindado con el mismo COALESCE de imagen
    if not resultados:
        with engine.connect() as conn:
            fb_sql = text("""
                SELECT v.sku, p.nombre, p.marca, p.modelo, p.categoria, 
                       COALESCE(p.macro_categoria, 'Lentes') AS macro_categoria, 
                       p.color_principal, g.nombre_grupo, g.descripcion AS descripcion_grupo, 
                       v.stock_interno, 
                       COALESCE(NULLIF(TRIM(v.url_imagen), ''), NULLIF(TRIM(p.url_imagen), '')) AS imagen
                FROM Productos p
                JOIN Variantes v ON p.id_producto = v.id_producto
                LEFT JOIN Grupos_Productos g ON v.id_grupo = g.id_grupo
                WHERE COALESCE(p.macro_categoria, 'Lentes') = :macro AND v.stock_interno > 0 
                LIMIT 50
            """)
            resultados = conn.execute(fb_sql, {"macro": macro_objetivo}).fetchall()
            
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
        'macro_categoria': producto_elegido.macro_categoria, # <-- Vital para que el generador inteligente sepa qué es
        'marca': producto_elegido.marca,
        'modelo': producto_elegido.modelo,
        'nombre': producto_elegido.nombre,
        'sku': producto_elegido.sku,
        'color_principal': producto_elegido.color_principal
    }
    nombre_final = generar_nombre_inteligente(datos_producto)

    return {
        "sku": producto_elegido.sku,
        "nombre": nombre_final,
        "macro_categoria": producto_elegido.macro_categoria,
        "color_principal": producto_elegido.color_principal,
        "grupo": producto_elegido.nombre_grupo,
        "descripcion_grupo": producto_elegido.descripcion_grupo,
        "stock": producto_elegido.stock_interno,
        "imagen": producto_elegido.imagen # <-- Ahora sí despacha la foto resuelta exacta
    }

# ==============================================================================
# FUNCIÓN INDEPENDIENTE PARA EL NOMBRE INTELIGENTE (USADA EN EL PANEL)
# ==============================================================================
def generar_nombre_inteligente(row):
    """
    Recibe una fila de base de datos (dict o Series de Pandas) y genera el nombre 
    comercial perfectamente formateado según sea Peluca o Lente de Contacto.
    """
    # 1. Extracción segura purgando nulos reales y falsos nulos de texto ('nan')
    def _get_str(key):
        val = row.get(key, '')
        if pd.isna(val) or val is None:
            return ''
        txt = str(val).strip()
        return '' if txt.lower() == 'nan' else txt

    macro = _get_str('macro_categoria')
    categoria = _get_str('categoria')
    marca = _get_str('marca')
    modelo = _get_str('modelo')
    nombre = _get_str('nombre')
    sku = _get_str('sku')

    # Fallback de rescate por si la consulta SQL omitió jalar la columna macro
    if not macro:
        macro = 'Pelucas' if sku.upper().startswith(('WB-', 'WIG-')) else 'Lentes'

    # =====================================================================
    # 2. MOTOR DE PREFIJOS DINÁMICO POR LÍNEA DE NEGOCIO
    # =====================================================================
    prefijo = ''

    if macro == 'Pelucas':
        cat_low = categoria.lower()
        if 'natural' in cat_low:
            # Si la marca es genérica o Pelucat, antepone la palabra "Peluca", si es marca externa la respeta
            prefijo = marca if marca.lower() not in ['pelucat', 'genérico', 'generico', ''] else 'Peluca'
        elif any(k in cat_low for x in ['fantas', 'cosplay', 'lace'] for k in [x]):
            prefijo = 'Peluca'
        else:
            prefijo = '' # Accesorios, redecillas, clips, extensiones

    else: # Lentes de Contacto (Conserva tu lógica histórica intacta al 100%)
        if categoria == 'Estilo Natural':
            prefijo = marca
        elif categoria == 'Estilo Fantasía':
            prefijo = 'Lente'
        else:
            prefijo = ''

    # =====================================================================
    # 3. ENSAMBLAJE BLINDADO (Evita redundancias léxicas)
    # =====================================================================
    partes_inicio = []

    if prefijo:
        partes_inicio.append(prefijo)

    if modelo:
        # Si el modelo ya empezaba escribiendo el prefijo (Ej: Modelo="Peluca Bob"), absorbe el prefijo
        if prefijo and modelo.lower().startswith(prefijo.lower()):
            partes_inicio = [modelo]
        else:
            partes_inicio.append(modelo)

    inicio = " ".join(partes_inicio).strip()

    # Combinamos cabecera con el Tono / Estilo
    if inicio and nombre:
        nombre_base = f"{inicio} - {nombre}"
    elif inicio:
        nombre_base = inicio
    else:
        nombre_base = nombre or "Artículo sin título"

    # Adosamos el código SKU final
    return f"{nombre_base} ({sku})".strip() if sku else nombre_base.strip()

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

def buscar_producto_aleatorio_en_stock(conn, macro_categoria, subcategorias_permitidas):
    """Busca 1 producto al azar con stock físico > 0 anclado a la foto del padre"""
    if not subcategorias_permitidas:
        return None

    lista_clean = [s.strip().lower() for s in subcategorias_permitidas]
    lista_sql = ', '.join([f"'{s}'" for s in lista_clean])

    query_principal = text(f"""
        SELECT
            p.id_producto, p.marca, p.modelo, p.nombre, p.categoria,
            p.url_imagen, -- <--- De vuelta estrictamente a la foto del Padre
            v.sku, v.precio
        FROM Variantes v
        JOIN Productos p ON v.id_producto = p.id_producto
        WHERE TRIM(p.macro_categoria) ILIKE :macro
          AND LOWER(TRIM(p.categoria)) IN ({lista_sql})
          AND COALESCE(v.stock_interno, 0) > 0
          AND p.url_imagen IS NOT NULL
          AND TRIM(p.url_imagen) != ''
        ORDER BY RANDOM()
        LIMIT 1
    """)

    row = conn.execute(query_principal, {"macro": f"%{macro_categoria.strip()}%"}).fetchone()
    if row: return dict(row._mapping)

    # --- FALLBACK DE RESCATE ---
    if macro_categoria == 'Pelucas':
        query_rescate = text("""
            SELECT p.id_producto, p.marca, p.modelo, p.nombre, p.categoria,
                   p.url_imagen, v.sku, v.precio
            FROM Variantes v
            JOIN Productos p ON v.id_producto = p.id_producto
            WHERE p.macro_categoria ILIKE '%peluca%'
              AND COALESCE(v.stock_interno, 0) > 0
              AND p.url_imagen IS NOT NULL
              AND TRIM(p.url_imagen) != ''
            ORDER BY RANDOM()
            LIMIT 1
        """)

        row_rescate = conn.execute(query_rescate).fetchone()
        if row_rescate: return dict(row_rescate._mapping)

    return None

def obtener_festividad_cercana():
    """
    Calcula si hay una festividad comercial importante en los próximos 20 días en Perú.
    """
    hoy = datetime.now().date()
    
    festividades = [
        {"mes": 2, "dia": 14, "nombre": "San Valentín / Día del Amor"},
        {"mes": 5, "dia": 10, "nombre": "Día de la Madre"}, # Fecha comercial aprox
        {"mes": 6, "dia": 15, "nombre": "Día del Padre"},   # Fecha comercial aprox
        {"mes": 7, "dia": 28, "nombre": "Fiestas Patrias de Perú"},
        {"mes": 10, "dia": 31, "nombre": "Halloween y el Día de la Canción Criolla"},
        {"mes": 12, "dia": 25, "nombre": "Navidad"},
        {"mes": 12, "dia": 31, "nombre": "Año Nuevo"}
    ]
    
    for fest in festividades:
        # Asumimos el año actual para el cálculo
        fecha_fest = datetime(hoy.year, fest["mes"], fest["dia"]).date()
        diferencia = (fecha_fest - hoy).days
        
        # Si la festividad ya pasó este año, evaluamos el próximo
        if diferencia < 0:
            fecha_fest = datetime(hoy.year + 1, fest["mes"], fest["dia"]).date()
            diferencia = (fecha_fest - hoy).days

        # Si estamos a 20 días o menos de la fecha clave, alertamos a la IA
        if 0 <= diferencia <= 20:
            return f"🚨 CONTEXTO DE TEMPORADA OBLIGATORIO: Estamos a {diferencia} días de {fest['nombre']}. Adapta el tono de tu mensaje sutilmente a esta festividad para generar más deseo de compra."
            
    return "" # Días normales sin festividades

def generar_texto_producto_ia(producto, es_estado=False, cliente_info=None):
    """
    Genera copys persuasivos con IA local (Ollama).
    Ahora incluye lectura de Prompts Dinámicos, Contexto de Festividades, 
    Enfoques de Base de Datos y salida doble (WhatsApp + Meta).
    """

    ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
    modelo_ia = os.getenv("OLLAMA_MODEL", "llama3.1")
    url_ia = f"{ollama_url.rstrip('/')}/api/generate"
    
    # 1. Resolución de Línea de Negocio y Tienda
    macro = producto.get('macro_categoria')
    sku = str(producto.get('sku', '')).strip()
    
    if not macro or str(macro).lower() in ['none', 'nan', '']:
        macro = 'Pelucas' if sku.upper().startswith(('WB-', 'WIG-')) else 'Lentes'

    if macro == 'Pelucas':
        tienda_actual = "pelucat.pe"
        tienda_cruzada = "kmlentes.pe"
        tipo_articulo = "esta espectacular peluca premium"
        cross_selling = f"✨ P.D.: ¿Sabías que también contamos con increíbles lentes de contacto en 👉 https://{tienda_cruzada}"
    else:
        tienda_actual = "kmlentes.pe"
        tienda_cruzada = "pelucat.pe"
        tipo_articulo = "estos hermosos lentes de contacto"
        cross_selling = f"✨ P.D.: ¡Complementa tu look descubriendo nuestra colección de pelucas importadas en 👉 https://{tienda_cruzada}!"

    # 2. Construcción inteligente del nombre y enlace
    marca = producto.get('marca') or ''
    modelo = producto.get('modelo') or ''
    nom_color = producto.get('nombre') or 'Producto'
    precio = producto.get('precio', '')
    
    titulo_prod = f"{marca} {modelo} ({nom_color})".strip() if marca and modelo else str(nom_color)
    categoria = str(producto.get('categoria', 'General')).lower()
    desc_grupo = producto.get('descripcion_grupo') or ''

    enlace_tienda = producto.get('url_tienda')
    enlace_compra = str(enlace_tienda).strip() if enlace_tienda and str(enlace_tienda).strip() != "" else f"https://{tienda_actual}/producto/{sku}"
    txt_precio = f"a solo S/ {precio}" if precio else ""

    # 3. Contextos Adicionales (Festividades, CRM y Enfoque Psicológico DB)
    contexto_festividad = obtener_festividad_cercana()
    
    notas_crm = ""
    if cliente_info and cliente_info.get('etiquetas'):
        notas_crm = f"\n    INTERESES REGISTRADOS DEL CLIENTE: '{cliente_info['etiquetas']}' (Haz una mención muy natural a esto)."

    # --- NUEVA CONEXIÓN DINÁMICA: Leemos la descripción que pusiste en el Panel ---
    enfoque_db = producto.get('enfoque_ia')
    if enfoque_db and str(enfoque_db).strip() != "":
        enfoque = f"ENFOQUE OBLIGATORIO: {str(enfoque_db).strip()}"
    else:
        enfoque = f"ENFOQUE OBLIGATORIO: Destaca la calidad indiscutible de {tipo_articulo} y su acabado exclusivo."
    # -----------------------------------------------------------------------------

    # 4. Obtener el Prompt Personalizado desde la Base de Datos
    prompt_personalizado_estado = ""
    prompt_personalizado_dm = ""
    try:
        with engine.connect() as conn:
            config = conn.execute(text("SELECT prompt_estado, prompt_dm FROM Configuracion_Campanas LIMIT 1")).fetchone()
            if config:
                prompt_personalizado_estado = config.prompt_estado if 'prompt_estado' in config._mapping else ""
                prompt_personalizado_dm = config.prompt_dm if 'prompt_dm' in config._mapping else ""
    except Exception as e:
        pass # Fallback silencioso si las columnas aún no existen

    # 5. Bifurcación del Prompt (BLINDAJE JSON)
    if es_estado:
        base_instruct = prompt_personalizado_estado or f"Eres un copywriter experto en marketing digital para {tienda_actual}."
        
        prompt = f"""{base_instruct}
        
        DATOS DEL ARTÍCULO: {titulo_prod}
        ATRACTIVO DEL PRODUCTO: {desc_grupo}
        {enfoque}
        PROMOCIÓN CRUZADA A INCLUIR: {cross_selling}
        {contexto_festividad}
        
        REGLA DE SEGURIDAD INQUEBRANTABLE:
        NO escribas texto conversacional (como "Aquí tienes el texto").
        DEBES responder ÚNICAMENTE con un objeto JSON válido que contenga estas DOS claves exactas:
        1. "estado_whatsapp": Un texto corto (max 35 palabras) y explosivo con emojis diseñado para historias de WhatsApp invitando al mensaje directo.
        2. "post_facebook": Un texto más elaborado y persuasivo (con hashtags y el enlace {enlace_compra}) listo para publicarse en el muro de Facebook/Instagram.
        """
        texto_reserva = {
            "estado_whatsapp": f"✨ ¡Reingresó stock de {titulo_prod}! Adquiérelo directo en https://{tienda_actual} 📲\n\n{cross_selling}",
            "post_facebook": f"¡Luce increíble con {titulo_prod}! 😍\nEncuéntralo aquí: {enlace_compra}"
        }
    else:
        base_instruct = prompt_personalizado_dm or f"Eres un experto en cierres de ventas por WhatsApp para la marca {tienda_actual}."
        
        prompt = f"""{base_instruct}
        
        PRODUCTO: {titulo_prod} {txt_precio}
        ENLACE DE COMPRA: {enlace_compra}
        {notas_crm}
        {contexto_festividad}
        ESTRATEGIA DE PERSUASIÓN: {enfoque}
        
        REGLA DE SEGURIDAD INQUEBRANTABLE:
        NO saludes al principio ni incluyas despedidas. NO uses frases como "Claro, aquí está".
        RESPONDE OBLIGATORIAMENTE SÓLO EN FORMATO JSON. La clave debe ser "mensaje" y contener un ARRAY de 4 párrafos cortos.
        Ejemplo: {{"mensaje": ["Párrafo 1", "Párrafo 2", "Párrafo 3", "Párrafo 4 con {cross_selling}"]}}
        """
        texto_reserva = f"¡Mira el hermoso modelo que acaba de reingresar a nuestro almacén!\n\n⭐ **{titulo_prod}** {txt_precio}.\n\nPuedes revisar fotos reales y pedirlo directo aquí:\n👉 {enlace_compra}\n\n{cross_selling}"

    # 6. Petición a Ollama
    try:
        response = requests.post(url_ia, json={"model": modelo_ia, "prompt": prompt, "stream": False, "format": "json"}, timeout=15)
        
        if response.status_code == 200:
            datos_json = json.loads(response.json().get("response", "").strip())
            
            if es_estado:
                # Retornamos el diccionario completo (WhatsApp y Facebook)
                return {
                    "estado_whatsapp": datos_json.get("estado_whatsapp", texto_reserva["estado_whatsapp"]),
                    "post_facebook": datos_json.get("post_facebook", texto_reserva["post_facebook"])
                }
            else:
                parrafos = datos_json.get("mensaje", [])
                texto_final = "\n\n".join(parrafos) if isinstance(parrafos, list) else str(parrafos)
                return texto_final.strip() if len(texto_final) > 12 else texto_reserva
        else:
            print(f"   ⚠️ [Aviso IA] Ollama respondió HTTP {response.status_code}.")
    except Exception as e:
        print(f"   ⚠️ [Aviso IA] Usando rescate: {e}")

    return texto_reserva
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

        response = requests.post(endpoint, json=payload, headers=headers, timeout=15)
        
        if response.status_code in [200, 201]:
            return True, "Estado subido correctamente a WhatsApp."
        else:
            return False, f"Error de WAHA: {response.text} (Código: {response.status_code})"

    except Exception as e:
        return False, f"Error interno al conectar con WAHA: {str(e)}"

import requests

def publicar_en_facebook_via_webhook(mensaje, url_imagen, webhook_url):
    """Sube el post a Facebook delegando la tarea a un Webhook de Make.com"""
    import requests
    
    if not webhook_url or "make.com" not in webhook_url:
         return False, "La URL del Webhook no parece válida."

    payload = {
        "mensaje": mensaje,
        "url_imagen": url_imagen
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        
        # Make.com devuelve 200 y el texto "Accepted" si todo va bien
        if response.status_code == 200:
            return True, response.text
        else:
            print(f"❌ Error enviando webhook: {response.text}")
            return False, response.text
    except Exception as e:
        return False, str(e)