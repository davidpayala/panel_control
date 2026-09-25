import os
import io
import pandas as pd
from sqlalchemy import create_engine, text
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from dotenv import load_dotenv # 🆕 Importamos dotenv
import utils 

# 🆕 Forzamos la lectura del archivo .env
load_dotenv()

# --- CONFIGURACIÓN ---
# Ahora sí encontrará la variable
DATABASE_URL = os.getenv("DATABASE_URL") 

if not DATABASE_URL:
    raise ValueError("❌ ERROR: No se encontró DATABASE_URL en el archivo .env")

TOKEN_FILE = 'token.json'
DRIVE_LN = '12eINvzNXf4iiB2jn7NY2TMwfw8JfvYvu' 

SCOPES = [
    'https://www.googleapis.com/auth/contacts',
    'https://www.googleapis.com/auth/drive'
]

def autenticar_drive():
    creds = None
    # 1. Cargamos el token que generaste en tu PC y pegaste en el servidor
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    # 2. Si el token expiró (dura 1 hora), se auto-renueva silenciosamente
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
            
    # 3. Si no hay token o es inválido, detenemos el proceso con un error claro
    if not creds or not creds.valid:
        raise Exception("❌ ERROR: El archivo token.json no existe o es inválido. Asegúrate de haberlo creado en la misma carpeta que este script.")

    return build('drive', 'v3', credentials=creds)

def subir_o_reemplazar_drive(servicio, buffer_pdf, nombre_archivo):
    """Busca si el archivo ya existe para reemplazarlo, si no, lo crea."""
    query = f"name='{nombre_archivo}' and '{DRIVE_LN}' in parents and trashed=false"
    resultados = servicio.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    archivos = resultados.get('files', [])

    media = MediaIoBaseUpload(buffer_pdf, mimetype='application/pdf', resumable=True)

    if archivos:
        file_id = archivos[0]['id']
        servicio.files().update(fileId=file_id, media_body=media).execute()
        print(f"🔄 Actualizado en Drive: {nombre_archivo}")
    else:
        file_metadata = {'name': nombre_archivo, 'parents': [DRIVE_LN]}
        servicio.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"✅ Creado en Drive: {nombre_archivo}")

def procesar_catalogos():
    engine = create_engine(DATABASE_URL)
    
    # Extraemos todos los Lentes de Estilo Natural
    query = """
        SELECT v.sku, v.precio, p.color_principal, p.diametro, v.medida,
               CASE WHEN p.macro_categoria ILIKE 'peluca%' THEN 'Pelucas' ELSE 'Lentes' END AS macro_categoria, 
               p.categoria, p.marca, p.modelo, p.nombre, v.stock_interno, v.stock_externo,
               COALESCE(NULLIF(TRIM(v.url_imagen), ''), NULLIF(TRIM(p.url_imagen), '')) AS url_imagen
        FROM Variantes v
        JOIN Productos p ON v.id_producto = p.id_producto
        WHERE (p.macro_categoria NOT ILIKE 'peluca%' AND p.macro_categoria = 'Lentes' OR p.macro_categoria IS NULL)
          AND p.categoria = 'Estilo Natural'
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        print("No hay productos que coincidan con la línea base.")
        return

    # Filtramos obligatoriamente los de Medida Cero
    valores_cero = ['0', '0.0', '0.00', 'nan', 'none', '']
    df_cero = df[df['medida'].astype(str).str.strip().str.lower().isin(valores_cero)].copy()

    # Separamos en Tienda y Laboratorio
    df_tienda = df_cero[df_cero['stock_interno'] > 0]
    df_lab = df_cero[(df_cero['stock_interno'] <= 0) & (df_cero['stock_externo'] > 0)]

    drive_service = autenticar_drive()

    # --- GENERAR: LENTES EN TIENDA ---
    colores_tienda = df_tienda['color_principal'].dropna().unique()
    for color in colores_tienda:
        df_color = df_tienda[df_tienda['color_principal'] == color]
        if not df_color.empty:
            pdf_buffer = utils.generar_pdf_catalogo(df_color)
            nombre_pdf = f"LN {str(color).capitalize()} - En Tienda.pdf"
            subir_o_reemplazar_drive(drive_service, pdf_buffer, nombre_pdf)

    # --- GENERAR: LENTES EN LABORATORIO ---
    colores_lab = df_lab['color_principal'].dropna().unique()
    for color in colores_lab:
        df_color = df_lab[df_lab['color_principal'] == color]
        if not df_color.empty:
            pdf_buffer = utils.generar_pdf_catalogo(df_color)
            nombre_pdf = f"LN {str(color).capitalize()} - En Laboratorio.pdf"
            subir_o_reemplazar_drive(drive_service, pdf_buffer, nombre_pdf)

if __name__ == "__main__":
    procesar_catalogos()