import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cargar configuración
load_dotenv()
db_url = os.getenv("DATABASE_URL")
if db_url and db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)

motor_seguro = create_engine(db_url)

def probar_prompt_por_sku(sku):
    print(f"\n🔍 Buscando el SKU: '{sku}' en la base de datos...")

    # 1. Extracción de datos exacta a utils.py
    with motor_seguro.connect() as conn:
        sql = text("""
            SELECT 
                v.sku, 
                p.nombre, 
                p.categoria,
                p.color_principal,
                g.nombre_grupo,
                g.descripcion AS descripcion_grupo
            FROM productos p
            JOIN variantes v ON p.id_producto = v.id_producto
            LEFT JOIN grupos_productos g ON v.id_grupo = g.id_grupo
            WHERE v.sku = :sku
        """)
        resultado = conn.execute(sql, {"sku": sku}).fetchone()

    if not resultado:
        print("❌ SKU no encontrado. Verifica que esté bien escrito (respeta mayúsculas y minúsculas).")
        return

    # 2. Armamos el diccionario de datos
    producto = {
        'nombre': resultado.nombre,
        'categoria': resultado.categoria,
        'color_principal': resultado.color_principal,
        'grupo': resultado.nombre_grupo,
        'descripcion_grupo': resultado.descripcion_grupo
    }

    print("\n📦 DATOS CRUDOS EXTRAÍDOS DE SQL:")
    for clave, valor in producto.items():
        print(f"   - {clave}: '{valor}'")  # Las comillas simples te ayudarán a ver si hay espacios invisibles

    # 3. Lógica idéntica a la que le llega a la IA
    nombre = producto.get('nombre') or 'Producto'
    categoria = producto.get('categoria') or 'Accesorio'
    color = producto.get('color_principal') or 'No especificado'
    grupo = producto.get('grupo') or 'General'
    desc_grupo = producto.get('descripcion_grupo') or ''

    cat_lower = str(categoria).lower()

    if 'natural' in cat_lower:
        enfoque_estricto = "ENFOQUE OBLIGATORIO: Habla sobre resaltar la belleza natural, el uso diario, cosmética y una mirada sutil pero impactante. NO hables de cosplay, ni disfraces."
    elif 'fantas' in cat_lower or 'cosplay' in cat_lower or 'anime' in cat_lower:
        enfoque_estricto = "ENFOQUE OBLIGATORIO: Habla sobre cosplay, disfraces, anime, eventos de cultura pop y transformaciones extremas. Usa un tono muy llamativo y atrevido."
    else:
        enfoque_estricto = "ENFOQUE OBLIGATORIO: Destaca su utilidad, diseño exclusivo y lo práctico que es como accesorio para complementar el estilo."

    prompt = f"""Eres un copywriter experto en marketing digital para Akiba.pe, tienda de lentes de contacto en Perú. 
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
3. Invita a que te envíen un mensaje al final.
4. Máximo 35 palabras.
5. Devuelve ÚNICAMENTE el texto final para copiar y pegar (sin comillas, sin decir 'Aquí tienes', sin saludos iniciales).
"""

    print("\n🤖 PROMPT FINAL QUE SE LE ENVÍA A OLLAMA (Llama 3.1):")
    print("-------------------------------------------------------------")
    print(prompt)
    print("-------------------------------------------------------------\n")

if __name__ == "__main__":
    while True:
        sku_ingresado = input("👉 Ingresa el SKU a evaluar (o escribe 'salir' para terminar): ")
        if sku_ingresado.lower() == 'salir':
            break
        probar_prompt_por_sku(sku_ingresado.strip())