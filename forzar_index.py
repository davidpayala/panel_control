import os
import cloudscraper
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

url_tienda = os.getenv("WOO_URL", "").strip()
clave = os.getenv("WOO_CONSUMER_KEY", "").strip()
secreto = os.getenv("WOO_CONSUMER_SECRET", "").strip()

if url_tienda.endswith("/"):
    url_tienda = url_tienda[:-1]

def indexar_todos_los_productos():
    print(f"Conectando a: {url_tienda} saltando protección anti-bots...")
    pagina = 1
    productos_actualizados = 0

    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    
    # Formato de autenticación que exige WooCommerce
    autenticacion = HTTPBasicAuth(clave, secreto)

    while True:
        print(f"Consultando página {pagina} de productos...")
        endpoint_get = f"{url_tienda}/wp-json/wc/v3/products"
        
        # Petición GET con Auth Básica
        respuesta = scraper.get(
            endpoint_get, 
            auth=autenticacion,
            params={"per_page": 100, "page": pagina}, 
            timeout=45
        )
        
        try:
            productos = respuesta.json()
        except Exception:
            print(f"\n❌ El servidor bloqueó la respuesta (No es JSON).")
            print(f"Contenido: {respuesta.text[:300]}")
            break

        if isinstance(productos, dict) and "message" in productos:
            print(f"\n❌ Error de WooCommerce: {productos['message']}")
            break

        if not productos:
            break

        for producto in productos:
            producto_id = producto.get("id")
            nombre = producto.get("name", "Desconocido")

            data = {
                "meta_data": [
                    {
                        "key": "rank_math_robots",
                        "value": ["index"]
                    }
                ]
            }

            endpoint_put = f"{url_tienda}/wp-json/wc/v3/products/{producto_id}"
            
            # Petición PUT con Auth Básica
            actualizacion = scraper.put(
                endpoint_put, 
                auth=autenticacion,
                json=data, 
                timeout=45
            )

            if actualizacion.status_code in [200, 201]:
                print(f"✅ Producto [{producto_id}] -> INDEXADO")
                productos_actualizados += 1
            else:
                print(f"❌ Error con producto [{producto_id}]: {actualizacion.text}")

        pagina += 1

    print(f"\n🚀 Proceso terminado. {productos_actualizados} productos actualizados.")

indexar_todos_los_productos()