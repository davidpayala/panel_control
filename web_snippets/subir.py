"""Sube los snippets PHP de web_snippets/ a kmlentes.pe (plugin Code Snippets) por REST.
Uso: python web_snippets/subir.py   -> actualiza los snippets existentes por nombre o los crea."""
import os, requests
from dotenv import load_dotenv

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(RAIZ, '.env'))
SNIPPETS = {
    'KM SEO: marcas y schema': 'web_snippets/km_seo_marcas.php',
    'KM: purgar caché': 'web_snippets/km_purgar_cache.php',
    'banner optimi': 'web_snippets/km_banner_preload.php',
    'KM: galería de producto': 'web_snippets/km_galeria_producto.php',
    'KM: Site Kit con contraseña de aplicación': 'web_snippets/km_sitekit_app_password.php',
    'KM: diseño y confianza': 'web_snippets/km_diseno.php',
    'KM: envíos': 'web_snippets/km_envios.php',
    'KM: redirecciones': 'web_snippets/km_redirecciones.php',
    'KM: promociones': 'web_snippets/km_promos.php',
    'KM: temas de temporada': 'web_snippets/km_temas.php',
}
B = 'https://kmlentes.pe/wp-json/code-snippets/v1/snippets'

def main():
    s = requests.Session()
    s.auth = (os.getenv('WP_USER'), os.getenv('WP_APP_PASSWORD'))
    existentes = {x['name']: x['id'] for x in s.get(B, timeout=60).json()}
    for nombre, ruta in SNIPPETS.items():
        datos = {'name': nombre, 'code': open(os.path.join(RAIZ, ruta), encoding='utf-8').read(),
                 'scope': 'global', 'active': True}
        if nombre in existentes:
            r = s.post(f"{B}/{existentes[nombre]}", json=datos, timeout=60)
        else:
            r = s.post(B, json=datos, timeout=60)
        print(nombre, r.status_code, r.json().get('id'), r.json().get('active'))
    print('web', requests.get('https://kmlentes.pe/?ping=1', timeout=60).status_code)

if __name__ == '__main__':
    main()
