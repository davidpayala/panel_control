"""Sube la cabecera y el pie de kmlentes.pe (partes de plantilla del tema) desde web_snippets/plantillas/.
Uso: python web_snippets/subir_plantillas.py   (guarda antes un respaldo en respaldos_web/)."""
import json, os, time, requests
from dotenv import load_dotenv

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(RAIZ, '.env'))
B = 'https://kmlentes.pe/wp-json/wp/v2/template-parts/'
PARTES = {'hostinger-ai-theme//header': 'header.html', 'hostinger-ai-theme//footer': 'footer.html'}

def main():
    s = requests.Session()
    s.auth = (os.getenv('WP_USER'), os.getenv('WP_APP_PASSWORD'))
    respaldo = {}
    for pid, archivo in PARTES.items():
        respaldo[pid] = s.get(B + pid, params={'context': 'edit'}, timeout=60).json()['content']['raw']
    ruta = os.path.join(RAIZ, 'respaldos_web', 'plantillas_' + time.strftime('%Y%m%d_%H%M%S') + '.json')
    json.dump(respaldo, open(ruta, 'w'), ensure_ascii=False, indent=1)
    for pid, archivo in PARTES.items():
        contenido = open(os.path.join(RAIZ, 'web_snippets', 'plantillas', archivo), encoding='utf-8').read()
        r = s.post(B + pid, json={'content': contenido}, timeout=60)
        print(pid, r.status_code)
    print('respaldo', ruta)

if __name__ == '__main__':
    main()
