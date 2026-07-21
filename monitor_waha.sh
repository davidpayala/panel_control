#!/bin/bash

# Asegurar que el script siempre trabaje en la carpeta donde está guardado
cd "$(dirname "$0")"

# --- CARGAR VARIABLES DESDE .env ---
if [ -f .env ]; then
    set -a
    source .env
    set +a
else
    echo "Error: Archivo .env no encontrado."
    exit 1
fi

# Definir la ruta absoluta de Docker (Vital para que Cron no falle)
DOCKER_CMD="/usr/bin/docker"

# 1. Comprobar si el contenedor está corriendo a nivel físico
if ! $DOCKER_CMD ps --format '{{.Names}}' | grep -Eq "^waha$"; then
    echo "Fallo crítico: Contenedor 'waha' apagado. Reiniciando..."
    $DOCKER_CMD restart waha
    curl -s -X POST -H "Content-Type: application/json" -d '{"tipo": "sistema", "mensaje": "Contenedor WAHA estaba apagado. Reinicio automático ejecutado."}' $PANEL_WEBHOOK
    exit 1
fi

# 2. Comprobar si la API está colgada (Timeout de 10 segundos)
HTTP_STATUS=$(curl -o /dev/null -s -w "%{http_code}\n" --max-time 10 "$WAHA_URL/api/sessions?all=true" -H "X-Api-Key: $WAHA_KEY" -H "Accept: application/json")

if [ "$HTTP_STATUS" != "200" ]; then
    echo "Fallo crítico: La API de WAHA no responde (HTTP $HTTP_STATUS). Reiniciando contenedor..."
    $DOCKER_CMD restart waha
    curl -s -X POST -H "Content-Type: application/json" -d '{"tipo": "sistema", "mensaje": "La API de WAHA se colgó. Reinicio automático ejecutado."}' $PANEL_WEBHOOK
    exit 1
fi

# 3. Comprobar el estado interno de las sesiones
SESSIONS_JSON=$(curl -s --max-time 10 "$WAHA_URL/api/sessions?all=true" -H "X-Api-Key: $WAHA_KEY" -H "Accept: application/json")

echo "$SESSIONS_JSON" | jq -c '.[]' | while read session; do
    SESSION_NAME=$(echo $session | jq -r '.name')
    SESSION_STATUS=$(echo $session | jq -r '.status')

    # Escenario A: Pide código QR (Un reinicio no sirve de nada, requiere un humano)
    if [ "$SESSION_STATUS" == "SCAN_QR_CODE" ]; then
        curl -s -X POST -H "Content-Type: application/json" -d "{
            \"tipo\": \"sesion\",
            \"sesion\": \"$SESSION_NAME\",
            \"estado\": \"$SESSION_STATUS\",
            \"mensaje\": \"Alerta: Escanea el QR para volver a conectar WhatsApp.\"
        }" $PANEL_WEBHOOK

    # Escenario B: Sesión zombie, colapsada o detenida (Ejecutamos reinicio automático)
    elif [ "$SESSION_STATUS" == "FAILED" ] || [ "$SESSION_STATUS" == "STOPPED" ]; then
        echo "Sesión $SESSION_NAME muerta. Aplicando electroshock por API..."
        
        # 1. Avisamos al panel
        curl -s -X POST -H "Content-Type: application/json" -d "{
            \"tipo\": \"sesion\",
            \"sesion\": \"$SESSION_NAME\",
            \"estado\": \"$SESSION_STATUS\",
            \"mensaje\": \"Sesión caída. Intentando reconexión automática...\"
        }" $PANEL_WEBHOOK
        
        # 2. REINICIAMOS LA SESIÓN
        curl -s -X POST "$WAHA_URL/api/sessions/$SESSION_NAME/restart" -H "X-Api-Key: $WAHA_KEY" -H "Accept: application/json"
    fi
done