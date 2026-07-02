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

# 1. Comprobar si el contenedor está corriendo a nivel de Docker
if ! docker ps --format '{{.Names}}' | grep -Eq "^waha$"; then
    echo "Fallo crítico: Contenedor 'waha' apagado. Reiniciando..."
    sudo docker restart waha
    curl -s -X POST -H "Content-Type: application/json" -d '{"tipo": "sistema", "mensaje": "El contenedor WAHA estaba caído y fue reiniciado automáticamente."}' $PANEL_WEBHOOK
    exit 1
fi

# 2. Comprobar si la API está colgada (Timeout de 10 segundos)
HTTP_STATUS=$(curl -o /dev/null -s -w "%{http_code}\n" --max-time 10 "$WAHA_URL/api/sessions?all=true" -H "X-Api-Key: $WAHA_KEY" -H "Accept: application/json")

if [ "$HTTP_STATUS" != "200" ]; then
    echo "Fallo crítico: La API de WAHA no responde (HTTP $HTTP_STATUS). Contenedor colgado. Reiniciando..."
    sudo docker restart waha
    curl -s -X POST -H "Content-Type: application/json" -d '{"tipo": "sistema", "mensaje": "La API de WAHA se colgó y dejó de responder. El contenedor fue reiniciado."}' $PANEL_WEBHOOK
    exit 1
fi

# 3. Comprobar el estado de las sesiones (Si llegamos aquí, el contenedor y la API están bien)
SESSIONS_JSON=$(curl -s --max-time 10 "$WAHA_URL/api/sessions?all=true" -H "X-Api-Key: $WAHA_KEY" -H "Accept: application/json")

# Recorrer cada sesión encontrada
echo "$SESSIONS_JSON" | jq -c '.[]' | while read session; do
    SESSION_NAME=$(echo $session | jq -r '.name')
    SESSION_STATUS=$(echo $session | jq -r '.status')

    # Si la sesión no está trabajando, enviamos alerta al Panel
    if [ "$SESSION_STATUS" == "FAILED" ] || [ "$SESSION_STATUS" == "SCAN_QR_CODE" ] || [ "$SESSION_STATUS" == "STOPPED" ]; then
        echo "Alerta de Sesión: $SESSION_NAME está en estado $SESSION_STATUS"
        
        # Enviar el JSON de alerta al Panel de Control
        curl -s -X POST -H "Content-Type: application/json" -d "{
            \"tipo\": \"sesion\",
            \"sesion\": \"$SESSION_NAME\",
            \"estado\": \"$SESSION_STATUS\",
            \"mensaje\": \"La sesión de WhatsApp requiere atención inmediata.\"
        }" $PANEL_WEBHOOK
    fi
done
