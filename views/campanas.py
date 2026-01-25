import streamlit as st
import pandas as pd
import time
import random
import re
from sqlalchemy import text
from database import engine
from utils import (
    enviar_mensaje_whatsapp, enviar_mensaje_media, 
    normalizar_telefono_maestro
)

# --- FUNCIÓN SPINTAX (Humanizador de Texto) ---
def procesar_spintax(texto):
    """
    Convierte: "{Hola|Buenas}, {oferta|promo} para ti"
    En: "Buenas, oferta para ti" (Aleatorio)
    Evita que WhatsApp detecte el mismo 'hash' de mensaje repetido.
    """
    if not texto: return ""
    pattern = r'\{([^{}]+)\}'
    while True:
        match = re.search(pattern, texto)
        if not match:
            break
        opciones = match.group(1).split('|')
        eleccion = random.choice(opciones)
        texto = texto[:match.start()] + eleccion + texto[match.end():]
    return texto

def render_campanas():
    st.title("📢 Campañas Masivas (Modo Seguro 🛡️)")
    st.warning("⚠️ **ESTADO DE ALERTA:** WhatsApp está estricto. Usa Spintax y pausas largas.")

    # --- 1. CARGA Y SEGMENTACIÓN ---
    st.subheader("1. Seleccionar Grupo")
    
    if st.button("🔄 Recargar Grupos"):
        with engine.connect() as conn:
            try:
                # Intentamos leer etiquetas
                query = "SELECT id_cliente, nombre_corto, telefono, estado, COALESCE(etiquetas, '') as etiquetas FROM Clientes WHERE activo = TRUE"
                df_raw = pd.read_sql(text(query), conn)
            except:
                st.error("Error leyendo base de datos.")
                return

        if df_raw.empty:
            st.warning("Sin clientes activos.")
            return

        # CLASIFICACIÓN
        def clasificar(row):
            tags = str(row['etiquetas']).upper()
            if "SPAM" in tags: return "🚫 SPAM (Pruebas)"
            if "VIP" in tags: return "💎 VIP (Prioridad)"
            if "COMPRÓ" in tags or "COMPRO" in tags: return "✅ Compradores"
            return "GENERAL"

        df_raw['segmento'] = df_raw.apply(clasificar, axis=1)
        opciones_segmentos = {}

        # Grupos especiales
        for seg in ["🚫 SPAM (Pruebas)", "💎 VIP (Prioridad)", "✅ Compradores"]:
            sub = df_raw[df_raw['segmento'] == seg]
            if not sub.empty:
                opciones_segmentos[f"{seg} ({len(sub)})"] = sub

        # Grupos Generales (Reducidos a 50 para seguridad actual)
        df_gen = df_raw[df_raw['segmento'] == "GENERAL"]
        if not df_gen.empty:
            tamano = 50 # REDUCIDO DE 200 A 50 POR SEGURIDAD
            total_gen = len(df_gen)
            for i in range(0, total_gen, tamano):
                grupo_num = (i // tamano) + 1
                subset = df_gen.iloc[i : i + tamano]
                nombre_grupo = f"📦 Grupo {grupo_num} (General) - {len(subset)} pax"
                opciones_segmentos[nombre_grupo] = subset

        st.session_state['mapa_segmentos'] = opciones_segmentos
        st.success("✅ Grupos recalculados (Máx 50 pax por seguridad).")

    # SELECTOR
    if 'mapa_segmentos' in st.session_state and st.session_state['mapa_segmentos']:
        opciones = list(st.session_state['mapa_segmentos'].keys())
        seleccion = st.selectbox("🎯 Audiencia:", opciones)
        st.session_state['audiencia_final'] = st.session_state['mapa_segmentos'][seleccion]
    else:
        st.info("👆 Carga los grupos primero.")

    st.divider()

    # --- 2. MENSAJE CON SPINTAX ---
    st.subheader("2. Mensaje (Usa Spintax)")
    st.markdown("""
    **¿Cómo evitar bloqueos?** Usa llaves `{}` para variar palabras.
    *Ejemplo:* `{Hola|Buenas|Qué tal} {amigo|cliente}, mira {esto|la oferta}.`
    """)
    
    c1, c2 = st.columns([2, 1])
    with c1:
        mensaje_base = st.text_area("Texto Base:", height=150, placeholder="{Hola|Hola que tal} estimad@...")
        
        # Previsualizador de Spintax
        if mensaje_base:
            st.caption(f"👁️ Ejemplo real: *{procesar_spintax(mensaje_base)}*")

    with c2:
        archivo_adjunto = st.file_uploader("Imagen", type=["png", "jpg", "jpeg"])

    # --- 3. INTERVALOS DE SEGURIDAD ---
    with st.expander("⚙️ Configuración Anti-Bloqueo (Recomendada)", expanded=True):
        col_t1, col_t2, col_t3 = st.columns(3)
        # Tiempos aumentados drásticamente
        min_delay = col_t1.number_input("Espera Mínima (seg)", value=45, min_value=30)
        max_delay = col_t2.number_input("Espera Máxima (seg)", value=90, min_value=60)
        batch_size = col_t3.number_input("Pausa larga cada X mensajes:", value=10, min_value=5)

    # --- 4. EJECUCIÓN ---
    st.divider()
    
    if 'audiencia_final' in st.session_state and not st.session_state['audiencia_final'].empty:
        df_lanzar = st.session_state['audiencia_final']
        
        st.error(f"⚠️ Estás a punto de enviar a {len(df_lanzar)} personas. Asegúrate de que tu número esté operativo.")
        
        if st.button("🔴 INICIAR CAMPAÑA SEGURA", type="primary"):
            bar = st.progress(0)
            status = st.empty()
            log_box = st.empty()
            logs = []
            exitos = 0
            
            # Preparar Imagen
            media_bytes = None
            mime_type = None
            filename = None
            if archivo_adjunto:
                media_bytes = archivo_adjunto.getvalue()
                mime_type = archivo_adjunto.type
                filename = archivo_adjunto.name

            total = len(df_lanzar)
            
            for i, row in df_lanzar.reset_index().iterrows():
                # --- PAUSA DE LOTE (COOL-DOWN) ---
                if i > 0 and i % batch_size == 0:
                    tiempo_lote = random.randint(180, 300) # 3 a 5 minutos
                    mins = tiempo_lote // 60
                    status.warning(f"🛑 PAUSA DE SEGURIDAD (Lote de {batch_size}): Esperando {mins} minutos para enfriar...")
                    # Cuenta regresiva visual
                    for s in range(tiempo_lote, 0, -1):
                        status.warning(f"🛑 Enfriando... {s} segundos restantes.")
                        time.sleep(1)
                    status.text("🟢 Reanudando...")

                nombre = row['nombre_corto']
                tel = row['telefono']
                
                # Generar texto único para este usuario
                mensaje_final = procesar_spintax(mensaje_base)

                bar.progress((i+1)/total)
                status.text(f"Procesando {i+1}/{total}: {nombre}")

                try:
                    norm = normalizar_telefono_maestro(tel)
                    if not norm:
                        logs.append(f"❌ {nombre}: Número inválido")
                        continue
                    
                    tel_final = norm['db']
                    res = False
                    
                    # Enviar
                    if media_bytes:
                        res, _ = enviar_mensaje_media(tel_final, media_bytes, mime_type, mensaje_final, filename)
                    elif mensaje_final:
                        res, _ = enviar_mensaje_whatsapp(tel_final, mensaje_final)
                    
                    if res:
                        exitos += 1
                        logs.append(f"✅ {nombre}: Enviado")
                        # Log DB
                        with engine.connect() as conn:
                            conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :c, (NOW() - INTERVAL '5 hours'), TRUE, :d)"), 
                                        {"t": tel_final, "c": mensaje_final, "d": media_bytes})
                            conn.commit()
                    else:
                        logs.append(f"⚠️ {nombre}: Falló envío (API)")

                except Exception as e:
                    logs.append(f"🔥 Error {nombre}: {e}")

                log_box.code("\n".join(logs[-6:]))
                
                # --- ESPERA ENTRE MENSAJES ---
                if i < total - 1:
                    # Variación extra aleatoria
                    ts = random.randint(min_delay, max_delay)
                    status.text(f"⏳ Esperando {ts}s (Antibot activo)...")
                    time.sleep(ts)

            bar.progress(100)
            st.success(f"🎉 Campaña terminada. {exitos}/{total} enviados.")
            st.balloons()