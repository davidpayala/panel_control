import streamlit as st
import pandas as pd
import time
import random
from sqlalchemy import text
from database import engine
from utils import (
    enviar_mensaje_whatsapp, enviar_mensaje_media, 
    normalizar_telefono_maestro
)

def render_campanas():
    st.title("📢 Campañas Masivas Segmentadas")
    st.info("⚠️ **Estrategia Antibloqueo:** El sistema dividirá automáticamente a los clientes 'generales' en grupos seguros de 200 personas.")

    # --- 1. CARGA Y SEGMENTACIÓN ---
    st.subheader("1. Seleccionar Grupo Objetivo")
    
    if st.button("🔄 Recargar y Calcular Grupos"):
        with engine.connect() as conn:
            # Traemos etiquetas para clasificar
            # Asegúrate de que la columna 'etiquetas' exista en tu DB. 
            # Si no, el código asumirá cadena vacía y los mandará a General.
            query = """
                SELECT id_cliente, nombre_corto, telefono, estado, COALESCE(etiquetas, '') as etiquetas 
                FROM Clientes 
                WHERE activo = TRUE
            """
            try:
                df_raw = pd.read_sql(text(query), conn)
            except:
                # Fallback si no existe columna etiquetas aún
                query_simple = "SELECT id_cliente, nombre_corto, telefono, estado, '' as etiquetas FROM Clientes WHERE activo = TRUE"
                df_raw = pd.read_sql(text(query_simple), conn)
        
        if df_raw.empty:
            st.warning("No hay clientes activos.")
            return

        # ALGORITMO DE CLASIFICACIÓN
        def clasificar(row):
            tags = str(row['etiquetas']).upper()
            if "SPAM" in tags: return "🚫 SPAM (Pruebas)"
            if "VIP" in tags: return "💎 VIP (Prioridad)"
            if "COMPRÓ" in tags or "COMPRO" in tags or "VENTA CERRADA" in str(row['estado']).upper(): 
                return "✅ Compradores"
            return "GENERAL"

        df_raw['segmento'] = df_raw.apply(clasificar, axis=1)

        # Crear Diccionario de Grupos
        opciones_segmentos = {}

        # 1. Grupos Especiales
        for seg in ["🚫 SPAM (Pruebas)", "💎 VIP (Prioridad)", "✅ Compradores"]:
            sub = df_raw[df_raw['segmento'] == seg]
            if not sub.empty:
                opciones_segmentos[f"{seg} ({len(sub)})"] = sub

        # 2. Grupos Generales (Chunks de 200)
        df_gen = df_raw[df_raw['segmento'] == "GENERAL"]
        if not df_gen.empty:
            tamano = 200
            total_gen = len(df_gen)
            for i in range(0, total_gen, tamano):
                grupo_num = (i // tamano) + 1
                subset = df_gen.iloc[i : i + tamano]
                nombre_grupo = f"📦 Grupo {grupo_num} (General) - {len(subset)} personas"
                opciones_segmentos[nombre_grupo] = subset

        st.session_state['mapa_segmentos'] = opciones_segmentos
        st.success("✅ Grupos calculados.")

    # SELECTOR
    if 'mapa_segmentos' in st.session_state and st.session_state['mapa_segmentos']:
        opciones = list(st.session_state['mapa_segmentos'].keys())
        seleccion = st.selectbox("🎯 ¿A quién enviamos?", opciones)
        
        df_target = st.session_state['mapa_segmentos'][seleccion]
        st.session_state['audiencia_final'] = df_target
        
        with st.expander(f"Ver lista: {seleccion}"):
            st.dataframe(df_target[['nombre_corto', 'telefono', 'etiquetas']], hide_index=True)
    else:
        st.info("👆 Presiona 'Recargar' para empezar.")

    st.divider()

    # --- 2. MENSAJE ---
    st.subheader("2. Configurar Mensaje")
    c1, c2 = st.columns([2, 1])
    with c1:
        mensaje_texto = st.text_area("Mensaje:", height=150)
    with c2:
        archivo_adjunto = st.file_uploader("Imagen (Opcional)", type=["png", "jpg", "jpeg"])

    # --- 3. CONFIGURACIÓN ENVÍO ---
    with st.expander("⚙️ Intervalos (Seguridad)", expanded=True):
        col_t1, col_t2 = st.columns(2)
        min_delay = col_t1.number_input("Mínimo (seg)", value=10, min_value=2)
        max_delay = col_t2.number_input("Máximo (seg)", value=20, min_value=5)

    # --- 4. EJECUCIÓN ---
    st.divider()
    
    if 'audiencia_final' in st.session_state and not st.session_state['audiencia_final'].empty:
        df_lanzar = st.session_state['audiencia_final']
        
        if st.checkbox(f"✅ Confirmo envío a {len(df_lanzar)} personas"):
            if st.button("🔴 EJECUTAR CAMPAÑA", type="primary"):
                bar = st.progress(0)
                status = st.empty()
                log_box = st.empty()
                logs = []
                exitos = 0
                
                # Preparar Archivo (Bytes para WAHA Plus)
                media_bytes = None
                mime_type = None
                filename = None
                if archivo_adjunto:
                    media_bytes = archivo_adjunto.getvalue()
                    mime_type = archivo_adjunto.type
                    filename = archivo_adjunto.name

                total = len(df_lanzar)
                
                for i, row in df_lanzar.reset_index().iterrows():
                    nombre = row['nombre_corto']
                    tel = row['telefono']
                    
                    bar.progress((i+1)/total)
                    status.text(f"Enviando {i+1}/{total}: {nombre}")

                    try:
                        norm = normalizar_telefono_maestro(tel)
                        if not norm:
                            logs.append(f"❌ {nombre}: Teléfono inválido")
                            continue
                            
                        tel_final = norm['db']
                        res = False
                        
                        if media_bytes:
                            res, _ = enviar_mensaje_media(tel_final, media_bytes, mime_type, mensaje_texto or "", filename)
                        elif mensaje_texto:
                            res, _ = enviar_mensaje_whatsapp(tel_final, mensaje_texto)
                        
                        if res:
                            exitos += 1
                            logs.append(f"✅ {nombre}: Enviado")
                            # Guardar Log en BD
                            with engine.connect() as conn:
                                conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :c, (NOW() - INTERVAL '5 hours'), TRUE, :d)"), 
                                            {"t": tel_final, "c": mensaje_texto or "Archivo", "d": media_bytes})
                                conn.commit()
                        else:
                            logs.append(f"⚠️ {nombre}: Falló envío")

                    except Exception as e:
                        logs.append(f"🔥 Error {nombre}: {e}")

                    log_box.code("\n".join(logs[-5:]))
                    
                    # Espera (menos en el último)
                    if i < total - 1:
                        ts = random.randint(min_delay, max_delay)
                        status.text(f"⏳ Esperando {ts}s...")
                        time.sleep(ts)

                bar.progress(100)
                status.success("¡Terminado!")
                st.success(f"Enviados: {exitos}/{total}")