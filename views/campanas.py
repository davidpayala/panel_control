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
    st.info("⚠️ **Estrategia Antibloqueo:** El sistema dividirá automáticamente a los clientes sin etiqueta en grupos de 200.")

    # --- 1. CARGA Y SEGMENTACIÓN DE AUDIENCIA ---
    st.subheader("1. Seleccionar Grupo Objetivo")
    
    # Botón de recarga
    if st.button("🔄 Calcular Grupos y Audiencia"):
        with engine.connect() as conn:
            # Traemos etiquetas también
            query = """
                SELECT id_cliente, nombre_corto, telefono, estado, etiquetas 
                FROM Clientes 
                WHERE activo = TRUE
            """
            df_raw = pd.read_sql(text(query), conn)
        
        if df_raw.empty:
            st.warning("No hay clientes activos.")
            return

        # --- ALGORITMO DE AGRUPACIÓN ---
        def clasificar_cliente(row):
            tags = str(row['etiquetas'] or "")
            if "SPAM" in tags: return "🚫 SPAM (Pruebas)"
            if "VIP" in tags: return "💎 VIP (Prioridad)"
            if "Compró" in tags or "Compro" in tags: return "✅ Compradores"
            return "GENERAL" # Estos irán a grupos numerados

        df_raw['segmento_base'] = df_raw.apply(clasificar_cliente, axis=1)

        # Diccionario de DataFrames para el selector
        opciones_segmentos = {}

        # 1. Grupos Especiales
        for seg in ["🚫 SPAM (Pruebas)", "💎 VIP (Prioridad)", "✅ Compradores"]:
            sub_df = df_raw[df_raw['segmento_base'] == seg]
            if not sub_df.empty:
                opciones_segmentos[f"{seg} ({len(sub_df)})"] = sub_df

        # 2. Grupos Numerados (Resto del mundo)
        df_general = df_raw[df_raw['segmento_base'] == "GENERAL"]
        if not df_general.empty:
            tamano_grupo = 200
            total_general = len(df_general)
            # Crear chunks
            for i in range(0, total_general, tamano_grupo):
                grupo_num = (i // tamano_grupo) + 1
                subset = df_general.iloc[i : i + tamano_grupo]
                titulo = f"📦 Grupo {grupo_num} (General) - {len(subset)} personas"
                opciones_segmentos[titulo] = subset

        st.session_state['mapa_segmentos'] = opciones_segmentos
        st.success("✅ Segmentación recalculada.")

    # --- SELECTOR DE GRUPO ---
    if 'mapa_segmentos' in st.session_state and st.session_state['mapa_segmentos']:
        opciones = list(st.session_state['mapa_segmentos'].keys())
        seleccion = st.selectbox("🎯 ¿A quién enviamos hoy?", opciones)
        
        # Guardar el DF seleccionado en session
        df_target = st.session_state['mapa_segmentos'][seleccion]
        st.session_state['audiencia_final'] = df_target
        
        # Previsualización
        with st.expander(f"Ver lista de: {seleccion}"):
            st.dataframe(df_target[['nombre_corto', 'telefono', 'etiquetas']], use_container_width=True)
    else:
        st.info("👆 Dale al botón 'Calcular' para empezar.")

    st.divider()

    # --- 2. CONFIGURAR MENSAJE ---
    st.subheader("2. Configurar Mensaje")
    
    c1, c2 = st.columns([2, 1])
    with c1:
        mensaje_texto = st.text_area("Mensaje:", height=150, placeholder="Hola! Tenemos nuevas ofertas en lentes...")
        st.caption("Tip: Usa *negritas* y emojis 🚀. El mensaje es igual para todos.")
    
    with c2:
        archivo_adjunto = st.file_uploader("Imagen (Opcional)", type=["png", "jpg", "jpeg", "pdf"])
        st.caption("Se enviará con WAHA Plus (Base64).")

    # --- 3. CONFIGURACIÓN DE ENVÍO (SEGURIDAD) ---
    with st.expander("⚙️ Configuración de Intervalos (Anti-Ban)", expanded=True):
        col_t1, col_t2 = st.columns(2)
        min_delay = col_t1.number_input("Espera Mínima (segundos)", value=10, min_value=2)
        max_delay = col_t2.number_input("Espera Máxima (segundos)", value=20, min_value=5)
        st.caption(f"Tiempo aleatorio entre **{min_delay} y {max_delay} segundos** por mensaje.")

    # --- 4. LANZAMIENTO ---
    st.divider()
    
    # Validaciones
    puede_lanzar = True
    if 'audiencia_final' not in st.session_state or st.session_state['audiencia_final'].empty:
        puede_lanzar = False
    if not mensaje_texto and not archivo_adjunto:
        st.warning("⚠️ Escribe texto o sube archivo.")
        puede_lanzar = False

    if puede_lanzar:
        df_lanzar = st.session_state['audiencia_final']
        st.markdown(f"### 🚀 Listo para enviar a {len(df_lanzar)} contactos")
        
        confirmacion = st.checkbox("✅ Confirmo que deseo iniciar el envío masivo.")
        
        if confirmacion and st.button("🔴 EJECUTAR CAMPAÑA", type="primary"):
            bar = st.progress(0)
            status_text = st.empty()
            log_box = st.empty()
            
            logs = []
            exitos = 0
            errores = 0
            total = len(df_lanzar)

            # Preparar archivo UNA SOLA VEZ (Bytes para WAHA Plus)
            media_bytes = None
            mime_type = None
            filename = None
            
            if archivo_adjunto:
                media_bytes = archivo_adjunto.getvalue() # Bytes crudos
                mime_type = archivo_adjunto.type
                filename = archivo_adjunto.name

            # BUCLE DE ENVÍO
            for i, row in df_lanzar.reset_index().iterrows():
                nombre = row['nombre_corto'] or "Cliente"
                telefono = row['telefono']
                
                # Actualizar UI
                progreso = (i + 1) / total
                bar.progress(progreso)
                status_text.text(f"Procesando {i+1}/{total}: {nombre}...")

                try:
                    resultado = False
                    resp = ""
                    
                    # 1. Normalizar
                    norm = normalizar_telefono_maestro(telefono)
                    if not norm:
                        logs.append(f"❌ {nombre}: Número inválido ({telefono})")
                        errores += 1
                        continue
                    
                    tel_final = norm['db']

                    # 2. Enviar (Imagen o Texto)
                    if media_bytes:
                        # WAHA Plus usa Base64 interno, pasamos los bytes directo a utils
                        resultado, resp = enviar_mensaje_media(tel_final, media_bytes, mime_type, mensaje_texto, filename)
                        contenido_log = f"📎 {filename} + {mensaje_texto}"
                    else:
                        resultado, resp = enviar_mensaje_whatsapp(tel_final, mensaje_texto)
                        contenido_log = mensaje_texto

                    # 3. Registrar Resultado
                    if resultado:
                        exitos += 1
                        logs.append(f"✅ {nombre}: Enviado")
                        
                        # Guardar historial en DB
                        with engine.connect() as conn:
                            # Aseguramos existencia en tabla clientes por si acaso
                            conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) VALUES (:t, TRUE, NOW(), :n) ON CONFLICT (telefono) DO NOTHING"), {"t": tel_final, "n": nombre})
                            
                            # Guardamos el mensaje como SALIENTE
                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) 
                                VALUES (:t, 'SALIENTE', :c, (NOW() - INTERVAL '5 hours'), TRUE, :d)
                            """), {"t": tel_final, "c": contenido_log, "d": media_bytes if media_bytes else None})
                            conn.commit()
                    else:
                        errores += 1
                        logs.append(f"⚠️ {nombre}: Fallo API -> {resp}")

                except Exception as e:
                    errores += 1
                    logs.append(f"🔥 {nombre}: Error crítico {e}")

                # Actualizar caja de logs
                log_box.code("\n".join(logs[-6:])) # Mostrar últimos 6

                # 4. Espera Aleatoria (Solo si no es el último)
                if i < total - 1:
                    tiempo_espera = random.randint(min_delay, max_delay)
                    status_text.text(f"⏳ Esperando {tiempo_espera}s para despistar al algoritmo...")
                    time.sleep(tiempo_espera)

            # FIN
            bar.progress(100)
            status_text.success("🎉 ¡Campaña Finalizada!")
            st.balloons()
            
            # Resumen Final
            c_res1, c_res2 = st.columns(2)
            c_res1.metric("Enviados con Éxito", exitos)
            c_res2.metric("Fallidos", errores)
            
            with st.expander("📄 Ver Log Completo"):
                st.text("\n".join(logs))