import streamlit as st
import pandas as pd
import time
import random
from sqlalchemy import text
from database import engine
from utils import (
    enviar_mensaje_whatsapp, enviar_mensaje_media, 
    subir_archivo_meta, normalizar_telefono_maestro
)

def render_campanas():
    st.title("📢 Campañas Masivas")
    st.info("⚠️ **Advertencia de Seguridad:** WhatsApp detecta el comportamiento robótico. Usa intervalos de al menos 15-30 segundos para listas frías.")

    # --- 1. SELECCIÓN DE AUDIENCIA ---
    st.subheader("1. Seleccionar Audiencia")
    
    col_filtro1, col_filtro2 = st.columns(2)
    
    # Filtro por Estado
    estados_disponibles = ["Todos", "Sin empezar", "Interesado en venta", "Venta cerrada", "Post-venta", "Proveedor nacional"]
    filtro_estado = col_filtro1.selectbox("Filtrar por Estado", estados_disponibles)
    
    # Filtro por Distrito (Opcional)
    filtro_distrito = col_filtro2.text_input("Filtrar por Distrito (Opcional)", placeholder="Ej: Lima")

    # Botón para calcular audiencia
    if st.button("🔍 Buscar Clientes"):
        query = "SELECT id_cliente, nombre_corto, telefono, estado FROM Clientes WHERE activo = TRUE"
        params = {}
        
        if filtro_estado != "Todos":
            query += " AND estado = :est"
            params["est"] = filtro_estado
        
        # Nota: El distrito está en la tabla Direcciones, para simplificar filtramos solo por tabla Clientes,
        # pero si quisieras distrito, requeriría un JOIN. Por ahora mantenemos simple la segmentación.
        
        with engine.connect() as conn:
            df_audiencia = pd.read_sql(text(query), conn, params=params)
        
        st.session_state['audiencia_df'] = df_audiencia

    # Mostrar tabla de audiencia si existe
    if 'audiencia_df' in st.session_state and not st.session_state['audiencia_df'].empty:
        df = st.session_state['audiencia_df']
        st.success(f"🎯 **Audiencia Seleccionada:** {len(df)} clientes.")
        st.dataframe(df, use_container_width=True, hide_index=True)
    elif 'audiencia_df' in st.session_state:
        st.warning("No se encontraron clientes con esos filtros.")

    st.divider()

    # --- 2. CONFIGURAR MENSAJE ---
    st.subheader("2. Configurar Mensaje")
    
    c1, c2 = st.columns([2, 1])
    with c1:
        mensaje_texto = st.text_area("Escribe el mensaje:", height=150, placeholder="Hola! Tenemos nuevas ofertas...")
        st.caption("Tip: Usa *negritas* y emojis 🚀.")
    
    with c2:
        archivo_adjunto = st.file_uploader("Adjuntar Imagen/PDF (Opcional)", type=["png", "jpg", "jpeg", "pdf"])
        st.caption("El archivo se enviará junto con el texto.")

    # --- 3. CONFIGURACIÓN DE ENVÍO (SEGURIDAD) ---
    with st.expander("⚙️ Configuración de Intervalos (Seguridad)", expanded=True):
        col_t1, col_t2 = st.columns(2)
        min_delay = col_t1.number_input("Espera Mínima (segundos)", value=10, min_value=5)
        max_delay = col_t2.number_input("Espera Máxima (segundos)", value=25, min_value=10)
        st.caption(f"El sistema esperará un tiempo aleatorio entre **{min_delay} y {max_delay} segundos** entre cada mensaje.")

    # --- 4. LANZAMIENTO ---
    st.divider()
    
    # Validaciones antes de mostrar el botón de lanzamiento
    puede_lanzar = True
    if 'audiencia_df' not in st.session_state or st.session_state['audiencia_df'].empty:
        puede_lanzar = False
    if not mensaje_texto and not archivo_adjunto:
        st.warning("⚠️ Debes escribir un texto o subir un archivo.")
        puede_lanzar = False

    if puede_lanzar:
        st.markdown("### 🚀 Lanzar Campaña")
        confirmacion = st.checkbox("Confirmo que deseo enviar este mensaje masivo ahora.")
        
        if confirmacion and st.button("🔴 INICIAR ENVÍO MASIVO", type="primary"):
            df_target = st.session_state['audiencia_df']
            total = len(df_target)
            bar = st.progress(0)
            status_text = st.empty()
            log_box = st.empty()
            
            logs = []
            exitos = 0
            errores = 0

            # Pre-procesar archivo si existe
            media_data = None
            mime_type = None
            filename = None
            if archivo_adjunto:
                media_data, error_media = subir_archivo_meta(archivo_adjunto.getvalue(), archivo_adjunto.type)
                if error_media:
                    st.error(f"Error procesando archivo: {error_media}")
                    return
                mime_type = archivo_adjunto.type
                filename = archivo_adjunto.name

            # BUCLE DE ENVÍO
            for i, row in df_target.iterrows():
                nombre = row['nombre_corto']
                telefono = row['telefono']
                
                # Actualizar barra
                progreso = (i + 1) / total
                bar.progress(progreso)
                status_text.text(f"Enviando {i+1}/{total}: {nombre}...")

                # 1. ENVIAR
                try:
                    resultado = False
                    resp = ""
                    
                    # Normalizar número
                    norm = normalizar_telefono_maestro(telefono)
                    if not norm:
                        logs.append(f"❌ {nombre}: Número inválido")
                        errores += 1
                        continue
                    
                    tel_final = norm['db']

                    if media_data:
                        # Enviar con archivo
                        resultado, resp = enviar_mensaje_media(tel_final, media_data, mime_type, mensaje_texto, filename)
                        contenido_log = f"📎 {filename} + {mensaje_texto}"
                    else:
                        # Solo texto
                        resultado, resp = enviar_mensaje_whatsapp(tel_final, mensaje_texto)
                        contenido_log = mensaje_texto

                    # 2. REGISTRAR EN DB
                    if resultado:
                        exitos += 1
                        logs.append(f"✅ {nombre}: Enviado")
                        # Guardar en historial de chat
                        with engine.connect() as conn:
                            # Asegurar cliente
                            conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro) VALUES (:t, TRUE, NOW()) ON CONFLICT (telefono) DO NOTHING"), {"t": tel_final})
                            # Guardar mensaje
                            binary_file = archivo_adjunto.getvalue() if archivo_adjunto else None
                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) 
                                VALUES (:t, 'SALIENTE', :c, (NOW() - INTERVAL '5 hours'), TRUE, :d)
                            """), {"t": tel_final, "c": contenido_log, "d": binary_file})
                            conn.commit()
                    else:
                        errores += 1
                        logs.append(f"❌ {nombre}: Falló API ({resp})")

                except Exception as e:
                    errores += 1
                    logs.append(f"❌ {nombre}: Error crítico {e}")

                # Mostrar últimos logs
                log_box.code("\n".join(logs[-5:]))

                # 3. ESPERA ALEATORIA (THROTTLING)
                if i < total - 1: # No esperar después del último
                    tiempo_espera = random.randint(min_delay, max_delay)
                    status_text.text(f"⏳ Esperando {tiempo_espera}s para el siguiente...")
                    time.sleep(tiempo_espera)

            # FIN
            bar.progress(100)
            status_text.success("🎉 ¡Campaña Finalizada!")
            st.balloons()
            
            with st.expander("Ver Reporte Completo"):
                st.text("\n".join(logs))
            
            st.metric("Mensajes Enviados", exitos)
            st.metric("Errores", errores)