import streamlit as st
import pandas as pd
from sqlalchemy import text
import json
import io
import os
import time
import streamlit.components.v1 as components 
import requests  # <--- AGREGAR ESTO
from datetime import datetime
from streamlit_autorefresh import st_autorefresh 
from database import engine 
from utils import (
    enviar_mensaje_media, enviar_mensaje_whatsapp, 
    normalizar_telefono_maestro, buscar_contacto_google, 
    crear_en_google, sincronizar_historial, render_chat
)

# Copiamos las mismas opciones para mantener consistencia
OPCIONES_TAGS = [
    "🚫 SPAM", "⚠️ Problemático", "💎 VIP / Recurrente", 
    "✅ Compró", "👀 Prospecto", "❓ Preguntón", 
    "📉 Pide Rebaja", "📦 Mayorista"
]
sincronizar_historial ()
render_chat ()

def mostrar_info_avanzada(telefono):
    """Ficha de cliente integrada en el chat"""
    with engine.connect() as conn:
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        if not res_cliente:
            st.warning("⚠️ No registrado.")
            if st.button("Crear Ficha Rápida"):
                 with engine.connect() as conn:
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) VALUES (:t, TRUE, NOW(), 'Nuevo Cliente')"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        cl = res_cliente._mapping
        id_cliente = cl.get('id_cliente')
        
        dirs = pd.DataFrame()
        if id_cliente:
            dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id"), conn, params={"id": id_cliente})

    # --- EDICIÓN PRINCIPAL (INCLUYE ETIQUETAS) ---
    with st.container():
        c1, c2 = st.columns(2)
        new_corto = c1.text_input("Alias", value=cl.get('nombre_corto') or "", key=f"in_corto_{telefono}")
        
        # Recuperar etiquetas actuales
        tags_actuales_db = cl.get('etiquetas', '') or ""
        lista_tags = [t for t in tags_actuales_db.split(',') if t] # Limpiar vacíos
        
        # Selector Múltiple
        new_tags = c2.multiselect("Etiquetas", OPCIONES_TAGS, default=[t for t in lista_tags if t in OPCIONES_TAGS], key=f"tag_chat_{telefono}")

    # --- GOOGLE ---
    st.markdown("#### 👤 Datos")
    col_nom, col_ape, col_btns = st.columns([1.5, 1.5, 1.5])
    
    new_nombre = col_nom.text_input("Nombre", value=cl.get('nombre') or "", key=f"in_nom_{telefono}")
    new_apellido = col_ape.text_input("Apellido", value=cl.get('apellido') or "", key=f"in_ape_{telefono}")

    with col_btns:
            st.write("") 
            # Cambiamos el texto del botón para reflejar que también crea
            if st.button("📥 Google (Buscar/Crear)", key=f"btn_search_{telefono}", use_container_width=True):
                with st.spinner("Conectando con Google..."):
                    norm = normalizar_telefono_maestro(telefono)
                    tel_format = norm['db']
                    
                    # 1. Intentamos BUSCAR primero
                    datos = buscar_contacto_google(tel_format) 
                    
                    if datos and datos['encontrado']:
                        # CASO A: ENCONTRADO -> Actualizamos local
                        with engine.connect() as conn:
                            conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc WHERE telefono=:t"), 
                                        {"n": datos['nombre'], "a": datos['apellido'], "gid": datos['google_id'], "nc": datos['nombre_completo'], "t": telefono})
                            conn.commit()
                        st.toast("✅ Sincronizado desde Google")
                        time.sleep(1)
                        st.rerun()
                    
                    else:
                        # CASO B: NO ENCONTRADO -> CREAMOS EN GOOGLE
                        # Verificamos si el usuario escribió un nombre en el input
                        if new_nombre:
                            gid_nuevo = crear_en_google(new_nombre, new_apellido, tel_format)
                            
                            if gid_nuevo:
                                # Guardamos el nuevo ID de Google en nuestra BD local
                                nombre_completo = f"{new_nombre} {new_apellido}".strip()
                                with engine.connect() as conn:
                                    conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc WHERE telefono=:t"), 
                                                {"n": new_nombre, "a": new_apellido, "gid": gid_nuevo, "nc": nombre_completo, "t": telefono})
                                    conn.commit()
                                
                                st.success(f"✅ Contacto creado en Google: {nombre_completo}")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ Error al intentar crear en Google Contacts.")
                        else:
                            st.warning("⚠️ Para crear el contacto, escribe primero el NOMBRE en la casilla.")

    # BOTÓN GUARDAR GENERAL (Guarda Alias, Etiquetas y Nombres)
    if st.button("💾 GUARDAR CAMBIOS", key=f"btn_save_loc_{telefono}", type="primary", use_container_width=True):
        tags_str = ",".join(new_tags)
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE Clientes SET nombre_corto=:nc, etiquetas=:tag, nombre=:n, apellido=:a WHERE telefono=:t
            """), {"nc": new_corto, "tag": tags_str, "n": new_nombre, "a": new_apellido, "t": telefono})
            conn.commit()
        st.toast("✅ Datos guardados")
        time.sleep(0.5)
        st.rerun()

    # DIRECCIONES
    st.markdown("---")
    if dirs.empty:
        st.caption("Sin direcciones.")
    else:
        for _, row in dirs.iterrows():
            tipo = row.get('tipo_envio', 'GENERAL')
            txt = row.get('direccion_texto') or ""
            dist = row.get('distrito') or ""
            st.markdown(f"📍 **{tipo}:** {txt} ({dist})")

def enviar_texto_chat(telefono, texto):
    ok, r = enviar_mensaje_whatsapp(telefono, texto)
    if ok: guardar_mensaje_saliente(telefono, texto, None); st.rerun()
    else: st.error(r)

def enviar_archivo_chat(telefono, archivo):
    ok, r = enviar_mensaje_media(telefono, archivo.getvalue(), archivo.type, "", archivo.name)
    if ok: guardar_mensaje_saliente(telefono, f"📎 {archivo.name}", archivo.getvalue()); st.rerun()
    else: st.error(r)

def guardar_mensaje_saliente(telefono, texto, data):
    norm = normalizar_telefono_maestro(telefono)
    if not norm: return
    t = norm['db']
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) VALUES (:t, TRUE, NOW(), 'Nuevo') ON CONFLICT (telefono) DO NOTHING"), {"t": t})
        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :c, NOW(), TRUE, :d)"), {"t": t, "c": texto, "d": data})
        conn.commit()