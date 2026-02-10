import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
from utils import buscar_contacto_google, crear_en_google, normalizar_telefono_maestro, generar_nombre_ia
import time

# ==============================================================================
# HERRAMIENTA DE FUSIÓN DE CLIENTES
# ==============================================================================
def render_herramienta_fusion():
    with st.expander("🔄 Fusionar Clientes Duplicados (Herramienta)", expanded=False):
        st.info("Utiliza esto para unir dos registros. Se migrarán chats, ventas y direcciones al cliente destino.")
        
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text("SELECT id_cliente, telefono, nombre_corto, whatsapp_internal_id FROM Clientes WHERE activo=TRUE ORDER BY nombre_corto"), conn)
                opciones = df.apply(lambda x: f"{x['nombre_corto']} | {x['telefono']} (ID: {x['id_cliente']})", axis=1).tolist()
                mapa_ids = dict(zip(opciones, df['id_cliente']))
                mapa_tels = dict(zip(opciones, df['telefono']))
                mapa_wids = dict(zip(opciones, df['whatsapp_internal_id']))

                c1, c2 = st.columns(2)
                with c1: sel_keep = st.selectbox("✅ Cliente a CONSERVAR (Destino)", opciones, key="fusion_keep")
                with c2: sel_del = st.selectbox("❌ Cliente a ELIMINAR (Origen)", opciones, key="fusion_del")

                if sel_keep and sel_del:
                    id_keep = mapa_ids[sel_keep]
                    id_del = mapa_ids[sel_del]
                    tel_keep = mapa_tels[sel_keep]
                    tel_del = mapa_tels[sel_del]
                    wid_keep = mapa_wids[sel_keep]
                    wid_del = mapa_wids[sel_del]

                    if id_keep == id_del:
                        st.error("Debes seleccionar dos clientes diferentes.")
                    else:
                        st.warning(f"⚠️ Al fusionar, **{sel_del}** desaparecerá y todos sus datos pasarán a **{sel_keep}**.")
                        id_final = wid_del if wid_del else wid_keep
                        st.caption(f"🆔 ID Interno resultante será: `{id_final or 'Ninguno'}`")

                        if st.button("🚀 Confirmar Fusión"):
                            with st.spinner("Fusionando historiales..."):
                                try:
                                    with engine.begin() as tx:
                                        # 1. Mover MENSAJES (Actualizamos el telefono dueño del mensaje)
                                        tx.execute(text("UPDATE mensajes SET telefono = :tel_new WHERE telefono = :tel_old"), 
                                                   {"tel_new": tel_keep, "tel_old": tel_del})
                                        # 2. Mover VENTAS y DIRECCIONES
                                        try: tx.execute(text("UPDATE Ventas SET id_cliente = :id_new WHERE id_cliente = :id_old"), {"id_new": id_keep, "id_old": id_del})
                                        except: pass
                                        try: tx.execute(text("UPDATE Direcciones SET id_cliente = :id_new WHERE id_cliente = :id_old"), {"id_new": id_keep, "id_old": id_del})
                                        except: pass
                                        # 3. ID Interno
                                        if wid_del:
                                            tx.execute(text("UPDATE Clientes SET whatsapp_internal_id=:wid WHERE id_cliente=:id"), {"wid": wid_del, "id": id_keep})
                                        # 4. ELIMINAR
                                        tx.execute(text("DELETE FROM Clientes WHERE id_cliente = :id"), {"id": id_del})
                                    
                                    st.success(f"¡Fusión completada!")
                                    time.sleep(1.5)
                                    st.rerun()
                                except Exception as e: st.error(f"Error en la fusión: {e}")

        except Exception as e: st.error(f"Error cargando herramienta: {e}")

# ==============================================================================
# RENDERIZADO PRINCIPAL
# ==============================================================================
def render_clientes():
    st.title("👤 Gestión de Clientes")
    render_herramienta_fusion()
    st.divider()

    if 'cliente_seleccionado' not in st.session_state: st.session_state['cliente_seleccionado'] = None
    if 'crear_google_mode' not in st.session_state: st.session_state['crear_google_mode'] = False

    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Lista de Clientes")
        search = st.text_input("Buscar cliente...", placeholder="Nombre o Teléfono")
        query = "SELECT * FROM Clientes WHERE activo = TRUE"
        params = {}
        if search:
            query += " AND (nombre_corto ILIKE :s OR telefono ILIKE :s OR nombre ILIKE :s)"
            params = {"s": f"%{search}%"}
        query += " ORDER BY id_cliente DESC LIMIT 50"

        with engine.connect() as conn: df = pd.read_sql(text(query), conn, params=params)

        if not df.empty:
            for index, row in df.iterrows():
                nombre_mostrar = row['nombre_corto'] if row['nombre_corto'] else row['telefono']
                tipo_btn = "primary" if st.session_state['cliente_seleccionado'] == row['id_cliente'] else "secondary"
                if st.button(f"{nombre_mostrar}\n📞 {row['telefono']}", key=f"cli_{row['id_cliente']}", use_container_width=True, type=tipo_btn):
                    st.session_state['cliente_seleccionado'] = row['id_cliente']
                    st.session_state['crear_google_mode'] = False
                    st.rerun()
        else: st.info("No se encontraron clientes.")

    with col2:
        if st.session_state['cliente_seleccionado']:
            id_cli_sel = st.session_state['cliente_seleccionado']
            with engine.connect() as conn:
                cliente = conn.execute(text("SELECT * FROM Clientes WHERE id_cliente = :id"), {"id": id_cli_sel}).fetchone()

            if cliente:
                st.subheader(f"Editar: {cliente.nombre_corto}")
                
                with st.form("form_cliente"):
                    c1, c2 = st.columns(2)
                    new_nombre = c1.text_input("Nombre Corto", value=cliente.nombre_corto or "")
                    
                    # Guardamos el telefono viejo para comparar
                    telefono_actual_db = cliente.telefono 
                    new_telefono = c2.text_input("Teléfono", value=cliente.telefono or "") 
                    
                    if cliente.whatsapp_internal_id:
                        st.warning(f"⚠️ ID vinculado: `{cliente.whatsapp_internal_id}`. Cambiar el número aquí moverá el historial de chat al nuevo número.")
                    
                    c3, c4 = st.columns(2)
                    new_nombre_real = c3.text_input("Nombre Real", value=cliente.nombre or "")
                    new_apellido = c4.text_input("Apellido", value=cliente.apellido or "")
                    new_etiquetas = st.text_area("Etiquetas / Notas", value=cliente.etiquetas or "")
                    
                    if cliente.google_id: st.caption(f"🔗 Google Contact ID: {cliente.google_id}")
                    else: st.caption("⚠️ No vinculado a Google Contactos")

                    submitted = st.form_submit_button("💾 Guardar Cambios")
                    
                    if submitted:
                        with engine.begin() as conn:
                            # 1. Actualizar Datos del Cliente
                            conn.execute(text("""
                                UPDATE Clientes 
                                SET nombre_corto=:nc, nombre=:n, apellido=:a, etiquetas=:e, telefono=:t
                                WHERE id_cliente=:id
                            """), {
                                "nc": new_nombre, "n": new_nombre_real, "a": new_apellido, 
                                "e": new_etiquetas, "t": new_telefono, "id": id_cli_sel
                            })
                            
                            # 2. MIGRACIÓN AUTOMÁTICA DE MENSAJES (Si cambió el número)
                            if telefono_actual_db != new_telefono:
                                conn.execute(text("""
                                    UPDATE mensajes 
                                    SET telefono = :new_tel 
                                    WHERE telefono = :old_tel
                                """), {"new_tel": new_telefono, "old_tel": telefono_actual_db})
                                st.toast(f"Historial migrado de {telefono_actual_db} a {new_telefono}", icon="📦")

                        st.success("Cambios guardados.")
                        time.sleep(1)
                        st.rerun()

                # --- BOTONES EXTRA (Google) ---
                col_g1, col_g2 = st.columns(2)
                with col_g1:
                    if st.button("🔍 Buscar/Vincular Google"):
                        res = buscar_contacto_google(cliente.telefono)
                        if res['encontrado']:
                            with engine.begin() as conn:
                                conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc WHERE id_cliente=:id"), 
                                            {"n": res['nombre'], "a": res['apellido'], "gid": res['google_id'], "nc": res['nombre_completo'], "id": id_cli_sel})
                            st.success(f"Vinculado con: {res['nombre_completo']}")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("No encontrado en Google.")
                            st.session_state['crear_google_mode'] = True

                with col_g2:
                    if st.session_state['crear_google_mode']:
                         if st.button("➕ Crear en Google Ahora"):
                            if cliente.nombre_corto:
                                partes = cliente.nombre_corto.split(" ", 1)
                                nom = partes[0]
                                ape = partes[1] if len(partes) > 1 else ""
                                gid = crear_en_google(nom, ape, cliente.telefono)
                                if gid:
                                    with engine.begin() as conn:
                                        conn.execute(text("UPDATE Clientes SET google_id=:gid WHERE id_cliente=:id"), {"gid": gid, "id": id_cli_sel})
                                    st.success("Creado en Google Contacts.")
                                    st.session_state['crear_google_mode'] = False
                                    time.sleep(1)
                                    st.rerun()
                                else: st.error("Error al crear en Google.")
                            else: st.warning("Se requiere nombre corto.")

                # --- DIRECCIONES ---
                st.divider()
                st.write("📍 Direcciones")
                try:
                    with engine.connect() as conn:
                        dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id AND activo=TRUE"), conn, params={"id": id_cli_sel})
                    if not dirs.empty:
                        for _, d in dirs.iterrows(): st.info(f"{d['distrito']} - {d['direccion_texto']} ({d['referencia']})")
                    else: st.caption("Sin direcciones.")
                except: st.caption("Error cargando direcciones.")
        else: st.info("Selecciona un cliente.")