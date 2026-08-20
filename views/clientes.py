import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
from utils import buscar_contacto_google, crear_en_google, normalizar_telefono_maestro, generar_nombre_ia, actualizar_en_google, obtener_lid_de_waha

import time

ESTADOS_CLIENTE_FALLBACK = [
    "Sin empezar", "Responder duda", "Interesado en venta", 
    "Proveedor nacional", "Proveedor internacional", 
    "Venta motorizado", "Venta agencia", "Venta express moto", 
    "En camino moto", "En camino agencia", "Contraentrega agencia", 
    "Pendiente agradecer", "Problema post"
]

def render_clientes():
    try:
        with engine.connect() as conn:
            df_etapas = pd.read_sql(text("SELECT id_etapa, subgrupo FROM EtapasCliente WHERE activo = TRUE ORDER BY grupo, id_etapa"), conn)
        if not df_etapas.empty:
            estados_opciones = df_etapas['subgrupo'].tolist()
            mapa_subgrupo_id = dict(zip(df_etapas['subgrupo'], df_etapas['id_etapa']))
        else:
            estados_opciones = ESTADOS_CLIENTE_FALLBACK
            mapa_subgrupo_id = {}
    except:
        estados_opciones = ESTADOS_CLIENTE_FALLBACK
        mapa_subgrupo_id = {}

    st.title("👤 Gestión de Clientes y Proveedores")

    # --- CREAR NUEVO CLIENTE ---
    with st.expander("➕ Registrar Nuevo Cliente / Proveedor", expanded=False):
        with st.form("form_nuevo_cliente"):
            c1, c2, c3 = st.columns([2, 2, 2])
            nuevo_tel = c1.text_input("Teléfono Principal (Obligatorio)")
            nuevo_alias = c2.text_input("Alias / Nombre Corto")
            nuevo_estado = c3.selectbox("Estado Inicial", options=estados_opciones, index=0)

            nuevas_etiquetas = st.text_input("Etiquetas (Separadas por coma)")
            
            st.write("")
            c_goo, c_mkt = st.columns(2)
            vincular_google = c_goo.checkbox("🔍 Intentar vincular con Google Contactos", value=True)
            excluir_publicidad = c_mkt.checkbox("🚫 Excluir de campañas publicitarias (Proveedor / No Molestar)", value=False)

            if st.form_submit_button("💾 Crear Registro", type="primary"):
                norm = normalizar_telefono_maestro(nuevo_tel)
                if not norm:
                    st.error("Número de teléfono inválido.")
                else:
                    tel_db = norm['db']
                    with engine.connect() as conn:
                        existe = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE telefono=:t AND activo=TRUE"), {"t": tel_db}).fetchone()

                    if existe:
                        st.warning(f"El teléfono {tel_db} ya pertenece al cliente ID {existe[0]}.")
                    else:
                        g_id, g_nom, g_ape = None, None, None
                        if not nuevo_alias: nuevo_alias = "Cliente Nuevo"
                        
                        if vincular_google:
                            res_g = buscar_contacto_google(tel_db)
                            if not (res_g and res_g.get('encontrado')):
                                tel_google = norm.get('google', tel_db)
                                res_g = buscar_contacto_google(tel_google)
                                
                            if res_g and res_g.get('encontrado'):
                                g_id = res_g['google_id']
                                g_nom = res_g['nombre']
                                g_ape = res_g['apellido']
                                nuevo_alias = f"{g_nom} {g_ape}".strip() if f"{g_nom} {g_ape}".strip() else nuevo_alias
                                st.toast("✅ Vinculado a un contacto en Google.", icon="🔗")
                            else:
                                tel_google = norm.get('google', tel_db)
                                nuevo_gid = crear_en_google(nuevo_alias, "", tel_google)
                                if nuevo_gid:
                                    g_id = nuevo_gid
                                    g_nom = nuevo_alias
                                    g_ape = ""
                                    st.toast("🆕 Nuevo contacto en Google.", icon="👤")

                        nombre_ia = generar_nombre_ia(nuevo_alias, g_nom or "")
                        id_etapa_val = mapa_subgrupo_id.get(nuevo_estado)

                        try:
                            with engine.begin() as conn:
                                conn.execute(text("UPDATE clientes SET telefono = NULL WHERE telefono = :t"), {"t": tel_db})
                                conn.execute(text("UPDATE telefonoscliente SET activo = FALSE WHERE telefono = :t"), {"t": tel_db})

                                res = conn.execute(text("""
                                    INSERT INTO clientes (nombre_corto, estado, id_etapa, etiquetas, google_id, nombre, apellido, nombre_ia, telefono, excluir_publicidad, activo, fecha_registro)
                                    VALUES (:nc, :e, :id_etapa, :et, :gid, :n, :a, :nia, :t, :exc, TRUE, NOW())
                                    RETURNING id_cliente
                                """), {"nc": nuevo_alias, "e": nuevo_estado, "id_etapa": id_etapa_val, "et": nuevas_etiquetas, "gid": g_id, "n": g_nom, "a": g_ape, "nia": nombre_ia, "t": tel_db, "exc": excluir_publicidad})
                                nuevo_id = res.fetchone()[0]

                                conn.execute(text("""
                                    INSERT INTO telefonoscliente (id_cliente, telefono, es_principal)
                                    VALUES (:id, :t, TRUE)
                                """), {"id": nuevo_id, "t": tel_db})

                            st.success(f"✅ Registro guardado con ID {nuevo_id}.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al insertar: {e}")

    st.divider()

    # --- BUSCADOR Y EDITOR MASIVO ---
    st.subheader("🔍 Buscador y Editor Masivo")
    busqueda = st.text_input("Buscar registro...", placeholder="Nombre, Teléfono, LID, Alias o Etiquetas")

    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
    term_tel = f"%{busqueda_limpia}%" if busqueda_limpia else f"%{busqueda}%"
    term_gen = f"%{busqueda}%"

    query = """
        SELECT c.id_cliente, c.nombre_corto, c.estado, c.excluir_publicidad, c.nombre, c.apellido, c.etiquetas, c.google_id, c.whatsapp_internal_id, c.nombre_ia,
               COALESCE((SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1), c.telefono) as tel_principal,
               COALESCE((SELECT STRING_AGG(telefono, ' | ') FROM telefonoscliente WHERE id_cliente = c.id_cliente AND activo = TRUE), c.telefono) as todos_telefonos
        FROM clientes c
        WHERE c.activo = TRUE
    """
    params = {}
    if busqueda:
        # 🚀 SE AGREGÓ t.alias Y t.lid A LA BÚSQUEDA DE LA TABLA SECUNDARIA
        query += """ AND (
            c.nombre_corto ILIKE :g OR c.nombre ILIKE :g OR c.apellido ILIKE :g OR c.etiquetas ILIKE :g OR c.nombre_ia ILIKE :g OR c.telefono ILIKE :g OR c.whatsapp_internal_id ILIKE :g
            OR EXISTS (SELECT 1 FROM telefonoscliente t WHERE t.id_cliente = c.id_cliente AND (t.telefono ILIKE :t OR t.lid ILIKE :g OR t.alias ILIKE :g) AND t.activo = TRUE)
        )"""
        params = {"g": term_gen, "t": term_tel}
    query += " ORDER BY c.id_cliente DESC LIMIT 50"

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    if not df.empty:
        df_view = df.copy()
        df_view['excluir_publicidad'] = df_view['excluir_publicidad'].fillna(False).astype(bool)
        df_view.insert(0, "Seleccionar", False)

        edited_df = st.data_editor(
            df_view,
            key="ed_clientes_main",
            column_config={
                "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "nombre_corto": st.column_config.TextColumn("Alias Original", width="medium"),
                "nombre_ia": st.column_config.TextColumn("Nombre IA", width="medium"),
                "estado": st.column_config.SelectboxColumn("Estado", options=estados_opciones, width="medium"),
                "excluir_publicidad": st.column_config.CheckboxColumn("🚫 Sin Mkt", help="Tilda para que el bot no le envíe publicidad"),
                "tel_principal": st.column_config.TextColumn("Telf. Principal", disabled=True),
                "todos_telefonos": st.column_config.TextColumn("Todos los Teléfonos", disabled=True, width="large"),
                "nombre": None, "apellido": None, "google_id": None, "whatsapp_internal_id": None, "etiquetas": None
            },
            hide_index=True, use_container_width=True
        )

        if st.button("💾 Guardar Cambios Rápidos", type="primary"):
            with engine.begin() as conn:
                for idx, row in edited_df.iterrows():
                    id_etapa_val = mapa_subgrupo_id.get(row['estado'])
                    nia_val = row['nombre_ia'] if pd.notna(row['nombre_ia']) else ""
                    exc_val = bool(row['excluir_publicidad'])
                    
                    conn.execute(text("""
                        UPDATE clientes 
                        SET nombre_corto=:nc, nombre_ia=:nia, estado=:est, id_etapa=:id_etapa, excluir_publicidad=:exc 
                        WHERE id_cliente=:id
                    """), {"nc": row['nombre_corto'], "nia": nia_val, "est": row['estado'], "id_etapa": id_etapa_val, "exc": exc_val, "id": row['id_cliente']})
            st.success("Cambios guardados.")
            time.sleep(1)
            st.rerun()

        # --- GESTIÓN INDIVIDUAL ---
        filas_sel = edited_df[edited_df["Seleccionar"] == True]
        if not filas_sel.empty:
            row_full = df.loc[filas_sel.index[0]]
            id_cli_sel = int(row_full['id_cliente'])

            st.divider()
            st.subheader(f"⚙️ Gestión Individual: {row_full['nombre_corto']}")

            tab_datos, tab_tel, tab_dir = st.tabs(["👤 Datos Personales", "📞 Teléfonos", "🏠 Direcciones"])

            # ==============================================================================
            # 1️⃣ PESTAÑA: DATOS PERSONALES
            # ==============================================================================
            with tab_datos:
                with st.form(f"form_cli_{id_cli_sel}"):
                    c1, c2, c3 = st.columns(3)
                    new_nombre = c1.text_input("Alias Original", value=row_full['nombre_corto'] or "")
                    val_nombre_ia = row_full['nombre_ia'] if pd.notna(row_full['nombre_ia']) else ""
                    new_nombre_ia = c2.text_input("Nombre IA", value=val_nombre_ia)
                    curr_est = row_full['estado']
                    new_estado = c3.selectbox("Estado", options=estados_opciones, index=estados_opciones.index(curr_est) if curr_est in estados_opciones else 0)

                    st.write("")
                    new_excluir = st.toggle("🚫 Excluir de campañas publicitarias automáticas", value=bool(row_full.get('excluir_publicidad', False)))

                    st.markdown("##### 👥 Sincronización Directa y Teléfono")
                    
                    val_nom = row_full['nombre'] if pd.notna(row_full['nombre']) else ""
                    val_ape = row_full['apellido'] if pd.notna(row_full['apellido']) else ""
                    val_eti = row_full['etiquetas'] if pd.notna(row_full['etiquetas']) else ""

                    # -----------------------------------------------------------
                    # LÓGICA DE TELÉFONO PRINCIPAL, ALIAS Y BLOQUEO GOOGLE
                    # -----------------------------------------------------------
                    with engine.connect() as conn:
                        prin_data = conn.execute(text("SELECT telefono, alias, lid FROM telefonoscliente WHERE id_cliente=:id AND es_principal=TRUE AND activo=TRUE LIMIT 1"), {"id": id_cli_sel}).fetchone()

                    tel_bd_actual = prin_data.telefono if prin_data else None
                    alias_bd_actual = prin_data.alias if prin_data else None
                    lid_bd_actual = prin_data.lid if prin_data else None

                    # ¿Es un número de verdad o un código interno oculto?
                    es_numero_real = tel_bd_actual and not (str(tel_bd_actual).startswith("LID_") or "@lid" in str(tel_bd_actual))

                    # Regla 3: Si hay teléfono, mostrarlo. Si no, mostrar alias. Si no, mostrar LID
                    if es_numero_real:
                        val_mostrar = tel_bd_actual
                    elif alias_bd_actual:
                        val_mostrar = alias_bd_actual
                    else:
                        val_mostrar = lid_bd_actual or "Sin número"

                    # Regla 4: Bloquear Google Contacts si no hay teléfono real
                    bloquear_google = not es_numero_real
                    
                    if bloquear_google:
                        st.info("⚠️ El contacto principal es un Alias/LID. Edita o selecciona un Teléfono Real en la pestaña 'Teléfonos' para habilitar la vinculación con Google.")

                    c4, c5, c6 = st.columns(3)
                    new_real_nombre = c4.text_input("Nombre Real", value=val_nom, disabled=bloquear_google)
                    new_apellido = c5.text_input("Apellido", value=val_ape, disabled=bloquear_google)
                    
                    # Regla 3: Teléfono ineditable en esta pestaña
                    c6.text_input("Teléfono Principal (Editable en Pestaña 'Teléfonos')", value=val_mostrar, disabled=True)
                    
                    new_etiquetas = st.text_area("Etiquetas / Notas", value=val_eti)

                    if st.form_submit_button("💾 Guardar Datos Personales"):
                        id_etapa_val = mapa_subgrupo_id.get(new_estado)
                        google_id_crudo = row_full['google_id']
                        tiene_google_id = pd.notna(google_id_crudo) and str(google_id_crudo).strip().lower() not in ['', 'nan', 'none']
                        
                        # Guardar en Google si está permitido
                        if tiene_google_id and not bloquear_google:
                            norm_t = normalizar_telefono_maestro(tel_bd_actual)
                            tel_g = norm_t['google'] if norm_t else tel_bd_actual
                            with st.spinner("Sincronizando con Google Contacts..."):
                                exito_google = actualizar_en_google(str(google_id_crudo), new_real_nombre, new_apellido, tel_g)
                            if exito_google: st.toast("✅ Contacto actualizado en Google", icon="👥")
                            else: st.error("❌ Falló la actualización en Google Contacts.")
                        
                        # Actualizar base de datos (Excluye el teléfono porque se maneja en la otra pestaña)
                        with engine.begin() as conn:
                            conn.execute(text("""
                                UPDATE clientes 
                                SET nombre_corto=:nc, nombre_ia=:nia, nombre=:n, apellido=:a, etiquetas=:e, estado=:est, id_etapa=:id_etapa, excluir_publicidad=:exc
                                WHERE id_cliente=:id
                            """), {
                                "nc": new_nombre, "nia": new_nombre_ia, "n": new_real_nombre, "a": new_apellido,
                                "e": new_etiquetas, "est": new_estado, "id_etapa": id_etapa_val, "exc": new_excluir, "id": id_cli_sel
                            })
                            
                        st.success("Guardado en Base de Datos.")
                        time.sleep(1)
                        st.rerun()

            # Forzar Sincronización Manual protegida
            if st.button("🔍 Forzar Sincronización Manual (Vincular o Crear)"):
                if es_numero_real and tel_bd_actual:
                    with st.spinner("Buscando exhaustivamente en Google Contacts..."):
                        res = buscar_contacto_google(tel_bd_actual)
                            
                    if res and res.get('encontrado'):
                        with engine.begin() as conn:
                            conn.execute(text("UPDATE clientes SET nombre=:n, apellido=:a, google_id=:gid WHERE id_cliente=:id"),
                                        {"n": res['nombre'], "a": res['apellido'], "gid": res['google_id'], "id": id_cli_sel})
                        st.success("✅ ¡Contacto encontrado y vinculado con éxito! (No se crearon duplicados)")
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.warning("⚠️ No existe en Google. Creando contacto nuevo...")
                        norm_t = normalizar_telefono_maestro(tel_bd_actual)
                        tel_google = norm_t['google'] if norm_t else tel_bd_actual
                        alias_crear = row_full['nombre_corto'] or f"Cliente {id_cli_sel}"
                        
                        nuevo_gid = crear_en_google(alias_crear, "", tel_google)
                        if nuevo_gid:
                            with engine.begin() as conn:
                                conn.execute(text("UPDATE clientes SET nombre=:n, apellido='', google_id=:gid WHERE id_cliente=:id"),
                                            {"n": alias_crear, "gid": nuevo_gid, "id": id_cli_sel})
                            st.success("✨ ¡Contacto nuevo creado y vinculado en Google!")
                            time.sleep(2)
                            st.rerun()
                        else: st.error("❌ Falló la creación en Google Contacts.")
                else: 
                    st.warning("⚠️ Este cliente no tiene un Teléfono Real asignado. No se puede vincular a Google.")

            # ==============================================================================
            # 2️⃣ PESTAÑA: TELÉFONOS Y ALIAS
            # ==============================================================================
            with tab_tel:
                st.markdown("##### 📱 Gestión de Números Asociados y Alias")
                with engine.connect() as conn:
                    tels = pd.read_sql(text("SELECT id_telefono, telefono, alias, lid, es_principal FROM telefonoscliente WHERE id_cliente=:id AND activo=TRUE ORDER BY es_principal DESC"), conn, params={"id": id_cli_sel})

                cambios = {}
                for idx, t_row in tels.iterrows():
                    es_prin_label = "⭐ Principal" if t_row['es_principal'] else "Secundario"
                    st.markdown(f"**Contacto {idx+1}** ({es_prin_label})")
                    
                    # Regla 1: Aparece alias editable, LID sin editar
                    col_t1, col_t2, col_t3, col_t4 = st.columns([2, 2, 2, 1.5])
                    
                    cambios[t_row['id_telefono']] = {
                        'tel_old': t_row['telefono'], # 👈 Memoria del teléfono original para comparar
                        'tel_new': col_t1.text_input("Teléfono", value=t_row['telefono'] or "", key=f"t_{t_row['id_telefono']}"),
                        'alias': col_t2.text_input("Alias", value=t_row['alias'] or "", key=f"a_{t_row['id_telefono']}"),
                        'lid': col_t3.text_input("LID", value=t_row['lid'] or "", disabled=True, key=f"l_{t_row['id_telefono']}")
                    }

                    if not t_row['es_principal']:
                        if col_t4.button("⭐ Principal", key=f"p_{t_row['id_telefono']}", use_container_width=True):
                            with engine.begin() as tx:
                                tx.execute(text("UPDATE telefonoscliente SET es_principal=FALSE WHERE id_cliente=:id"), {"id": id_cli_sel})
                                tx.execute(text("UPDATE telefonoscliente SET es_principal=TRUE, activo=TRUE WHERE id_telefono=:idt"), {"idt": t_row['id_telefono']})
                            st.rerun()
                    
                    if len(tels) > 1:
                        if col_t4.button("🗑️ Eliminar", key=f"d_{t_row['id_telefono']}", use_container_width=True):
                            with engine.begin() as tx:
                                tx.execute(text("UPDATE telefonoscliente SET activo=FALSE WHERE id_telefono=:idt"), {"idt": t_row['id_telefono']})
                            st.rerun()
                            
                    st.write("") # Espaciador
                
                # Regla 2 y Escudo Anti-Duplicados: Motor de guardado con Alertas
                if st.button("💾 Guardar Cambios de Teléfonos", type="primary", key=f"btn_save_tels_{id_cli_sel}"):
                    hay_error = False
                    avisos_waha = []  # 👈 NUEVO: Recolector de fallos silenciosos
                    
                    with engine.begin() as tx:
                        for id_tel, data in cambios.items():
                            t_old = data['tel_old']
                            t_new = data['tel_new'].strip() if data['tel_new'] else None
                            a_val = data['alias'].strip() if data['alias'] else None
                            
                            if t_new:
                                norm_t = normalizar_telefono_maestro(t_new)
                                t_clean = norm_t['db'] if norm_t else t_new
                            else:
                                t_clean = None
                                
                            # 🚨 EVALUAR SI EL USUARIO REALMENTE MODIFICÓ EL NÚMERO
                            if t_clean != t_old:
                                if t_clean:
                                    # 1. Validar que no estemos duplicando/robando un número de otro cliente activo
                                    ex_activo = tx.execute(text("""
                                        SELECT c.id_cliente, c.nombre_corto 
                                        FROM telefonoscliente t 
                                        JOIN clientes c ON t.id_cliente = c.id_cliente 
                                        WHERE t.telefono = :t AND t.activo = TRUE AND c.activo = TRUE AND t.id_cliente != :id
                                    """), {"t": t_clean, "id": id_cli_sel}).fetchone()

                                    if ex_activo:
                                        st.error(f"⚠️ El número {t_clean} ya pertenece a **{ex_activo.nombre_corto}** (ID: {ex_activo.id_cliente}). Fusiónalos en la pestaña 'Mantenimiento'.")
                                        hay_error = True
                                        continue
                                        
                                    # 2. Buscar si el NUEVO número tiene un LID en el historial interno
                                    lid_existente = tx.execute(text("SELECT lid FROM telefonoscliente WHERE telefono = :t AND lid IS NOT NULL LIMIT 1"), {"t": t_clean}).scalar()
                                    
                                    # 🚀 3. LA MAGIA: Si no lo tenemos, se lo preguntamos en vivo a WAHA
                                    if not lid_existente:
                                        lid_api = obtener_lid_de_waha(t_clean)
                                        if lid_api:
                                            lid_existente = lid_api
                                        else:
                                            # 👈 NUEVO: Capturamos el error si WAHA no lo devuelve
                                            avisos_waha.append(f"No se pudo extraer el LID para {t_clean}. Puede ser restricción de privacidad de WhatsApp o sesión incorrecta.")
                                    
                                    # 4. Guardar número nuevo y SOBRESCRIBIR el LID
                                    tx.execute(text("UPDATE telefonoscliente SET telefono=:t, alias=:a, lid=:l WHERE id_telefono=:id"), 
                                               {"t": t_clean, "a": a_val, "l": lid_existente, "id": id_tel})
                                else:
                                    # El usuario borró el teléfono de la caja, dejamos solo el alias y limpiamos el LID viejo
                                    tx.execute(text("UPDATE telefonoscliente SET telefono=NULL, alias=:a, lid=NULL WHERE id_telefono=:id"), 
                                               {"a": a_val, "id": id_tel})
                            else:
                                # =====================================================================
                                # 🚀 AUTO-COMPLETADO DE LID: El número NO cambió, pero revisamos si falta LID
                                # =====================================================================
                                if t_clean:
                                    # Verificamos si en la base de datos la celda de LID está vacía
                                    lid_actual = tx.execute(text("SELECT lid FROM telefonoscliente WHERE id_telefono = :id"), {"id": id_tel}).scalar()
                                    
                                    if not lid_actual:
                                        # Le preguntamos a WAHA y actualizamos silenciosamente
                                        lid_api = obtener_lid_de_waha(t_clean)
                                        if lid_api:
                                            tx.execute(text("UPDATE telefonoscliente SET alias=:a, lid=:l WHERE id_telefono=:id"), 
                                                       {"a": a_val, "l": lid_api, "id": id_tel})
                                        else:
                                            # 👈 NUEVO: Informamos el fallo silencioso del auto-completado
                                            avisos_waha.append(f"Intento de auto-completar fallido: WAHA devolvió 'None' para {t_clean}. (Revisa la privacidad del usuario).")
                                            tx.execute(text("UPDATE telefonoscliente SET alias=:a WHERE id_telefono=:id"), 
                                                       {"a": a_val, "id": id_tel})
                                    else:
                                        # Ya tenía LID, simplemente guardamos el alias sin molestar a la API
                                        tx.execute(text("UPDATE telefonoscliente SET alias=:a WHERE id_telefono=:id"), 
                                                   {"a": a_val, "id": id_tel})
                                else:
                                    # Es una celda completamente vacía (sin número), solo guardamos alias
                                    tx.execute(text("UPDATE telefonoscliente SET alias=:a WHERE id_telefono=:id"), 
                                               {"a": a_val, "id": id_tel})
                                
                    if not hay_error:
                        st.success("Teléfonos actualizados correctamente en la Base de Datos.")
                        
                        # 🧹 NUEVO: Destruir la memoria caché (Session State) de las cajas de texto
                        # Para forzar a Streamlit a mostrar los nuevos LIDs recién obtenidos
                        for id_t in cambios.keys():
                            for prefijo in ['t_', 'a_', 'l_']:
                                clave_memoria = f"{prefijo}{id_t}"
                                if clave_memoria in st.session_state:
                                    del st.session_state[clave_memoria]
                        
                        # Mostrar las advertencias recopiladas antes de recargar
                        if avisos_waha:
                            for aviso in avisos_waha:
                                st.warning(f"🕵️ {aviso}")
                            # Le damos 4 segundos al usuario para que lea los mensajes antes de que la pantalla parpadee
                            time.sleep(4)
                        else:
                            time.sleep(1)
                            
                        st.rerun()

                st.divider()
                with st.form(f"add_tel_{id_cli_sel}", clear_on_submit=True):
                    st.write("➕ Agregar Número o Alias")
                    c_n1, c_n2 = st.columns(2)
                    new_tel = c_n1.text_input("Teléfono (Ej: +51 999...)")
                    new_alias = c_n2.text_input("Alias")
                    
                    if st.form_submit_button("Añadir Contacto"):
                        norm_t = normalizar_telefono_maestro(new_tel) if new_tel else None
                        tel_clean = norm_t['db'] if norm_t else (new_tel.strip() if new_tel else None)
                        alias_clean = new_alias.strip() if new_alias else None
                        
                        if tel_clean or alias_clean:
                            with engine.begin() as tx:
                                lid_existente = None
                                if tel_clean:
                                    lid_existente = tx.execute(text("SELECT lid FROM telefonoscliente WHERE telefono = :t AND lid IS NOT NULL LIMIT 1"), {"t": tel_clean}).scalar()
                                    
                                    # 🚀 LA MAGIA: Preguntar a WAHA si el número es nuevo
                                    if not lid_existente:
                                        lid_api = obtener_lid_de_waha(tel_clean)
                                        if lid_api:
                                            lid_existente = lid_api
                                            
                                tx.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono, alias, lid, es_principal, activo) VALUES (:id, :t, :a, :l, FALSE, TRUE)"), 
                                           {"id": id_cli_sel, "t": tel_clean, "a": alias_clean, "l": lid_existente})
                            st.success("Añadido exitosamente.")
                            time.sleep(1)
                            st.rerun()
                        else: 
                            st.error("Debes ingresar un teléfono o un alias.")

            with tab_dir:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE direcciones ADD COLUMN IF NOT EXISTS es_principal BOOLEAN DEFAULT FALSE"))
                except: pass

                st.markdown("#### 🏠 Lista de Direcciones")
                with engine.connect() as conn:
                    dirs = pd.read_sql(text("""
                        SELECT id_direccion, tipo_envio, nombre_receptor, telefono_receptor, distrito, 
                               direccion_texto, referencia, gps_link, dni_receptor, agencia_nombre, 
                               sede_entrega, observacion, es_principal 
                        FROM direcciones 
                        WHERE id_cliente=:id AND activo=TRUE 
                        ORDER BY es_principal DESC, id_direccion DESC
                    """), conn, params={"id": id_cli_sel})

                mapa_ui_to_db = {"Motorizado": "MOTO", "Agencia": "AGENCIA", "Otros": "OTROS"}
                mapa_db_to_ui = {"MOTO": "Motorizado", "AGENCIA": "Agencia", "OTROS": "Otros"}

                if not dirs.empty:
                    for _, d_row in dirs.iterrows():
                        col_d1, col_d2, col_d3 = st.columns([4, 2, 0.5])
                        es_prin_dir = "⭐ Principal" if d_row['es_principal'] else "Secundaria"
                        
                        tipo_db = d_row['tipo_envio']
                        tipo_ui = mapa_db_to_ui.get(tipo_db, "Otros")
                        tipo_label = f"[{tipo_ui}]"
                        
                        if tipo_db == 'MOTO':
                            resumen_dir = f"{d_row['direccion_texto']}, {d_row['distrito']}"
                        elif tipo_db == 'AGENCIA':
                            resumen_dir = f"Agencia: {d_row['agencia_nombre']} ({d_row['sede_entrega'] or 'Sede no especificada'})"
                        else:
                            resumen_dir = d_row['observacion'][:40] + "..." if d_row['observacion'] else "Sin detalles"

                        col_d1.markdown(f"**{tipo_label} {d_row['nombre_receptor'] or 'Receptor'}** — {resumen_dir} ({es_prin_dir})")

                        if not d_row['es_principal']:
                            if col_d2.button("Hacer Principal", key=f"p_dir_{d_row['id_direccion']}"):
                                with engine.begin() as tx:
                                    tx.execute(text("UPDATE direcciones SET es_principal=FALSE WHERE id_cliente=:id"), {"id": id_cli_sel})
                                    tx.execute(text("UPDATE direcciones SET es_principal=TRUE WHERE id_direccion=:idd"), {"idd": int(d_row['id_direccion'])})
                                st.rerun()

                        if col_d3.button("🗑️", key=f"d_dir_{d_row['id_direccion']}"):
                            with engine.begin() as tx:
                                tx.execute(text("UPDATE direcciones SET activo=FALSE WHERE id_direccion=:idd"), {"idd": int(d_row['id_direccion'])})
                            st.rerun()

                st.divider()

                dirs_view = dirs.copy()
                dirs_view.insert(0, "Editar", False)
                ed_dirs = st.data_editor(
                    dirs_view[["Editar", "id_direccion", "tipo_envio", "nombre_receptor", "distrito"]],
                    key="ed_dirs_panel",
                    column_config={"Editar": st.column_config.CheckboxColumn("✏️", width="small"), "id_direccion": None},
                    hide_index=True, use_container_width=True
                )
                
                dir_sel = ed_dirs[ed_dirs["Editar"] == True]
                if not dir_sel.empty:
                    r_dir = dirs.loc[dir_sel.index[0]]
                    
                    with st.form("form_edit_dir"):
                        st.markdown("##### 📝 Modificar Dirección Seleccionada")
                        
                        tipo_db_act = r_dir['tipo_envio']
                        tipo_ui_act = mapa_db_to_ui.get(tipo_db_act, "Otros")
                        opciones_tipo = ["Motorizado", "Agencia", "Otros"]
                        idx_tipo = opciones_tipo.index(tipo_ui_act) if tipo_ui_act in opciones_tipo else 2
                        
                        e_tipo_ui = st.selectbox("Tipo de Envío", opciones_tipo, index=idx_tipo)
                        e_tipo_db = mapa_ui_to_db[e_tipo_ui]
                        
                        c1, c2 = st.columns(2)
                        e_nom = c1.text_input("Nombre Receptor", value=r_dir['nombre_receptor'] or "")
                        e_tel = c2.text_input("Telf. Receptor", value=r_dir['telefono_receptor'] or "")
                        
                        e_dist, e_dir, e_ref, e_gps_link = None, None, None, None
                        e_dni, e_agencia, e_sede = None, None, None
                        
                        if e_tipo_db == "MOTO":
                            d1, d2 = st.columns(2)
                            e_dist = d1.text_input("Distrito", value=r_dir['distrito'] or "")
                            e_dir = d2.text_input("Dirección Exacta", value=r_dir['direccion_texto'] or "")
                            e_ref = st.text_input("Referencia", value=r_dir['referencia'] or "")
                            e_gps_link = st.text_input("Link GPS", value=r_dir['gps_link'] or "")
                        elif e_tipo_db == "AGENCIA":
                            d1, d2, d3 = st.columns(3)
                            e_dni = d1.text_input("DNI Receptor", value=r_dir['dni_receptor'] or "")
                            e_agencia = d2.text_input("Nombre Agencia", value=r_dir['agencia_nombre'] or "")
                            e_sede = d3.text_input("Sede de Entrega", value=r_dir['sede_entrega'] or "")
                            
                        e_obs = st.text_area("Observación", value=r_dir['observacion'] or "")
                        
                        if st.form_submit_button("💾 Guardar Dirección"):
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    UPDATE direcciones 
                                    SET tipo_envio=:tipo, nombre_receptor=:n, telefono_receptor=:t, distrito=:dis, 
                                        direccion_texto=:dt, referencia=:r, gps_link=:glink, dni_receptor=:dni, 
                                        agencia_nombre=:anom, sede_entrega=:sede, observacion=:obs
                                    WHERE id_direccion=:id
                                """), {
                                    "tipo": e_tipo_db, "n": e_nom, "t": e_tel, "dis": e_dist, "dt": e_dir, "r": e_ref, 
                                    "glink": e_gps_link, "dni": e_dni, "anom": e_agencia, "sede": e_sede, "obs": e_obs, 
                                    "id": int(r_dir['id_direccion'])
                                })
                            st.success("Dirección actualizada con éxito.")
                            time.sleep(1)
                            st.rerun()
                else:
                    st.info("El cliente no cuenta con direcciones registradas.")

            with st.expander("➕ Agregar Nueva Dirección", expanded=False):
                nn_tipo_ui = st.selectbox("Tipo de Envío para Nueva Dirección", ["Motorizado", "Agencia", "Otros"], key="sb_new_tipo")
                nn_tipo_db = mapa_ui_to_db[nn_tipo_ui]
                
                with st.form("form_new_dir"):
                    n1, n2 = st.columns(2)
                    nn_nom = n1.text_input("Nombre Receptor")
                    nn_tel = n2.text_input("Telf. Receptor")
                    
                    nn_dist, nn_dir, nn_ref, nn_gps_link = None, None, None, None
                    nn_dni, nn_agencia, nn_sede = None, None, None
                    
                    if nn_tipo_db == "MOTO":
                        d1, d2 = st.columns(2)
                        nn_dist = d1.text_input("Distrito")
                        nn_dir = d2.text_input("Dirección Exacta")
                        nn_ref = st.text_input("Referencia")
                        nn_gps_link = st.text_input("Link GPS")
                    elif nn_tipo_db == "AGENCIA":
                        d1, d2, d3 = st.columns(3)
                        nn_dni = d1.text_input("DNI Receptor")
                        nn_agencia = d2.text_input("Nombre Agencia (Ej: Olva, Shalom)")
                        nn_sede = d3.text_input("Sede de Entrega")
                        
                    nn_obs = st.text_area("Observación")
                    
                    if st.form_submit_button("Crear Dirección"):
                        es_primera_direccion = True if dirs.empty else False
                        
                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, distrito, 
                                                         direccion_texto, referencia, gps_link, dni_receptor, agencia_nombre, 
                                                         sede_entrega, observacion, activo, es_principal)
                                VALUES (:idc, :tipo, :n, :t, :dis, :dt, :r, :glink, :dni, :anom, :sede, :obs, TRUE, :es_prin)
                            """), {
                                "idc": id_cli_sel, "tipo": nn_tipo_db, "n": nn_nom, "t": nn_tel, "dis": nn_dist, "dt": nn_dir, 
                                "r": nn_ref, "glink": nn_gps_link, "dni": nn_dni, "anom": nn_agencia, "sede": nn_sede, "obs": nn_obs,
                                "es_prin": es_primera_direccion
                            })
                        st.success("Nueva dirección creada.")
                        time.sleep(1)
                        st.rerun()

    else:
        st.info("No se encontraron clientes activos.")
        
    st.divider()
    # ==============================================================================
    # OPCIONES ADICIONALES (FUSIÓN, SINCRONIZACIÓN Y REACTIVACIÓN)
    # ==============================================================================
    with st.expander("⚙️ Opciones Adicionales y Mantenimiento", expanded=False):
        
        st.markdown("#### 🔄 Fusionar Clientes Duplicados")
        st.info("El número de teléfono o identificador LID del cliente que elimines se guardará como un **teléfono adicional** del cliente que decidas conservar.")
        try:
            with engine.connect() as conn:
                df_fusion = pd.read_sql(text("""
                    SELECT c.id_cliente, c.nombre_corto, c.whatsapp_internal_id, c.telefono,
                           COALESCE(
                               (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                               c.telefono,
                               c.whatsapp_internal_id,
                               'Sin número'
                           ) as tel_prin
                    FROM clientes c WHERE c.activo=TRUE ORDER BY c.nombre_corto
                """), conn)
                
                if not df_fusion.empty:
                    opciones = df_fusion.apply(lambda x: f"{x['nombre_corto']} | {x['tel_prin']} (ID: {x['id_cliente']})", axis=1).tolist()
                    mapa_ids = dict(zip(opciones, df_fusion['id_cliente']))
                    mapa_wids = dict(zip(opciones, df_fusion['whatsapp_internal_id']))
                    mapa_tels = dict(zip(opciones, df_fusion['telefono']))

                    c1, c2 = st.columns(2)
                    sel_keep = c1.selectbox("✅ Cliente a CONSERVAR (Destino)", opciones, key="fusion_keep")
                    sel_del = c2.selectbox("❌ Cliente a ELIMINAR (Origen)", opciones, key="fusion_del")

                    if st.button("🚀 Confirmar Fusión"):
                        if sel_keep and sel_del:
                            id_keep = mapa_ids[sel_keep]
                            id_del = mapa_ids[sel_del]
                            wid_keep = mapa_wids.get(sel_keep)
                            wid_del = mapa_wids.get(sel_del)
                            tel_del = mapa_tels.get(sel_del)

                            if id_keep == id_del:
                                st.error("Debes seleccionar dos clientes diferentes.")
                            else:
                                with st.spinner("Fusionando..."):
                                    try:
                                        with engine.begin() as tx:
                                            # 1. Transferir números registrados en telefonoscliente
                                            tx.execute(text("UPDATE telefonoscliente SET id_cliente = :new, es_principal = FALSE WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            
                                            # 2. Si el cliente origen tenía su LID/teléfono en c.telefono, insertarlo en telefonoscliente del conservado
                                            if tel_del and str(tel_del).strip():
                                                tel_clean = str(tel_del).strip()
                                                existe_tel = tx.execute(text("SELECT 1 FROM telefonoscliente WHERE id_cliente = :id AND telefono = :t"), {"id": id_keep, "t": tel_clean}).fetchone()
                                                if not existe_tel:
                                                    tx.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono, es_principal, activo) VALUES (:id, :t, FALSE, TRUE)"), {"id": id_keep, "t": tel_clean})

                                            # 3. Transferir ventas y direcciones
                                            tx.execute(text("UPDATE ventas SET id_cliente = :new WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            tx.execute(text("UPDATE direcciones SET id_cliente = :new WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            
                                            # 4. Transferir whatsapp_internal_id (LID) al cliente conservado si el origen lo tenía
                                            if wid_del and str(wid_del).strip():
                                                tx.execute(text("UPDATE clientes SET whatsapp_internal_id = :wid WHERE id_cliente = :id"), {"wid": str(wid_del).strip(), "id": id_keep})
                                            
                                            # 5. Desactivar cliente origen liberando su identificador
                                            fake_wid = f"MERGED_{id_del}_{wid_del or 'NONE'}"[:140]
                                            tx.execute(text("UPDATE clientes SET activo = FALSE, whatsapp_internal_id = :fake WHERE id_cliente = :id"), {"fake": fake_wid, "id": id_del})

                                        st.success("¡Fusión completada con éxito!")
                                        time.sleep(1)
                                        st.rerun()
                                    except Exception as e: st.error(f"Error: {e}")
        except Exception as e: st.error(f"Error cargando herramienta de fusión: {e}")
        
        st.divider()

        st.markdown("#### 📱 Sincronización Masiva con Google Contacts")
        st.caption("Busca clientes activos sin vincular y los asocia a Google.")
        if st.button("🚀 Iniciar Sincronización Masiva"):
            with st.spinner("Sincronizando historial..."):
                try:
                    with engine.connect() as conn:
                        df_sin_sync = pd.read_sql(text("""
                            SELECT c.id_cliente, c.nombre_corto, 
                                   COALESCE(
                                       (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                                       c.telefono
                                   ) as tel_prin
                            FROM clientes c 
                            WHERE c.activo=TRUE AND (c.google_id IS NULL OR TRIM(c.google_id) = '')
                        """), conn)
                    
                    if df_sin_sync.empty:
                        st.success("¡Todos los clientes ya se encuentran vinculados!")
                    else:
                        cont = 0
                        detalles_omisiones = []
                        with engine.begin() as conn_tx:
                            for idx, row in df_sin_sync.iterrows():
                                id_cli = row['id_cliente']
                                nombre = row['nombre_corto']
                                tel = row['tel_prin']
                                
                                if pd.isna(tel) or str(tel).strip().lower() in ['nan', '']:
                                    detalles_omisiones.append(f"⚠️ ID {id_cli} ({nombre}): Sin ningún teléfono en base de datos.")
                                    continue
                                    
                                norm = normalizar_telefono_maestro(tel)
                                if not norm:
                                    detalles_omisiones.append(f"⚠️ ID {id_cli} ({nombre}): Formato inválido para '{tel}'.")
                                    continue
                                    
                                tel_db = norm['db']
                                tel_google = norm.get('google', tel_db)
                                
                                res_g = buscar_contacto_google(tel_db)
                                if not (res_g and res_g.get('encontrado')):
                                    res_g = buscar_contacto_google(tel_google)
                                
                                g_id = None
                                if res_g and res_g.get('encontrado'):
                                    g_id = res_g['google_id']
                                else:
                                    if crear_en_google(nombre, "", tel_google):
                                        res_g2 = buscar_contacto_google(tel_db)
                                        if not (res_g2 and res_g2.get('encontrado')):
                                            res_g2 = buscar_contacto_google(tel_google)
                                            
                                        if res_g2 and res_g2.get('encontrado'):
                                            g_id = res_g2['google_id']
                                
                                if g_id:
                                    conn_tx.execute(text("UPDATE clientes SET google_id = :gid WHERE id_cliente = :id"), {"gid": g_id, "id": id_cli})
                                    cont += 1
                        
                        if detalles_omisiones:
                            for msg in detalles_omisiones:
                                st.warning(msg)
                                
                        st.success(f"¡Sincronización completada! Se vincularon {cont} clientes con éxito.")
                        if cont > 0:
                            time.sleep(1)
                            st.rerun()
                except Exception as e:
                    st.error(f"Error en la sincronización masiva: {e}")
                    
        st.divider()

        st.markdown("#### ♻️ Reactivar Clientes Bloqueados")
        try:
            with engine.connect() as conn:
                df_inactivos = pd.read_sql(text("""
                    SELECT c.id_cliente, c.nombre_corto, 
                           COALESCE((SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE LIMIT 1), c.telefono) as telefono, 
                           c.estado, c.etiquetas 
                    FROM clientes c WHERE c.activo = FALSE ORDER BY c.id_cliente DESC
                """), conn)
            
            if not df_inactivos.empty:
                df_inactivos.insert(0, "Reactivar", False)
                
                ed_inactivos = st.data_editor(
                    df_inactivos,
                    key="ed_reactivar_clientes",
                    column_config={
                        "Reactivar": st.column_config.CheckboxColumn("✅", width="small"),
                        "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                        "nombre_corto": st.column_config.TextColumn("Alias", disabled=True),
                        "telefono": st.column_config.TextColumn("Teléfono / LID", disabled=True),
                        "estado": st.column_config.TextColumn("Estado", disabled=True),
                        "etiquetas": st.column_config.TextColumn("Etiquetas", disabled=True),
                    },
                    hide_index=True, use_container_width=True
                )
                
                filas_reactivar = ed_inactivos[ed_inactivos["Reactivar"] == True]
                
                if not filas_reactivar.empty:
                    if st.button("♻️ Reactivar Seleccionados", type="primary"):
                        ids_a_reactivar = filas_reactivar["id_cliente"].tolist()
                        try:
                            with engine.begin() as conn:
                                for id_cli in ids_a_reactivar:
                                    conn.execute(text("UPDATE clientes SET activo = TRUE WHERE id_cliente = :id"), {"id": int(id_cli)})
                            st.success(f"¡Se han reactivado {len(ids_a_reactivar)} cliente(s) exitosamente!")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al reactivar: {e}")
            else:
                st.write("✅ No hay clientes inactivos o bloqueados en el sistema.")
        except Exception as e:
            st.error(f"Error cargando inactivos: {e}")