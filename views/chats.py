import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
import os
import threading
import base64
import zipfile
import io
import requests
from datetime import datetime, timedelta
from database import engine 
import re # Asegurar la importación al inicio del bucle o del archivo

# --- CONFIGURACIÓN ---
WAHA_URL = os.getenv("WAHA_URL")
WAHA_KEY = os.getenv("WAHA_KEY")

try:
    from utils import marcar_chat_como_leido_waha as marcar_leido_waha
    from utils import normalizar_telefono_maestro 
except ImportError:
    def marcar_leido_waha(*args): pass
    def normalizar_telefono_maestro(t): return {"db": "".join(filter(str.isdigit, str(t)))}

# ==========================================
# 📡 RESOLUTOR API PARA LIDs
# ==========================================
def resolver_telefono_api(lid, session):
    if not WAHA_URL or not lid: return None
    try:
        lid_safe = lid.replace('@', '%40')
        url = f"{WAHA_URL.rstrip('/')}/api/{session}/lids/{lid_safe}"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            pn = data.get('pn')
            if pn:
                return pn.split('@')[0]
    except: pass
    return None

def mandar_mensaje_api(telefono, texto, sesion):
    if not WAHA_URL: return False, "Falta WAHA_URL"
    try:
        res_norm = normalizar_telefono_maestro(telefono)
        if isinstance(res_norm, dict):
            telefono_final = res_norm.get('db') 
        else:
            telefono_final = str(res_norm) if res_norm else "".join(filter(str.isdigit, str(telefono)))
            
        if not telefono_final: return False, "Número inválido"

        url = f"{WAHA_URL.rstrip('/')}/api/sendText"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        
        payload = {"session": sesion, "chatId": f"{telefono_final}@c.us", "text": texto}
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        if r.status_code in [200, 201]: return True, ""
        return False, r.text
    except Exception as e:
        return False, str(e)

def get_table_name(conn):
    try:
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        return "\"Clientes\""

# ==========================================
# 🕵️ VIGÍA INVISIBLE
# ==========================================
try:
    run_poller = st.fragment(run_every=3) 
except AttributeError:
    run_poller = lambda f: f

@run_poller
def poller_cambios_db():
    st.markdown("<div style='display:none;'>vigia_activo</div>", unsafe_allow_html=True)
    try:
        with engine.connect() as conn: 
            conn.commit() 
            version_actual = conn.execute(text("SELECT version FROM sync_estado WHERE id = 1")).scalar() or 0
            if 'db_version' not in st.session_state:
                st.session_state['db_version'] = version_actual
            elif st.session_state['db_version'] != version_actual:
                st.session_state['db_version'] = version_actual
                st.rerun()
    except Exception: pass

def render_boton_chat(row, cat, chat_actual, cambiar_chat_func):
    c_id = row['chat_id']
    c_leidos = row['no_leidos']
    icono = "🔴" if c_leidos > 0 else "👤"
    texto_leidos = f" **({c_leidos})**" if c_leidos > 0 else ""
    
    extra = f" [{row['estado']}]" if cat in ["📁 Otros Estados", "🔴 Mensajes Nuevos"] and pd.notna(row['estado']) else ""
    
    label = f"{icono} {row['nombre']}{extra}{texto_leidos}"
    tipo = "primary" if str(chat_actual) == str(c_id) else "secondary"
    st.button(label, key=f"c_{c_id}", use_container_width=True, type=tipo, on_click=cambiar_chat_func, args=(c_id,))

# ==========================================
# VISTA PRINCIPAL
# ==========================================
def render_chat():
    c_tit, c_time = st.columns([80, 20])
    c_tit.title("💬 Chat Center")
    
    lima_time = datetime.utcnow() - timedelta(hours=5)
    c_time.caption(f"🔄 {lima_time.strftime('%H:%M:%S')}")

    poller_cambios_db()

    # --- CARGA DINÁMICA DE ETAPAS ---
    try:
        with engine.connect() as conn:
            df_etapas = pd.read_sql(text("SELECT id_etapa, grupo, subgrupo FROM EtapasCliente WHERE activo = TRUE"), conn)
            
        if not df_etapas.empty:
            todos_los_estados = df_etapas['subgrupo'].tolist()
            mapa_subgrupo_id = dict(zip(df_etapas['subgrupo'], df_etapas['id_etapa']))
            
            estados_e0 = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 0']['subgrupo'].tolist()
            estados_e1 = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 1']['subgrupo'].tolist()
            estados_e2 = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 2']['subgrupo'].tolist()
            estados_e3 = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 3']['subgrupo'].tolist()
            estados_e4 = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 4']['subgrupo'].tolist()
        else:
            raise Exception("Tabla vacía")
    except Exception:
        todos_los_estados = ["Sin empezar", "Responder duda", "Interesado en venta", "Venta motorizado", "En camino moto", "Pendiente agradecer"]
        mapa_subgrupo_id = {}
        estados_e0 = ["Sin empezar"]
        estados_e1 = ["Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional"]
        estados_e2 = ["Venta motorizado", "Venta agencia", "Venta express moto", "Recojo en Almacen"]
        estados_e3 = ["En camino moto", "En camino agencia", "Contraentrega agencia"]
        estados_e4 = ["Pendiente agradecer", "Problema post"]

    def cambiar_chat(chat_id):
        st.session_state['chat_actual_id'] = str(chat_id)

    if 'chat_actual_id' not in st.session_state:
        st.session_state['chat_actual_id'] = None

    chat_actual = st.session_state['chat_actual_id']
    col_lista, col_chat = st.columns([35, 65])

    # --- BANDEJA DE ENTRADA ---
    with col_lista:
        c_h1, c_h2 = st.columns([85, 15])
        with c_h1:
            st.subheader("Bandeja")
        with c_h2:
            with st.expander("🧹"):
                if st.button("✅ Confirmar", help="Marcar TODO como leído", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE leido = FALSE AND tipo = 'ENTRANTE'"))
                    st.rerun()

        with st.expander("➕ Iniciar Nuevo Chat"):
            with st.form("form_nuevo_chat", clear_on_submit=True):
                nuevo_numero = st.text_input("Número (con código de país, ej: 51999888777):")
                nueva_sesion = st.selectbox(
                    "Línea de envío:", 
                    options=["principal", "default"], 
                    format_func=lambda x: "📱 KM (Principal)" if x == "principal" else "👓 LENTES (Default)"
                )
                nuevo_mensaje = st.text_area("Mensaje inicial:")
                btn_enviar_nuevo = st.form_submit_button("Enviar Mensaje", type="primary", use_container_width=True)

                if btn_enviar_nuevo:
                    if not nuevo_numero.strip() or not nuevo_mensaje.strip():
                        st.error("Ingresa el número y el mensaje.")
                    else:
                        ok, res = mandar_mensaje_api(nuevo_numero, nuevo_mensaje, nueva_sesion)
                        if ok:
                            res_norm = normalizar_telefono_maestro(nuevo_numero)
                            num_final = res_norm.get('db') if isinstance(res_norm, dict) else str(res_norm)
                            if not num_final:
                                num_final = "".join(filter(str.isdigit, str(nuevo_numero)))
                                
                            st.session_state['chat_actual_id'] = num_final
                            st.success("Enviado. Cargando chat...")
                            time.sleep(1.5) 
                            st.rerun()
                        else:
                            st.error(f"Error al enviar: {res}")

        try:
            with engine.connect() as conn:
                conn.commit() 
                tabla = get_table_name(conn)
                busqueda = st.text_input("🔍 Buscar:", placeholder="Nombre o teléfono...")
                
                query = f"""
                    WITH msg_vinculados AS (
                        SELECT m.id_mensaje, m.telefono, m.fecha, m.leido, m.tipo, 
                               COALESCE(t.id_cliente, (SELECT id_cliente FROM {tabla} WHERE telefono = m.telefono AND activo = TRUE LIMIT 1)) as id_cliente
                        FROM mensajes m
                        LEFT JOIN telefonoscliente t ON m.telefono = t.telefono AND t.activo = TRUE
                    ),
                    chat_list AS (
                        SELECT 
                            COALESCE(CAST(id_cliente AS VARCHAR), telefono) as chat_id,
                            MAX(id_cliente) as id_cliente,
                            MAX(telefono) as telefono_contacto,
                            MAX(fecha) as ultima_interaccion,
                            COALESCE(SUM(CASE WHEN leido = FALSE AND tipo = 'ENTRANTE' THEN 1 ELSE 0 END), 0) as no_leidos
                        FROM msg_vinculados
                        GROUP BY COALESCE(CAST(id_cliente AS VARCHAR), telefono)
                    )
                    SELECT cl.*, c.nombre_corto, c.whatsapp_internal_id, c.estado, c.nivel_zombie, c.ultimo_msg_zombie,
                           CASE 
                               WHEN c.nombre_corto IS NOT NULL AND c.nombre_corto != '' THEN c.nombre_corto
                               ELSE cl.telefono_contacto
                           END as nombre
                    FROM chat_list cl
                    LEFT JOIN {tabla} c ON cl.id_cliente = c.id_cliente AND c.activo = TRUE
                    WHERE 1=1
                """
                
                if busqueda:
                    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
                    filtro = f" AND (COALESCE(c.nombre_corto,'') ILIKE '%{busqueda}%'"
                    if busqueda_limpia: filtro += f" OR cl.telefono_contacto ILIKE '%{busqueda_limpia}%' OR EXISTS (SELECT 1 FROM telefonoscliente tc WHERE tc.id_cliente = cl.id_cliente AND tc.telefono ILIKE '%{busqueda_limpia}%'))"
                    else: filtro += ")"
                    query += filtro
                
                query += " ORDER BY no_leidos DESC, ultima_interaccion DESC NULLS LAST LIMIT 100"
                df_clientes = pd.read_sql(text(query), conn)

            with st.container(height=600):
                if df_clientes.empty:
                    st.info("No se encontraron chats.")
                else:
                    cat_map = {
                        "💰 Venta realizada": estados_e2,
                        "🗣️ Conversación": estados_e1,
                        "🚚 En camino": estados_e3,
                        "🛡️ Post-Venta": estados_e4,
                        "🆕 Sin empezar": estados_e0
                    }
                    
                    def asignar_categoria(row):
                        if row['no_leidos'] > 0: return "🔴 Mensajes Nuevos"
                        estado_raw = row['estado']
                        if not estado_raw or str(estado_raw).strip() == "": return "🆕 Sin empezar"
                        
                        estado_clean = str(estado_raw).strip().lower()
                        for cat, estados in cat_map.items():
                            estados_clean = [str(e).strip().lower() for e in estados]
                            if estado_clean in estados_clean: return cat
                        return "📁 Otros Estados"
                        
                    df_clientes['categoria'] = df_clientes.apply(asignar_categoria, axis=1)
                    
                    orden_categorias = [
                        "🔴 Mensajes Nuevos",
                        "💰 Venta realizada", 
                        "🗣️ Conversación", 
                        "🚚 En camino", 
                        "🛡️ Post-Venta", 
                        "📁 Otros Estados",
                        "🆕 Sin empezar"
                    ]

                    for cat in orden_categorias:
                        df_cat = df_clientes[df_clientes['categoria'] == cat]
                        if not df_cat.empty:
                            if cat == "🆕 Sin empezar":
                                df_cat = df_cat.head(30)
                                
                            no_leidos_cat = int(df_cat['no_leidos'].sum())
                            badge = f" :red-background[**{no_leidos_cat}**]" if no_leidos_cat > 0 else ""
                            chat_activo_aqui = str(chat_actual) in df_cat['chat_id'].astype(str).values
                            
                            expandido = (cat == "🔴 Mensajes Nuevos") or chat_activo_aqui or (cat == "💰 Venta realizada")
                            
                            with st.expander(f"{cat} ({len(df_cat)}){badge}", expanded=expandido):
                                if cat == "🗣️ Conversación":
                                    for sub in estados_e1:
                                        df_sub = df_cat[df_cat['estado'].str.lower().str.strip() == str(sub).strip().lower()]
                                        if not df_sub.empty:
                                            st.markdown(f"<div style='font-size: 11px; color: #777; margin-top: 10px; margin-bottom: 2px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;'>📌 {sub}</div>", unsafe_allow_html=True)
                                            for _, row in df_sub.iterrows():
                                                render_boton_chat(row, cat, chat_actual, cambiar_chat)
                                else:
                                    for _, row in df_cat.iterrows():
                                        render_boton_chat(row, cat, chat_actual, cambiar_chat)

        except Exception as e:
            st.error(f"Error cargando lista: {e}")

    # --- CHAT ---
    with col_chat:
        if not chat_actual:
            st.info("👈 Selecciona un chat.")
        else:
            try:
                es_cliente = str(chat_actual).isdigit() and len(str(chat_actual)) < 10

                # AUTO-RESOLUCIÓN LIDs
                if str(chat_actual).startswith("LID_"):
                    with st.spinner("🕵️‍♂️ Consultando número real en WAHA..."):
                        with engine.connect() as conn:
                            info = conn.execute(text(f"SELECT id_cliente, whatsapp_internal_id FROM {tabla} WHERE telefono=:t"), {"t": chat_actual}).fetchone()
                            if info and info.whatsapp_internal_id and info.whatsapp_internal_id.endswith("@lid"):
                                num_real = resolver_telefono_api(info.whatsapp_internal_id, "default")
                                if not num_real: num_real = resolver_telefono_api(info.whatsapp_internal_id, "principal")
                                
                                if num_real:
                                    norm = normalizar_telefono_maestro(num_real)
                                    real_db = norm.get('db') if isinstance(norm, dict) else norm
                                    if real_db:
                                        with engine.begin() as t_conn:
                                            existente = t_conn.execute(text(f"SELECT id_cliente FROM {tabla} WHERE telefono=:t AND activo = TRUE"), {"t": real_db}).fetchone()
                                            if existente:
                                                t_conn.execute(text("UPDATE mensajes SET telefono=:n WHERE telefono=:o"), {"n": real_db, "o": chat_actual})
                                                t_conn.execute(text("UPDATE telefonoscliente SET id_cliente = :new, es_principal = FALSE WHERE id_cliente = :old"), {"new": existente.id_cliente, "old": info.id_cliente})
                                                t_conn.execute(text(f"UPDATE {tabla} SET estado='Duplicado', activo=FALSE, whatsapp_internal_id=:fake WHERE id_cliente=:old"), {"fake": f"MERGED_{chat_actual}", "old": info.id_cliente})
                                                t_conn.execute(text(f"UPDATE {tabla} SET whatsapp_internal_id=:lid WHERE id_cliente=:new"), {"lid": info.whatsapp_internal_id, "new": existente.id_cliente})
                                                st.session_state['chat_actual_id'] = str(existente.id_cliente)
                                            else:
                                                t_conn.execute(text("UPDATE mensajes SET telefono=:n WHERE telefono=:o"), {"n": real_db, "o": chat_actual})
                                                t_conn.execute(text(f"UPDATE {tabla} SET telefono=:n WHERE id_cliente=:id"), {"n": real_db, "id": info.id_cliente})
                                                
                                                lid_ex = t_conn.execute(text("SELECT id_telefono FROM telefonoscliente WHERE id_cliente = :id AND telefono = :o"), {"id": info.id_cliente, "o": chat_actual}).fetchone()
                                                if lid_ex:
                                                    t_conn.execute(text("UPDATE telefonoscliente SET telefono = :n, es_principal = TRUE WHERE id_telefono = :idt"), {"n": real_db, "idt": lid_ex[0]})
                                                else:
                                                    t_conn.execute(text("UPDATE telefonoscliente SET es_principal = FALSE WHERE id_cliente = :id"), {"id": info.id_cliente})
                                                    t_conn.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono, es_principal) VALUES (:id, :n, TRUE)"), {"id": info.id_cliente, "n": real_db})
                                                st.session_state['chat_actual_id'] = str(info.id_cliente)
                                        st.rerun()

                # Marcar leido
                with engine.connect() as conn:
                    conn.commit() 
                    tels_condition = "SELECT telefono FROM telefonoscliente WHERE id_cliente = :id AND activo = TRUE" if es_cliente else "SELECT :id"
                    param_id = int(chat_actual) if es_cliente else chat_actual
                    
                    unreads_query = conn.execute(text(f"SELECT COUNT(*), MAX(session_name) FROM mensajes WHERE telefono IN ({tels_condition}) AND tipo='ENTRANTE' AND leido=FALSE"), {"id": param_id}).fetchone()
                    if unreads_query and unreads_query[0] > 0:
                        sesion_unread = unreads_query[1] if unreads_query[1] else 'default'
                        conn.execute(text(f"UPDATE mensajes SET leido=TRUE WHERE telefono IN ({tels_condition}) AND tipo='ENTRANTE'"), {"id": param_id})
                        conn.commit()
                        
                        tels_api = conn.execute(text(f"SELECT telefono FROM mensajes WHERE telefono IN ({tels_condition}) GROUP BY telefono"), {"id": param_id}).fetchall()
                        for r_t in tels_api:
                            try: threading.Thread(target=marcar_leido_api, args=(r_t[0], sesion_unread)).start()
                            except: pass

                # Cargar datos Unificados e Información de la Dirección Principal y Deuda
                with engine.connect() as conn:
                    conn.commit()
                    if es_cliente:
                        info = conn.execute(text(f"SELECT * FROM {tabla} WHERE id_cliente=:id"), {"id": int(chat_actual)}).fetchone()
                    else:
                        info = conn.execute(text(f"SELECT * FROM {tabla} WHERE telefono=:t"), {"t": chat_actual}).fetchone()
                        
                    nombre = info.nombre_corto if info and info.nombre_corto else chat_actual
                    estado_actual_cliente = info.estado if info and hasattr(info, 'estado') and info.estado else "Sin empezar"
                    
                    # --- NUEVA CONSULTA: EXTRAER DIRECCIÓN PRINCIPAL Y PENDIENTE PAGO ---
                    dir_info = None
                    venta_info = None
                    if info:
                        dir_info = conn.execute(text("""
                            SELECT tipo_envio, distrito, direccion_texto, referencia, gps_link, observacion, 
                                   dni_receptor, agencia_nombre, sede_entrega, nombre_receptor, telefono_receptor, id_direccion
                            FROM direcciones 
                            WHERE id_cliente = :id AND activo = TRUE 
                            ORDER BY es_principal DESC, id_direccion DESC LIMIT 1
                        """), {"id": int(info.id_cliente)}).fetchone()
                        
                        venta_info = conn.execute(text("""
                            SELECT pendiente_pago FROM Ventas 
                            WHERE id_cliente = :id AND anulado = FALSE 
                            ORDER BY id_venta DESC LIMIT 1
                        """), {"id": int(info.id_cliente)}).fetchone()

                    pendiente_pago = float(venta_info.pendiente_pago) if venta_info and venta_info.pendiente_pago else 0.0
                    
                    # --- DETALLES DINÁMICOS CORTO PARA EL TITULO ---
                    sub_detalles = ""
                    if dir_info:
                        if dir_info.tipo_envio == "MOTO":
                            sub_detalles = f"📍 {dir_info.distrito or ''}"
                        elif dir_info.tipo_envio == "AGENCIA":
                            sub_detalles = f"🏢 {dir_info.agencia_nombre or ''} - {dir_info.sede_entrega or ''}"
                        elif dir_info.tipo_envio == "OTROS":
                            sub_detalles = f"📦 {dir_info.observacion or ''}"
                        
                        if len(sub_detalles) > 45:
                            sub_detalles = sub_detalles[:42] + "..."

                    msgs = pd.read_sql(text(f"""
                        SELECT * FROM (
                            SELECT m.* FROM mensajes m
                            LEFT JOIN telefonoscliente t ON m.telefono = t.telefono AND t.activo = TRUE
                            LEFT JOIN {tabla} c ON m.telefono = c.telefono AND c.activo = TRUE
                            WHERE COALESCE(CAST(t.id_cliente AS VARCHAR), CAST(c.id_cliente AS VARCHAR), m.telefono) = :chat_id
                            ORDER BY m.fecha DESC LIMIT 100
                        ) sub ORDER BY fecha ASC
                    """), conn, params={"chat_id": str(chat_actual)})

                # --- CONTROL DE ZONA HORARIA LIMA ---
                if not msgs.empty and 'fecha' in msgs.columns:
                    msgs['fecha'] = pd.to_datetime(msgs['fecha'])
                    
                    # Si el motor de la base de datos asignó una zona horaria, la quitamos
                    if msgs['fecha'].dt.tz is not None:
                        msgs['fecha'] = msgs['fecha'].dt.tz_localize(None)
                        
                    # Sumamos 5 horas para corregir el desfase del webhook y empatar con la hora de Lima
                    msgs['fecha'] = msgs['fecha'] + pd.Timedelta(hours=5)

                # --- HEADER MEJORADO CON DETALLES Y DEUDA VISUAL ---
                badge_deuda = f"<span style='color: #856404; background-color: #ffeeba; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 13px; margin-left: 10px;'>⚠️ Cobrar: S/ {pendiente_pago:.2f}</span>" if pendiente_pago > 0 else ""
                
                if sub_detalles or badge_deuda:
                    st.markdown(f"### 👤 {nombre} <span style='font-size: 15px; font-weight: 400; color: #777; margin-left: 8px;'>{sub_detalles}</span> {badge_deuda}", unsafe_allow_html=True)
                else:
                    st.subheader(f"👤 {nombre}")
                
                c_head_1, c_head_2, c_head_3 = st.columns([25, 40, 35])
                with c_head_1: st.caption("🗂️ Chat Unificado" if es_cliente else f"📱 {chat_actual}")
                
                with c_head_2:
                    opciones_selectbox = todos_los_estados.copy()
                    estado_actual_clean = str(estado_actual_cliente).strip().lower()
                    opciones_clean = [str(e).strip().lower() for e in opciones_selectbox]
                    
                    if estado_actual_clean in opciones_clean:
                        idx_estado = opciones_clean.index(estado_actual_clean)
                    else:
                        opciones_selectbox.insert(0, estado_actual_cliente)
                        idx_estado = 0
                    
                    nuevo_estado = st.selectbox("Estado del Cliente:", opciones_selectbox, index=idx_estado, key=f"st_{chat_actual}", label_visibility="collapsed")
                    
                    if str(nuevo_estado).strip().lower() != estado_actual_clean:
                        id_etapa_val = mapa_subgrupo_id.get(nuevo_estado)
                        es_sin_empezar = (str(nuevo_estado).strip().lower() == "sin empezar")
                        
                        try:
                            with engine.begin() as conn:
                                if es_cliente:
                                    conn.execute(text(f"UPDATE {tabla} SET estado = :e, id_etapa = :id_etapa WHERE id_cliente = :id"), {"e": nuevo_estado, "id_etapa": id_etapa_val, "id": int(chat_actual)})
                                    if es_sin_empezar:
                                        conn.execute(text("UPDATE Ventas SET pendiente_pago = 0 WHERE id_cliente = :id"), {"id": int(chat_actual)})
                                else:
                                    existente = conn.execute(text(f"SELECT id_cliente FROM {tabla} WHERE telefono = :t"), {"t": str(chat_actual)}).fetchone()
                                    if existente:
                                        conn.execute(text(f"UPDATE {tabla} SET estado = :e, id_etapa = :id_etapa WHERE telefono = :t"), {"e": nuevo_estado, "id_etapa": id_etapa_val, "t": str(chat_actual)})
                                        if es_sin_empezar:
                                            conn.execute(text("UPDATE Ventas SET pendiente_pago = 0 WHERE id_cliente = :id"), {"id": existente.id_cliente})
                                    else:
                                        res = conn.execute(text(f"""
                                            INSERT INTO {tabla} (nombre_corto, telefono, estado, id_etapa, activo, fecha_registro) 
                                            VALUES (:nc, :t, :e, :id_etapa, TRUE, NOW()) 
                                            RETURNING id_cliente
                                        """), {"nc": str(chat_actual), "t": str(chat_actual), "e": nuevo_estado, "id_etapa": id_etapa_val})
                                        nuevo_id = res.fetchone()[0]
                                        conn.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono, es_principal) VALUES (:id, :t, TRUE)"), {"id": nuevo_id, "t": str(chat_actual)})
                                        st.session_state['chat_actual_id'] = str(nuevo_id)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al actualizar estado: {e}")

                with c_head_3:
                    nivel_z = info.nivel_zombie if info and hasattr(info, 'nivel_zombie') and info.nivel_zombie is not None else 0
                    niveles_map = {
                        0: "🟢 Cliente Normal", 
                        1: "🧟 Zombie (Espera N1)", 
                        2: "🧟 Zombie (Espera N2)", 
                        3: "🧟 Nivel 2 Enviado"
                    }
                    idx_z = nivel_z if nivel_z in niveles_map else 0
                    
                    nuevo_nivel = st.selectbox(
                        "Etiqueta Zombie:", 
                        options=list(niveles_map.keys()), 
                        format_func=lambda x: niveles_map[x], 
                        index=list(niveles_map.keys()).index(idx_z), 
                        key=f"zmb_{chat_actual}", 
                        label_visibility="collapsed"
                    )
                    
                    if nuevo_nivel != nivel_z:
                        with engine.begin() as conn:
                            tiempo_update = ", ultimo_msg_zombie = NOW()" if nuevo_nivel > 0 else ""
                            if es_cliente:
                                conn.execute(text(f"UPDATE {tabla} SET nivel_zombie = :n {tiempo_update} WHERE id_cliente = :id"), {"n": nuevo_nivel, "id": int(chat_actual)})
                            else:
                                conn.execute(text(f"UPDATE {tabla} SET nivel_zombie = :n {tiempo_update} WHERE telefono = :t"), {"n": nuevo_nivel, "t": str(chat_actual)})
                        st.rerun()

                # =========================================
                # 🚀 MOTOR DE RENDERIZADO
                # =========================================

                html_blocks = []
                imagenes_galeria = []
                
                if not msgs.empty:
                    ultima_fecha = None
                    ahora_lima = datetime.utcnow() - timedelta(hours=5)
                    hoy = ahora_lima.date()
                    ayer = hoy - timedelta(days=1)

                    for _, m in msgs.iterrows():
                        try: fecha_msg = m['fecha'].date() if pd.notna(m['fecha']) else None
                        except: fecha_msg = None

                        if fecha_msg and fecha_msg != ultima_fecha:
                            if fecha_msg == hoy: texto_fecha = "Hoy"
                            elif fecha_msg == ayer: texto_fecha = "Ayer"
                            else: texto_fecha = fecha_msg.strftime("%d/%m/%Y")
                            html_blocks.append(f"<div class='date-separator'><span>{texto_fecha}</span></div>")
                            ultima_fecha = fecha_msg

                        es_mio = (m['tipo'] == 'SALIENTE')
                        clase_row = "msg-row msg-mio" if es_mio else "msg-row msg-otro"
                        clase_bub = "b-mio" if es_mio else "b-otro"
                        hora = m['fecha'].strftime("%H:%M") if pd.notna(m['fecha']) else ""
                        
                        icono_estado = ""
                        if es_mio:
                            estado = m.get('estado_waha', 'pendiente')
                            if estado == 'leido': icono_estado = "<span class='check-read'>✓✓</span>"
                            elif estado == 'recibido': icono_estado = "<span class='check-sent'>✓✓</span>"
                            elif estado == 'enviado': icono_estado = "<span class='check-sent'>✓</span>"
                            else: icono_estado = "🕒"

                        etiqueta_sess = ""
                        if 'session_name' in m and pd.notna(m['session_name']):
                            s_name = str(m['session_name']).strip().lower()
                            if s_name == 'principal': etiqueta_sess = "<span class='session-tag'>KM</span>"
                            elif s_name == 'default': etiqueta_sess = "<span class='session-tag'>LENTES</span>"

                        reply_html = ""
                        if pd.notna(m.get('reply_content')) and str(m['reply_content']).strip() != "":
                            reply_html = f"<div class='reply-box'>↪️ {str(m['reply_content'])}</div>"

                        media_html = ""
                        raw_data = m.get('archivo_data')
                        if raw_data is not None and not pd.isna(raw_data):
                            try:
                                b = bytes(raw_data)
                                if b:
                                    b64 = base64.b64encode(b).decode('utf-8')
                                    mime, ext, nombre_archivo = 'application/octet-stream', 'bin', 'Documento'

                                    if b.startswith(b'\xff\xd8'): mime, ext = 'image/jpeg', 'jpg'
                                    elif b.startswith(b'\x89PNG'): mime, ext = 'image/png', 'png'
                                    elif b'WEBP' in b[:50]: mime, ext = 'image/webp', 'webp'
                                    elif b.startswith(b'OggS'): mime, ext = 'audio/ogg', 'ogg'
                                    elif b'ftyp' in b[:20]: mime, ext = 'video/mp4', 'mp4'
                                    elif b.startswith(b'%PDF'): mime, ext = 'application/pdf', 'pdf'

                                    if mime.startswith('image/'):
                                        media_html = f"<img src='data:{mime};base64,{b64}' style='max-width: 200px; max-height: 200px; border-radius: 8px; margin-bottom: 5px; object-fit: contain; background: transparent; cursor: default;' />"
                                        fecha_corta = m['fecha'].strftime("%d/%m %H:%M") if pd.notna(m['fecha']) else ""
                                        imagenes_galeria.append({"bytes": b, "caption": fecha_corta})
                                    elif mime.startswith('audio/'): media_html = f"<audio controls style='max-width: 250px; height: 40px; margin-bottom: 5px;'><source src='data:{mime};base64,{b64}' type='{mime}'></audio>"
                                    elif mime.startswith('video/'): media_html = f"<video controls style='max-width: 250px; border-radius: 8px; margin-bottom: 5px;'><source src='data:{mime};base64,{b64}' type='{mime}'></video>"
                                    else:
                                        media_html = f"<a href='data:{mime};base64,{b64}' download='{nombre_archivo}.{ext}' style='display: flex; align-items: center; justify-content: center; background: rgba(0,0,0,0.05); padding: 10px; border-radius: 8px; text-decoration: none; color: inherit; font-size: 13px; font-weight: bold; margin-bottom: 5px; border: 1px solid rgba(0,0,0,0.1);'>📄 Descargar Archivo</a>"
                            except:
                                media_html = "<div style='color: gray; font-size: 10px;'>Archivo corrupto</div>"

                        contenido_str = str(m['contenido']) if pd.notna(m['contenido']) else ""
                        
                        # Reemplazo de negritas seguro para Markdown (**) y WhatsApp (*)
                        contenido_str = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', contenido_str)
                        contenido_str = re.sub(r'\*(.*?)\*', r'<b>\1</b>', contenido_str)

                        if contenido_str in ["📷 Archivo Multimedia", "📷 Archivo", "📷 Archivo (Recuperado)"] and media_html:
                            contenido_str = ""
                        
                        texto_html = f"<div style='white-space: pre-wrap;'>{contenido_str}</div>" if contenido_str.strip() else ""
                        
                        tel_label = f"<div style='font-size: 11px; color: {'#6b8e23' if es_mio else '#888'}; margin-bottom: 2px; font-weight: 600; text-align: {'right' if es_mio else 'left'};'>📱 {m['telefono']}</div>"

                        html_msg = f"<div class='{clase_row}'><div class='bubble {clase_bub}'>{tel_label}{reply_html}{media_html}{texto_html}<div class='meta'>{hora} {icono_estado}{etiqueta_sess}</div></div></div>"
                        html_blocks.append(html_msg)

                    html_blocks.reverse()

                if not msgs.empty:
                    # Cambio visual sutil si hay deuda
                    bg_color_chat = "rgba(255, 243, 205, 0.4)" if pendiente_pago > 0 else "transparent"
                    border_color_chat = "#ffeeba" if pendiente_pago > 0 else "rgba(128, 128, 128, 0.2)"
                    
                    css_y_html = f"""<style>
.chat-container {{ display: flex; flex-direction: column-reverse; height: 500px; overflow-y: auto; padding: 10px; border: 2px solid {border_color_chat}; border-radius: 10px; background-image: url('https://user-images.githubusercontent.com/15075759/28719144-86dc0f70-73b1-11e7-911d-60d70fcded21.png'); background-color: {bg_color_chat}; background-blend-mode: overlay; }}
.msg-row {{ display: flex; margin-bottom: 5px; }}
.msg-mio {{ justify-content: flex-end; }}
.msg-otro {{ justify-content: flex-start; }}
.bubble {{ padding: 8px 12px; border-radius: 10px; font-size: 15px; max-width: 80%; display: flex; flex-direction: column; box-shadow: 0 1px 0.5px rgba(0,0,0,0.13); }}
.b-mio {{ background-color: #dcf8c6; color: black; border-top-right-radius: 0; }}
.b-otro {{ background-color: #ffffff; color: black; border-top-left-radius: 0; }}
.meta {{ font-size: 10px; color: #777; text-align: right; margin-top: 3px; display: inline-block; }}
.check-read {{ color: #34B7F1; font-weight: bold; font-size: 12px; }}
.check-sent {{ color: #999; font-size: 12px; }}
.reply-box {{ background-color: rgba(0, 0, 0, 0.05); border-left: 4px solid #34B7F1; padding: 6px 8px; border-radius: 4px; font-size: 13px; margin-bottom: 6px; color: #555; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; text-overflow: ellipsis; }}
.b-mio .reply-box {{ border-left-color: #075E54; background-color: rgba(0, 0, 0, 0.08); }}
.date-separator {{ display: flex; justify-content: center; margin: 15px 0; }}
.date-separator span {{ background-color: #e1f3fb; color: #555; padding: 4px 12px; border-radius: 10px; font-size: 12px; font-weight: bold; box-shadow: 0 1px 0.5px rgba(0,0,0,0.13); }}
.session-tag {{ margin-left: 6px; padding: 1px 4px; border-radius: 4px; font-size: 9px; font-weight: 800; color: #666; background-color: rgba(0,0,0,0.06); }}
.b-mio .session-tag {{ background-color: rgba(0,0,0,0.08); color: #444; }}
</style><div class='chat-container'>{''.join(html_blocks)}</div>"""
                    st.markdown(css_y_html, unsafe_allow_html=True)
                else:
                    st.caption("Inicio de la conversación.")

                # --- INPUT DE ESCRITURA Y SELECTORES ---
                ultima_sesion = None
                if not msgs.empty and 'session_name' in msgs.columns:
                    sesiones_validas = msgs['session_name'].dropna().astype(str).str.strip().str.lower()
                    sesiones_validas = sesiones_validas[sesiones_validas != ""]
                    if not sesiones_validas.empty:
                        ultima_sesion = sesiones_validas.iloc[-1]

                idx_sesion = 1 if ultima_sesion == 'default' else 0

                tel_defecto = None
                msg_recibidos = msgs[msgs['tipo'] == 'ENTRANTE']
                if not msg_recibidos.empty: tel_defecto = msg_recibidos.iloc[-1]['telefono']
                elif not msgs.empty: tel_defecto = msgs.iloc[-1]['telefono']

                st.write("") 
                c_num, c_sel, c_warn = st.columns([30, 25, 45])
                
                with c_num:
                    if es_cliente:
                        with engine.connect() as conn:
                            lista_tels = pd.read_sql(text("SELECT telefono FROM telefonoscliente WHERE id_cliente = :id AND activo = TRUE ORDER BY es_principal DESC"), conn, params={"id": int(chat_actual)})['telefono'].tolist()
                        
                        if tel_defecto and tel_defecto not in lista_tels: lista_tels.append(tel_defecto)
                        if not lista_tels: lista_tels = [tel_defecto] if tel_defecto else [chat_actual]
                        
                        idx_tel = lista_tels.index(tel_defecto) if tel_defecto in lista_tels else 0
                        telefono_destino = st.selectbox("Enviar a:", options=lista_tels, index=idx_tel, key=f"dest_{chat_actual}", label_visibility="collapsed")
                    else:
                        telefono_destino = tel_defecto or chat_actual
                        st.text_input("Enviar a:", value=telefono_destino, disabled=True, label_visibility="collapsed")

                with c_sel:
                    sesion_elegida = st.selectbox(
                        "Línea de envío:", 
                        options=["principal", "default"], 
                        index=idx_sesion,
                        format_func=lambda x: "📱 KM (Principal)" if x == "principal" else "👓 LENTES (Default)",
                        key=f"sess_{chat_actual}",
                        label_visibility="collapsed"
                    )
                with c_warn:
                    if ultima_sesion and ultima_sesion != sesion_elegida:
                        nombre_ult = "KM" if ultima_sesion == 'principal' else "LENTES"
                        st.markdown(f"<div style='color: #856404; background-color: #fff3cd; border: 1px solid #ffeeba; padding: 6px 10px; border-radius: 5px; font-size: 13px; font-weight: bold; margin-top: 1px;'>⚠️ Último msg. por {nombre_ult}.</div>", unsafe_allow_html=True)

                txt = st.chat_input("Escribe un mensaje...")
                
                if txt:
                    ok, res = mandar_mensaje_api(telefono_destino, txt, sesion_elegida)
                    if ok:
                        st.rerun()
                    else:
                        st.error(f"Error al enviar: {res}")

                # --- 🛠️ NUEVA SECCIÓN EXPANSIBLE: OPCIONES ADICIONALES (AL FINAL DEL CHAT) ---
                st.write("")
                with st.expander("🛠️ Opciones Adicionales", expanded=False):
                    tab_info_dir, tab_galeria_img = st.tabs(["🏠 Dirección Principal", "🖼️ Galería de Imágenes"])
                    
                    with tab_info_dir:
                        texto_cobro = f"\n\n**⚠️ Monto por cobrar:** S/ {pendiente_pago:.2f}" if pendiente_pago > 0 else ""
                        
                        if dir_info:
                            st.markdown("##### 📦 Guía Completa de Entrega")
                            if dir_info.tipo_envio == 'MOTO':
                                texto_dir = (f"**Tipo:** 🛵 Motorizado / Interno\n\n"
                                             f"**Recibe:** {dir_info.nombre_receptor or ''}\n\n"
                                             f"**Teléfono:** {dir_info.telefono_receptor or ''}\n\n"
                                             f"**Dirección:** {dir_info.direccion_texto or ''} ({dir_info.distrito or ''})\n\n"
                                             f"**Referencia:** {dir_info.referencia or ''}\n\n"
                                             f"**Link GPS:** {dir_info.gps_link or ''}\n\n"
                                             f"**Observación:** {dir_info.observacion or ''}"
                                             f"{texto_cobro}")
                            elif dir_info.tipo_envio == 'AGENCIA':
                                texto_dir = (f"**Tipo:** 🏢 Agencia\n\n"
                                             f"**Recibe:** {dir_info.nombre_receptor or ''}\n\n"
                                             f"**DNI:** {dir_info.dni_receptor or ''}\n\n"
                                             f"**Teléfono:** {dir_info.telefono_receptor or ''}\n\n"
                                             f"**Agencia:** {dir_info.agencia_nombre or ''} — {dir_info.sede_entrega or ''}\n\n"
                                             f"**Observación:** {dir_info.observacion or ''}"
                                             f"{texto_cobro}")
                            else:
                                texto_dir = (f"**Tipo:** 📦 Otros\n\n"
                                             f"**Recibe:** {dir_info.nombre_receptor or ''}\n\n"
                                             f"**Teléfono:** {dir_info.telefono_receptor or ''}\n\n"
                                             f"**Observación / Notas:** {dir_info.observacion or ''}"
                                             f"{texto_cobro}")
                            st.markdown(texto_dir)
                        else:
                            st.caption("⚠️ Este cliente no tiene ninguna dirección activa registrada.")
                            if pendiente_pago > 0:
                                st.markdown(f"**⚠️ Monto por cobrar de última venta:** S/ {pendiente_pago:.2f}")
                            
                    with tab_galeria_img:
                        if imagenes_galeria:
                            st.caption("Haz click en las flechas de la imagen para ver en pantalla completa.")
                            cols = st.columns(4)
                            for i, img in enumerate(reversed(imagenes_galeria)):
                                with cols[i % 4]:
                                    st.image(img['bytes'], caption=img['caption'], use_container_width=True)
                        else:
                            st.caption("No se han compartido imágenes en este chat todavía.")

            except Exception as e:
                st.error(f"Error detallado en el chat: {str(e)}")