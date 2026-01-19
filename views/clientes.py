import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine
# Importamos la normalización y funciones de Google
from utils import (
    buscar_contacto_google, crear_en_google, actualizar_en_google, 
    normalizar_telefono_maestro
)

def render_clientes():
    st.subheader("👥 Gestión de Clientes")

    # ==============================================================================
    # 1. CREAR NUEVO CLIENTE (INTELIGENTE)
    # ==============================================================================
    with st.expander("➕ Nuevo Cliente (Sincronizado)", expanded=True):
        st.info("💡 Tip: Si ingresas solo el teléfono, el sistema intentará buscar los datos en Google Contacts automáticamente.")
        
        with st.form("form_nuevo_cliente"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 👤 Datos Personales")
                telefono_input = st.text_input("📱 Teléfono (Obligatorio)")
                nombre_real = st.text_input("Nombre (Google)", placeholder="Ej: Juan")
                apellido_real = st.text_input("Apellido (Google)", placeholder="Ej: Perez")
                
            with col2:
                st.markdown("#### 🏢 Datos Internos")
                nombre_corto = st.text_input("📝 Alias / Nombre Corto", placeholder="Ej: Juan Perez (Cliente)")
                medio = st.selectbox("Medio de Contacto", ["WhatsApp", "Instagram", "Facebook", "TikTok", "Recomendado", "Web"])
                estado_ini = st.selectbox("Estado Inicial", ["Interesado en venta", "Responder duda", "Proveedor nacional"])
                codigo = st.text_input("Código (DNI/RUC/Otro)")

            btn_crear = st.form_submit_button("💾 Guardar y Sincronizar", type="primary")

            if btn_crear:
                # A. VALIDACIÓN Y NORMALIZACIÓN
                norm = normalizar_telefono_maestro(telefono_input)
                
                if not norm:
                    st.error("❌ El número de teléfono no es válido.")
                else:
                    telefono_db = norm['db']       # 51986...
                    telefono_google = norm['corto'] # 986...
                    
                    # B. VERIFICAR DUPLICADOS EN BD LOCAL
                    with engine.connect() as conn:
                        existe_db = conn.execute(text("SELECT COUNT(*) FROM Clientes WHERE telefono = :t"), {"t": telefono_db}).scalar()
                    
                    if existe_db > 0:
                        st.error(f"⚠️ El número {telefono_db} ya existe en la base de datos.")
                    else:
                        # C. INTELIGENCIA GOOGLE
                        google_id_final = None
                        nombre_final = nombre_real
                        apellido_final = apellido_real
                        
                        # Buscamos si existe en Google
                        datos_google = buscar_contacto_google(telefono_db) # Buscamos con formato robusto
                        
                        if datos_google and datos_google['encontrado']:
                            st.toast(f"✅ Encontrado en Google: {datos_google['nombre_completo']}")
                            google_id_final = datos_google['google_id']
                            
                            # Si el usuario NO escribió nombre, usamos el de Google
                            if not nombre_final: nombre_final = datos_google['nombre']
                            if not apellido_final: apellido_final = datos_google['apellido']
                            
                            # Si el usuario SÍ escribió nombre, ACTUALIZAMOS Google
                            elif nombre_real: 
                                actualizar_en_google(google_id_final, nombre_real, apellido_real, telefono_db)
                        
                        else:
                            # No existe en Google -> Lo creamos
                            if nombre_real: # Solo si hay nombre
                                google_id_final = crear_en_google(nombre_real, apellido_real, telefono_db)
                                if google_id_final: st.toast("☁️ Creado en Google Contacts")

                        # Definir Nombre Corto si está vacío
                        if not nombre_corto:
                            nombre_corto = f"{nombre_final} {apellido_final}".strip() or "Cliente Nuevo"

                        # D. GUARDAR EN BASE DE DATOS
                        try:
                            with engine.connect() as conn:
                                conn.execute(text("""
                                    INSERT INTO Clientes (
                                        nombre_corto, nombre, apellido, telefono, medio_contacto, 
                                        codigo_contacto, estado, fecha_seguimiento, google_id, activo, fecha_registro
                                    ) VALUES (
                                        :nc, :nom, :ape, :tel, :medio, :cod, :est, CURRENT_DATE, :gid, TRUE, NOW()
                                    )
                                """), {
                                    "nc": nombre_corto, "nom": nombre_final, "ape": apellido_final,
                                    "tel": telefono_db, "medio": medio, "cod": codigo,
                                    "est": estado_ini, "gid": google_id_final
                                })
                                conn.commit()
                            st.success(f"✅ Cliente {nombre_corto} registrado correctamente.")
                            time.sleep(1.5)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error base de datos: {e}")

    st.divider()

    # ==============================================================================
    # 2. BUSCADOR Y EDICIÓN MASIVA
    # ==============================================================================
    st.subheader("🔍 Editar Clientes")

    col_search, _ = st.columns([3, 1])
    with col_search:
        busqueda = st.text_input("Buscar por nombre, alias o teléfono:", placeholder="Escribe aquí...")

    OPCIONES_ESTADO = [
        "Sin empezar", "Responder duda", "Interesado en venta", 
        "Proveedor nacional", "Proveedor internacional", 
        "Venta motorizado", "Venta agencia", "Venta express moto",
        "En camino moto", "En camino agencia", "Contraentrega agencia",
        "Pendiente agradecer", "Problema post", "Venta cerrada"
    ]

    df_resultados = pd.DataFrame()

    if busqueda:
        with engine.connect() as conn:
            # Búsqueda flexible
            query = text("""
                SELECT id_cliente, nombre_corto, estado, nombre, apellido, telefono, google_id 
                FROM Clientes 
                WHERE (nombre_corto ILIKE :b OR telefono ILIKE :b OR nombre ILIKE :b) AND activo = TRUE 
                ORDER BY nombre_corto ASC LIMIT 20
            """)
            df_resultados = pd.read_sql(query, conn, params={"b": f"%{busqueda}%"})

            if not df_resultados.empty:
                st.caption(f"Resultados: {len(df_resultados)}")
                
                cambios = st.data_editor(
                    df_resultados,
                    key="editor_busqueda",
                    column_config={
                        "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                        "google_id": None, # Oculto
                        "nombre_corto": st.column_config.TextColumn("Alias (Interno)", required=True),
                        "estado": st.column_config.SelectboxColumn("Estado", options=OPCIONES_ESTADO, required=True),
                        "nombre": st.column_config.TextColumn("Nombre (Google)"),
                        "apellido": st.column_config.TextColumn("Apellido (Google)"),
                        "telefono": st.column_config.TextColumn("Teléfono (Editable)", required=True)
                    },
                    hide_index=True,
                    use_container_width=True
                )

                if st.button("💾 Guardar Cambios en Lote", type="primary"):
                    with engine.connect() as conn:
                        trans = conn.begin()
                        errores = []
                        try:
                            for idx, row in cambios.iterrows():
                                # 1. NORMALIZAR EL TELÉFONO EDITADO
                                # Esto es vital: si el usuario editó el número y puso espacios o guiones,
                                # normalizar_telefono_maestro lo arregla a 51...
                                norm_edit = normalizar_telefono_maestro(row['telefono'])
                                
                                if not norm_edit:
                                    errores.append(f"Fila ID {row['id_cliente']}: Número inválido")
                                    continue
                                
                                tel_final = norm_edit['db']

                                # 2. Actualizar BD
                                conn.execute(text("""
                                    UPDATE Clientes 
                                    SET nombre=:n, apellido=:a, telefono=:t, nombre_corto=:nc, estado=:est
                                    WHERE id_cliente=:id
                                """), {
                                    "n": row['nombre'], "a": row['apellido'], 
                                    "t": tel_final, "nc": row['nombre_corto'], 
                                    "est": row['estado'], "id": row['id_cliente']
                                })
                                
                                # 3. Actualizar Google (Si tiene ID)
                                if row['google_id']:
                                    actualizar_en_google(row['google_id'], row['nombre'], row['apellido'], tel_final)
                                    
                            trans.commit()
                            if errores:
                                st.warning("Se guardaron los datos excepto: " + ", ".join(errores))
                            else:
                                st.success("✅ Todos los datos actualizados y sincronizados.")
                            
                            time.sleep(1.5)
                            st.rerun()
                            
                        except Exception as e:
                            trans.rollback()
                            st.error(f"Error crítico al guardar: {e}")
            else:
                st.info("No se encontraron coincidencias.")

    # ==============================================================================
    # 3. FUSIÓN DE DUPLICADOS (Sin cambios, tu lógica era buena)
    # ==============================================================================
    st.divider()
    st.subheader("🧬 Fusión de Clientes Duplicados")
    
    with st.expander("Abrir herramienta de fusión"):
        col_dup, col_orig = st.columns(2)
        
        # ... (Tu código de fusión se mantiene igual abajo, es funcional) ...
        # Solo asegúrate de copiar la lógica de fusión que ya tenías
        
        # 1. CLIENTE A ELIMINAR
        with col_dup:
            st.markdown("### ❌ A Eliminar")
            search_dup = st.text_input("Buscar duplicado:", key="search_dup")
            id_duplicado = None
            info_duplicado = None
            if search_dup:
                with engine.connect() as conn:
                    res = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo=TRUE LIMIT 5"), conn, params={"s":f"%{search_dup}%"})
                if not res.empty:
                    opts_dup = res.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID:{x['id_cliente']}", axis=1).tolist()
                    sel_dup = st.selectbox("Sel. Duplicado:", opts_dup)
                    id_duplicado = int(sel_dup.split("ID:")[1])
                    info_duplicado = sel_dup

        # 2. CLIENTE PRINCIPAL
        with col_orig:
            st.markdown("### ✅ Principal")
            search_orig = st.text_input("Buscar principal:", key="search_orig")
            id_original = None
            if search_orig:
                with engine.connect() as conn:
                    res2 = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo=TRUE LIMIT 5"), conn, params={"s":f"%{search_orig}%"})
                if not res2.empty:
                    opts_orig = res2.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID:{x['id_cliente']}", axis=1).tolist()
                    sel_orig = st.selectbox("Sel. Principal:", opts_orig)
                    id_original = int(sel_orig.split("ID:")[1])

        # 3. ACCIÓN
        if id_duplicado and id_original:
            if id_duplicado == id_original:
                st.error("Son el mismo cliente.")
            elif st.button("🚀 FUSIONAR"):
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        # Recuperar teléfono viejo
                        old_tel = conn.execute(text("SELECT telefono FROM Clientes WHERE id_cliente=:id"), {"id": id_duplicado}).scalar()
                        # Mover Ventas y Direcciones
                        conn.execute(text("UPDATE Ventas SET id_cliente=:new WHERE id_cliente=:old"), {"new":id_original, "old":id_duplicado})
                        conn.execute(text("UPDATE Direcciones SET id_cliente=:new WHERE id_cliente=:old"), {"new":id_original, "old":id_duplicado})
                        # Mover Mensajes (NUEVO: Importante no perder chats)
                        conn.execute(text("UPDATE mensajes SET id_cliente=:new WHERE id_cliente=:old"), {"new":id_original, "old":id_duplicado})
                        # Guardar teléfono viejo en secundario
                        conn.execute(text("UPDATE Clientes SET telefono_secundario=:tel WHERE id_cliente=:id AND (telefono_secundario IS NULL OR telefono_secundario='')"), {"tel":old_tel, "id":id_original})
                        # Desactivar
                        conn.execute(text("UPDATE Clientes SET activo=FALSE, nombre_corto=nombre_corto||' (FUSIONADO)' WHERE id_cliente=:id"), {"id":id_duplicado})
                        trans.commit()
                        st.success("Fusión completada.")
                        time.sleep(1.5)
                        st.rerun()
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error: {e}")