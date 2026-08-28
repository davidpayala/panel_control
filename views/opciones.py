import streamlit as st
import pandas as pd
import time
import subprocess # <- Importante para poder enviar comandos a Linux
from sqlalchemy import text
from database import engine
import subprocess
import time
import psutil
import platform

def render_opciones():
    # Estructura mejorada con nombres descriptivos para facilitar agregar nuevas pestañas
    tab_estados, tab_jerarquia, tab_usuarios, tab_sistema = st.tabs([
        "📋 Estados de Clientes", 
        "📁 Jerarquía de Categorías", 
        "👥 Usuarios",
        "⚙️ Sistema y Mantenimiento" # <--- Tu nueva pestaña
    ])

    # =========================================================================
    # PESTAÑA 1: GESTIÓN DE ETAPAS / ESTADOS
    # =========================================================================
    with tab_estados:
        st.subheader("Gestión de Etapas y Estados")
        st.info("Aquí puedes editar los grupos y subgrupos. También puedes agregar nuevas filas al final de la tabla para crear nuevos estados.")
        
        with engine.connect() as conn:
            df_etapas = pd.read_sql(text("""
                SELECT id_etapa, grupo, subgrupo, activo 
                FROM EtapasCliente 
                ORDER BY grupo, id_etapa
            """), conn)
            
        edited_df = st.data_editor(
            df_etapas,
            column_config={
                "id_etapa": st.column_config.NumberColumn("ID", disabled=True),
                "grupo": st.column_config.TextColumn("Grupo (Ej: Etapa 1)", required=True),
                "subgrupo": st.column_config.TextColumn("Subgrupo (Estado)", required=True),
                "activo": st.column_config.CheckboxColumn("Activo")
            },
            num_rows="dynamic",
            key="editor_etapas",
            use_container_width=True
        )
        
        if st.button("💾 Guardar Cambios de Etapas", type="primary"):
            try:
                with engine.begin() as conn:
                    for index, row in edited_df.iterrows():
                        if pd.isna(row['id_etapa']):
                            conn.execute(text("""
                                INSERT INTO EtapasCliente (grupo, subgrupo, activo) 
                                VALUES (:g, :s, :a)
                            """), {"g": row['grupo'], "s": row['subgrupo'], "a": row['activo']})
                        else:
                            conn.execute(text("""
                                UPDATE EtapasCliente 
                                SET grupo = :g, subgrupo = :s, activo = :a 
                                WHERE id_etapa = :id
                            """), {
                                "g": row['grupo'], 
                                "s": row['subgrupo'], 
                                "a": row['activo'], 
                                "id": int(row['id_etapa'])
                            })
                            
                st.success("✅ Estados y etapas guardados correctamente en la Base de Datos.")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Error al guardar los estados: {e}")

    # =========================================================================
    # PESTAÑA 2: DEPURACIÓN Y GESTIÓN DE CATEGORÍAS
    # =========================================================================
    with tab_jerarquia:
        st.subheader("🛠️ Depuración de Categorías y Estructura")
        
        with engine.connect() as conn:
            erroneos_df = pd.read_sql(
                text("""
                    SELECT id_producto, marca, modelo, nombre, categoria 
                    FROM Productos 
                    WHERE categoria ILIKE :buscar OR categoria IS NULL
                """), 
                conn, 
                params={"buscar": "%contacto%"}
            )
            
        if not erroneos_df.empty:
            st.warning(f"⚠️ Se han detectado **{len(erroneos_df)} productos** con la categoría obsoleta o sin clasificar.")
            
            with st.container(border=True):
                st.markdown("**Reclasificación Rápida de Producto:**")
                opciones_prod = {row['id_producto']: f"[{row['marca']} {row['modelo']}] - {row['nombre']} ({row['categoria']})" for _, row in erroneos_df.iterrows()}
                id_prod_sel = st.selectbox("Selecciona el producto a corregir:", options=list(opciones_prod.keys()), format_func=lambda x: opciones_prod[x])
                
                with engine.connect() as conn_sub:
                    subs_lentes = [r[0] for r in conn_sub.execute(text("SELECT subcategoria FROM Subcategorias_Sistema WHERE macro_categoria = 'Lentes' ORDER BY subcategoria")).fetchall()]
                if not subs_lentes: subs_lentes = ["Estilo Natural", "Estilo Fantasía", "Accesorios"]

                nueva_subcat = st.selectbox("Asignar a Subcategoría Correcta:", subs_lentes)
                
                if st.button("🔄 Corregir Categoría de Producto", type="primary"):
                    with engine.begin() as conn_tx:
                        conn_tx.execute(text("""
                            UPDATE Productos 
                            SET macro_categoria = 'Lentes', categoria = :nueva 
                            WHERE id_producto = :id
                        """), {"nueva": nueva_subcat, "id": int(id_prod_sel)})
                    st.toast(f"✅ Producto corregido a {nueva_subcat} exitosamente.")
                    time.sleep(1)
                    st.rerun()
        else:
            st.success("🎉 ¡Excelente! No quedan productos con etiquetas obsoletas de contacto.")

        st.divider()

        st.markdown("### 📂 Subcategorías Oficiales del Sistema")
        st.info("Las subcategorías registradas aquí alimentan directamente las listas desplegables en la edición de Productos.")
        
        with engine.connect() as conn:
            df_maestras = pd.read_sql(text("SELECT macro_categoria AS \"Línea Mayor\", subcategoria AS \"Subcategoría Registrada\" FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
        st.dataframe(df_maestras, use_container_width=True, hide_index=True)
        
        with st.expander("➕ Agregar Nueva Subcategoría al Sistema", expanded=True):
            with st.form("nueva_categoria_form"):
                macro_input = st.selectbox("Línea Mayor (Asociar a):", ["Pelucas", "Lentes", "Accesorios"])
                sub_input = st.text_input("Nombre de la Nueva Subcategoría (Ej: Cosplay Premium, Lace Front):")
                
                if st.form_submit_button("Registrar Subcategoría en BD"):
                    if not sub_input.strip():
                        st.error("El nombre de la subcategoría no puede estar vacío.")
                    else:
                        try:
                            with engine.begin() as conn_tx:
                                conn_tx.execute(text("""
                                    INSERT INTO Subcategorias_Sistema (macro_categoria, subcategoria)
                                    VALUES (:m, :s)
                                    ON CONFLICT DO NOTHING
                                """), {"m": macro_input, "s": sub_input.strip()})
                            st.success(f"✅ ¡Subcategoría '{sub_input.strip()}' agregada permanentemente a '{macro_input}'!")
                            time.sleep(1.2)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error SQL: {e}")

        # --- SUB-SECCIÓN C: DESTRUCTOR DE FANTASMAS ---
        st.divider()
        st.markdown("### 👻 Destructor de Registros Fantasma (Vacíos)")
        with engine.connect() as conn:
            fantasmas_df = pd.read_sql(text("""
                SELECT p.id_producto, p.macro_categoria, p.categoria, p.marca, p.modelo, p.nombre, v.sku 
                FROM Productos p
                LEFT JOIN Variantes v ON p.id_producto = v.id_producto
                WHERE TRIM(COALESCE(p.nombre, '')) = '' OR (v.sku IS NOT NULL AND TRIM(v.sku) = '')
            """), conn)
            
        if not fantasmas_df.empty:
            st.error(f"🚨 **¡Alerta de Fantasma!** Se ha detectado {len(fantasmas_df)} registro(s) con datos en blanco:")
            st.dataframe(fantasmas_df, use_container_width=True)
            id_destruir = st.selectbox("Selecciona el ID del registro vacío a eliminar:", fantasmas_df['id_producto'].unique())
            if st.button("💥 Eliminar Registro Fantasma Definitivamente", type="primary"):
                with engine.begin() as tx:
                    tx.execute(text("DELETE FROM Variantes WHERE id_producto = :id"), {"id": int(id_destruir)})
                    tx.execute(text("DELETE FROM Productos WHERE id_producto = :id"), {"id": int(id_destruir)})
                st.success("¡Registro eliminado de la base de datos!")
                time.sleep(1.5)
                st.rerun()
        else:
            st.caption("No se detectaron registros fantasma en la base de datos.")

    # =========================================================================
    # PESTAÑA 3: USUARIOS
    # =========================================================================
    with tab_usuarios:
        st.subheader("👥 Gestión de Usuarios del Sistema")
        with engine.connect() as conn:
            df_usuarios = pd.read_sql(text("SELECT id, usuario, rol, modulos FROM Usuarios ORDER BY id"), conn)
            
        st.dataframe(df_usuarios, hide_index=True, use_container_width=True)
        st.info("Para modificar contraseñas o permisos de acceso, utiliza tu cliente SQL o pgAdmin conectado a la base de datos local.")

# =========================================================================
    # PESTAÑA 4: SISTEMA Y MANTENIMIENTO
    # =========================================================================
    with tab_sistema:
        # Función auxiliar para consultar el hardware a Linux silenciosamente
        def consultar_linux(comando):
            try:
                return subprocess.check_output(comando, shell=True, text=True, stderr=subprocess.DEVNULL).strip()
            except Exception:
                return ""

        st.subheader("🖥️ Especificaciones de Hardware")
        col_hw1, col_hw2 = st.columns(2)
        
        with col_hw1:
            st.markdown("**1. Procesador (CPU):**")
            cpu_modelo = consultar_linux("cat /proc/cpuinfo | grep 'model name' | head -n 1 | cut -d ':' -f 2").strip()
            st.info(f"🧠 **{cpu_modelo if cpu_modelo else platform.processor()}**")
            
            st.markdown("**2. Tarjeta de Video (GPU):**")
            gpu_raw = consultar_linux("lspci | grep -i -E 'vga|3d|display' | cut -d ':' -f 3")
            if gpu_raw:
                gpus = [g.strip() for g in gpu_raw.split('\n') if g.strip()]
                for i, gpu in enumerate(gpus):
                    st.caption(f"🎮 GPU {i+1}: **{gpu}**")
            else:
                st.caption("🎮 Gráficos Integrados / No detectada")

        with col_hw2:
            st.markdown("**3. Placa Madre (Motherboard):**")
            board_vendor = consultar_linux("cat /sys/class/dmi/id/board_vendor")
            board_name = consultar_linux("cat /sys/class/dmi/id/board_name")
            st.success(f"🎛️ **{board_vendor} {board_name}**".strip() if board_vendor else "🎛️ Acceso restringido")
            
            st.markdown("**4. Memoria RAM:**")
            total_ram = psutil.virtual_memory().total / (1024**3)
            st.success(f"⚡ **Total Instalada: {total_ram:.1f} GB**")
            
            # --- MEJORA: Filtro inteligente de Slots de RAM ---
            ram_slots = consultar_linux("sudo dmidecode -t memory | grep -E 'Size:|Locator:' | grep -v 'Bank Locator'")
            if ram_slots:
                lineas = ram_slots.split('\n')
                modulos_activos = []
                size_tmp = ""
                
                for linea in lineas:
                    linea = linea.strip()
                    if linea.startswith("Size:"):
                        size_tmp = linea.replace("Size:", "").strip()
                    elif linea.startswith("Locator:"):
                        loc_tmp = linea.replace("Locator:", "").strip()
                        # Solo guardamos si el slot tiene una memoria insertada
                        if size_tmp and size_tmp != "No Module Installed":
                            modulos_activos.append(f"🔌 **{loc_tmp}**: {size_tmp}")

                with st.expander("Ver detalle de Módulos (Slots)"):
                    if modulos_activos:
                        for mod in modulos_activos:
                            st.markdown(mod)
                    else:
                        st.info("No se pudo extraer la distribución de los slots.")

        st.divider()

        # --- SECCIÓN DE MÉTRICAS EN VIVO E HISTÓRICAS ---
        st.subheader("📊 Consumo de Recursos y Estadísticas")
        
        cpu_pct = psutil.cpu_percent(interval=0.2)
        ram_info = psutil.virtual_memory()
        disco_info = psutil.disk_usage('/')
        
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("💻 CPU (En Vivo)", f"{cpu_pct}%", delta="Normal" if cpu_pct < 80 else "Saturado", delta_color="inverse")
        col_m2.metric("🧠 RAM (En Vivo)", f"{ram_info.percent}%", delta=f"{ram_info.used / (1024**3):.1f} GB en uso", delta_color="inverse")
        col_m3.metric("💽 Disco Duro (Raíz)", f"{disco_info.percent}%", delta=f"{disco_info.free / (1024**3):.1f} GB libres", delta_color="normal")

        try:
            with engine.connect() as conn:
                df_historial = pd.read_sql(text("""
                    SELECT 
                        DATE_TRUNC('hour', fecha) as "Hora",
                        ROUND(AVG(cpu_pct), 1) as "Promedio CPU (%)",
                        ROUND((SUM(CASE WHEN cpu_pct >= 95 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 1) as "CPU al 100% (%)",
                        ROUND(AVG(ram_pct), 1) as "Promedio RAM (%)",
                        ROUND((SUM(CASE WHEN ram_pct >= 95 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 1) as "RAM al 100% (%)"
                    FROM Servidor_Metricas
                    WHERE fecha >= NOW() - INTERVAL '24 hours'
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT 12
                """), conn)
                
            if not df_historial.empty:
                df_historial['Hora'] = pd.to_datetime(df_historial['Hora'])
                
                # --- ⏱️ CORRECCIÓN DEFINITIVA DE HORA (UTC-5 PERÚ) ---
                # Si el servidor guardó en UTC (Londres), lo forzamos a hora de Lima
                df_historial['Hora'] = df_historial['Hora'] + pd.Timedelta(hours=5)
                df_historial['Hora'] = df_historial['Hora'].dt.strftime('%H:00')
                
                st.markdown("**⏱️ Historial de las últimas 12 horas:**")
                st.dataframe(df_historial, use_container_width=True, hide_index=True)
            else:
                st.info("⏳ Recolectando datos... El historial aparecerá en unos minutos.")
        except Exception as e:
            st.info("⏳ El cron de monitoreo acaba de ser configurado. Los datos aparecerán pronto.")

        st.divider()

        # --- BOTONES DE PELIGRO EN EL FONDO ---
        st.subheader("⚠️ Zona de Peligro (Opciones de Energía)")
        st.write("Para evitar accidentes, primero debes desbloquear el candado del botón que deseas utilizar.")

        col_dan1, col_dan2 = st.columns(2)
        
        with col_dan1:
            with st.container(border=True):
                st.markdown("#### ♻️ Motor WAHA (WhatsApp)")
                st.caption("Limpia la memoria si notas lentitud en el envío masivo.")
                candado_waha = st.checkbox("🔓 Desbloquear WAHA")
                if st.button("Reiniciar WAHA", type="primary", use_container_width=True, disabled=not candado_waha):
                    with st.spinner("⏳ Apagando y limpiando WAHA..."):
                        try:
                            res = subprocess.run(["docker", "restart", "waha"], capture_output=True, text=True, timeout=30)
                            if res.returncode == 0:
                                time.sleep(3) 
                                st.success("✅ WAHA reiniciado.")
                                st.rerun()
                            else:
                                st.error(f"❌ Error: {res.stderr}")
                        except Exception as e:
                            st.error(f"🔥 Error crítico: {e}")

        with col_dan2:
            with st.container(border=True):
                st.markdown("#### ☢️ Servidor (Linux)")
                st.caption("Apaga físicamente la máquina. El Panel y la BD caerán temporalmente.")
                candado_linux = st.checkbox("🔓 Desbloquear Sistema")
                if st.button("🔥 Reiniciar Servidor", type="primary", use_container_width=True, disabled=not candado_linux):
                    st.info("⏳ Iniciando reinicio profundo... Espera 2 minutos y presiona F5.")
                    time.sleep(2)
                    try:
                        subprocess.Popen(["sudo", "reboot"])
                    except Exception as e:
                        st.error(f"❌ Error: {e}")