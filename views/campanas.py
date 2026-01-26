import streamlit as st
import pandas as pd
import time
import random
import threading
import re
from datetime import datetime
from sqlalchemy import text
from database import engine
from utils import (
    enviar_mensaje_whatsapp, enviar_mensaje_media, 
    normalizar_telefono_maestro, verificar_numero_waha
)

# ==============================================================================
# 🧠 SISTEMA DE MEMORIA (PERSISTENCIA EN DB)
# ==============================================================================
def init_ajustes_db():
    """Crea la tabla de ajustes si no existe"""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS Ajustes (
                    clave TEXT PRIMARY KEY,
                    valor TEXT
                );
            """))
    except Exception as e:
        print(f"Error init ajustes: {e}")

def get_ajuste(clave, default=""):
    """Lee un valor de la base de datos"""
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT valor FROM Ajustes WHERE clave = :k"), {"k": clave}).fetchone()
            return res[0] if res else default
    except:
        return default

def set_ajuste(clave, valor):
    """Guarda un valor en la base de datos"""
    try:
        with engine.begin() as conn:
            # Upsert para PostgreSQL
            conn.execute(text("""
                INSERT INTO Ajustes (clave, valor) VALUES (:k, :v)
                ON CONFLICT (clave) DO UPDATE SET valor = :v
            """), {"k": clave, "v": str(valor)})
    except Exception as e:
        print(f"Error guardando ajuste {clave}: {e}")

# ==============================================================================
# 🧠 CEREBRO DE LA CAMPAÑA (BACKGROUND RUNNER)
# ==============================================================================
class CampaignManager:
    def __init__(self):
        self.running = False
        self.paused = False
        self.stop_signal = False
        
        # Estado de la campaña actual
        self.df_pendientes = pd.DataFrame()
        
        # ESTRUCTURA DEL MENSAJE MODULAR
        self.cuerpos = [] # Lista de opciones de cuerpo
        self.ctas = []    # Lista de opciones de cierre
        
        self.media_bytes = None
        self.mime_type = None
        self.filename = None
        self.config_delay = (30, 60)
        self.batch_size = 10
        
        # Métricas
        self.progreso = 0
        self.total = 0
        self.exitos = 0
        self.errores = 0
        self.logs = []
        self.current_client_name = "Esperando..."

    def iniciar_hilo(self, df, cuerpos, ctas, media, mime, fname, delay_range, batch):
        if self.running: return 
        
        self.df_pendientes = df
        self.cuerpos = [c for c in cuerpos if c.strip()] # Solo textos no vacíos
        self.ctas = [c for c in ctas if c.strip()]       # Solo textos no vacíos
        
        self.media_bytes = media
        self.mime_type = mime
        self.filename = fname
        self.config_delay = delay_range
        self.batch_size = batch
        
        self.total = len(df)
        self.progreso = 0
        self.exitos = 0
        self.errores = 0
        self.logs = ["🚀 Campaña Modular iniciada en segundo plano..."]
        
        self.running = True
        self.paused = False
        self.stop_signal = False
        
        hilo = threading.Thread(target=self._proceso_envio)
        hilo.start()

    def pausar(self):
        self.paused = True
        self.logs.append("⏸️ Campaña PAUSADA por el usuario.")

    def reanudar(self):
        self.paused = False
        self.logs.append("▶️ Campaña REANUDADA.")

    def detener(self):
        self.stop_signal = True
        self.running = False
        self.logs.append("🛑 Campaña DETENIDA definitivamente.")

    def _construir_mensaje(self, row):
        """Construye el mensaje único combinando las 3 partes"""
        # 1. SALUDO INTELIGENTE
        saludos_base = ["Hola", "Saludos", "Buen día", "Qué tal", "Hola hola"]
        saludo_azar = random.choice(saludos_base)
        
        nombre_ia = row.get('nombre_ia')
        if nombre_ia and str(nombre_ia).strip():
            saludo_final = f"{saludo_azar} {nombre_ia}"
        else:
            saludo_final = saludo_azar # Si no hay nombre, solo "Hola"
            
        # 2. CUERPO (Alternativa Aleatoria)
        cuerpo_final = random.choice(self.cuerpos) if self.cuerpos else ""
        
        # 3. LLAMADA A LA ACCIÓN (Alternativa Aleatoria)
        cta_final = random.choice(self.ctas) if self.ctas else ""
        
        # UNIÓN FINAL (Con saltos de línea)
        mensaje_completo = f"{saludo_final}\n\n{cuerpo_final}\n\n{cta_final}".strip()
        
        return procesar_spintax(mensaje_completo)

    def _proceso_envio(self):
        count_batch = 0
        lista_clientes = self.df_pendientes.to_dict('records')

        for i, row in enumerate(lista_clientes):
            if self.stop_signal: break
            while self.paused:
                time.sleep(1)
                if self.stop_signal: break

            self.current_client_name = row.get('nombre_corto', 'Cliente')
            self.progreso = i + 1
            
            # --- PAUSA DE LOTE ---
            if count_batch >= self.batch_size:
                ts_lote = random.randint(180, 300)
                self.logs.append(f"🛑 PAUSA SEGURIDAD: Enfriando {ts_lote//60} min...")
                for _ in range(ts_lote):
                    if self.stop_signal: break
                    time.sleep(1)
                count_batch = 0

            # --- LÓGICA DE ENVÍO ---
            nombre = row.get('nombre_corto', 'Cliente')
            tel_bruto = row.get('telefono', '')
            
            try:
                norm = normalizar_telefono_maestro(tel_bruto)
                if not norm:
                    self.logs.append(f"❌ {nombre}: Teléfono inválido")
                    self.errores += 1
                    continue
                
                tel_final = norm['db']
                id_cliente = row.get('id_cliente')

                # VERIFICAR WAHA
                existe = verificar_numero_waha(tel_final)
                if existe is False:
                    self.logs.append(f"🗑️ {nombre}: NO TIENE WHATSAPP. Eliminando...")
                    self.errores += 1
                    with engine.connect() as conn:
                        conn.execute(text("UPDATE Clientes SET telefono = NULL, activo = FALSE, notas = 'Número no existe en WA' WHERE id_cliente = :id"), {"id": id_cliente})
                        conn.commit()
                    continue

                # CONSTRUIR MENSAJE MODULAR
                msg_final = self._construir_mensaje(row)

                # ENVIAR
                res = False
                if self.media_bytes:
                    res, _ = enviar_mensaje_media(tel_final, self.media_bytes, self.mime_type, msg_final, self.filename)
                elif msg_final:
                    res, _ = enviar_mensaje_whatsapp(tel_final, msg_final)

                if res:
                    self.exitos += 1
                    self.logs.append(f"✅ {nombre}: Enviado")
                    count_batch += 1
                    with engine.connect() as conn:
                         conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :c, (NOW() - INTERVAL '5 hours'), TRUE, :d)"), 
                                        {"t": tel_final, "c": msg_final, "d": self.media_bytes})
                         conn.commit()
                else:
                    self.errores += 1
                    self.logs.append(f"⚠️ {nombre}: Fallo API")

            except Exception as e:
                self.errores += 1
                self.logs.append(f"🔥 Error {nombre}: {e}")

            if i < self.total - 1:
                ts = random.randint(self.config_delay[0], self.config_delay[1])
                time.sleep(ts)

        self.running = False
        self.logs.append("🎉 Campaña Finalizada.")

@st.cache_resource
def get_manager():
    return CampaignManager()

# ==============================================================================
# UI Y UTILIDADES
# ==============================================================================
def procesar_spintax(texto):
    if not texto: return ""
    pattern = r'\{([^{}]+)\}'
    while True:
        match = re.search(pattern, texto)
        if not match: break
        opciones = match.group(1).split('|')
        eleccion = random.choice(opciones)
        texto = texto[:match.start()] + eleccion + texto[match.end():]
    return texto

def render_campanas():
    st.title("📢 Campañas Masivas 3.5 (Persistente)")
    
    # Inicializar DB de ajustes la primera vez
    init_ajustes_db()
    
    manager = get_manager()

    # --- ZONA DE CONTROL (EN EJECUCIÓN) ---
    if manager.running or manager.paused:
        st.success("🚀 **CAMPAÑA EN CURSO** (Segundo Plano)")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Progreso", f"{manager.progreso}/{manager.total}")
        c2.metric("Éxitos", manager.exitos)
        c3.metric("Fallos", manager.errores)
        c4.metric("Estado", "⏸️ PAUSADO" if manager.paused else "▶️ CORRIENDO")
        st.progress(manager.progreso / manager.total if manager.total > 0 else 0)
        
        col_btns = st.columns(3)
        if manager.paused:
            if col_btns[0].button("▶️ REANUDAR"): manager.reanudar(); st.rerun()
        else:
            if col_btns[0].button("⏸️ PAUSAR"): manager.pausar(); st.rerun()
        if col_btns[2].button("🛑 DETENER"): manager.detener(); st.rerun()

        with st.expander("📜 Ver Logs en Vivo", expanded=True):
            st.code("\n".join(manager.logs[-8:]))
            if st.button("Actualizar Vista"): st.rerun()
        return

    # --- ZONA DE CONFIGURACIÓN ---
    st.info("Configura tu mensaje. Se guardará automáticamente.")

    # 1. CARGA DE AUDIENCIA
    if st.button("🔄 Cargar Grupos"):
        with engine.connect() as conn:
            try:
                # IMPORTANTE: Traemos 'nombre_ia'
                query = "SELECT id_cliente, nombre_corto, nombre_ia, telefono, estado, COALESCE(etiquetas, '') as etiquetas FROM Clientes WHERE activo = TRUE AND length(telefono) > 6"
                df_raw = pd.read_sql(text(query), conn)
                
                def clasificar(row):
                    tags = str(row['etiquetas']).upper()
                    if "SPAM" in tags: return "🚫 SPAM (Pruebas)"
                    if "VIP" in tags: return "💎 VIP (Prioridad)"
                    if "COMPRÓ" in tags: return "✅ Compradores"
                    return "GENERAL"
                
                df_raw['segmento'] = df_raw.apply(clasificar, axis=1)
                grupos = {}
                for seg in ["🚫 SPAM (Pruebas)", "💎 VIP (Prioridad)", "✅ Compradores"]:
                    sub = df_raw[df_raw['segmento'] == seg]
                    if not sub.empty: grupos[f"{seg} ({len(sub)})"] = sub
                
                df_gen = df_raw[df_raw['segmento'] == "GENERAL"]
                tamano = 100
                for i in range(0, len(df_gen), tamano):
                    sub = df_gen.iloc[i:i+tamano]
                    grupos[f"📦 Grupo {(i//tamano)+1} (General) - {len(sub)} pax"] = sub
                
                st.session_state['grupos_disp'] = grupos
            except Exception as e:
                st.error(f"Error cargando DB: {e}")

    if 'grupos_disp' in st.session_state:
        seleccion = st.selectbox("🎯 Audiencia:", list(st.session_state['grupos_disp'].keys()))
        df_target = st.session_state['grupos_disp'][seleccion]
    else:
        st.warning("Carga los grupos primero.")
        return

    st.divider()

    # 2. CONSTRUCCIÓN DEL MENSAJE (PERSISTENTE)
    st.subheader("📝 Diseña tu Mensaje Modular")
    
    # Cargar valores guardados
    val_body1 = get_ajuste("camp_body1", "Tenemos oferta en lentes...")
    val_body2 = get_ajuste("camp_body2", "")
    val_body3 = get_ajuste("camp_body3", "")
    val_cta1 = get_ajuste("camp_cta1", "Responde SI para ver catálogo")
    val_cta2 = get_ajuste("camp_cta2", "")
    val_cta3 = get_ajuste("camp_cta3", "")
    
    with st.expander("1️⃣ Parte 1: Saludo (Automático)", expanded=True):
        st.markdown("Automático: *Hola / Saludos / Buen día* + **Nombre IA** (si existe).")

    with st.expander("2️⃣ Parte 2: Contenido Principal (3 Alternativas)", expanded=True):
        st.info("Escribe al menos 1 opción. El sistema rotará entre las opciones llenas.")
        c_body1 = st.text_area("Opción A (Principal):", height=100, value=val_body1, key="txt_b1")
        c_body2 = st.text_area("Opción B (Variación):", height=100, value=val_body2, key="txt_b2")
        c_body3 = st.text_area("Opción C (Variación):", height=100, value=val_body3, key="txt_b3")

    with st.expander("3️⃣ Parte 3: Llamada a la Acción (3 Alternativas)", expanded=True):
        st.info("Cómo quieres que respondan.")
        c_cta1 = st.text_input("Cierre A:", value=val_cta1, key="txt_c1")
        c_cta2 = st.text_input("Cierre B:", value=val_cta2, key="txt_c2")
        c_cta3 = st.text_input("Cierre C:", value=val_cta3, key="txt_c3")

    # Botón explícito de guardar (por seguridad mental)
    if st.button("💾 Guardar Borrador (Sin enviar)"):
        set_ajuste("camp_body1", c_body1)
        set_ajuste("camp_body2", c_body2)
        set_ajuste("camp_body3", c_body3)
        set_ajuste("camp_cta1", c_cta1)
        set_ajuste("camp_cta2", c_cta2)
        set_ajuste("camp_cta3", c_cta3)
        st.toast("Borrador guardado en base de datos.")

    st.divider()
    col_img, col_prev = st.columns([1, 1])
    file_img = col_img.file_uploader("Imagen (Opcional)", type=["jpg", "png", "jpeg"])
    
    # SIMULADOR
    if col_prev.button("🎲 Simular Mensaje"):
        saludo = random.choice(["Hola Juan", "Saludos Juan", "Buen día Juan", "Hola"])
        bodies = [b for b in [c_body1, c_body2, c_body3] if b]
        body = random.choice(bodies) if bodies else "[FALTA CUERPO]"
        ctas = [c for c in [c_cta1, c_cta2, c_cta3] if c]
        cta = random.choice(ctas) if ctas else "[FALTA CTA]"
        st.success("--- VISTA PREVIA ---")
        st.markdown(f"**{saludo}**\n\n{body}\n\n**{cta}**")

    # 3. TIEMPOS
    with st.expander("⚙️ Tiempos de Seguridad"):
        c1, c2, c3 = st.columns(3)
        t_min = c1.number_input("Mínimo (s)", 30, 300, 45)
        t_max = c2.number_input("Máximo (s)", 45, 600, 90)
        batch = c3.number_input("Pausa Larga cada N msjs", 5, 50, 10)

    # 4. LANZAR
    if st.button("🚀 INICIAR CAMPAÑA", type="primary"):
        cuerpos_list = [c_body1, c_body2, c_body3]
        ctas_list = [c_cta1, c_cta2, c_cta3]
        
        if not any(cuerpos_list):
            st.error("Debes escribir al menos una opción en la Parte 2.")
            return

        # GUARDAR AUTOMÁTICAMENTE ANTES DE LANZAR
        set_ajuste("camp_body1", c_body1)
        set_ajuste("camp_body2", c_body2)
        set_ajuste("camp_body3", c_body3)
        set_ajuste("camp_cta1", c_cta1)
        set_ajuste("camp_cta2", c_cta2)
        set_ajuste("camp_cta3", c_cta3)

        media = file_img.getvalue() if file_img else None
        mime = file_img.type if file_img else None
        fname = file_img.name if file_img else None
        
        manager.iniciar_hilo(
            df_target, cuerpos_list, ctas_list, media, mime, fname, (t_min, t_max), batch
        )
        st.rerun()