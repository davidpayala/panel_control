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
# 🧠 CEREBRO DE LA CAMPAÑA (BACKGROUND RUNNER)
# ==============================================================================
class CampaignManager:
    def __init__(self):
        self.running = False
        self.paused = False
        self.stop_signal = False
        
        # Estado de la campaña actual
        self.df_pendientes = pd.DataFrame()
        self.mensaje_base = ""
        self.media_bytes = None
        self.mime_type = None
        self.filename = None
        self.config_delay = (30, 60)
        self.batch_size = 10
        
        # Métricas en tiempo real
        self.progreso = 0
        self.total = 0
        self.exitos = 0
        self.errores = 0
        self.logs = []
        self.current_client_name = "Esperando..."

    def iniciar_hilo(self, df, msg, media, mime, fname, delay_range, batch):
        if self.running: return # Ya hay una corriendo
        
        self.df_pendientes = df
        self.mensaje_base = msg
        self.media_bytes = media
        self.mime_type = mime
        self.filename = fname
        self.config_delay = delay_range
        self.batch_size = batch
        
        self.total = len(df)
        self.progreso = 0
        self.exitos = 0
        self.errores = 0
        self.logs = ["🚀 Campaña iniciada en segundo plano..."]
        
        self.running = True
        self.paused = False
        self.stop_signal = False
        
        # Lanzar el hilo independiente
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

    def _proceso_envio(self):
        """Lógica que se ejecuta en el servidor (No en el navegador)"""
        count_batch = 0
        
        # Iteramos sobre una copia para poder modificar el original si queremos
        # Usamos to_dict para iterar seguros
        lista_clientes = self.df_pendientes.to_dict('records')

        for i, row in enumerate(lista_clientes):
            # 1. Chequeos de control
            if self.stop_signal: break
            
            while self.paused:
                time.sleep(1) # Dormir mientras está en pausa
                if self.stop_signal: break

            self.current_client_name = row.get('nombre_corto', 'Cliente')
            self.progreso = i + 1
            
            # --- PAUSA DE LOTE (COOL-DOWN) ---
            if count_batch >= self.batch_size:
                ts_lote = random.randint(180, 300) # 3 a 5 min
                self.logs.append(f"🛑 PAUSA SEGURIDAD: Enfriando {ts_lote//60} min...")
                # Dormir en pasos pequeños para poder detener
                for _ in range(ts_lote):
                    if self.stop_signal: break
                    time.sleep(1)
                count_batch = 0

            # --- LÓGICA DE ENVÍO ---
            nombre = row.get('nombre_corto', 'Cliente')
            tel_bruto = row.get('telefono', '')
            
            try:
                # A. Normalizar
                norm = normalizar_telefono_maestro(tel_bruto)
                if not norm:
                    self.logs.append(f"❌ {nombre}: Teléfono inválido {tel_bruto}")
                    self.errores += 1
                    continue
                
                tel_final = norm['db']
                id_cliente = row.get('id_cliente')

                # B. VERIFICAR EXISTENCIA EN WAHA (NUEVO REQUERIMIENTO)
                existe = verificar_numero_waha(tel_final)
                
                if existe is False: # Solo si devuelve False explícito
                    self.logs.append(f"🗑️ {nombre}: NO TIENE WHATSAPP. Eliminando número...")
                    self.errores += 1
                    # Borrar número de la DB para siempre
                    with engine.connect() as conn:
                        conn.execute(text("UPDATE Clientes SET telefono = NULL, activo = FALSE, notas = 'Número no existe en WA' WHERE id_cliente = :id"), {"id": id_cliente})
                        conn.commit()
                    continue # Saltamos al siguiente

                # C. Verificar si ya se envió hoy (Para evitar dobles al reanudar)
                # (Opcional, pero recomendado)
                
                # D. Preparar Spintax
                msg_final = procesar_spintax(self.mensaje_base)

                # E. Enviar
                res = False
                if self.media_bytes:
                    res, _ = enviar_mensaje_media(tel_final, self.media_bytes, self.mime_type, msg_final, self.filename)
                elif msg_final:
                    res, _ = enviar_mensaje_whatsapp(tel_final, msg_final)

                if res:
                    self.exitos += 1
                    self.logs.append(f"✅ {nombre}: Enviado")
                    count_batch += 1
                    # Guardar en DB
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

            # Espera entre mensajes (Random)
            if i < self.total - 1:
                ts = random.randint(self.config_delay[0], self.config_delay[1])
                time.sleep(ts)

        self.running = False
        self.logs.append("🎉 Campaña Finalizada.")


# Singleton: Se mantiene vivo en memoria del servidor
@st.cache_resource
def get_manager():
    return CampaignManager()

# ==============================================================================
# FUNCIONES DE UI
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
    st.title("📢 Campañas Masivas 2.0 (Background)")
    manager = get_manager()

    # --- ZONA DE CONTROL (SI ESTÁ CORRIENDO) ---
    if manager.running or manager.paused:
        st.success("🚀 **CAMPAÑA EN CURSO** (Puedes cerrar esta pestaña)")
        
        # Métricas en vivo
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Progreso", f"{manager.progreso}/{manager.total}")
        c2.metric("Éxitos", manager.exitos)
        c3.metric("Errores/Eliminados", manager.errores)
        c4.metric("Estado", "⏸️ PAUSADO" if manager.paused else "▶️ CORRIENDO")
        
        st.progress(manager.progreso / manager.total if manager.total > 0 else 0)
        st.caption(f"Procesando actualmente: **{manager.current_client_name}**")

        # Botones de Control
        col_btns = st.columns(3)
        if manager.paused:
            if col_btns[0].button("▶️ REANUDAR ENVÍO"):
                manager.reanudar()
                st.rerun()
        else:
            if col_btns[0].button("⏸️ PAUSAR"):
                manager.pausar()
                st.rerun()
        
        if col_btns[2].button("🛑 DETENER DEFINITIVAMENTE", type="primary"):
            manager.detener()
            st.rerun()

        # Logs en tiempo real
        with st.expander("📜 Ver Logs en Vivo", expanded=True):
            st.code("\n".join(manager.logs[-10:])) # Ver últimos 10
            if st.button("Actualizar Vista"):
                st.rerun()
        
        return # Si está corriendo, no mostramos el formulario de configuración

    # --- ZONA DE CONFIGURACIÓN (SI NO HAY CAMPAÑA) ---
    st.info("Configura tu nueva campaña. El sistema validará si los números existen antes de enviar.")

    # 1. CARGA DE AUDIENCIA
    if st.button("🔄 Cargar Grupos"):
        with engine.connect() as conn:
            try:
                # Obtenemos id_cliente también para poder borrarlo si falla
                query = "SELECT id_cliente, nombre_corto, telefono, estado, COALESCE(etiquetas, '') as etiquetas FROM Clientes WHERE activo = TRUE AND telefono IS NOT NULL AND length(telefono) > 6"
                df_raw = pd.read_sql(text(query), conn)
                
                # Clasificación
                def clasificar(row):
                    tags = str(row['etiquetas']).upper()
                    if "SPAM" in tags: return "🚫 SPAM (Pruebas)"
                    if "VIP" in tags: return "💎 VIP (Prioridad)"
                    if "COMPRÓ" in tags: return "✅ Compradores"
                    if "PROVEEDOR" in tags: return "📦 Proveedor"
                    return "GENERAL"
                
                df_raw['segmento'] = df_raw.apply(clasificar, axis=1)
                
                grupos = {}
                for seg in ["🚫 SPAM (Pruebas)", "💎 VIP (Prioridad)", "✅ Compradores","📦 Proveedor"]:
                    sub = df_raw[df_raw['segmento'] == seg]
                    if not sub.empty: grupos[f"{seg} ({len(sub)})"] = sub
                
                df_gen = df_raw[df_raw['segmento'] == "GENERAL"]
                tamano = 100 # Grupos seguros
                for i in range(0, len(df_gen), tamano):
                    sub = df_gen.iloc[i:i+tamano]
                    grupos[f"📦 Grupo {(i//tamano)+1} (General) - {len(sub)} pax"] = sub
                
                st.session_state['grupos_disp'] = grupos
            except Exception as e:
                st.error(f"Error cargando DB: {e}")

    if 'grupos_disp' in st.session_state:
        seleccion = st.selectbox("🎯 Audiencia:", list(st.session_state['grupos_disp'].keys()))
        df_target = st.session_state['grupos_disp'][seleccion]
        st.dataframe(df_target.head(3), hide_index=True)
    else:
        st.warning("Carga los grupos primero.")
        return

    # 2. MENSAJE
    col_msg, col_img = st.columns([2, 1])
    txt_msg = col_msg.text_area("Mensaje (Spintax permitido):", placeholder="{Hola|Buenas}, oferta para ti...")
    file_img = col_img.file_uploader("Imagen", type=["jpg", "png", "jpeg"])

    # 3. TIEMPOS
    with st.expander("⚙️ Tiempos de Seguridad"):
        c1, c2, c3 = st.columns(3)
        t_min = c1.number_input("Mínimo (s)", 30, 300, 45)
        t_max = c2.number_input("Máximo (s)", 45, 600, 90)
        batch = c3.number_input("Pausa Larga cada N mensajes", 5, 50, 10)

    # 4. LANZAR
    if st.button("🚀 INICIAR CAMPAÑA EN SEGUNDO PLANO", type="primary"):
        media = file_img.getvalue() if file_img else None
        mime = file_img.type if file_img else None
        fname = file_img.name if file_img else None
        
        manager.iniciar_hilo(
            df_target, txt_msg, media, mime, fname, (t_min, t_max), batch
        )
        st.rerun()