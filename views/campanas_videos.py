"""🎬 Panel: videos automáticos para estados (Campañas → General y → Estados)."""
import datetime
import json
import os
import subprocess
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from sqlalchemy import text

from database import engine

LIMA = ZoneInfo('America/Lima')
RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASOS = {
    'naturales': '1 · Naturales al azar (stock tienda)',
    'grupo': '2 · Mismo modelo, todos sus tonos (tienda y/o proveedor)',
    'mixto': '3 · 50 % naturales + 50 % fantasía (stock tienda)',
}
SESIONES_VIS = {'default': '👁️ Lentes', 'principal': '💇 Principal'}


def _cfg(conn):
    from recurrente.videos_diarios import DEFECTO
    from videos.estados import DEFECTO_ESTADOS
    d = {**DEFECTO, **DEFECTO_ESTADOS}
    v = conn.execute(text("SELECT valor FROM ajustes WHERE clave = 'videos_auto'")).scalar()
    if v:
        try:
            d.update(json.loads(v))
        except Exception:
            pass
    if not d.get('plantillas') and d.get('plantilla'):
        d['plantillas'] = [d['plantilla']]
    return d


def _guardar(cfg):
    cfg.pop('plantilla', None)
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO ajustes (clave, valor) VALUES ('videos_auto', :v)
            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor"""), {'v': json.dumps(cfg, ensure_ascii=False)})


def _asegurar():
    from recurrente.videos_diarios import asegurar_tablas
    from videos.estados import asegurar_tabla
    with engine.begin() as conn:
        asegurar_tablas(conn)
        asegurar_tabla(conn)


def _videos_hoy(conn, fecha):
    vids = pd.read_sql(text("""SELECT id, caso, plantilla, musica, duracion, archivo, skus, grupo, creado
        FROM videos_generados WHERE fecha = :f
        ORDER BY CASE caso WHEN 'naturales' THEN 1 WHEN 'grupo' THEN 2 ELSE 3 END"""), conn, params={'f': fecha})
    est = pd.read_sql(text("""SELECT id, id_video, sesion, programado, publicado, intentos, resultado
        FROM videos_estados WHERE fecha = :f ORDER BY programado"""), conn, params={'f': fecha})
    return vids, est


def _estado_txt(r):
    if pd.notna(r.publicado):
        return f"✅ {pd.Timestamp(r.publicado):%H:%M}"
    if (r.intentos or 0) >= 3:
        return "❌ falló"
    return f"⏳ {pd.Timestamp(r.programado):%H:%M}" + (" (reintento)" if (r.intentos or 0) else "")


# ---------------------------------------------------------------------------- General
def render_resumen_general():
    """Bloque compacto para Campañas → General."""
    try:
        _asegurar()
        hoy = datetime.datetime.now(LIMA).date()
        with engine.connect() as conn:
            vids, est = _videos_hoy(conn, hoy)
    except Exception as e:
        st.caption(f"🎬 Videos automáticos: no se pudo leer ({e})")
        return
    st.markdown("**🎬 Videos automáticos en estados (hoy)**")
    if vids.empty:
        st.caption("Aún no se generaron los videos de hoy (se generan a las 5:35 am).")
        return
    pub = int(est.publicado.notna().sum()) if not est.empty else 0
    c1, c2, c3 = st.columns(3)
    c1.metric("Videos generados", len(vids))
    c2.metric("Estados publicados", f"{pub} / {len(est)}")
    prox = est[est.publicado.isna()].programado.min() if not est.empty else None
    c3.metric("Próximo", f"{pd.Timestamp(prox):%H:%M}" if prox is not None and pd.notna(prox) else "—")
    filas = []
    for v in vids.itertuples():
        e = est[est.id_video == v.id]
        fila = {'Video': CASOS.get(v.caso, v.caso)}
        for ses, nom in SESIONES_VIS.items():
            ee = e[e.sesion == ses]
            fila[nom] = _estado_txt(ee.iloc[0]) if not ee.empty else '—'
        filas.append(fila)
    st.dataframe(pd.DataFrame(filas), hide_index=True, width='stretch')


# ---------------------------------------------------------------------------- Estados
def render_tab_videos():
    from videos.plantillas import PLANTILLAS
    from videos.musica import CANCIONES
    _asegurar()
    hoy = datetime.datetime.now(LIMA).date()
    with engine.connect() as conn:
        cfg = _cfg(conn)
        vids, est = _videos_hoy(conn, hoy)

    st.subheader("🎬 Videos automáticos para estados")
    st.caption("Cada día a las 5:35 am se generan 3 videos verticales (naturales, mismo modelo y mixto). "
               "Se publican al azar: 1 en la mañana y 2 en la tarde, en las cuentas elegidas. "
               "La música se elige al azar entre 10 villancicos de dominio público, sin repetir en el día.")

    # ---------- videos de hoy
    with st.container(border=True):
        st.markdown(f"**📅 Videos de hoy ({hoy:%d/%m})**")
        if vids.empty:
            st.info("Todavía no hay videos de hoy.")
        for v in vids.itertuples():
            e = est[est.id_video == v.id]
            horarios = ' · '.join(f"{SESIONES_VIS.get(r.sesion, r.sesion)} {_estado_txt(r)}" for r in e.itertuples()) or 'sin programar'
            pl = PLANTILLAS.get(v.plantilla)
            tit = (f"{CASOS.get(v.caso, v.caso)} — {float(v.duracion or 0):.0f} s · "
                   f"{getattr(pl, 'NOMBRE', v.plantilla)} · 🎵 {CANCIONES.get(v.musica, {}).get('titulo', v.musica or '—')}")
            with st.expander(f"{tit}  |  {horarios}"):
                c1, c2 = st.columns([1, 1.3])
                with c1:
                    if v.archivo and os.path.exists(v.archivo):
                        st.video(v.archivo)
                    else:
                        st.warning("El archivo ya no existe.")
                with c2:
                    st.write("**Lentes:** " + ', '.join(v.skus or []))
                    if v.grupo:
                        st.write(f"**Grupo:** {v.grupo}")
                    for r in e.itertuples():
                        if r.resultado and pd.isna(r.publicado):
                            st.caption(f"{SESIONES_VIS.get(r.sesion, r.sesion)}: último error → {r.resultado}")
                    pend = e[e.publicado.isna()]
                    if not pend.empty and st.button("📤 Publicar ahora en estados", key=f"pub_{v.id}"):
                        from videos.estados import publicar_pendientes
                        ahora = datetime.datetime.now(LIMA).replace(tzinfo=None)
                        with engine.begin() as conn:
                            conn.execute(text("UPDATE videos_estados SET programado = :t, intentos = 0 "
                                              "WHERE id_video = :v AND publicado IS NULL"), {'t': ahora, 'v': v.id})
                        logs = []
                        publicar_pendientes(engine, log=logs.append)
                        for l in logs:
                            st.write(l)
                    if v.archivo and os.path.exists(v.archivo):
                        with open(v.archivo, 'rb') as f:
                            st.download_button("⬇️ Descargar MP4", f, file_name=os.path.basename(v.archivo),
                                               mime='video/mp4', key=f"dl_{v.id}")
        c1, c2 = st.columns(2)
        if c1.button("🔄 Volver a generar los videos de hoy", help="Elige otros lentes y otra música. Los horarios de estados se mantienen."):
            subprocess.Popen(['nice', '-n', '10', os.path.join(RAIZ, '.venv/bin/python'), '-u',
                              os.path.join(RAIZ, 'recurrente/videos_diarios.py'), '--rehacer'],
                             cwd=RAIZ, stdout=open(os.path.expanduser('~/logs_videos/videos.log'), 'a'),
                             stderr=subprocess.STDOUT, start_new_session=True)
            st.success("Generando… tarda 1–2 minutos. Recarga la página después.")
        if c2.button("🎲 Sortear de nuevo los horarios de hoy", help="Solo los estados que aún no se publicaron."):
            from videos.estados import planificar
            import random
            with engine.begin() as conn:
                planificar(conn, hoy, list(vids.id), cfg=cfg, rnd=random.Random())
            st.rerun()

    # ---------- configuración
    with st.form("form_videos_auto"):
        st.markdown("**⚙️ Configuración**")
        activo_gen = st.toggle("Generar los 3 videos cada día", value=bool(cfg.get('activo', True)))
        activo_est = st.toggle("Publicarlos en estados", value=bool(cfg.get('estados_activo', True)))

        st.markdown("**🎨 Plantillas activas** (cada video usa una al azar entre las vigentes)")
        opciones = list(PLANTILLAS)
        etiquetas = {}
        for k, m in PLANTILLAS.items():
            vig = (not hasattr(m, 'vigente')) or m.vigente(hoy)
            etiquetas[k] = f"{getattr(m, 'NOMBRE', k)}" + ("" if vig else " (fuera de temporada)")
        plantillas = st.multiselect("Plantillas", opciones, default=[p for p in cfg.get('plantillas', []) if p in opciones],
                                    format_func=lambda k: etiquetas[k], label_visibility='collapsed')
        for k, m in PLANTILLAS.items():
            st.caption(f"• **{etiquetas[k]}** — {getattr(m, 'DESCRIPCION', '')}")

        st.markdown("**📱 Cuentas y horarios**")
        sesiones = st.multiselect("Cuentas donde se publican", list(SESIONES_VIS), default=cfg.get('estados_sesiones', []),
                                  format_func=lambda s: SESIONES_VIS[s])
        fotos_lentes = st.checkbox("Mantener también los estados con FOTO en la cuenta Lentes",
                                   value=bool(cfg.get('fotos_lentes', False)),
                                   help="Apagado: Lentes solo publica los videos. Principal (pelucas) sigue con sus estados normales.")

        def t(s):
            h, m = (int(x) for x in s.split(':'))
            return datetime.time(h, m)
        c1, c2, c3, c4 = st.columns(4)
        m_ini = c1.time_input("☀️ Mañana desde", t(cfg['manana'][0]))
        m_fin = c2.time_input("☀️ Mañana hasta", t(cfg['manana'][1]))
        t_ini = c3.time_input("🌇 Tarde desde", t(cfg['tarde'][0]))
        t_fin = c4.time_input("🌇 Tarde hasta", t(cfg['tarde'][1]))
        sep = st.number_input("Separación mínima entre videos (min)", 15, 300, int(cfg.get('separacion_min', 90)), step=15)
        texto = st.text_area("Texto que acompaña al video en el estado", cfg.get('texto_estado', ''), height=70)

        st.markdown("**🖼️ Contenido de los videos**")
        c1, c2, c3, c4, c5 = st.columns(5)
        seg = c1.number_input("Segundos por foto", 1.0, 5.0, float(cfg.get('segundos_foto', 2.0)), step=0.5)
        n_nat = c2.number_input("Fotos video 1", 3, 20, int(cfg.get('fotos_naturales', 10)))
        n_mix = c3.number_input("Fotos video 3", 2, 20, int(cfg.get('fotos_mixto', 10)), step=2)
        g_min = c4.number_input("Video 2: mín. tonos", 2, 10, int(cfg.get('grupo_min', 3)))
        g_max = c5.number_input("Video 2: máx. tonos", 3, 20, int(cfg.get('grupo_max', 12)))
        dias = st.number_input("Días sin repetir lentes", 0, 60, int(cfg.get('dias_sin_repetir', 7)))

        if st.form_submit_button("💾 Guardar videos automáticos", type="primary"):
            if m_ini >= m_fin or t_ini >= t_fin:
                st.error("Revisa los horarios: el «desde» debe ser antes del «hasta».")
            else:
                cfg.update({
                    'activo': activo_gen, 'estados_activo': activo_est, 'plantillas': plantillas,
                    'estados_sesiones': sesiones, 'fotos_lentes': fotos_lentes,
                    'manana': [m_ini.strftime('%H:%M'), m_fin.strftime('%H:%M')],
                    'tarde': [t_ini.strftime('%H:%M'), t_fin.strftime('%H:%M')],
                    'separacion_min': int(sep), 'texto_estado': texto.strip(),
                    'segundos_foto': float(seg), 'fotos_naturales': int(n_nat), 'fotos_mixto': int(n_mix),
                    'grupo_min': int(g_min), 'grupo_max': int(max(g_min, g_max)), 'dias_sin_repetir': int(dias),
                })
                _guardar(cfg)
                st.toast("✅ Videos automáticos guardados (los horarios nuevos aplican desde mañana).")
                st.rerun()
