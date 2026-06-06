"""
Dashboard d'alerte précoce aux crues — Afrique de l'Ouest
"""
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import streamlit as st

from config import ALERT_ACTIONS, ALERT_LEVELS, STATIONS
from database import (
    get_last_prediction, get_mesures, get_predictions_history,
    get_sms_log, get_previous_niveau, init_schema,
)

st.set_page_config(
    page_title="Alerte Crues — Afrique de l'Ouest",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

init_schema()


# ── Mise à jour automatique ───────────────────────────────────────────────────

def _run_update_silent(run_date: str):
    """Exécute la mise à jour pour toutes les stations sans interaction UI."""
    from data_fetcher import fetch_station_data
    from feature_builder import build_features
    from predictor import predict
    from database import upsert_mesure, upsert_prediction

    for name in STATIONS:
        try:
            df = fetch_station_data(name)
            for _, row in df.iterrows():
                rec = {c: (None if pd.isna(v := row.get(c)) else v)
                       for c in ["Q", "precip_mm", "t2m_mean", "t2m_max", "t2m_min",
                                 "rh2m_pct", "pression_hpa", "sm_surface", "sm_root"]}
                upsert_mesure(name, row["date"].strftime("%Y-%m-%d"), rec)
            feat = build_features(df, name)
            pred = predict(feat, name)
            upsert_prediction(name, run_date,
                              pred["q_j1"], pred["q_j3"],
                              pred["niveau_j1"], pred["niveau_j3"])
            q_actuel = float(df["Q"].dropna().iloc[-1]) if not df["Q"].dropna().empty else 0.0
            _send_sms_if_configured(name, run_date, q_actuel, pred)
        except Exception as exc:
            print(f"[AUTO-UPDATE] {name}: {exc}")


def _maybe_auto_update():
    """Déclenche une mise à jour si les données ont plus de 24h ou sont absentes."""
    today = date.today().isoformat()
    first_station = list(STATIONS.keys())[0]
    last = get_last_prediction(first_station)
    if last and last["run_date"] >= today:
        return  # données fraîches

    with st.spinner("🔄 Mise à jour automatique des données en cours…"):
        _run_update_silent(today)
    st.toast("✅ Données mises à jour automatiquement", icon="✅")


_maybe_auto_update()


# ── Thème ─────────────────────────────────────────────────────────────────────
T = {
    "bg":       "#0E1117",
    "card":     "#1E2130",
    "border":   "#2D3250",
    "text":     "#FAFAFA",
    "subtext":  "#8899AA",
    "accent":   "#2196F3",
}


def inject_css():
    st.markdown(f"""<style>
    .stApp {{
        background-color: {T['bg']} !important;
        color: {T['text']} !important;
    }}
    section[data-testid="stSidebar"] > div {{
        background-color: {T['card']} !important;
        border-right: 1px solid {T['border']};
    }}
    section[data-testid="stSidebar"] * {{
        color: {T['text']} !important;
    }}
    .stRadio label, .stSelectbox label, .stTextInput label {{
        color: {T['text']} !important;
    }}
    .stMarkdown, .stText, p, h1, h2, h3, h4, li {{
        color: {T['text']} !important;
    }}
    div[data-testid="stMetricValue"] {{
        color: {T['text']} !important;
    }}
    div[data-testid="stMetricLabel"] {{
        color: {T['subtext']} !important;
    }}
    .stDataFrame {{
        background-color: {T['card']};
        border-radius: 10px;
        border: 1px solid {T['border']};
    }}
    .stExpander {{
        background-color: {T['card']};
        border: 1px solid {T['border']};
        border-radius: 8px;
    }}
    .stAlert {{
        background-color: {T['card']};
        border-color: {T['border']};
    }}
    div[data-testid="column"] > div {{
        background-color: {T['card']};
        border-radius: 10px;
        border: 1px solid {T['border']};
        padding: 14px 18px;
        margin-bottom: 8px;
    }}
    .kpi-card {{
        background: {T['card']};
        border: 1px solid {T['border']};
        border-radius: 10px;
        padding: 16px 18px;
        text-align: center;
        margin-bottom: 8px;
    }}
    .kpi-value {{
        font-size: 1.9rem;
        font-weight: 700;
        margin: 4px 0;
        color: {T['text']};
    }}
    .kpi-label {{
        font-size: 0.78rem;
        color: {T['subtext']};
        text-transform: uppercase;
        letter-spacing: .05em;
    }}
    .section-title {{
        font-size: 1rem;
        font-weight: 700;
        color: {T['text']};
        margin: 18px 0 10px 0;
        padding-left: 10px;
        border-left: 4px solid {T['accent']};
    }}
    #MainMenu, footer {{ visibility: hidden; }}
    </style>""", unsafe_allow_html=True)


inject_css()


def kpi(label, value, color=None):
    c = color or T["accent"]
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value" style="color:{c}">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>""", unsafe_allow_html=True)


def section(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"### Alerte Précoce Crues")
    st.caption("Sénégal · Niger · Volta")
    st.divider()

    page = st.radio(
        "Page",
        ["Vue globale", "Détail station", "Historique SMS"],
        label_visibility="collapsed",
    )

    st.divider()
    st.caption(f"Mise à jour : {datetime.now().strftime('%d/%m/%Y %H:%M')}")


# ═════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Vue globale
# ═════════════════════════════════════════════════════════════════════════════

def page_vue_globale():
    st.title("Vue globale — Niveaux d'alerte")

    rows = []
    for name, cfg in STATIONS.items():
        pred = get_last_prediction(name)
        if pred:
            n1, n3 = pred["niveau_j1"], pred["niveau_j3"]
            rows.append({
                "Station":       name,
                "Bassin":        cfg["basin"],
                "_n1":           n1,
                "Alerte J+1":    f"{ALERT_LEVELS[n1]['emoji']} {ALERT_LEVELS[n1]['label']}",
                "Débit J+1":     f"{pred['Q_predit_j1']:,.0f} m³/s",
                "Alerte J+3":    f"{ALERT_LEVELS[n3]['emoji']} {ALERT_LEVELS[n3]['label']}",
                "Débit J+3":     f"{pred['Q_predit_j3']:,.0f} m³/s",
                "Mise à jour":   str(pred["run_date"]),
                "lat": cfg["lat"], "lon": cfg["lon"],
            })
        else:
            rows.append({
                "Station":    name, "Bassin": cfg["basin"], "_n1": -1,
                "Alerte J+1": "—", "Débit J+1": "—",
                "Alerte J+3": "—", "Débit J+3": "—",
                "Mise à jour": "Aucune",
                "lat": cfg["lat"], "lon": cfg["lon"],
            })

    n_urg = sum(1 for r in rows if r["_n1"] == 3)
    n_ale = sum(1 for r in rows if r["_n1"] == 2)
    n_vig = sum(1 for r in rows if r["_n1"] == 1)
    n_nor = sum(1 for r in rows if r["_n1"] == 0)

    c1, c2, c3, c4 = st.columns(4)
    with c1: kpi("Urgence",   str(n_urg), "#F44336")
    with c2: kpi("Alerte",    str(n_ale), "#FF9800")
    with c3: kpi("Vigilance", str(n_vig), "#FFC107")
    with c4: kpi("Normal",    str(n_nor), "#4CAF50")

    st.divider()
    section("Localisation des stations")
    st.map(pd.DataFrame(rows)[["lat", "lon"]], zoom=4)

    section("Récapitulatif des prédictions")
    display = pd.DataFrame(rows)[[
        "Station", "Bassin",
        "Alerte J+1", "Débit J+1",
        "Alerte J+3", "Débit J+3",
        "Mise à jour",
    ]]
    st.dataframe(display, use_container_width=True, hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Détail station
# ═════════════════════════════════════════════════════════════════════════════

def page_detail():
    st.title("Détail station")
    station = st.selectbox("Station", list(STATIONS.keys()))
    cfg     = STATIONS[station]
    pred    = get_last_prediction(station)

    if pred:
        n1, n3   = pred["niveau_j1"], pred["niveau_j3"]
        al1, al3 = ALERT_LEVELS[n1], ALERT_LEVELS[n3]

        st.markdown(f"""
        <div style="background:{al1['color']}22;border-left:5px solid {al1['color']};
             border-radius:8px;padding:14px 18px;margin-bottom:16px">
            <strong style="color:{al1['color']};font-size:1.1rem">
                {al1['emoji']} {al1['label'].upper()} — Prévision J+1
            </strong><br>
            <span>{ALERT_ACTIONS[n1]}</span>
        </div>""", unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1: kpi("Débit prédit J+1",  f"{pred['Q_predit_j1']:,.0f} m³/s", al1["color"])
        with c2: kpi("Débit prédit J+3",  f"{pred['Q_predit_j3']:,.0f} m³/s", al3["color"])
        with c3: kpi("Dernière mise à jour", str(pred["run_date"]))
    else:
        st.info("Aucune prédiction disponible. Lancez une mise à jour.")

    st.divider()
    section("Seuils hydrologiques")
    sc1, sc2, sc3 = st.columns(3)
    with sc1: kpi("Q50 — Vigilance", f"{cfg['q50']:,} m³/s", "#FFC107")
    with sc2: kpi("Q75 — Alerte",    f"{cfg['q75']:,} m³/s", "#FF9800")
    with sc3: kpi("Q90 — Urgence",   f"{cfg['q90']:,} m³/s", "#F44336")

    st.divider()
    section("Historique du débit")
    mesures = get_mesures(station, n_days=90)
    if mesures:
        import plotly.graph_objects as go
        df_m = pd.DataFrame(mesures)
        df_m["date"] = pd.to_datetime(df_m["date"])

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_m["date"], y=df_m["Q"],
            mode="lines", name="Q observé",
            line=dict(color="#2196F3", width=2),
            fill="tozeroy", fillcolor="rgba(33,150,243,0.10)",
        ))
        for lbl, qval, col in [
            ("Q50", cfg["q50"], "#FFC107"),
            ("Q75", cfg["q75"], "#FF9800"),
            ("Q90", cfg["q90"], "#F44336"),
        ]:
            fig.add_hline(y=qval, line_dash="dash", line_color=col,
                          annotation_text=lbl, annotation_position="top right",
                          annotation_font_color=col)

        preds_hist = get_predictions_history(station, n=30)
        if preds_hist:
            df_p = pd.DataFrame(preds_hist)
            df_p["run_date"] = pd.to_datetime(df_p["run_date"])
            fig.add_trace(go.Scatter(
                x=df_p["run_date"], y=df_p["Q_predit_j1"],
                mode="markers+lines", name="Prédit J+1",
                line=dict(color="#9C27B0", dash="dot", width=1.5), marker=dict(size=5),
            ))
            fig.add_trace(go.Scatter(
                x=df_p["run_date"], y=df_p["Q_predit_j3"],
                mode="markers+lines", name="Prédit J+3",
                line=dict(color="#E91E63", dash="dot", width=1.5), marker=dict(size=5),
            ))

        bg = T["card"]  # toujours mode sombre
        fig.update_layout(
            paper_bgcolor=bg, plot_bgcolor=bg, font_color=T["text"],
            xaxis=dict(gridcolor=T["border"]),
            yaxis=dict(gridcolor=T["border"], title="Débit (m³/s)"),
            legend=dict(orientation="h", y=1.1, bgcolor="rgba(0,0,0,0)"),
            height=380, margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Données météo récentes"):
            cols_ok = [c for c in ["Q","precip_mm","t2m_mean","rh2m_pct","sm_surface"]
                       if c in df_m.columns]
            st.dataframe(df_m.set_index("date")[cols_ok].tail(14).round(2),
                         use_container_width=True)
    else:
        st.info("Pas encore de mesures.")



def _get_twilio_cfg() -> Optional[dict]:
    """Lit les credentials Twilio depuis st.secrets ou les variables d'environnement."""
    import os
    try:
        sid = st.secrets.get("TWILIO_ACCOUNT_SID", "")
    except Exception:
        sid = ""
    if not sid:
        sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    if not sid or "xxx" in sid:
        return None
    try:
        return {
            "account_sid": sid,
            "auth_token":  st.secrets.get("TWILIO_AUTH_TOKEN")  or os.environ.get("TWILIO_AUTH_TOKEN", ""),
            "from_number": st.secrets.get("TWILIO_FROM")        or os.environ.get("TWILIO_FROM", ""),
            "to_number":   st.secrets.get("TWILIO_TO")          or os.environ.get("TWILIO_TO", ""),
        }
    except Exception:
        return None


def _send_sms_if_configured(station_name, run_date, q_actuel, pred, forcer=False):
    cfg = _get_twilio_cfg()
    if cfg is None:
        return None

    from sms_service import send_alert
    from database import get_previous_niveau, log_sms, sms_sent_today

    # Pas de message si niveau insuffisant
    if pred["niveau_j1"] < 2:
        return {"sent": False, "reason": "niveau < Alerte", "sid": None, "message": ""}

    # Un seul message par station par jour — sauf si l'utilisateur force l'envoi
    if not forcer and sms_sent_today(station_name, run_date):
        return {"sent": False, "reason": "déjà envoyé aujourd'hui", "sid": None, "message": ""}

    prev   = get_previous_niveau(station_name, run_date)
    result = send_alert(
        station_name, run_date, q_actuel,
        pred["q_j1"], pred["q_j3"], pred["niveau_j1"], prev,
        cfg["account_sid"], cfg["auth_token"],
        cfg["from_number"], cfg["to_number"],
    )
    if result["sent"]:
        log_sms(station_name, run_date, pred["niveau_j1"],
                result["message"], result["sid"])
    return result


# ═════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Historique SMS
# ═════════════════════════════════════════════════════════════════════════════

def page_sms():
    st.title("Historique des alertes SMS")
    logs = get_sms_log(n=100)
    if not logs:
        st.info("Aucune alerte envoyée.")
        return

    df = pd.DataFrame(logs)
    df["Niveau"] = df["niveau"].apply(
        lambda n: f"{ALERT_LEVELS[n]['emoji']} {ALERT_LEVELS[n]['label']}"
    )

    c1, c2, c3 = st.columns(3)
    with c1: kpi("Total alertes",    str(len(df)),                     T["accent"])
    with c2: kpi("Stations touchées", str(df["station"].nunique()),     "#FF9800")
    with c3: kpi("Urgences",          str((df["niveau"] == 3).sum()),   "#F44336")

    st.divider()
    display = df[["ts","station","Niveau","statut","sid"]].copy()
    display.columns = ["Horodatage","Station","Niveau","Statut","SID Twilio"]
    st.dataframe(display, use_container_width=True, hide_index=True)

    with st.expander("Dernier message envoyé"):
        st.code(logs[0].get("message","—"), language=None)




# ── Routage ───────────────────────────────────────────────────────────────────
if page == "Vue globale":
    page_vue_globale()
elif page == "Détail station":
    page_detail()
elif page == "Historique SMS":
    page_sms()
