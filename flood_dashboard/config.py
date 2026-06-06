"""
Configuration centrale du système d'alerte précoce.
Stations, coordonnées GPS, seuils hydrologiques, chemins modèles.
"""
from pathlib import Path

# ── Répertoires ───────────────────────────────────────────────────────────────
ROOT_DIR   = Path(__file__).parent.parent          # donnees_debit_fluviaux/
MODEL_DIR  = ROOT_DIR / "trained_models_global"
CSV_DIR    = ROOT_DIR / "ml_datasets"

# ── Twilio SMS — credentials lus depuis st.secrets ou les variables d'env ─────
# En local  : remplir .streamlit/secrets.toml (fichier gitignore)
# Sur Cloud : Settings > Secrets dans share.streamlit.io

# ── Stations : coordonnées GPS + station_id + seuils ─────────────────────────
# Seuils Q50/Q75/Q90 calculés sur l'historique complet (notebook §8)
# Coordonnées GPS pour l'API Open-Meteo
STATIONS = {
    "AMBIDEDI (Senegal)": {
        "lat": 14.63, "lon": -11.72, "station_id": 0,
        "basin": "Senegal", "color": "#FF9800",
        "q50": 171, "q75": 237, "q90": 658,
        "csv": "ambidedi_senegal_ml.csv",
    },
    "BAKEL (Senegal)": {
        "lat": 14.90, "lon": -12.46, "station_id": 1,
        "basin": "Senegal", "color": "#FF9800",
        "q50": 178, "q75": 274, "q90": 887,
        "csv": "bakel_senegal_ml.csv",
    },
    "BAMBOI (Volta)": {
        "lat": 8.15, "lon": -2.03, "station_id": 2,
        "basin": "Volta", "color": "#4CAF50",
        "q50": 197, "q75": 460, "q90": 998,
        "csv": "bamboi_volta_ml.csv",
    },
    "FARANAH (Niger)": {
        "lat": 10.04, "lon": -10.75, "station_id": 4,
        "basin": "Niger", "color": "#2196F3",
        "q50": 39, "q75": 122, "q90": 196,
        "csv": "faranah_niger_ml.csv",
    },
    "KAYES (Senegal)": {
        "lat": 14.44, "lon": -11.43, "station_id": 5,
        "basin": "Senegal", "color": "#FF9800",
        "q50": 172, "q75": 237, "q90": 653,
        "csv": "kayes_senegal_ml.csv",
    },
    "KOULIKORO (Niger)": {
        "lat": 12.86, "lon": -7.56, "station_id": 7,
        "basin": "Niger", "color": "#2196F3",
        "q50": 491, "q75": 2070, "q90": 4330,
        "csv": "koulikoro_niger_ml.csv",
    },
    "KOUROUSSA (Niger)": {
        "lat": 10.65, "lon": -9.88, "station_id": 9,
        "basin": "Niger", "color": "#2196F3",
        "q50": 77, "q75": 328, "q90": 620,
        "csv": "kouroussa_niger_ml.csv",
    },
    "LAWRA (Volta)": {
        "lat": 10.65, "lon": -2.90, "station_id": 10,
        "basin": "Volta", "color": "#4CAF50",
        "q50": 16, "q75": 60, "q90": 158,
        "csv": "lawra_volta_ml.csv",
    },
    "MATAM (Senegal)": {
        "lat": 15.66, "lon": -13.25, "station_id": 11,
        "basin": "Senegal", "color": "#FF9800",
        "q50": 175, "q75": 273, "q90": 887,
        "csv": "matam_senegal_ml.csv",
    },
    "NAWUNI (Volta)": {
        "lat": 9.50, "lon": -1.10, "station_id": 12,
        "basin": "Volta", "color": "#4CAF50",
        "q50": 74, "q75": 225, "q90": 847,
        "csv": "nawuni_volta_ml.csv",
    },
    "NIAMEY (Niger)": {
        "lat": 13.52, "lon": 2.12, "station_id": 14,
        "basin": "Niger", "color": "#2196F3",
        "q50": 983, "q75": 1448, "q90": 1717,
        "csv": "niamey_niger_ml.csv",
    },
}

# ── Niveaux d'alerte ──────────────────────────────────────────────────────────
ALERT_LEVELS = {
    0: {"label": "Normal",    "color": "#4CAF50", "emoji": "🟢"},
    1: {"label": "Vigilance", "color": "#FFC107", "emoji": "🟡"},
    2: {"label": "Alerte",    "color": "#FF9800", "emoji": "🟠"},
    3: {"label": "Urgence",   "color": "#F44336", "emoji": "🔴"},
}

ALERT_ACTIONS = {
    0: "Aucune action requise",
    1: "Surveillance renforcée",
    2: "Préparer les évacuations",
    3: "Évacuation immédiate requise",
}

# ── 68 features dans l'ordre exact du modèle RF ───────────────────────────────
RF_FEATURES = [
    "Q", "precip_mm", "t2m_mean", "t2m_max", "t2m_min",
    "rh2m_pct", "pression_hpa", "sm_surface", "sm_root",
    "jour_annee", "mois", "semaine", "annee",
    "sin_jour", "cos_jour", "sin_mois", "cos_mois", "saison",
    "Q_lag1", "Q_lag2", "Q_lag3", "Q_lag5", "Q_lag7",
    "Q_lag14", "Q_lag21", "Q_lag30",
    "logQ_lag1", "logQ_lag2", "logQ_lag3", "logQ_lag7",
    "Q_mean7d", "Q_std7d", "Q_max7d",
    "Q_mean14d", "Q_std14d", "Q_max14d",
    "Q_mean30d", "Q_std30d", "Q_max30d",
    "Q_mean60d", "Q_std60d", "Q_max60d",
    "Q_mean90d", "Q_std90d", "Q_max90d",
    "Q_ratio_mean30",
    "precip_lag1", "precip_lag2", "precip_lag3", "precip_lag5",
    "precip_lag7", "precip_lag10", "precip_lag14",
    "precip_sum3d", "precip_sum7d", "precip_sum14d",
    "precip_sum30d", "precip_sum60d",
    "api",
    "t2m_amplitude", "t2m_anomalie",
    "sm_surface_lag1", "sm_surface_lag7", "sm_surface_mean30d",
    "sm_root_lag1", "sm_root_lag7", "sm_root_mean30d",
    "station_id",
]

# ── Open-Meteo : mapping variables ───────────────────────────────────────────
WEATHER_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "surface_pressure",
    "relative_humidity_2m_mean",
    "soil_moisture_0_to_7cm_mean",
    "soil_moisture_7_to_28cm_mean",
]
FLOOD_DAILY_VARS = ["river_discharge"]

# Nombre de jours d'historique nécessaires pour les features rolling 90j
HISTORY_DAYS = 95
