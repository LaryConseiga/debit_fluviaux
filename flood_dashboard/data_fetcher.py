"""
Récupération des données temps réel depuis Open-Meteo.

Stratégie par station :
  - GloFAS disponible (BAKEL, KAYES, KOUROUSSA) :
      météo réelle + débit GloFAS corrigé
  - GloFAS indisponible (8 autres stations) :
      météo réelle Open-Meteo + débit saisonnier estimé à partir du CSV
      → le modèle utilise la vraie météo d'aujourd'hui + la normale
        climatologique du fleuve pour la période de l'année en cours
  - API inaccessible :
      fallback complet sur CSV historique
"""
from datetime import date, timedelta

import numpy as np
import pandas as pd
import requests

from config import CSV_DIR, HISTORY_DAYS, STATIONS

# ── URLs ──────────────────────────────────────────────────────────────────────
_MAIN_URL = "https://api.open-meteo.com/v1/forecast"

# Variables daily disponibles nativement
_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "river_discharge",
]

# Variables hourly → agrégation daily
_HOURLY_VARS = [
    "surface_pressure",
    "relative_humidity_2m",
    "soil_moisture_0_to_7cm",
    "soil_moisture_7_to_28cm",
]

_DAILY_MAP = {
    "temperature_2m_max":  "t2m_max",
    "temperature_2m_min":  "t2m_min",
    "temperature_2m_mean": "t2m_mean",
    "precipitation_sum":   "precip_mm",
    "river_discharge":     "Q_api",
}
_HOURLY_MAP = {
    "surface_pressure":        "pression_hpa",
    "relative_humidity_2m":    "rh2m_pct",
    "soil_moisture_0_to_7cm":  "sm_surface",
    "soil_moisture_7_to_28cm": "sm_root",
}


# ── Cache biais historiques ────────────────────────────────────────────────────

_HIST_MEANS: dict = {}

def _get_hist_means() -> dict:
    global _HIST_MEANS
    if _HIST_MEANS:
        return _HIST_MEANS
    for name, cfg in STATIONS.items():
        csv_path = CSV_DIR / cfg["csv"]
        if not csv_path.exists():
            _HIST_MEANS[name] = {"Q": 1.0, "precip": 1.0}
            continue
        df = pd.read_csv(csv_path)
        _HIST_MEANS[name] = {
            "Q":      max(df["Q"].clip(lower=0).mean(), 1e-6),
            "precip": max(df["precip_mm"].clip(lower=0).mean(), 1e-6)
                      if "precip_mm" in df else 1.0,
        }
    return _HIST_MEANS


# ── Débit saisonnier ────────────────────────────────────────────────────────────

_SEASONAL_Q_CACHE: dict = {}

def _build_seasonal_q(station_name: str) -> dict:
    """
    Construit un dictionnaire {jour_annee: Q_median} à partir du CSV complet.
    Fenêtre glissante de ±10 jours pour lisser les valeurs.
    Mis en cache après le premier calcul.
    """
    if station_name in _SEASONAL_Q_CACHE:
        return _SEASONAL_Q_CACHE[station_name]

    csv_path = CSV_DIR / STATIONS[station_name]["csv"]
    if not csv_path.exists():
        _SEASONAL_Q_CACHE[station_name] = {}
        return {}

    df = pd.read_csv(csv_path, usecols=["date", "Q"], parse_dates=["date"])
    df = df.dropna(subset=["Q"])
    df["Q"] = pd.to_numeric(df["Q"], errors="coerce").clip(lower=0)
    df["doy"] = df["date"].dt.day_of_year

    doy_q: dict = {}
    for doy in range(1, 367):
        # Fenêtre ±10 jours (cyclique sur l'année)
        nearby = [(doy - 11 + k) % 365 + 1 for k in range(22)]
        vals = df[df["doy"].isin(nearby)]["Q"].dropna().values
        doy_q[doy] = float(np.median(vals)) if len(vals) > 0 else 0.0

    _SEASONAL_Q_CACHE[station_name] = doy_q
    return doy_q


def _seasonal_q_for_dates(station_name: str,
                           dates: pd.Series) -> pd.Series:
    """Retourne la série de Q saisonnier pour une liste de dates."""
    doy_q = _build_seasonal_q(station_name)
    if not doy_q:
        return pd.Series(0.0, index=dates.index)
    return dates.apply(
        lambda d: doy_q.get(pd.Timestamp(d).day_of_year, 0.0)
    )


# ── Appel API Open-Meteo ───────────────────────────────────────────────────────

def _fetch_all_daily(lat: float, lon: float,
                     past_days: int = HISTORY_DAYS) -> pd.DataFrame:
    """Récupère météo daily + hourly agrégée + GloFAS depuis Open-Meteo."""
    params = {
        "latitude":      lat,
        "longitude":     lon,
        "daily":         ",".join(_DAILY_VARS),
        "hourly":        ",".join(_HOURLY_VARS),
        "past_days":     min(past_days, 92),
        "forecast_days": 1,
        "timezone":      "Africa/Abidjan",
    }
    r = requests.get(_MAIN_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame({"date": pd.to_datetime(data["daily"]["time"])})
    for api_var, col in _DAILY_MAP.items():
        df[col] = data["daily"].get(api_var)

    df_h = pd.DataFrame({"datetime": pd.to_datetime(data["hourly"]["time"])})
    for api_var, col in _HOURLY_MAP.items():
        df_h[col] = data["hourly"].get(api_var)
    df_h["date"] = df_h["datetime"].dt.normalize()
    df_agg = (
        df_h.groupby("date")[list(_HOURLY_MAP.values())]
        .mean()
        .reset_index()
    )

    return df.merge(df_agg, on="date", how="left")


# ── Fallback CSV complet ───────────────────────────────────────────────────────

def _load_from_csv(station_name: str,
                   past_days: int = HISTORY_DAYS) -> pd.DataFrame:
    cfg      = STATIONS[station_name]
    csv_path = CSV_DIR / cfg["csv"]
    if not csv_path.exists():
        return pd.DataFrame()

    cols = ["date", "Q", "precip_mm", "t2m_mean", "t2m_max", "t2m_min",
            "rh2m_pct", "pression_hpa", "sm_surface", "sm_root"]
    df = pd.read_csv(csv_path, parse_dates=["date"],
                     usecols=[c for c in cols
                               if c in pd.read_csv(csv_path, nrows=0).columns])
    df = df.sort_values("date").tail(past_days).reset_index(drop=True)
    df["date"]   = pd.to_datetime(df["date"])
    df["source"] = "csv_fallback"
    return df


# ── Fonction principale ────────────────────────────────────────────────────────

def fetch_station_data(station_name: str,
                       past_days: int = HISTORY_DAYS) -> pd.DataFrame:
    """
    Retourne un DataFrame avec météo réelle + Q (GloFAS ou saisonnier).

    Colonnes : date, Q, precip_mm, t2m_mean, t2m_max, t2m_min,
               rh2m_pct, pression_hpa, sm_surface, sm_root, source
    """
    cfg = STATIONS[station_name]
    lat, lon = cfg["lat"], cfg["lon"]
    hm = _get_hist_means()

    try:
        df = _fetch_all_daily(lat, lon, past_days=past_days)

        # ── Correction biais précipitations ───────────────────────────────────
        prec_hist = hm.get(station_name, {}).get("precip", None)
        prec_mean = df["precip_mm"].dropna().mean()
        if prec_hist and prec_mean and prec_mean > 0:
            ratio = prec_hist / prec_mean
            if 0.5 <= ratio <= 5.0:
                df["precip_mm"] = df["precip_mm"] * ratio

        # ── Débit Q ───────────────────────────────────────────────────────────
        q_api_valid = df["Q_api"].notna().any()

        if q_api_valid:
            # GloFAS disponible → correction de biais
            q_hist    = hm.get(station_name, {}).get("Q", None)
            q_api_mean = df["Q_api"].dropna().mean()
            if q_hist and q_hist > 0 and q_api_mean > 0:
                df["Q"] = df["Q_api"] * (q_hist / q_api_mean)
            else:
                df["Q"] = df["Q_api"]
            df["source"] = "temps_reel_glofas"
            print(f"[INFO] {station_name} — GloFAS OK, météo réelle")

        else:
            # GloFAS indisponible → débit saisonnier + météo réelle
            df["Q"]      = _seasonal_q_for_dates(station_name, df["date"])
            df["source"] = "temps_reel_meteo+Q_saisonnier"
            print(f"[INFO] {station_name} — météo réelle + Q saisonnier "
                  f"(GloFAS indisponible)")

        df = df.drop(columns=["Q_api"], errors="ignore")

    except Exception as exc:
        print(f"[WARN] {station_name} API inaccessible ({exc}) — fallback CSV")
        df = _load_from_csv(station_name, past_days=past_days)
        if df.empty:
            raise RuntimeError(
                f"API inaccessible et CSV introuvable pour {station_name}"
            ) from exc

    # ── Nettoyage ─────────────────────────────────────────────────────────────
    df["Q"]         = pd.to_numeric(df["Q"], errors="coerce").fillna(0).clip(lower=0)
    df["precip_mm"] = pd.to_numeric(df["precip_mm"], errors="coerce").fillna(0).clip(lower=0)
    df = df.sort_values("date").reset_index(drop=True)
    return df


def fetch_all_stations(past_days: int = HISTORY_DAYS) -> dict:
    result = {}
    for name in STATIONS:
        try:
            result[name] = fetch_station_data(name, past_days=past_days)
        except Exception as exc:
            print(f"[ERROR] {name} : {exc}")
            result[name] = pd.DataFrame()
    return result
