"""
Récupération des données temps réel depuis Open-Meteo.

Stratégie par station :
  - GloFAS disponible (BAKEL, KAYES, KOUROUSSA) :
      météo réelle + débit GloFAS corrigé
  - GloFAS indisponible (8 autres stations) :
      météo réelle Open-Meteo + débit saisonnier (seasonal_q.csv)
  - API inaccessible :
      fallback complet sur zéros (données météo indisponibles)
"""
from datetime import date, timedelta
from functools import lru_cache

import numpy as np
import pandas as pd
import requests

from config import CSV_DIR, HISTORY_DAYS, STATIONS

# ── URLs ──────────────────────────────────────────────────────────────────────
_MAIN_URL = "https://api.open-meteo.com/v1/forecast"

_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "river_discharge",
]
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


# ── Chargement des fichiers résumé (singletons) ───────────────────────────────

@lru_cache(maxsize=1)
def _load_bias_means() -> pd.DataFrame:
    """Moyennes historiques Q et précip par station (bias_means.csv)."""
    path = CSV_DIR / "bias_means.csv"
    if not path.exists():
        return pd.DataFrame(columns=["station", "q_mean", "precip_mean"])
    return pd.read_csv(path).set_index("station")


@lru_cache(maxsize=1)
def _load_seasonal_q() -> pd.DataFrame:
    """Médianes saisonnières Q par station et jour de l'année (seasonal_q.csv)."""
    path = CSV_DIR / "seasonal_q.csv"
    if not path.exists():
        return pd.DataFrame(columns=["station", "doy", "q_median"])
    return pd.read_csv(path)


def _seasonal_q_for_dates(station_name: str, dates: pd.Series) -> pd.Series:
    """Retourne la série de Q saisonnier pour une liste de dates."""
    df_sq = _load_seasonal_q()
    if df_sq.empty:
        return pd.Series(0.0, index=dates.index)
    sq = df_sq[df_sq["station"] == station_name].set_index("doy")["q_median"]
    return dates.apply(
        lambda d: sq.get(pd.Timestamp(d).day_of_year, 0.0)
    )


def _get_bias(station_name: str) -> dict:
    """Retourne les moyennes historiques Q et précip pour une station."""
    df_bm = _load_bias_means()
    if station_name not in df_bm.index:
        return {"Q": 1.0, "precip": 1.0}
    row = df_bm.loc[station_name]
    return {
        "Q":      float(row.get("q_mean", 1.0)),
        "precip": float(row.get("precip_mean", 1.0)),
    }


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
    bias = _get_bias(station_name)

    try:
        df = _fetch_all_daily(lat, lon, past_days=past_days)

        # ── Correction biais précipitations ───────────────────────────────────
        prec_mean = df["precip_mm"].dropna().mean()
        if bias["precip"] and prec_mean and prec_mean > 0:
            ratio = bias["precip"] / prec_mean
            if 0.5 <= ratio <= 5.0:
                df["precip_mm"] = df["precip_mm"] * ratio

        # ── Débit Q ───────────────────────────────────────────────────────────
        q_api_valid = df["Q_api"].notna().any()

        if q_api_valid:
            q_api_mean = df["Q_api"].dropna().mean()
            if bias["Q"] > 0 and q_api_mean > 0:
                df["Q"] = df["Q_api"] * (bias["Q"] / q_api_mean)
            else:
                df["Q"] = df["Q_api"]
            df["source"] = "temps_reel_glofas"
            print(f"[INFO] {station_name} — GloFAS OK, météo réelle")
        else:
            df["Q"]      = _seasonal_q_for_dates(station_name, df["date"])
            df["source"] = "temps_reel_meteo+Q_saisonnier"
            print(f"[INFO] {station_name} — météo réelle + Q saisonnier")

        df = df.drop(columns=["Q_api"], errors="ignore")

    except Exception as exc:
        print(f"[WARN] {station_name} API inaccessible ({exc})")
        raise RuntimeError(
            f"Open-Meteo inaccessible pour {station_name}: {exc}"
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
