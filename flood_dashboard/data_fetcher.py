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
from typing import Optional

import numpy as np
import pandas as pd
import requests

from config import CSV_DIR, HISTORY_DAYS, STATIONS

# ── URLs ──────────────────────────────────────────────────────────────────────
_MAIN_URL = "https://api.open-meteo.com/v1/forecast"
_HIST_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
_FLOOD_URL = "https://flood-api.open-meteo.com/v1/flood"

# Période de référence pour les facteurs de correction de biais (années complètes)
_BIAS_REF_START = "2022-01-01"
_BIAS_REF_END   = "2025-12-31"
# Facteurs hors de cet intervalle = sources incomparables → pas de correction
_BIAS_RATIO_MIN, _BIAS_RATIO_MAX = 0.5, 5.0

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


# ── Facteurs de correction de biais (fixes, sur une longue période) ──────────
# Le facteur compare la moyenne historique d'entraînement (bias_means.csv) à la
# moyenne de la source temps réel sur plusieurs années complètes. Il est donc
# constant : une crue en cours n'est pas ramenée vers la moyenne, contrairement
# à un facteur calculé sur la fenêtre récente.

def _long_term_mean(url: str, lat: float, lon: float, var: str) -> Optional[float]:
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": _BIAS_REF_START, "end_date": _BIAS_REF_END,
        "daily": var,
    }
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        values = pd.Series(r.json()["daily"][var], dtype=float).dropna()
    except Exception as exc:
        print(f"[WARN] moyenne long terme {var} indisponible ({exc})")
        return None
    return float(values.mean()) if not values.empty else None


def _ratio(reference: float, source_mean: Optional[float]) -> float:
    if not reference or not source_mean or source_mean <= 0:
        return 1.0
    ratio = reference / source_mean
    return ratio if _BIAS_RATIO_MIN <= ratio <= _BIAS_RATIO_MAX else 1.0


_BIAS_FACTORS_CACHE: dict[str, dict] = {}


def _bias_factors(station_name: str) -> dict:
    """Facteurs multiplicatifs {"Q": .., "precip": ..} pour une station (1.0 = aucune correction)."""
    if station_name in _BIAS_FACTORS_CACHE:
        return _BIAS_FACTORS_CACHE[station_name]

    cfg  = STATIONS[station_name]
    bias = _get_bias(station_name)
    precip_mean = _long_term_mean(_HIST_FORECAST_URL, cfg["lat"], cfg["lon"], "precipitation_sum")
    q_mean      = _long_term_mean(_FLOOD_URL, cfg["lat"], cfg["lon"], "river_discharge")
    factors = {
        "Q":      _ratio(bias["Q"], q_mean),
        "precip": _ratio(bias["precip"], precip_mean),
    }
    # Pas de mise en cache si un appel a échoué : on réessaiera au prochain passage
    if precip_mean is not None and q_mean is not None:
        _BIAS_FACTORS_CACHE[station_name] = factors
    return factors


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

    try:
        df = _fetch_all_daily(lat, lon, past_days=past_days)
        factors = _bias_factors(station_name)

        # ── Correction biais précipitations ───────────────────────────────────
        df["precip_mm"] = df["precip_mm"] * factors["precip"]

        # ── Débit Q ───────────────────────────────────────────────────────────
        q_api_valid = df["Q_api"].notna().any()

        if q_api_valid:
            df["Q"] = df["Q_api"] * factors["Q"]
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
