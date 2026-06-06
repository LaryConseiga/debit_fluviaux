"""
Reconstruction des 68 features du modèle XGBoost à partir de :
- l'historique de Q (colonnes de lag / rolling)
- les variables météo du jour (précipitations, temp, humidité, SM)
- les variables calendaires (sin/cos, saison…)
- le station_id constant

La fonction principale retourne un DataFrame d'une seule ligne
prêt à être passé au modèle.
"""
import math
from datetime import date

import numpy as np
import pandas as pd

from config import RF_FEATURES, STATIONS


# ── Constantes saisonnières ────────────────────────────────────────────────────
# Saison sèche = 0, saison des pluies = 1
_SAISON_PLUIES = {6, 7, 8, 9, 10}


def _saison(mois: int) -> int:
    return 1 if mois in _SAISON_PLUIES else 0


# ── Fonction principale ────────────────────────────────────────────────────────

def build_features(df_hist: pd.DataFrame, station_name: str) -> pd.DataFrame:
    """
    Construit le vecteur de 68 features pour la dernière date disponible dans df_hist.

    Paramètres
    ----------
    df_hist : DataFrame trié par date ascendante, colonnes minimales attendues :
              date, Q, precip_mm, t2m_mean, t2m_max, t2m_min,
              rh2m_pct, pression_hpa, sm_surface, sm_root
    station_name : clé dans STATIONS

    Retourne
    --------
    DataFrame d'une ligne, colonnes = RF_FEATURES (dans l'ordre exact).
    Lève ValueError si l'historique est insuffisant.
    """
    df = df_hist.copy().sort_values("date").reset_index(drop=True)

    # Forward-fill toutes les colonnes numériques (Q peut être None si API indisponible)
    num_cols = ["Q", "precip_mm", "t2m_mean", "t2m_max", "t2m_min",
                "rh2m_pct", "pression_hpa", "sm_surface", "sm_root"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").ffill().bfill().fillna(0)

    if len(df) < 91:
        raise ValueError(
            f"Historique insuffisant ({len(df)} jours, minimum 91 requis)"
        )

    # Index de la dernière ligne (= aujourd'hui ou dernier disponible)
    i = len(df) - 1
    row = df.iloc[i]

    # ── Météo & Q bruts ───────────────────────────────────────────────────────
    Q           = row["Q"]
    precip_mm   = row.get("precip_mm", 0.0) or 0.0
    t2m_mean    = row.get("t2m_mean")
    t2m_max     = row.get("t2m_max")
    t2m_min     = row.get("t2m_min")
    rh2m_pct    = row.get("rh2m_pct")
    pression    = row.get("pression_hpa")
    sm_surface  = row.get("sm_surface")
    sm_root     = row.get("sm_root")

    # ── Calendaire ────────────────────────────────────────────────────────────
    d = pd.to_datetime(row["date"])
    jour_annee  = d.day_of_year
    mois        = d.month
    semaine     = d.isocalendar().week
    annee       = d.year

    sin_jour = math.sin(2 * math.pi * jour_annee / 365)
    cos_jour = math.cos(2 * math.pi * jour_annee / 365)
    sin_mois = math.sin(2 * math.pi * mois / 12)
    cos_mois = math.cos(2 * math.pi * mois / 12)
    saison   = _saison(mois)

    # ── Helpers extractions ───────────────────────────────────────────────────
    Qs = df["Q"].values          # tableau numpy

    def lag(k: int) -> float:
        j = i - k
        return float(Qs[j]) if j >= 0 else float(Qs[0])

    def rolling_mean(k: int) -> float:
        start = max(0, i - k + 1)
        return float(np.mean(Qs[start: i + 1]))

    def rolling_std(k: int) -> float:
        start = max(0, i - k + 1)
        window = Qs[start: i + 1]
        return float(np.std(window)) if len(window) > 1 else 0.0

    def rolling_max(k: int) -> float:
        start = max(0, i - k + 1)
        return float(np.max(Qs[start: i + 1]))

    # ── Q lags ────────────────────────────────────────────────────────────────
    Q_lag1  = lag(1)
    Q_lag2  = lag(2)
    Q_lag3  = lag(3)
    Q_lag5  = lag(5)
    Q_lag7  = lag(7)
    Q_lag14 = lag(14)
    Q_lag21 = lag(21)
    Q_lag30 = lag(30)

    logQ_lag1 = math.log1p(max(Q_lag1, 0))
    logQ_lag2 = math.log1p(max(Q_lag2, 0))
    logQ_lag3 = math.log1p(max(Q_lag3, 0))
    logQ_lag7 = math.log1p(max(Q_lag7, 0))

    # ── Rolling Q ─────────────────────────────────────────────────────────────
    Q_mean7d  = rolling_mean(7);  Q_std7d  = rolling_std(7);  Q_max7d  = rolling_max(7)
    Q_mean14d = rolling_mean(14); Q_std14d = rolling_std(14); Q_max14d = rolling_max(14)
    Q_mean30d = rolling_mean(30); Q_std30d = rolling_std(30); Q_max30d = rolling_max(30)
    Q_mean60d = rolling_mean(60); Q_std60d = rolling_std(60); Q_max60d = rolling_max(60)
    Q_mean90d = rolling_mean(90); Q_std90d = rolling_std(90); Q_max90d = rolling_max(90)

    Q_ratio_mean30 = Q / Q_mean30d if Q_mean30d > 0 else 1.0

    # ── Précipitation lags & rolling ─────────────────────────────────────────
    precs = df["precip_mm"].fillna(0).values

    def prec_lag(k: int) -> float:
        j = i - k
        return float(precs[j]) if j >= 0 else 0.0

    def prec_sum(k: int) -> float:
        start = max(0, i - k + 1)
        return float(np.sum(precs[start: i + 1]))

    precip_lag1  = prec_lag(1)
    precip_lag2  = prec_lag(2)
    precip_lag3  = prec_lag(3)
    precip_lag5  = prec_lag(5)
    precip_lag7  = prec_lag(7)
    precip_lag10 = prec_lag(10)
    precip_lag14 = prec_lag(14)

    precip_sum3d  = prec_sum(3)
    precip_sum7d  = prec_sum(7)
    precip_sum14d = prec_sum(14)
    precip_sum30d = prec_sum(30)
    precip_sum60d = prec_sum(60)

    # API (Antecedent Precipitation Index) : somme exponentielle décroissante
    api = sum(precs[max(0, i - k)] * (0.9 ** k) for k in range(1, min(i + 1, 31)))

    # ── Température ───────────────────────────────────────────────────────────
    t2m_amplitude = (t2m_max or 0) - (t2m_min or 0)

    # Anomalie vs moyenne mobile 30j
    t2m_series = df["t2m_mean"].values
    start30 = max(0, i - 29)
    t2m_mean30 = float(np.nanmean(t2m_series[start30: i + 1]))
    t2m_anomalie = (t2m_mean or 0) - t2m_mean30

    # ── Soil moisture lags ────────────────────────────────────────────────────
    sm_surf_arr = df["sm_surface"].ffill().fillna(0).values
    sm_root_arr = df["sm_root"].ffill().fillna(0).values

    def sm_lag(arr, k):
        j = i - k
        return float(arr[j]) if j >= 0 else float(arr[0])

    def sm_mean(arr, k):
        start = max(0, i - k + 1)
        return float(np.nanmean(arr[start: i + 1]))

    sm_surface_lag1    = sm_lag(sm_surf_arr, 1)
    sm_surface_lag7    = sm_lag(sm_surf_arr, 7)
    sm_surface_mean30d = sm_mean(sm_surf_arr, 30)

    sm_root_lag1       = sm_lag(sm_root_arr, 1)
    sm_root_lag7       = sm_lag(sm_root_arr, 7)
    sm_root_mean30d    = sm_mean(sm_root_arr, 30)

    # ── station_id ────────────────────────────────────────────────────────────
    station_id = STATIONS[station_name]["station_id"]

    # ── Assemblage dans l'ordre exact ─────────────────────────────────────────
    feat = {
        "Q": Q, "precip_mm": precip_mm,
        "t2m_mean": t2m_mean, "t2m_max": t2m_max, "t2m_min": t2m_min,
        "rh2m_pct": rh2m_pct, "pression_hpa": pression,
        "sm_surface": sm_surface, "sm_root": sm_root,
        "jour_annee": jour_annee, "mois": mois, "semaine": semaine, "annee": annee,
        "sin_jour": sin_jour, "cos_jour": cos_jour,
        "sin_mois": sin_mois, "cos_mois": cos_mois,
        "saison": saison,
        "Q_lag1": Q_lag1, "Q_lag2": Q_lag2, "Q_lag3": Q_lag3,
        "Q_lag5": Q_lag5, "Q_lag7": Q_lag7,
        "Q_lag14": Q_lag14, "Q_lag21": Q_lag21, "Q_lag30": Q_lag30,
        "logQ_lag1": logQ_lag1, "logQ_lag2": logQ_lag2,
        "logQ_lag3": logQ_lag3, "logQ_lag7": logQ_lag7,
        "Q_mean7d": Q_mean7d, "Q_std7d": Q_std7d, "Q_max7d": Q_max7d,
        "Q_mean14d": Q_mean14d, "Q_std14d": Q_std14d, "Q_max14d": Q_max14d,
        "Q_mean30d": Q_mean30d, "Q_std30d": Q_std30d, "Q_max30d": Q_max30d,
        "Q_mean60d": Q_mean60d, "Q_std60d": Q_std60d, "Q_max60d": Q_max60d,
        "Q_mean90d": Q_mean90d, "Q_std90d": Q_std90d, "Q_max90d": Q_max90d,
        "Q_ratio_mean30": Q_ratio_mean30,
        "precip_lag1": precip_lag1, "precip_lag2": precip_lag2,
        "precip_lag3": precip_lag3, "precip_lag5": precip_lag5,
        "precip_lag7": precip_lag7, "precip_lag10": precip_lag10,
        "precip_lag14": precip_lag14,
        "precip_sum3d": precip_sum3d, "precip_sum7d": precip_sum7d,
        "precip_sum14d": precip_sum14d, "precip_sum30d": precip_sum30d,
        "precip_sum60d": precip_sum60d,
        "api": api,
        "t2m_amplitude": t2m_amplitude, "t2m_anomalie": t2m_anomalie,
        "sm_surface_lag1": sm_surface_lag1, "sm_surface_lag7": sm_surface_lag7,
        "sm_surface_mean30d": sm_surface_mean30d,
        "sm_root_lag1": sm_root_lag1, "sm_root_lag7": sm_root_lag7,
        "sm_root_mean30d": sm_root_mean30d,
        "station_id": station_id,
    }

    # Garantit l'ordre exact attendu par le modèle
    return pd.DataFrame([feat])[RF_FEATURES]
