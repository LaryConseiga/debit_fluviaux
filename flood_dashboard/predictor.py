"""
Chargement des modèles XGBoost et prédiction du niveau d'alerte.
"""
import pickle
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from config import MODEL_DIR, STATIONS, ALERT_LEVELS


# ── Chargement des modèles (singleton) ───────────────────────────────────────

@lru_cache(maxsize=1)
def _load_models():
    j1_path = MODEL_DIR / "xgb_global_j1.pkl"
    j3_path = MODEL_DIR / "xgb_global_j3.pkl"

    if not j1_path.exists() or not j3_path.exists():
        raise FileNotFoundError(
            f"Modèles introuvables dans {MODEL_DIR}. "
            "Exécutez d'abord train_flood_model.ipynb §5."
        )

    with open(j1_path, "rb") as f:
        model_j1 = pickle.load(f)
    with open(j3_path, "rb") as f:
        model_j3 = pickle.load(f)

    return model_j1, model_j3


# ── Classification en niveau d'alerte ────────────────────────────────────────

def classify(q: float, station_name: str) -> int:
    """
    Retourne le niveau d'alerte (0-3) selon les seuils de la station.
    0=Normal, 1=Vigilance, 2=Alerte, 3=Urgence
    """
    cfg = STATIONS[station_name]
    if q >= cfg["q90"]:
        return 3
    if q >= cfg["q75"]:
        return 2
    if q >= cfg["q50"]:
        return 1
    return 0


# ── Prédiction principale ─────────────────────────────────────────────────────

def predict(features: pd.DataFrame, station_name: str) -> dict:
    """
    Retourne un dict avec :
        q_j1, q_j3          : débits prédits (m³/s)
        niveau_j1, niveau_j3 : niveaux d'alerte correspondants
        label_j1, label_j3  : libellés ("Normal", "Alerte"…)
        emoji_j1, emoji_j3  : emoji couleur
    """
    model_j1, model_j3 = _load_models()

    q_j1 = float(model_j1.predict(features)[0])
    q_j3 = float(model_j3.predict(features)[0])
    q_j1 = max(0.0, q_j1)
    q_j3 = max(0.0, q_j3)

    niveau_j1 = classify(q_j1, station_name)
    niveau_j3 = classify(q_j3, station_name)

    return {
        "q_j1":      q_j1,
        "q_j3":      q_j3,
        "niveau_j1": niveau_j1,
        "niveau_j3": niveau_j3,
        "label_j1":  ALERT_LEVELS[niveau_j1]["label"],
        "label_j3":  ALERT_LEVELS[niveau_j3]["label"],
        "emoji_j1":  ALERT_LEVELS[niveau_j1]["emoji"],
        "emoji_j3":  ALERT_LEVELS[niveau_j3]["emoji"],
    }


def run_all_stations(features_map: dict[str, pd.DataFrame]) -> dict[str, dict]:
    """
    Prédiction pour toutes les stations.
    features_map : {station_name: DataFrame_features_1_ligne}
    Retourne     : {station_name: result_dict}
    """
    results = {}
    for name, feat in features_map.items():
        try:
            results[name] = predict(feat, name)
        except Exception as exc:
            results[name] = {"error": str(exc)}
    return results
