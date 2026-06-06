"""
Initialisation de la base SQLite depuis les CSV historiques.
À exécuter une seule fois avant le premier lancement du dashboard.

Usage :
    python init_db.py
"""
import sys
from pathlib import Path

import pandas as pd

# Rend les imports relatifs fonctionnels quand lancé directement
sys.path.insert(0, str(Path(__file__).parent))

from config import CSV_DIR, HISTORY_DAYS, STATIONS
from database import count_mesures, init_schema, upsert_mesure


COLS_METEO = [
    "precip_mm", "t2m_mean", "t2m_max", "t2m_min",
    "rh2m_pct", "pression_hpa", "sm_surface", "sm_root",
]

# Mapping noms colonnes CSV → noms colonnes internes
CSV_RENAME = {
    "Date":                        "date",
    "Q":                           "Q",
    "precip_mm":                   "precip_mm",
    "t2m_mean":                    "t2m_mean",
    "t2m_max":                     "t2m_max",
    "t2m_min":                     "t2m_min",
    "rh2m_pct":                    "rh2m_pct",
    "pression_hpa":                "pression_hpa",
    "sm_surface":                  "sm_surface",
    "sm_root":                     "sm_root",
}


def load_station_csv(station_name: str) -> pd.DataFrame:
    cfg  = STATIONS[station_name]
    path = CSV_DIR / cfg["csv"]
    if not path.exists():
        print(f"  [SKIP] CSV introuvable : {path}")
        return pd.DataFrame()

    df = pd.read_csv(path, parse_dates=["date"])

    # Trie et prend les HISTORY_DAYS dernières lignes
    df = df.sort_values("date").tail(HISTORY_DAYS).reset_index(drop=True)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df


def init_station(station_name: str, force: bool = False) -> int:
    """
    Insère les données historiques d'une station.
    Si force=False, saute si la station a déjà des données.
    Retourne le nombre de lignes insérées.
    """
    if not force and count_mesures(station_name) > 0:
        n = count_mesures(station_name)
        print(f"  [OK] {station_name} : déjà {n} mesures, ignoré (--force pour réinitialiser)")
        return 0

    df = load_station_csv(station_name)
    if df.empty:
        return 0

    inserted = 0
    for _, row in df.iterrows():
        rec = {col: (None if pd.isna(row.get(col)) else row.get(col))
               for col in COLS_METEO + ["Q"]}
        upsert_mesure(station_name, row["date"], rec)
        inserted += 1

    print(f"  [OK] {station_name} : {inserted} lignes insérées")
    return inserted


def main():
    force = "--force" in sys.argv
    print("=== Initialisation de la base de données flood_alerts.db ===\n")
    init_schema()

    total = 0
    for name in STATIONS:
        total += init_station(name, force=force)

    print(f"\nTotal : {total} mesures insérées.")
    print("Base prête. Vous pouvez lancer : streamlit run app.py")


if __name__ == "__main__":
    main()
