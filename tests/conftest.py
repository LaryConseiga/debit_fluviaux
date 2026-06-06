import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Les modules du tableau de bord s'importent entre eux sans préfixe de paquet
sys.path.insert(0, str(Path(__file__).parent.parent / "flood_dashboard"))


@pytest.fixture
def df_hist():
    """95 jours d'historique synthétique : Q = 100, 101, …, 194."""
    n = 95
    return pd.DataFrame({
        "date":         pd.date_range("2026-06-01", periods=n, freq="D"),
        "Q":            np.arange(100, 100 + n, dtype=float),
        "precip_mm":    np.full(n, 2.0),
        "t2m_mean":     np.full(n, 28.0),
        "t2m_max":      np.full(n, 34.0),
        "t2m_min":      np.full(n, 22.0),
        "rh2m_pct":     np.full(n, 60.0),
        "pression_hpa": np.full(n, 980.0),
        "sm_surface":   np.full(n, 0.2),
        "sm_root":      np.full(n, 0.25),
    })


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    import database
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "test.db")
    database.init_schema()
    return database
