"""
Couche d'accès SQLite — aucun serveur requis, fonctionne en local et sur Streamlit Cloud.
"""
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "flood_alerts.db"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ── Initialisation du schéma ──────────────────────────────────────────────────

def init_schema():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS mesures (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            station      TEXT    NOT NULL,
            date         TEXT    NOT NULL,
            Q            REAL,
            precip_mm    REAL,
            t2m_mean     REAL,
            t2m_max      REAL,
            t2m_min      REAL,
            rh2m_pct     REAL,
            pression_hpa REAL,
            sm_surface   REAL,
            sm_root      REAL,
            source       TEXT DEFAULT 'openmeteo',
            UNIQUE (station, date)
        );

        CREATE TABLE IF NOT EXISTS predictions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            station      TEXT    NOT NULL,
            run_date     TEXT    NOT NULL,
            Q_predit_j1  REAL,
            Q_predit_j3  REAL,
            niveau_j1    INTEGER,
            niveau_j3    INTEGER,
            UNIQUE (station, run_date)
        );

        CREATE TABLE IF NOT EXISTS sms_log (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            station   TEXT    NOT NULL,
            run_date  TEXT    NOT NULL,
            niveau    INTEGER NOT NULL,
            message   TEXT    NOT NULL,
            sid       TEXT,
            statut    TEXT    DEFAULT 'sent',
            ts        TEXT    DEFAULT (datetime('now'))
        );
        """)


# ── Insertions ─────────────────────────────────────────────────────────────────

def upsert_mesure(station: str, date_str: str, row: dict):
    sql = """
        INSERT INTO mesures
            (station, date, Q, precip_mm, t2m_mean, t2m_max, t2m_min,
             rh2m_pct, pression_hpa, sm_surface, sm_root)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(station, date) DO UPDATE SET
            Q=excluded.Q, precip_mm=excluded.precip_mm,
            t2m_mean=excluded.t2m_mean, t2m_max=excluded.t2m_max,
            t2m_min=excluded.t2m_min, rh2m_pct=excluded.rh2m_pct,
            pression_hpa=excluded.pression_hpa,
            sm_surface=excluded.sm_surface, sm_root=excluded.sm_root
    """
    values = (
        station, date_str,
        row.get("Q"), row.get("precip_mm"), row.get("t2m_mean"),
        row.get("t2m_max"), row.get("t2m_min"), row.get("rh2m_pct"),
        row.get("pression_hpa"), row.get("sm_surface"), row.get("sm_root"),
    )
    with get_conn() as conn:
        conn.execute(sql, values)


def upsert_prediction(station: str, run_date: str,
                      q_j1: float, q_j3: float,
                      niveau_j1: int, niveau_j3: int):
    sql = """
        INSERT INTO predictions
            (station, run_date, Q_predit_j1, Q_predit_j3, niveau_j1, niveau_j3)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(station, run_date) DO UPDATE SET
            Q_predit_j1=excluded.Q_predit_j1, Q_predit_j3=excluded.Q_predit_j3,
            niveau_j1=excluded.niveau_j1, niveau_j3=excluded.niveau_j3
    """
    with get_conn() as conn:
        conn.execute(sql, (station, run_date, q_j1, q_j3, niveau_j1, niveau_j3))


def log_sms(station: str, run_date: str, niveau: int,
            message: str, sid: Optional[str] = None, statut: str = "sent"):
    sql = """
        INSERT INTO sms_log (station, run_date, niveau, message, sid, statut)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    with get_conn() as conn:
        conn.execute(sql, (station, run_date, niveau, message, sid, statut))


# ── Lectures ───────────────────────────────────────────────────────────────────

def _rows(cursor_result) -> list[dict]:
    return [dict(r) for r in cursor_result]


def get_mesures(station: str, n_days: int = 95) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM mesures
            WHERE station = ?
            ORDER BY date DESC
            LIMIT ?
        """, (station, n_days)).fetchall()
    return list(reversed(_rows(rows)))


def get_last_prediction(station: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("""
            SELECT * FROM predictions
            WHERE station = ?
            ORDER BY run_date DESC
            LIMIT 1
        """, (station,)).fetchone()
    return dict(row) if row else None


def get_predictions_history(station: str, n: int = 30) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM predictions
            WHERE station = ?
            ORDER BY run_date DESC
            LIMIT ?
        """, (station, n)).fetchall()
    return list(reversed(_rows(rows)))


def get_sms_log(n: int = 50) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM sms_log
            ORDER BY ts DESC
            LIMIT ?
        """, (n,)).fetchall()
    return _rows(rows)


def get_previous_niveau(station: str, before_date: str) -> Optional[int]:
    with get_conn() as conn:
        row = conn.execute("""
            SELECT niveau_j1 FROM predictions
            WHERE station = ? AND run_date < ?
            ORDER BY run_date DESC
            LIMIT 1
        """, (station, before_date)).fetchone()
    return dict(row)["niveau_j1"] if row else None


def count_mesures(station: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM mesures WHERE station = ?", (station,)
        ).fetchone()
    return dict(row)["n"]


def sms_sent_today(station: str, run_date: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM sms_log WHERE station = ? AND run_date = ?",
            (station, run_date),
        ).fetchone()
    return dict(row)["n"] > 0
