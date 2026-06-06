"""
Envoi des alertes WhatsApp/SMS via Twilio (messages en français, grand public).
"""
from datetime import datetime, timedelta
from typing import Optional

from config import ALERT_LEVELS, STATIONS

# Descriptions lisibles du danger par niveau
_DANGER = {
    2: "⚠️ RISQUE ÉLEVÉ D'INONDATION",
    3: "🆘 DANGER EXTRÊME — CRUE IMMINENTE",
}

# Instructions pratiques par niveau
_INSTRUCTIONS = {
    2: (
        "• Éloignez-vous des bords du fleuve\n"
        "• Préparez vos affaires importantes (documents, médicaments)\n"
        "• Soyez prêts à évacuer rapidement\n"
        "• Prévenez vos voisins et votre famille\n"
        "• Suivez les consignes des autorités locales"
    ),
    3: (
        "• QUITTEZ IMMÉDIATEMENT les zones proches du fleuve\n"
        "• Rejoignez un terrain en hauteur\n"
        "• N'attendez pas — chaque minute compte\n"
        "• Prévenez vos voisins et votre famille\n"
        "• Appelez les secours si besoin\n"
        "• Suivez les consignes des autorités locales"
    ),
}

_BASIN_FR = {
    "Niger":   "Fleuve Niger",
    "Senegal": "Fleuve Sénégal",
    "Volta":   "Fleuve Volta",
}


def _format_message(station_name: str, run_date: str,
                    q_actuel: float, q_j1: float, q_j3: float,
                    niveau_j1: int, niveau_precedent: Optional[int]) -> str:

    basin_fr  = _BASIN_FR.get(STATIONS[station_name]["basin"], STATIONS[station_name]["basin"])
    danger    = _DANGER.get(niveau_j1, _DANGER[2])
    instruct  = _INSTRUCTIONS.get(niveau_j1, _INSTRUCTIONS[2])

    # Dates concrètes
    try:
        d0       = datetime.strptime(run_date, "%Y-%m-%d").date()
        date_j1  = (d0 + timedelta(days=1)).strftime("%d/%m/%Y")
        date_j3  = (d0 + timedelta(days=3)).strftime("%d/%m/%Y")
    except Exception:
        date_j1, date_j3 = "demain", "dans 3 jours"

    label_j1 = ALERT_LEVELS[niveau_j1]["label"]

    # Tendance
    if niveau_precedent is None or niveau_precedent < niveau_j1:
        tendance = "en hausse"
    elif niveau_precedent > niveau_j1:
        tendance = "en baisse"
    else:
        tendance = "stable"

    lines = [
        f"{danger}",
        f"",
        f"📍 Lieu      : {station_name}",
        f"🌊 Cours d'eau : {basin_fr}",
        f"📅 Aujourd'hui : {run_date}",
        f"📈 Tendance  : {tendance}",
        f"",
        f"⏰ Prévision d'inondation :",
        f"  → Demain ({date_j1})     : risque {label_j1.upper()} ({q_j1:,.0f} m³/s)",
        f"  → Dans 3 jours ({date_j3}) : risque {label_j1.upper()} ({q_j3:,.0f} m³/s)",
        f"  → Débit actuel du fleuve : {q_actuel:,.0f} m³/s",
        f"",
        f"✅ Que faire maintenant ?",
        instruct,
        f"",
        f"─────────────────────────",
        f"Système d'alerte précoce aux crues — Afrique de l'Ouest",
    ]
    return "\n".join(lines)


# ── Envoi Twilio ───────────────────────────────────────────────────────────────

def send_alert(station_name: str, run_date: str,
               q_actuel: float, q_j1: float, q_j3: float,
               niveau_j1: int, niveau_precedent: Optional[int],
               account_sid: str, auth_token: str,
               from_number: str, to_number: str) -> dict:
    """
    Envoie un SMS d'alerte si les conditions sont réunies.

    Retourne un dict :
        sent    : bool — True si SMS expédié
        reason  : str  — raison du non-envoi si sent=False
        sid     : str  — Twilio message SID (si envoyé)
        message : str  — texte complet
    """
    # ── Conditions d'envoi ────────────────────────────────────────────────────
    if niveau_j1 < 2:
        return {"sent": False, "reason": "niveau < Alerte", "sid": None, "message": ""}

    message = _format_message(
        station_name, run_date, q_actuel, q_j1, q_j3,
        niveau_j1, niveau_precedent,
    )

    # ── Appel Twilio ──────────────────────────────────────────────────────────
    try:
        from twilio.rest import Client  # import tardif : évite crash si non installé
        client = Client(account_sid, auth_token)
        msg = client.messages.create(
            body=message,
            from_=from_number,
            to=to_number,
        )
        return {"sent": True, "reason": "ok", "sid": msg.sid, "message": message}

    except ImportError:
        return {
            "sent": False,
            "reason": "twilio non installé (pip install twilio)",
            "sid": None,
            "message": message,
        }
    except Exception as exc:
        return {
            "sent": False,
            "reason": str(exc),
            "sid": None,
            "message": message,
        }


# ── Orchestration pour toutes les stations ────────────────────────────────────

def send_alerts_all(predictions: dict, q_actuels: dict,
                    previous_niveaux: dict,
                    account_sid: str, auth_token: str,
                    from_number: str, to_number: str,
                    run_date: str) -> list[dict]:
    """
    Parcourt toutes les stations et envoie les SMS nécessaires.

    predictions      : {station: {"q_j1": .., "q_j3": .., "niveau_j1": ..}}
    q_actuels        : {station: float}  — Q observé du jour
    previous_niveaux : {station: int|None} — niveau de J-1
    """
    results = []
    for station, pred in predictions.items():
        if "error" in pred:
            continue
        result = send_alert(
            station_name=station,
            run_date=run_date,
            q_actuel=q_actuels.get(station, 0.0),
            q_j1=pred["q_j1"],
            q_j3=pred["q_j3"],
            niveau_j1=pred["niveau_j1"],
            niveau_precedent=previous_niveaux.get(station),
            account_sid=account_sid,
            auth_token=auth_token,
            from_number=from_number,
            to_number=to_number,
        )
        result["station"] = station
        results.append(result)
    return results
