import pytest

import sms_service
from config import SMS_MIN_LEVEL

STATION = "BAKEL (Senegal)"


def _send(niveau_j1):
    return sms_service.send_alert(
        STATION, "2026-09-01", q_actuel=500, q_j1=900, q_j3=950,
        niveau_j1=niveau_j1, niveau_precedent=None,
        account_sid="AC_test", auth_token="t", from_number="+1", to_number="+2",
    )


def test_pas_de_sms_sous_le_niveau_minimal():
    result = _send(SMS_MIN_LEVEL - 1)
    assert result["sent"] is False
    assert result["message"] == ""


def test_message_contient_station_et_debits():
    msg = sms_service._format_message(STATION, "2026-09-01", 500, 900, 950, 3, 2)
    assert STATION in msg
    assert "02/09/2026" in msg and "04/09/2026" in msg
    assert "900 m³/s" in msg and "950 m³/s" in msg
    assert "en hausse" in msg


@pytest.mark.xfail(strict=True, reason="bug connu : la ligne J+3 affiche le niveau de J+1")
def test_message_j3_affiche_son_propre_niveau():
    # niveau_j1 = Urgence mais Q J+3 (300 m³/s) correspond à Alerte à BAKEL
    msg = sms_service._format_message(STATION, "2026-09-01", 500, 900, 300, 3, 2)
    ligne_j3 = next(l for l in msg.splitlines() if "Dans 3 jours" in l)
    assert "ALERTE" in ligne_j3


def test_source_du_debit_enregistree(tmp_db):
    tmp_db.upsert_mesure(STATION, "2026-09-01", {"Q": 100, "source": "temps_reel_glofas"})
    tmp_db.upsert_mesure(STATION, "2026-09-02", {"Q": 110, "source": "temps_reel_meteo+Q_saisonnier"})
    assert tmp_db.get_last_source(STATION) == "temps_reel_meteo+Q_saisonnier"
    assert tmp_db.get_last_source("INCONNUE") is None


def test_un_echec_d_envoi_ne_bloque_pas_un_nouvel_essai(tmp_db):
    tmp_db.log_sms(STATION, "2026-09-01", 3, "msg", None, statut="échec : timeout")
    assert tmp_db.sms_sent_today(STATION, "2026-09-01") is False

    tmp_db.log_sms(STATION, "2026-09-01", 3, "msg", "SM123")
    assert tmp_db.sms_sent_today(STATION, "2026-09-01") is True
