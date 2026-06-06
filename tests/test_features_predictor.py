import pytest

from config import RF_FEATURES, STATIONS
from feature_builder import build_features
from predictor import classify, predict

STATION = "BAKEL (Senegal)"


def test_build_features_colonnes_dans_l_ordre_du_modele(df_hist):
    feat = build_features(df_hist, STATION)
    assert list(feat.columns) == RF_FEATURES
    assert len(feat) == 1


def test_build_features_lags_et_moyennes(df_hist):
    feat = build_features(df_hist, STATION).iloc[0]
    assert feat["Q"] == 194
    assert feat["Q_lag1"] == 193
    assert feat["Q_lag30"] == 164
    assert feat["Q_mean7d"] == pytest.approx(191)   # moyenne de 188..194
    assert feat["Q_max90d"] == 194
    assert feat["precip_sum7d"] == pytest.approx(14)
    assert feat["station_id"] == STATIONS[STATION]["station_id"]


def test_build_features_historique_insuffisant(df_hist):
    with pytest.raises(ValueError, match="Historique insuffisant"):
        build_features(df_hist.tail(90), STATION)


@pytest.mark.parametrize("q, niveau", [
    (0, 0), (177, 0), (178, 1), (273, 1), (274, 2), (886, 2), (887, 3), (5000, 3),
])
def test_classify_bornes_bakel(q, niveau):
    # BAKEL : q50=178, q75=274, q90=887
    assert classify(q, STATION) == niveau


def test_predict_charge_les_modeles_ubj(df_hist):
    pred = predict(build_features(df_hist, STATION), STATION)
    assert pred["q_j1"] >= 0 and pred["q_j3"] >= 0
    assert pred["niveau_j1"] == classify(pred["q_j1"], STATION)
    assert pred["niveau_j3"] == classify(pred["q_j3"], STATION)
