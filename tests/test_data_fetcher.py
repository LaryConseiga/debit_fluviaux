import pytest

import data_fetcher

STATION = "BAKEL (Senegal)"


@pytest.fixture(autouse=True)
def vide_cache():
    data_fetcher._BIAS_FACTORS_CACHE.clear()
    yield
    data_fetcher._BIAS_FACTORS_CACHE.clear()


@pytest.mark.parametrize("reference, source_mean, attendu", [
    (2.0, 1.0, 2.0),      # facteur plausible → appliqué
    (2.0, None, 1.0),     # source indisponible → pas de correction
    (2.0, 0.0, 1.0),      # moyenne nulle → pas de correction
    (800, 0.14, 1.0),     # facteur aberrant (maille hors fleuve) → pas de correction
    (1.0, 4.0, 1.0),      # facteur < 0.5 → pas de correction
])
def test_ratio(reference, source_mean, attendu):
    assert data_fetcher._ratio(reference, source_mean) == pytest.approx(attendu)


def test_facteurs_mis_en_cache_seulement_si_les_appels_reussissent(monkeypatch):
    appels = []

    def faux_long_terme(url, lat, lon, var):
        appels.append(var)
        return None  # API indisponible

    monkeypatch.setattr(data_fetcher, "_long_term_mean", faux_long_terme)
    assert data_fetcher._bias_factors(STATION) == {"Q": 1.0, "precip": 1.0}
    data_fetcher._bias_factors(STATION)
    assert len(appels) == 4  # pas de cache : deuxième passage refait les 2 appels

    monkeypatch.setattr(data_fetcher, "_long_term_mean", lambda *a: 1.0)
    data_fetcher._bias_factors(STATION)
    monkeypatch.setattr(data_fetcher, "_long_term_mean", lambda *a: pytest.fail("cache ignoré"))
    data_fetcher._bias_factors(STATION)


def test_facteur_fixe_ne_depend_pas_de_la_fenetre_recente(monkeypatch, df_hist):
    """Une crue dans la fenêtre récente ne doit pas être ramenée vers la moyenne."""
    bias = data_fetcher._get_bias(STATION)
    monkeypatch.setattr(data_fetcher, "_long_term_mean",
                        lambda url, lat, lon, var: bias["precip"] / 1.5
                        if var == "precipitation_sum" else bias["Q"] / 2)

    fenetre = df_hist.drop(columns=["Q"]).assign(Q_api=df_hist["Q"] * 10)  # crue ×10
    monkeypatch.setattr(data_fetcher, "_fetch_all_daily", lambda *a, **k: fenetre.copy())

    df = data_fetcher.fetch_station_data(STATION)
    assert df["source"].iloc[-1] == "temps_reel_glofas"
    assert df["Q"].iloc[-1] == pytest.approx(194 * 10 * 2)
    assert df["precip_mm"].iloc[-1] == pytest.approx(2.0 * 1.5)
