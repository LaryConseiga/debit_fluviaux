# debit_fluviaux

Sous-ensemble **Caravan** pour les bassins **Niger**, **Volta** et **Sénégal**, avec filtre **pays** optionnel.

## Tableau de bord d'alerte crues (`flood_dashboard/`)

Application Streamlit qui prévoit le débit à J+1 et J+3 pour 11 stations
(Niger, Sénégal, Volta) avec deux modèles XGBoost, et envoie une alerte SMS
(Twilio) quand une station passe en **Urgence**.

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Au démarrage, l'application récupère 3 mois de météo Open-Meteo pour chaque
station, calcule les prévisions et les enregistre dans `flood_dashboard/flood_alerts.db`
(SQLite). La mise à jour est faite une fois par jour ; les stations en échec sont
réessayées l'heure suivante.

**Source du débit.** Quand le débit GloFAS n'est pas disponible, le débit utilisé
est la médiane saisonnière de la station (`ml_datasets/seasonal_q.csv`). La source
est affichée pour chaque station : une prévision basée sur la médiane saisonnière
ne peut pas détecter une crue exceptionnelle.

**Seuils.** Définis par station dans `flood_dashboard/config.py` (Q50 Vigilance,
Q75 Alerte, Q90 Urgence). Le niveau qui déclenche un SMS est `SMS_MIN_LEVEL`
(Urgence par défaut).

**SMS (optionnel).** Renseigner `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`,
`TWILIO_FROM` et `TWILIO_TO` dans `.streamlit/secrets.toml` (local, non versionné)
ou dans *Settings → Secrets* sur Streamlit Cloud. Sans ces valeurs, aucun SMS
n'est envoyé. La base SQLite est effacée à chaque redémarrage de Streamlit Cloud,
historique des SMS compris.

**Modèles.** `trained_models_global/xgb_global_j1.ubj` et `xgb_global_j3.ubj`
(format natif XGBoost, entraînés avec xgboost 3.2). Après un réentraînement,
les enregistrer avec `model.save_model("xgb_global_j1.ubj")`. Les fichiers
`lstm_*` ne sont pas utilisés par le tableau de bord.

**Données observées (optionnel).** `python flood_dashboard/init_db.py` charge les
débits observés si les CSV par station (`ml_datasets/<station>_ml.csv`, non
versionnés) sont présents.

**Tests.**

```bash
pip install -r requirements-dev.txt
pytest tests
```

## Prérequis

- Python 3.10+
- Archive **Caravan CSV** extraite depuis [Caravan CSV sur Zenodo (fichiers)](https://zenodo.org/records/15530022) : la racine doit contenir `attributes/` et `timeseries/csv/`. L’URL directe du `tar.gz` utilise l’enregistrement **15530022** (la page **15530021** ne sert pas le fichier — erreur 404).

## Installation

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Téléchargement partiel (hysets, streaming Zenodo)

1. Copier `.env.example` vers `.env` et renseigner `SAVE_PATH` (dossier **local** synchronisé avec Google Drive de préférence, ou URL du dossier + `CARAVAN_LOCAL_SAVE`).
2. Lancer :

```bash
python scripts/download_caravan.py
```

Le script lit `SAVE_PATH` via **python-dotenv**, extrait les CSV `hysets`, écrit `data/config/drive_ids.yaml` (modèle d’IDs Drive à compléter) et affiche un résumé.

**Connexion coupée (WinError 10053, etc.)** : utilisez `CARAVAN_DOWNLOAD_MODE=file` dans `.env` (défaut du script). L’archive (~29 Go) est alors téléchargée dans le dossier **`.cache/`** du projet (hors OneDrive), puis l’extraction est faite depuis le disque — bien plus fiable que le flux HTTP direct (`stream`).

## Sélection des stations

1. Copier ou extraire Caravan dans un dossier local (ou disque cloud), par exemple `Caravan/`.
2. Définir la variable d’environnement ou éditer `data/config/caravan_subset.yaml` :

```bash
set CARAVAN_ROOT=C:\chemin\vers\Caravan
python scripts/build_subset.py
```

Sorties :

- `data/processed/selected_gauges.csv` — `gauge_id`, `basin`, `subdataset`, `country`, coordonnées ;
- `data/processed/build_manifest.json` — effectifs par bassin.

Les **rectangles** dans le YAML sont une première approximation ; pour un périmètre hydrologique fidèle, renseignez `basins_geojson` avec des polygones (ex. HydroBASINS) et des propriétés `basin_id` / `priority`.

## Configuration

| Fichier | Rôle |
|--------|------|
| `data/config/caravan_subset.yaml` | Pays autorisés, emprises ou GeoJSON, chemins de sortie |
| `data/config/columns_caravan.yaml` | Aide-mémoire des noms de colonnes forcings / cible |

## Jeu de données

Kratzert *et al.*, *Scientific Data* (Caravan) : [article](https://www.nature.com/articles/s41597-023-01975-w). Citer la source et les licences du dépôt Zenodo utilisé.
