# debit_fluviaux

Sous-ensemble **Caravan** pour les bassins **Niger**, **Volta** et **Sénégal**, avec filtre **pays** optionnel.

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
