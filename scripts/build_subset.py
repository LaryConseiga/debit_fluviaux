"""
Construit la liste des stations Caravan pour Niger, Volta et Sénégal,
avec filtre pays optionnel, à partir de la racine du jeu extrait (Zenodo CSV).

Usage:
  set CARAVAN_ROOT=C:\\chemin\\vers\\Caravan
  python scripts/build_subset.py --config data/config/caravan_subset.yaml
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yaml
from shapely.geometry import Point, box, shape


@dataclass(order=True)
class BasinRule:
    priority: int
    basin_id: str
    geometry: Any  # shapely geometry


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_caravan_attributes(caravan_root: Path) -> pd.DataFrame:
    """Charge et fusionne tous les CSV d'attributs (même logique que NeuralHydrology)."""
    attr_root = caravan_root / "attributes"
    if not attr_root.is_dir():
        raise FileNotFoundError(
            f"Dossier attributes introuvable : {attr_root}. "
            "Indiquez la racine Caravan extraite (Zenodo CSV)."
        )

    frames: list[pd.DataFrame] = []
    for subdir in sorted(p for p in attr_root.iterdir() if p.is_dir()):
        parts: list[pd.DataFrame] = []
        for csv_path in sorted(subdir.glob("*.csv")):
            df = pd.read_csv(csv_path, index_col="gauge_id", low_memory=False)
            parts.append(df)
        if not parts:
            continue
        merged = pd.concat(parts, axis=1)
        frames.append(merged)

    if not frames:
        raise FileNotFoundError(f"Aucun CSV trouvé sous {attr_root}")

    out = pd.concat(frames, axis=0)
    # Colonnes dupliquées (même nom dans hydroatlas et caravan) : garder première occurrence
    out = out.loc[:, ~out.columns.duplicated()]
    return out


def resolve_lon_lat_country(df: pd.DataFrame) -> tuple[str, str, str]:
    """Trouve les noms de colonnes lat/lon/pays."""
    lower = {c.lower(): c for c in df.columns}

    def pick(*candidates: str) -> Optional[str]:
        for cand in candidates:
            if cand in lower:
                return lower[cand]
            if cand.lower() in lower:
                return lower[cand.lower()]
        return None

    lat_col = pick("gauge_lat", "latitude", "lat")
    lon_col = pick("gauge_lon", "longitude", "lon")
    country_col = pick("country")
    missing = [n for n, c in [("latitude", lat_col), ("longitude", lon_col)] if c is None]
    if missing:
        raise KeyError(
            f"Colonnes lat/lon introuvables. Colonnes disponibles (extrait) : {list(df.columns)[:30]}..."
        )
    if country_col is None:
        country_col = ""
    return lon_col, lat_col, country_col


def rules_from_config(cfg: dict[str, Any], config_path: Path) -> list[BasinRule]:
    geo_path = cfg.get("basins_geojson")
    if geo_path:
        path = Path(geo_path)
        if not path.is_absolute():
            path = config_path.parent.parent.parent / path
        if not path.is_file():
            raise FileNotFoundError(f"basins_geojson introuvable : {path}")
        return _rules_from_geojson(path)

    basins_cfg = cfg.get("basins") or []
    rules: list[BasinRule] = []
    for b in basins_cfg:
        bid = b["id"]
        prio = int(b["priority"])
        bounds = b["bounds"]
        min_lon, min_lat, max_lon, max_lat = bounds
        geom = box(min_lon, min_lat, max_lon, max_lat)
        rules.append(BasinRule(priority=prio, basin_id=bid, geometry=geom))
    return sorted(rules)


def _rules_from_geojson(path: Path) -> list[BasinRule]:
    import json

    with path.open(encoding="utf-8") as f:
        gj = json.load(f)
    feats = gj.get("features") or []
    rules: list[BasinRule] = []
    for feat in feats:
        props = feat.get("properties") or {}
        bid = props.get("basin_id")
        prio = props.get("priority")
        if not bid:
            raise ValueError(f"Propriété basin_id manquante dans une entité GeoJSON ({path})")
        if prio is None:
            prio = 999
        rules.append(
            BasinRule(priority=int(prio), basin_id=str(bid), geometry=shape(feat["geometry"]))
        )
    return sorted(rules)


def assign_basin(lon: float, lat: float, rules: list[BasinRule]) -> Optional[str]:
    pt = Point(lon, lat)
    for rule in rules:
        if rule.geometry.covers(pt):
            return rule.basin_id
    return None


def subdataset_from_gauge(gauge_id: str) -> str:
    return gauge_id.split("_")[0]


def timeseries_csv_path(caravan_root: Path, gauge_id: str) -> Path:
    sub = subdataset_from_gauge(gauge_id)
    return caravan_root / "timeseries" / "csv" / sub / f"{gauge_id}.csv"


def build_subset(
    caravan_root: Path,
    cfg: dict[str, Any],
    config_path: Path,
) -> pd.DataFrame:
    df = load_caravan_attributes(caravan_root)
    lon_col, lat_col, country_col = resolve_lon_lat_country(df)

    countries = cfg.get("countries") or []
    if countries and country_col:
        allowed = set(countries)
        before = len(df)
        df = df[df[country_col].isin(allowed)]
        if df.empty:
            raise RuntimeError(
                f"Aucune station après filtre pays {allowed} (avant : {before}). "
                "Vérifiez l'orthographe des noms dans Caravan (colonne country)."
            )
    elif countries and not country_col:
        raise RuntimeError("Filtre pays demandé mais colonne country absente des attributs.")

    rules = rules_from_config(cfg, config_path)
    if not rules:
        raise RuntimeError("Aucune règle de bassin (basins ou basins_geojson).")

    lons = pd.to_numeric(df[lon_col], errors="coerce")
    lats = pd.to_numeric(df[lat_col], errors="coerce")
    df = df.assign(_lon=lons, _lat=lats).dropna(subset=["_lon", "_lat"])

    basin_ids: list[Optional[str]] = []
    for _, row in df.iterrows():
        basin_ids.append(assign_basin(float(row["_lon"]), float(row["_lat"]), rules))
    df = df.assign(basin=basin_ids)
    df = df[df["basin"].notna()].copy()

    if df.empty:
        raise RuntimeError(
            "Aucune station dans les emprises définies. Affinez les polygones / GeoJSON "
            "(les rectangles par défaut sont approximatifs) ou élargissez les bounds."
        )

    df["subdataset"] = df.index.map(subdataset_from_gauge)
    out_cols = [
        "basin",
        "subdataset",
        country_col,
        lat_col,
        lon_col,
    ]
    out_cols = [c for c in out_cols if c]
    summary = df[out_cols].copy()
    summary.insert(0, "gauge_id", df.index)
    summary = summary.rename(
        columns={
            country_col: "country",
            lat_col: "latitude",
            lon_col: "longitude",
        }
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Sélection Caravan : Niger, Volta, Sénégal + pays.")
    parser.add_argument(
        "--config",
        type=Path,
        default=project_root() / "data" / "config" / "caravan_subset.yaml",
    )
    parser.add_argument(
        "--caravan-root",
        type=Path,
        default=None,
        help="Remplace caravan_root du YAML et CARAVAN_ROOT",
    )
    args = parser.parse_args()
    cfg_path = args.config
    if not cfg_path.is_absolute():
        cfg_path = project_root() / cfg_path
    cfg = load_yaml(cfg_path)

    caravan_root = args.caravan_root
    if caravan_root is None:
        env = os.environ.get("CARAVAN_ROOT")
        caravan_root = cfg.get("caravan_root") or env
    if caravan_root is None:
        print(
            "Erreur : précisez la racine Caravan (clé caravan_root dans le YAML, "
            "--caravan-root, ou variable CARAVAN_ROOT).",
            file=sys.stderr,
        )
        sys.exit(1)
    caravan_root = Path(caravan_root).expanduser()
    if not caravan_root.is_dir():
        print(f"Erreur : dossier Caravan introuvable : {caravan_root}", file=sys.stderr)
        sys.exit(1)

    summary = build_subset(caravan_root, cfg, cfg_path)

    if cfg.get("verify_timeseries_csv", True):
        missing: list[str] = []
        for gid in summary["gauge_id"]:
            p = timeseries_csv_path(caravan_root, gid)
            if not p.is_file():
                missing.append(str(p))
        if missing:
            print(
                f"Avertissement : {len(missing)} fichier(s) série temporelle manquant(s). "
                "Exemple :",
                file=sys.stderr,
            )
            for m in missing[:5]:
                print(f"  {m}", file=sys.stderr)

    root = project_root()
    out_csv = Path(cfg.get("output_gauges_csv", "data/processed/selected_gauges.csv"))
    if not out_csv.is_absolute():
        out_csv = root / out_csv
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_csv, index=False)

    manifest = {
        "n_gauges": int(len(summary)),
        "by_basin": summary.groupby("basin").size().to_dict(),
        "caravan_root": str(caravan_root.resolve()),
        "config": str(cfg_path.resolve()),
    }
    out_m = Path(cfg.get("output_manifest_json", "data/processed/build_manifest.json"))
    if not out_m.is_absolute():
        out_m = root / out_m
    out_m.parent.mkdir(parents=True, exist_ok=True)
    with out_m.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"Écrit {out_csv} ({len(summary)} stations).")
    print(f"Manifeste : {out_m}")


if __name__ == "__main__":
    main()
