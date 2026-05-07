"""
Téléchargement Caravan (Zenodo) : extraction des CSV dont le chemin contient FILTER_KEY (ex. hysets).

Modes (.env) :
- CARAVAN_DOWNLOAD_MODE=file (recommandé sous Windows) : télécharge l’archive dans .cache/
  puis extraction locale (tar fiable, reprend après coupure réseau possible en relançant).
- CARAVAN_DOWNLOAD_MODE=stream : flux HTTP direct vers tarfile (économise ~29 Go sur disque
  mais fragile : coupures réseau, antivirus, seek interne tar/gzip).

Configuration : .env (python-dotenv), racine du projet.
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import requests
from dotenv import load_dotenv
from tqdm import tqdm
from urllib3.exceptions import ProtocolError

# --- Valeurs par défaut si absentes du .env ---
ZENODO_URL = "https://zenodo.org/records/15530022/files/Caravan-csv.tar.gz?download=1"
FILTER_KEY = "hysets"
# file = télécharger l’archive puis extraire (robuste) ; stream = tout en flux HTTP (fragile).
DEFAULT_DOWNLOAD_MODE = "file"

_RE_DRIVE_FOLDER = re.compile(r"/folders/([a-zA-Z0-9_-]+)")


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def verify_drive_path(save_path: Path) -> bool:
    """
    Vérifie que le chemin ressemble à un dossier cloud synchronisé localement
    (Google Drive ou Microsoft OneDrive).
    """
    resolved = str(save_path.resolve())
    norm = resolved.replace("\\", "/").lower()
    ok = (
        "google drive" in norm
        or "googledrive" in norm
        or "onedrive" in norm
    )
    if not ok:
        print(
            "\n⚠️  Avertissement : SAVE_PATH ne ressemble pas à un dossier Google Drive ou OneDrive.\n"
            "   Les fichiers seront écrits en local pur ; pour une synchro cloud, pointez vers\n"
            "   …/Mon Drive/…, …/Google Drive/… ou …/OneDrive/… sur ce PC.\n",
            file=sys.stderr,
        )
    return ok


def _extract_drive_folder_id(url: str) -> str | None:
    m = _RE_DRIVE_FOLDER.search(url.strip())
    return m.group(1) if m else None


def resolve_save_path(project_root: Path) -> tuple[Path, str | None]:
    """
    Détermine le dossier local d'écriture et, si SAVE_PATH est une URL Drive,
    l'ID du dossier partagé.

    - SAVE_PATH = chemin local → écriture directe.
    - SAVE_PATH = https://drive.google.com/drive/folders/... → pas d'écriture sur l'URL :
      écriture sous CARAVAN_LOCAL_SAVE si défini, sinon data/caravan à la racine du projet.
    """
    raw = os.environ.get("SAVE_PATH", "").strip()
    if not raw:
        return (project_root / "data" / "caravan").resolve(), None

    if raw.startswith("http://") or raw.startswith("https://"):
        folder_id = _extract_drive_folder_id(raw)
        local_raw = os.environ.get("CARAVAN_LOCAL_SAVE", "").strip()
        if local_raw:
            local = Path(local_raw).expanduser().resolve()
        else:
            local = (project_root / "data" / "caravan").resolve()
            print(
                "\nℹ️  SAVE_PATH est une URL Google Drive : l'écriture se fait sur le disque local :\n"
                f"   {local}\n"
                "   Pour viser directement un dossier Drive synchronisé, définissez soit :\n"
                "   • SAVE_PATH=C:\\…\\Mon Drive\\votre_dossier\n"
                "   • ou CARAVAN_LOCAL_SAVE avec ce chemin, en gardant SAVE_PATH comme URL de référence.\n",
                file=sys.stderr,
            )
        return local, folder_id

    return Path(raw).expanduser().resolve(), None


def ensure_output_directories(save_root: Path) -> None:
    """
    Crée SAVE_PATH/timeseries/ et SAVE_PATH/attributes/.staging/.
    Affiche une aide claire si le chemin est invalide (mauvais utilisateur Windows, OneDrive absent, etc.).
    Attend un save_root déjà résolu (chemins absolus).
    """
    ts_dir = save_root / "timeseries"
    attr_dir = save_root / "attributes"
    staging = attr_dir / ".staging"
    try:
        ts_dir.mkdir(parents=True, exist_ok=True)
        staging.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        home = Path.home()
        print(
            "\n❌ Impossible de créer les dossiers de sortie.\n"
            f"   SAVE_PATH résolu : {save_root}\n"
            f"   Erreur système : {exc!s}\n\n"
            "   Causes fréquentes sous Windows :\n"
            "   • Chemin avec un mauvais nom de profil : utilisez le dossier réel de votre compte,\n"
            "     par ex. celui renvoyé par Path.home() en Python ou %USERPROFILE% dans l’invite cmd.\n"
            f"     Ici, profil détecté : {home}\n"
            "   • Dossier OneDrive inexistant ou non synchronisé : créez d’abord H1_Caravan\\data dans\n"
            "     l’Explorateur de fichiers sous OneDrive, puis corrigez SAVE_PATH dans .env.\n"
            "   • Pas de droits d’écriture : ne pointez pas vers un lecteur ou un autre utilisateur protégé.\n",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc


def write_drive_ids_yaml(project_root: Path, parent_folder_id: str | None) -> Path:
    """Génère (ou écrase) data/config/drive_ids.yaml avec le modèle demandé."""
    path = project_root / "data" / "config" / "drive_ids.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)

    header_lines: list[str] = [
        f"# Généré par download_caravan.py le {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ]
    if parent_folder_id:
        header_lines.append(f"# ID dossier Drive (extrait de SAVE_PATH URL) : {parent_folder_id}")
    header_lines.extend(
        [
            "# Remplir les IDs après upload Google Drive",
            "# Récupérer l'ID depuis : Drive → fichier → clic droit → Partager",
            "# → Copier le lien → extraire l'ID entre /d/ et /view",
            "drive_ids:",
            '  niger: "REMPLACER_PAR_ID"',
            '  volta: "REMPLACER_PAR_ID"',
            '  senegal: "REMPLACER_PAR_ID"',
            '  attributes: "REMPLACER_PAR_ID"',
        ]
    )
    path.write_text("\n".join(header_lines) + "\n", encoding="utf-8")
    return path


class TqdmHTTPReader:
    """Enveloppe lecture du flux HTTP avec barre tqdm (octets lus)."""

    def __init__(self, response: requests.Response):
        self._raw = response.raw
        cl = response.headers.get("content-length")
        total = int(cl) if cl and cl.isdigit() else None
        self._pbar = tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="Streaming Zenodo",
        )

    def read(self, size: int = -1) -> bytes:
        chunk = self._raw.read(size)
        if chunk:
            self._pbar.update(len(chunk))
        return chunk

    def close(self) -> None:
        self._pbar.close()

    def __enter__(self) -> TqdmHTTPReader:
        return self

    def __exit__(self, *args) -> None:
        self.close()


def _safe_member_path(name: str) -> bool:
    if not name or name.startswith("/") or ".." in name:
        return False
    return True


def _classify_member(member: tarfile.TarInfo, key: str) -> str | None:
    if not member.isfile():
        return None
    n = member.name.replace("\\", "/")
    if key.lower() not in n.lower():
        return None
    if not n.lower().endswith(".csv"):
        return None
    lower = n.lower()
    if "/timeseries/" in lower:
        return "timeseries"
    if "/attributes/" in lower:
        return "attributes"
    return None


def extract_matching_members(
    tf: tarfile.TarFile,
    filter_key: str,
    ts_dir: Path,
    staging: Path,
) -> tuple[int, list[str], list[Path]]:
    ts_count = 0
    ts_samples: list[str] = []
    attr_staging_files: list[Path] = []
    for member in tf:
        if not _safe_member_path(member.name):
            continue
        kind = _classify_member(member, filter_key)
        if kind is None:
            continue
        fobj = tf.extractfile(member)
        if fobj is None:
            continue
        data = fobj.read()

        if kind == "timeseries":
            name = PurePosixPath(member.name).name
            out = ts_dir / name
            out.write_bytes(data)
            ts_count += 1
            if len(ts_samples) < 5:
                ts_samples.append(name)
        else:
            part = staging / PurePosixPath(member.name).name
            part.write_bytes(data)
            attr_staging_files.append(part)
    return ts_count, ts_samples, attr_staging_files


def _merge_attribute_csvs(sources: list[Path], destination: Path) -> None:
    if not sources:
        return
    if len(sources) == 1:
        shutil.copy2(sources[0], destination)
        return

    merged: dict[str, dict[str, str]] = {}
    field_order: list[str] = []

    for src in sorted(sources, key=lambda p: p.name):
        with src.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue
            for col in reader.fieldnames:
                if col not in field_order:
                    field_order.append(col)
            gid_key = reader.fieldnames[0]
            for row in reader:
                gid = row.get(gid_key, "")
                if gid not in merged:
                    merged[gid] = {}
                for k, v in row.items():
                    if k is None:
                        continue
                    merged[gid][k] = v if v is not None else ""

    if not merged:
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=field_order, extrasaction="ignore")
        writer.writeheader()
        for gid in sorted(merged.keys()):
            writer.writerow(merged[gid])


def download_tarball_to_file(url: str, dest: Path, headers: dict[str, str]) -> None:
    """Télécharge l’archive complète sur disque (barre de progression)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    chunk_size = 4 * 1024 * 1024
    with requests.get(
        url,
        stream=True,
        timeout=(120, None),
        headers=headers,
        allow_redirects=True,
    ) as resp:
        resp.raise_for_status()
        cl = resp.headers.get("content-length")
        total = int(cl) if cl and cl.isdigit() else None
        with (
            open(part, "wb") as out_f,
            tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Téléchargement archive",
            ) as pbar,
        ):
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    out_f.write(chunk)
                    pbar.update(len(chunk))
    part.replace(dest)


def _connection_error_hint() -> None:
    print(
        "\n💡 Connexion interrompue pendant le téléchargement (réseau, pare-feu, antivirus, VPN, veille).\n"
        "   • Utilisez le mode fichier (recommandé) : dans .env,\n"
        "     CARAVAN_DOWNLOAD_MODE=file\n"
        "   • Évitez de placer le cache sur OneDrive : par défaut il va dans le dossier .cache/ du projet.\n"
        "   • Vérifiez ~30 Go d’espace libre pour l’archive ; après extraction, CARAVAN_DELETE_ARCHIVE=1\n"
        "     supprime le .tar.gz du cache (défaut : le garder pour relancer sans retélécharger).\n",
        file=sys.stderr,
    )


def main() -> int:
    root = project_root()
    load_dotenv(root / ".env")

    zenodo_url = os.environ.get("ZENODO_URL", ZENODO_URL).strip() or ZENODO_URL
    filter_key = os.environ.get("FILTER_KEY", FILTER_KEY).strip() or FILTER_KEY
    mode = os.environ.get("CARAVAN_DOWNLOAD_MODE", DEFAULT_DOWNLOAD_MODE).strip().lower()
    if mode not in ("file", "stream"):
        print(f"Valeur invalide pour CARAVAN_DOWNLOAD_MODE : {mode!r} (attendu : file ou stream)", file=sys.stderr)
        return 1

    save_root, drive_folder_id = resolve_save_path(root)
    save_root = save_root.resolve()
    verify_drive_path(save_root)
    ensure_output_directories(save_root)

    ts_dir = save_root / "timeseries"
    attr_dir = save_root / "attributes"
    staging = attr_dir / ".staging"

    headers = {"User-Agent": "Caravan-hysets-stream/1.0 (research; +https://zenodo.org/records/15530022)"}

    ts_count = 0
    ts_samples: list[str] = []
    attr_staging_files: list[Path] = []

    print(f"Mode téléchargement : {mode} (CARAVAN_DOWNLOAD_MODE dans .env)\n")

    try:
        if mode == "file":
            cache_raw = os.environ.get("CARAVAN_ARCHIVE_PATH", "").strip()
            if cache_raw:
                archive_path = Path(cache_raw).expanduser().resolve()
            else:
                archive_path = (root / ".cache" / "Caravan-csv.tar.gz").resolve()
            if not archive_path.is_file():
                print(f"Téléchargement vers : {archive_path}\n", file=sys.stderr)
                download_tarball_to_file(zenodo_url, archive_path, headers)
            else:
                print(f"Archive déjà présente, réutilisation : {archive_path}\n", file=sys.stderr)
            with tarfile.open(archive_path, mode="r:gz") as tf:
                ts_count, ts_samples, attr_staging_files = extract_matching_members(
                    tf, filter_key, ts_dir, staging
                )
            if os.environ.get("CARAVAN_DELETE_ARCHIVE", "").strip().lower() in ("1", "true", "yes", "oui"):
                archive_path.unlink(missing_ok=True)
                print(f"Archive supprimée : {archive_path}\n", file=sys.stderr)
        else:
            resp = requests.get(
                zenodo_url,
                stream=True,
                timeout=(120, None),
                headers=headers,
                allow_redirects=True,
            )
            try:
                resp.raise_for_status()
                resp.raw.decode_content = False
                reader = TqdmHTTPReader(resp)
                try:
                    with tarfile.open(fileobj=reader, mode="r|gz") as tf:
                        ts_count, ts_samples, attr_staging_files = extract_matching_members(
                            tf, filter_key, ts_dir, staging
                        )
                finally:
                    reader.close()
            finally:
                resp.close()
    except requests.RequestException as e:
        print(f"Erreur HTTP : {e}", file=sys.stderr)
        _connection_error_hint()
        return 1
    except (tarfile.TarError, OSError, ProtocolError) as e:
        print(f"Erreur archive / réseau : {e}", file=sys.stderr)
        _connection_error_hint()
        return 1

    attr_out = attr_dir / "hysets_attributes.csv"
    if attr_staging_files:
        _merge_attribute_csvs(attr_staging_files, attr_out)
        for p in attr_staging_files:
            p.unlink(missing_ok=True)
    shutil.rmtree(staging, ignore_errors=True)

    drive_yaml = write_drive_ids_yaml(root, drive_folder_id)

    save_display = str(save_root.resolve())

    print()
    print("✅ Extraction terminée")
    print(f"📁 Fichiers extraits dans : {save_display}")
    print("📋 Fichiers créés :")
    if ts_samples:
        for s in ts_samples:
            print(f"   - {s} (timeseries)")
        if ts_count > len(ts_samples):
            print(f"   - … et {ts_count - len(ts_samples)} autre(s) fichier(s) hysets_*.csv (timeseries)")
    else:
        print("   - (aucune série temporelle extraite — vérifier FILTER_KEY et l’archive)")
    print("   - hysets_attributes.csv (attributes)")
    print()
    print(f"👉 Prochaine étape : ouvrir {drive_yaml}")
    print("   et remplir les IDs Google Drive (niger, volta, senegal, attributes).")
    if drive_folder_id:
        print(f"   (ID du dossier partagé indiqué en commentaire en tête du YAML : {drive_folder_id})")
    print()
    print(
        "Conseil : si vous aviez déjà édité drive_ids.yaml avec de vrais IDs, "
        "restaurez-les depuis Git ou une copie : ce fichier est régénéré à chaque extraction réussie."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
