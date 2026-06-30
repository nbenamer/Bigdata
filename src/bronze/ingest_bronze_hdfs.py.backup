#!/usr/bin/env python3
"""
Ingestion Bronze autonome pour le projet BCE/KBO.

Ce script ne dépend pas du notebook. Il :
1. ingère les CSV déjà présents dans data/ ;
2. récupère les pages HTML brutes de la BCE/KBO ;
3. récupère les dépôts NBB/CBSO en JSON et leurs CSV/PDF ;
4. récupère les pages eJustice en HTML ;
5. récupère les statuts Notaire en JSON si COOKIE_NOTAIRE est défini ;
6. copie tous les fichiers bruts dans HDFS via WebHDFS.

Prérequis :
    pip install requests beautifulsoup4 hdfs python-dotenv

Variables d'environnement facultatives :
    HDFS_URL=http://localhost:9870
    HDFS_USER=root
    HDFS_BASE=/datalake/bronze
    COOKIE_NOTAIRE=...
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

try:
    from hdfs import InsecureClient
except ImportError:
    print(
        "Le paquet Python 'hdfs' est absent.\n"
        "Installe-le avec : pip install hdfs",
        file=sys.stderr,
    )
    raise


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
LANDING_DIR = DATA_DIR / "landing"

ENTITIES = {
    "google": "0878065378",
    "apple": "0836157420",
    "sncb": "0203430576",
}

KBO_ROOT = "https://kbopub.economie.fgov.be/kbopub/"
KBO_ENTERPRISE = urljoin(KBO_ROOT, "toonondernemingps.html")
KBO_ESTABLISHMENTS = urljoin(KBO_ROOT, "vestiginglijst.html")

EJUSTICE_ROOT = "https://www.ejustice.just.fgov.be"
EJUSTICE_LIST = f"{EJUSTICE_ROOT}/cgi_tsv/list.pl"

CBSO_API = "https://consult.cbso.nbb.be/api/rs-consult/published-deposits"
CBSO_BROKER = "https://consult.cbso.nbb.be/api/external/broker/public/deposits"

NOTAIRE_API = (
    "https://statuts.notaire.be/stapor_v1/api/enterprises/{number}/statutes"
)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BCE-Bronze-Ingestion/1.0)",
    "Accept-Language": "fr-FR,fr;q=0.9",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_number(value: str) -> str:
    return re.sub(r"\D", "", value).zfill(10)


def kbo_query_number(value: str) -> str:
    return normalize_number(value).lstrip("0")


class BronzeIngestion:
    def __init__(
        self,
        hdfs_url: str,
        hdfs_user: str,
        hdfs_base: str,
        max_pages: int,
        include_establishment_details: bool,
        dry_run: bool,
    ) -> None:
        self.hdfs_url = hdfs_url.rstrip("/")
        self.hdfs_user = hdfs_user
        self.hdfs_base = hdfs_base.rstrip("/")
        self.max_pages = max_pages
        self.include_establishment_details = include_establishment_details
        self.dry_run = dry_run

        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.ingestion_date = datetime.now().strftime("%Y-%m-%d")
        self.run_dir = LANDING_DIR / f"run_{self.run_id}"
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

        self.hdfs = None if dry_run else InsecureClient(
            self.hdfs_url,
            user=self.hdfs_user,
        )

        self.manifest: list[dict[str, Any]] = []

    def check_hdfs(self) -> None:
        if self.dry_run:
            print("[DRY-RUN] Connexion HDFS non testée.")
            return

        try:
            status = self.hdfs.status("/", strict=False)
        except Exception as exc:
            raise RuntimeError(
                f"Connexion HDFS impossible sur {self.hdfs_url}. "
                "Vérifie que le NameNode/WebHDFS fonctionne."
            ) from exc

        if status is None:
            raise RuntimeError("La racine HDFS est introuvable.")

        print(f"HDFS connecté : {self.hdfs_url} (utilisateur {self.hdfs_user})")

    def hdfs_path(
        self,
        source: str,
        filename: str,
        entity_number: str | None = None,
    ) -> str:
        parts = [
            self.hdfs_base,
            source,
            f"ingestion_date={self.ingestion_date}",
        ]
        if entity_number:
            parts.append(f"entity_number={normalize_number(entity_number)}")
        parts.append(filename)
        return "/" + "/".join(part.strip("/") for part in parts if part)

    def register_and_upload(
        self,
        local_path: Path,
        source: str,
        source_url: str | None = None,
        entity_number: str | None = None,
    ) -> None:
        destination = self.hdfs_path(
            source=source,
            filename=local_path.name,
            entity_number=entity_number,
        )

        entry = {
            "run_id": self.run_id,
            "ingested_at_utc": utc_now(),
            "source": source,
            "entity_number": (
                normalize_number(entity_number) if entity_number else None
            ),
            "source_url": source_url,
            "local_path": str(local_path.relative_to(PROJECT_ROOT)),
            "hdfs_path": destination,
            "size_bytes": local_path.stat().st_size,
            "sha256": sha256_file(local_path),
        }

        if self.dry_run:
            print(f"[DRY-RUN] {local_path} -> {destination}")
        else:
            parent = destination.rsplit("/", 1)[0]
            self.hdfs.makedirs(parent)
            self.hdfs.upload(destination, str(local_path), overwrite=True)
            print(f"OK HDFS : {destination}")

        self.manifest.append(entry)

    def save_bytes(
        self,
        source: str,
        filename: str,
        content: bytes,
        source_url: str | None = None,
        entity_number: str | None = None,
    ) -> Path:
        directory = self.run_dir / source
        if entity_number:
            directory /= f"entity_number={normalize_number(entity_number)}"
        directory.mkdir(parents=True, exist_ok=True)

        path = directory / filename
        path.write_bytes(content)

        self.register_and_upload(
            path,
            source=source,
            source_url=source_url,
            entity_number=entity_number,
        )
        return path

    def save_json(
        self,
        source: str,
        filename: str,
        value: Any,
        source_url: str | None = None,
        entity_number: str | None = None,
    ) -> Path:
        content = json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            default=str,
        ).encode("utf-8")

        return self.save_bytes(
            source=source,
            filename=filename,
            content=content,
            source_url=source_url,
            entity_number=entity_number,
        )

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 60,
    ) -> requests.Response:
        response = self.session.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        return response

    # ------------------------------------------------------------------
    # 1. CSV locaux BCE/KBO
    # ------------------------------------------------------------------
    def ingest_local_csv_files(self) -> None:
        csv_files = sorted(DATA_DIR.glob("*.csv"))

        if not csv_files:
            print("Aucun CSV trouvé directement dans data/.")
            return

        print(f"\nCSV locaux trouvés : {len(csv_files)}")
        for path in csv_files:
            self.register_and_upload(
                path,
                source="kbo_open_data",
            )

    # ------------------------------------------------------------------
    # 2. BCE/KBO Public Search : HTML brut
    # ------------------------------------------------------------------
    def ingest_kbo_enterprise(self, entity_number: str) -> None:
        params = {
            "lang": "fr",
            "ondernemingsnummer": kbo_query_number(entity_number),
        }
        response = self.get(KBO_ENTERPRISE, params=params)
        self.save_bytes(
            source="kbo_public_search",
            filename="enterprise.html",
            content=response.content,
            source_url=response.url,
            entity_number=entity_number,
        )

    def ingest_kbo_establishments(self, entity_number: str) -> None:
        seen_establishments: set[str] = set()

        for page in range(1, self.max_pages + 1):
            params = {
                "lang": "fr",
                "ondernemingsnummer": kbo_query_number(entity_number),
                "page": page,
            }

            response = self.get(KBO_ESTABLISHMENTS, params=params)
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all(
                "a",
                href=re.compile(r"toonvestigingps\.html\?vestigingsnummer="),
            )

            if not links:
                break

            self.save_bytes(
                source="kbo_establishments",
                filename=f"list_page_{page:03d}.html",
                content=response.content,
                source_url=response.url,
                entity_number=entity_number,
            )

            if self.include_establishment_details:
                for link in links:
                    match = re.search(
                        r"vestigingsnummer=(\d+)",
                        link.get("href", ""),
                    )
                    if not match:
                        continue

                    establishment_number = match.group(1)
                    if establishment_number in seen_establishments:
                        continue
                    seen_establishments.add(establishment_number)

                    detail_url = urljoin(KBO_ROOT, link["href"])
                    detail_response = self.get(detail_url)

                    self.save_bytes(
                        source="kbo_establishment_details",
                        filename=f"{establishment_number}.html",
                        content=detail_response.content,
                        source_url=detail_response.url,
                        entity_number=entity_number,
                    )
                    time.sleep(0.25)

            time.sleep(0.25)

    # ------------------------------------------------------------------
    # 3. NBB/CBSO : JSON brut + PDF/CSV
    # ------------------------------------------------------------------
    def fetch_cbso_deposits(self, entity_number: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 0

        while True:
            params = {
                "page": page,
                "size": 100,
                "enterpriseNumber": normalize_number(entity_number),
                "sort": ["periodEndDate,desc", "depositDate,desc"],
            }

            response = self.get(
                CBSO_API,
                params=params,
                headers={"Accept": "application/json"},
            )
            payload = response.json()
            batch = payload.get("content", [])
            items.extend(batch)

            if payload.get("last", True) or not batch:
                break

            page += 1
            time.sleep(0.3)

        self.save_json(
            source="nbb_deposits",
            filename="deposits.json",
            value=items,
            source_url=CBSO_API,
            entity_number=entity_number,
        )
        return items

    @staticmethod
    def selected_deposits(
        deposits: list[dict[str, Any]],
        year_min: int = 2021,
        year_max: int = 2025,
    ) -> dict[int, dict[str, Any]]:
        selected: dict[int, dict[str, Any]] = {}

        for deposit in deposits:
            model_id = str(deposit.get("modelId") or "").lower()
            model_name = str(deposit.get("modelName") or "").lower()

            if (
                model_id.startswith(("m120", "m122", "mc"))
                or "consolid" in model_name
                or "geconsolideerde" in model_name
            ):
                continue

            year = deposit.get("periodEndDateYear")
            if not isinstance(year, int) or not year_min <= year <= year_max:
                continue

            current = selected.get(year)
            if current is None:
                selected[year] = deposit
                continue

            candidate_fr = str(deposit.get("language") or "").upper() == "FR"
            current_fr = str(current.get("language") or "").upper() == "FR"
            current_is_partial = str(current.get("modelId") or "").endswith("-p")
            candidate_is_partial = str(deposit.get("modelId") or "").endswith("-p")

            if (
                (candidate_fr and not current_fr)
                or (current_is_partial and not candidate_is_partial)
            ):
                selected[year] = deposit

        return dict(sorted(selected.items()))

    def ingest_nbb_documents(
        self,
        entity_number: str,
        deposits: list[dict[str, Any]],
    ) -> None:
        for year, deposit in self.selected_deposits(deposits).items():
            deposit_id = deposit.get("id")
            if not deposit_id:
                continue

            pdf_url = f"{CBSO_BROKER}/pdf/{deposit_id}"
            pdf_response = self.get(pdf_url, timeout=120)
            self.save_bytes(
                source="nbb_documents",
                filename=f"{year}.pdf",
                content=pdf_response.content,
                source_url=pdf_response.url,
                entity_number=entity_number,
            )

            csv_url = f"{CBSO_BROKER}/consult/csv/{deposit_id}"
            csv_response = self.get(csv_url, timeout=120)
            self.save_bytes(
                source="nbb_documents",
                filename=f"{year}.csv",
                content=csv_response.content,
                source_url=csv_response.url,
                entity_number=entity_number,
            )

            time.sleep(0.4)

    # ------------------------------------------------------------------
    # 4. eJustice : pages HTML brutes
    # ------------------------------------------------------------------
    def ingest_ejustice(self, entity_number: str) -> None:
        for page in range(1, self.max_pages + 1):
            params = {
                "language": "fr",
                "btw": normalize_number(entity_number),
                "page": page,
            }

            response = self.get(EJUSTICE_LIST, params=params)
            soup = BeautifulSoup(response.text, "html.parser")
            blocks = soup.select("div.list-item--content")

            if not blocks:
                break

            self.save_bytes(
                source="ejustice",
                filename=f"page_{page:03d}.html",
                content=response.content,
                source_url=response.url,
                entity_number=entity_number,
            )
            time.sleep(0.3)

    # ------------------------------------------------------------------
    # 5. Notaire : JSON brut, cookie uniquement via .env
    # ------------------------------------------------------------------
    def ingest_notaire(
        self,
        entity_number: str,
        cookie: str | None,
    ) -> None:
        if not cookie:
            print(
                f"Notaire ignoré pour {entity_number} : "
                "COOKIE_NOTAIRE absent."
            )
            return

        url = NOTAIRE_API.format(number=normalize_number(entity_number))
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": (
                "https://statuts.notaire.be/stapor_v1/enterprise/"
                f"{normalize_number(entity_number)}/statutes"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Cookie": cookie,
        }

        items: list[dict[str, Any]] = []
        offset = 0
        limit = 20
        total: int | None = None

        for _ in range(self.max_pages):
            params = {
                "deedDate": "",
                "offset": offset,
                "limit": limit,
            }

            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 429:
                print("Notaire : HTTP 429, arrêt temporaire.")
                break

            response.raise_for_status()

            if "json" not in response.headers.get("content-type", "").lower():
                print("Notaire : réponse non JSON, cookie probablement expiré.")
                break

            payload = response.json()
            items.extend(payload.get("statutes", []))
            total = payload.get("totalItems", total)

            offset += limit
            if total is not None and offset >= total:
                break

            time.sleep(0.5)

        self.save_json(
            source="notaire",
            filename="statutes.json",
            value=items,
            source_url=url,
            entity_number=entity_number,
        )

    def write_manifest(self) -> Path:
        manifest_dir = self.run_dir / "manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = manifest_dir / f"manifest_{self.run_id}.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "run_id": self.run_id,
                    "ingestion_date": self.ingestion_date,
                    "created_at_utc": utc_now(),
                    "hdfs_url": self.hdfs_url,
                    "hdfs_base": self.hdfs_base,
                    "file_count": len(self.manifest),
                    "files": self.manifest,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        self.register_and_upload(
            manifest_path,
            source="_manifests",
        )
        return manifest_path

    def run(self, entities: dict[str, str], cookie: str | None) -> None:
        self.check_hdfs()
        self.ingest_local_csv_files()

        for name, entity_number in entities.items():
            print(f"\n{'=' * 70}")
            print(f"Ingestion de {name.upper()} — {entity_number}")
            print(f"{'=' * 70}")

            tasks = [
                ("KBO entreprise", self.ingest_kbo_enterprise),
                ("KBO établissements", self.ingest_kbo_establishments),
                ("eJustice", self.ingest_ejustice),
            ]

            for label, task in tasks:
                try:
                    task(entity_number)
                except Exception as exc:
                    print(f"ERREUR {label} : {exc}", file=sys.stderr)

            try:
                deposits = self.fetch_cbso_deposits(entity_number)
                self.ingest_nbb_documents(entity_number, deposits)
            except Exception as exc:
                print(f"ERREUR NBB : {exc}", file=sys.stderr)

            try:
                self.ingest_notaire(entity_number, cookie)
            except Exception as exc:
                print(f"ERREUR Notaire : {exc}", file=sys.stderr)

        manifest_path = self.write_manifest()

        print(f"\nIngestion terminée.")
        print(f"Fichiers enregistrés : {len(self.manifest)}")
        print(f"Manifeste local : {manifest_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingestion autonome des données brutes BCE vers HDFS."
    )
    parser.add_argument(
        "--entity",
        choices=list(ENTITIES),
        action="append",
        help=(
            "Entreprise à ingérer. Répéter l'option pour plusieurs entreprises. "
            "Par défaut : google, apple et sncb."
        ),
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Nombre maximal de pages par source.",
    )
    parser.add_argument(
        "--include-establishment-details",
        action="store_true",
        help="Télécharger aussi chaque page individuelle d'établissement.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collecter localement sans envoyer dans HDFS.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    selected_names = args.entity or list(ENTITIES)
    selected_entities = {
        name: ENTITIES[name]
        for name in selected_names
    }

    ingestion = BronzeIngestion(
        hdfs_url=os.getenv("HDFS_URL", "http://localhost:9870"),
        hdfs_user=os.getenv("HDFS_USER", "root"),
        hdfs_base=os.getenv("HDFS_BASE", "/datalake/bronze"),
        max_pages=args.max_pages,
        include_establishment_details=args.include_establishment_details,
        dry_run=args.dry_run,
    )

    ingestion.run(
        entities=selected_entities,
        cookie=os.getenv("COOKIE_NOTAIRE"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
