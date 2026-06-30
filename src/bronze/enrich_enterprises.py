#!/usr/bin/env python3
"""
Traite un petit lot d'entreprises depuis PostgreSQL.

Le worker :
- réserve un lot PENDING/RETRY dans PostgreSQL ;
- récupère les fichiers externes ;
- les écrit dans HDFS avec la classe BronzeIngestion existante ;
- met à jour le statut de reprise dans PostgreSQL.

Ne pas lancer des millions d'entreprises en une seule fois.
Commencer avec --limit 1 puis --limit 10.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.bronze.ingest_bronze_hdfs import BronzeIngestion


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bce"),
        user=os.getenv("POSTGRES_USER", "bce_user"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def claim_batch(
    connection: psycopg.Connection,
    limit: int,
) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH picked AS (
                SELECT entity_number
                FROM pipeline.enrichment_queue
                WHERE status IN ('PENDING', 'RETRY')
                  AND (
                      next_retry_at IS NULL
                      OR next_retry_at <= NOW()
                  )
                ORDER BY priority, entity_number
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE pipeline.enrichment_queue AS queue
            SET
                status = 'RUNNING',
                attempts = attempts + 1,
                started_at = NOW(),
                finished_at = NULL,
                error_message = NULL,
                updated_at = NOW()
            FROM picked
            WHERE queue.entity_number = picked.entity_number
            RETURNING queue.entity_number
            """,
            (limit,),
        )
        entities = [row[0] for row in cursor.fetchall()]

    connection.commit()
    return entities


def update_status(
    connection: psycopg.Connection,
    entity_number: str,
    status: str,
    error_message: str | None = None,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline.enrichment_queue
            SET
                status = %s,
                finished_at = NOW(),
                error_message = %s,
                updated_at = NOW()
            WHERE entity_number = %s
            """,
            (status, error_message, entity_number),
        )

    connection.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrichit un lot d'entreprises vers HDFS."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Nombre d'entreprises à traiter pendant cette exécution.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
    )
    parser.add_argument(
        "--include-establishment-details",
        action="store_true",
    )
    parser.add_argument(
        "--sleep-between",
        type=float,
        default=1.0,
        help="Pause entre deux entreprises.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    database = connect()
    entities = claim_batch(database, args.limit)

    if not entities:
        print("Aucune entreprise PENDING/RETRY à traiter.")
        database.close()
        return 0

    print(f"Entreprises réservées : {len(entities)}")

    ingestion = BronzeIngestion(
        hdfs_url=os.getenv("HDFS_URL", "http://localhost:9870"),
        hdfs_user=os.getenv("HDFS_USER", "root"),
        hdfs_base=os.getenv("HDFS_BASE", "/datalake/bronze"),
        max_pages=args.max_pages,
        include_establishment_details=args.include_establishment_details,
        dry_run=False,
    )
    ingestion.check_hdfs()

    cookie = os.getenv("COOKIE_NOTAIRE")

    for index, entity_number in enumerate(entities, start=1):
        print(
            f"\n[{index}/{len(entities)}] "
            f"Entreprise {entity_number}"
        )

        errors: list[str] = []

        tasks = [
            ("KBO entreprise", ingestion.ingest_kbo_enterprise),
            ("KBO établissements", ingestion.ingest_kbo_establishments),
            ("eJustice", ingestion.ingest_ejustice),
        ]

        for label, task in tasks:
            try:
                task(entity_number)
            except Exception as exc:
                message = f"{label}: {type(exc).__name__}: {exc}"
                print(f"ERREUR {message}")
                errors.append(message)

        try:
            deposits = ingestion.fetch_cbso_deposits(entity_number)
            ingestion.ingest_nbb_documents(entity_number, deposits)
        except Exception as exc:
            message = f"NBB: {type(exc).__name__}: {exc}"
            print(f"ERREUR {message}")
            errors.append(message)

        try:
            ingestion.ingest_notaire(entity_number, cookie)
        except Exception as exc:
            message = f"Notaire: {type(exc).__name__}: {exc}"
            print(f"ERREUR {message}")
            errors.append(message)

        if errors:
            update_status(
                database,
                entity_number,
                "PARTIAL",
                " | ".join(errors)[:10000],
            )
        else:
            update_status(
                database,
                entity_number,
                "SUCCESS",
            )

        time.sleep(args.sleep_between)

    manifest_path = ingestion.write_manifest()
    print(f"\nManifeste : {manifest_path}")

    database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
