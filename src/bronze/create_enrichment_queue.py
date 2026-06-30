#!/usr/bin/env python3
"""
Crée une file d'enrichissement contenant tous les numéros d'entreprise.

La table source est `bronze.enterprise`. La colonne du numéro d'entreprise
est détectée automatiquement parmi plusieurs noms possibles.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bce"),
        user=os.getenv("POSTGRES_USER", "bce_user"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def find_enterprise_number_column(
    connection: psycopg.Connection,
) -> str:
    candidates = [
        "enterprise_number",
        "entity_number",
        "enterprisenumber",
        "entitynumber",
        "number",
    ]

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = 'enterprise'
            ORDER BY ordinal_position
            """
        )
        columns = [row[0] for row in cursor.fetchall()]

    for candidate in candidates:
        if candidate in columns:
            return candidate

    raise RuntimeError(
        "Colonne du numéro d'entreprise introuvable dans bronze.enterprise. "
        f"Colonnes détectées : {columns}"
    )


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    with connect() as connection:
        enterprise_column = find_enterprise_number_column(connection)

        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS pipeline")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline.enrichment_queue (
                    entity_number TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK (
                            status IN (
                                'PENDING',
                                'RUNNING',
                                'SUCCESS',
                                'PARTIAL',
                                'NO_DATA',
                                'RETRY',
                                'FAILED'
                            )
                        ),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    priority INTEGER NOT NULL DEFAULT 100,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    next_retry_at TIMESTAMPTZ,
                    error_message TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

            cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO pipeline.enrichment_queue (entity_number)
                    SELECT DISTINCT
                        LPAD(
                            REGEXP_REPLACE({}, '[^0-9]', '', 'g'),
                            10,
                            '0'
                        )
                    FROM bronze.enterprise
                    WHERE {} IS NOT NULL
                      AND BTRIM({}) <> ''
                    ON CONFLICT (entity_number) DO NOTHING
                    """
                ).format(
                    sql.Identifier(enterprise_column),
                    sql.Identifier(enterprise_column),
                    sql.Identifier(enterprise_column),
                )
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_enrichment_queue_status
                ON pipeline.enrichment_queue (
                    status,
                    priority,
                    entity_number
                )
                """
            )

            cursor.execute(
                """
                SELECT status, COUNT(*)
                FROM pipeline.enrichment_queue
                GROUP BY status
                ORDER BY status
                """
            )

            print(
                "Colonne utilisée : "
                f"bronze.enterprise.{enterprise_column}"
            )
            print("\nÉtat de la file :")
            for status, count in cursor.fetchall():
                print(f"- {status}: {count:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
