#!/usr/bin/env python3
"""
Charge les CSV Open Data BCE/KBO dans PostgreSQL.

- Une table brute PostgreSQL est créée par fichier CSV dans le schéma `bronze`.
- Toutes les colonnes sont chargées en TEXT pour conserver les valeurs sources.
- Le chargement utilise COPY FROM STDIN, adapté aux très gros fichiers.
- Chaque fichier est chargé dans sa propre transaction.
- Une table d'audit est tenue dans `pipeline.ingestion_audit`.

Exemples :
    python src/bronze/load_kbo_postgres.py --files enterprise denomination
    python src/bronze/load_kbo_postgres.py --files activity
    python src/bronze/load_kbo_postgres.py
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Iterable

import psycopg
from dotenv import load_dotenv
from psycopg import sql


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

DEFAULT_FILES = [
    "enterprise",
    "denomination",
    "address",
    "contact",
    "establishment",
    "branch",
    "code",
    "meta",
    "activity",
]


def snake_case(value: str) -> str:
    value = value.strip().lstrip("\ufeff")
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^0-9A-Za-z]+", "_", value)
    value = value.strip("_").lower()

    if not value:
        value = "column"

    if value[0].isdigit():
        value = f"column_{value}"

    return value


def unique_names(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    counters: dict[str, int] = {}

    for value in values:
        base = snake_case(value)
        count = counters.get(base, 0)
        counters[base] = count + 1
        result.append(base if count == 0 else f"{base}_{count + 1}")

    return result


def detect_encoding(path: Path) -> str:
    sample = path.read_bytes()[:1024 * 1024]

    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            sample.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue

    return "latin1"


def detect_delimiter(path: Path, encoding: str) -> str:
    with path.open("r", encoding=encoding, newline="") as stream:
        sample = stream.read(128 * 1024)

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except csv.Error:
        first_line = sample.splitlines()[0] if sample else ""
        candidates = [",", ";", "|", "\t"]
        return max(candidates, key=first_line.count)


def read_header(path: Path, encoding: str, delimiter: str) -> list[str]:
    with path.open("r", encoding=encoding, newline="") as stream:
        reader = csv.reader(stream, delimiter=delimiter)
        try:
            raw_header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"Le fichier est vide : {path}") from exc

    columns = unique_names(raw_header)

    if len(columns) < 1:
        raise ValueError(f"Aucune colonne détectée dans {path}")

    return columns


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bce"),
        user=os.getenv("POSTGRES_USER", "bce_user"),
        password=os.getenv("POSTGRES_PASSWORD"),
        autocommit=False,
    )


def ensure_database_objects(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS pipeline")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline.ingestion_audit (
                run_id UUID NOT NULL,
                source_file TEXT NOT NULL,
                table_name TEXT NOT NULL,
                status TEXT NOT NULL,
                encoding TEXT,
                delimiter TEXT,
                row_count BIGINT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                error_message TEXT,
                PRIMARY KEY (run_id, source_file)
            )
            """
        )

    connection.commit()


def create_raw_table(
    cursor: psycopg.Cursor,
    table_name: str,
    columns: list[str],
) -> None:
    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS bronze.{}").format(
            sql.Identifier(table_name)
        )
    )

    column_definitions = sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(column))
        for column in columns
    )

    cursor.execute(
        sql.SQL("CREATE TABLE bronze.{} ({})").format(
            sql.Identifier(table_name),
            column_definitions,
        )
    )


def copy_csv(
    cursor: psycopg.Cursor,
    path: Path,
    table_name: str,
    columns: list[str],
    encoding: str,
    delimiter: str,
) -> int:
    copy_statement = sql.SQL(
        """
        COPY bronze.{} ({})
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE,
            DELIMITER {},
            QUOTE '"',
            ESCAPE '"'
        )
        """
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.Literal(delimiter),
    )

    with cursor.copy(copy_statement) as copy:
        with path.open("r", encoding=encoding, newline="") as stream:
            while chunk := stream.read(4 * 1024 * 1024):
                copy.write(chunk)

    return cursor.rowcount if cursor.rowcount >= 0 else 0


def create_useful_index(
    cursor: psycopg.Cursor,
    table_name: str,
    columns: list[str],
) -> None:
    candidates = [
        "enterprise_number",
        "entity_number",
        "establishment_number",
    ]

    indexed_column = next(
        (candidate for candidate in candidates if candidate in columns),
        None,
    )

    if indexed_column is None:
        return

    index_name = f"idx_{table_name}_{indexed_column}"

    cursor.execute(
        sql.SQL("CREATE INDEX {} ON bronze.{} ({})").format(
            sql.Identifier(index_name),
            sql.Identifier(table_name),
            sql.Identifier(indexed_column),
        )
    )


def load_one_file(
    connection: psycopg.Connection,
    stem: str,
    run_id: uuid.UUID,
    create_indexes: bool,
) -> None:
    path = DATA_DIR / f"{stem}.csv"

    if not path.exists():
        raise FileNotFoundError(f"Fichier absent : {path}")

    table_name = snake_case(stem)
    encoding = detect_encoding(path)
    delimiter = detect_delimiter(path, encoding)
    columns = read_header(path, encoding, delimiter)
    started = time.perf_counter()

    print(
        f"\n[{stem}] fichier={path.name} "
        f"encodage={encoding} séparateur={repr(delimiter)} "
        f"colonnes={len(columns)}"
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline.ingestion_audit (
                    run_id,
                    source_file,
                    table_name,
                    status,
                    encoding,
                    delimiter
                )
                VALUES (%s, %s, %s, 'RUNNING', %s, %s)
                """,
                (
                    run_id,
                    path.name,
                    f"bronze.{table_name}",
                    encoding,
                    delimiter,
                ),
            )

            create_raw_table(cursor, table_name, columns)
            copied_rows = copy_csv(
                cursor=cursor,
                path=path,
                table_name=table_name,
                columns=columns,
                encoding=encoding,
                delimiter=delimiter,
            )

            if create_indexes:
                create_useful_index(cursor, table_name, columns)

            cursor.execute(
                sql.SQL("ANALYZE bronze.{}").format(
                    sql.Identifier(table_name)
                )
            )

            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM bronze.{}").format(
                    sql.Identifier(table_name)
                )
            )
            exact_rows = cursor.fetchone()[0]

            cursor.execute(
                """
                UPDATE pipeline.ingestion_audit
                SET
                    status = 'SUCCESS',
                    row_count = %s,
                    finished_at = NOW()
                WHERE run_id = %s
                  AND source_file = %s
                """,
                (exact_rows, run_id, path.name),
            )

        connection.commit()

        duration = time.perf_counter() - started
        print(
            f"[{stem}] OK : {exact_rows:,} lignes "
            f"(COPY={copied_rows:,}) en {duration / 60:.1f} min"
        )

    except Exception as exc:
        connection.rollback()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline.ingestion_audit (
                    run_id,
                    source_file,
                    table_name,
                    status,
                    encoding,
                    delimiter,
                    finished_at,
                    error_message
                )
                VALUES (%s, %s, %s, 'FAILED', %s, %s, NOW(), %s)
                ON CONFLICT (run_id, source_file)
                DO UPDATE SET
                    status = 'FAILED',
                    finished_at = NOW(),
                    error_message = EXCLUDED.error_message
                """,
                (
                    run_id,
                    path.name,
                    f"bronze.{table_name}",
                    encoding,
                    delimiter,
                    str(exc),
                ),
            )

        connection.commit()
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Charge les CSV BCE/KBO dans PostgreSQL."
    )
    parser.add_argument(
        "--files",
        nargs="+",
        choices=DEFAULT_FILES,
        default=DEFAULT_FILES,
        help="Fichiers à charger, sans extension.",
    )
    parser.add_argument(
        "--create-indexes",
        action="store_true",
        help=(
            "Crée les index après chargement. "
            "Pour activity.csv, cela peut être long."
        ),
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    run_id = uuid.uuid4()

    print(f"Run PostgreSQL : {run_id}")
    print(f"Dossier source : {DATA_DIR}")

    connection = connect()

    try:
        ensure_database_objects(connection)

        for stem in args.files:
            load_one_file(
                connection=connection,
                stem=stem,
                run_id=run_id,
                create_indexes=args.create_indexes,
            )

    finally:
        connection.close()

    print("\nChargement PostgreSQL terminé.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
