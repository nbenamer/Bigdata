#!/usr/bin/env python3
"""
Construit le modèle PostgreSQL orienté "entreprise" à partir des tables brutes
du schéma bronze.

Résultat principal :
    company.company

Une ligne par entreprise avec :
- numéro BCE normalisé ;
- dénomination officielle ;
- statut ;
- situation juridique ;
- forme juridique ;
- type d'entreprise ;
- date de création ;
- adresse principale ;
- téléphone, e-mail et site web.

Le script crée également des vues détaillées pour les dénominations, adresses,
contacts, activités et établissements, ainsi que les tables destinées aux
enrichissements externes (KBO web, dirigeants, liens, eJustice, NBB, finances).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

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
        autocommit=False,
    )


def table_columns(
    connection: psycopg.Connection,
    schema: str,
    table: str,
) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [row[0] for row in cursor.fetchall()]


def require_column(
    connection: psycopg.Connection,
    table: str,
    candidates: Iterable[str],
) -> str:
    columns = table_columns(connection, "bronze", table)

    for candidate in candidates:
        if candidate in columns:
            return candidate

    raise RuntimeError(
        f"Colonne introuvable dans bronze.{table}. "
        f"Candidats={list(candidates)} ; colonnes={columns}"
    )


def optional_column(
    connection: psycopg.Connection,
    table: str,
    candidates: Iterable[str],
) -> str | None:
    columns = table_columns(connection, "bronze", table)

    for candidate in candidates:
        if candidate in columns:
            return candidate

    return None


def normalize_number_expression(column: str) -> sql.Composed:
    return sql.SQL(
        "LPAD(REGEXP_REPLACE({}, '[^0-9]', '', 'g'), 10, '0')"
    ).format(sql.Identifier(column))


def create_indexes(connection: psycopg.Connection) -> None:
    definitions = [
        (
            "enterprise",
            ["enterprise_number", "entity_number"],
            "idx_bronze_enterprise_number",
        ),
        (
            "denomination",
            ["entity_number", "enterprise_number"],
            "idx_bronze_denomination_entity",
        ),
        (
            "address",
            ["entity_number", "enterprise_number"],
            "idx_bronze_address_entity",
        ),
        (
            "contact",
            ["entity_number", "enterprise_number"],
            "idx_bronze_contact_entity",
        ),
        (
            "activity",
            ["entity_number", "enterprise_number"],
            "idx_bronze_activity_entity",
        ),
        (
            "establishment",
            ["enterprise_number", "entity_number"],
            "idx_bronze_establishment_enterprise",
        ),
        (
            "code",
            ["category"],
            "idx_bronze_code_category",
        ),
    ]

    with connection.cursor() as cursor:
        for table, candidates, index_name in definitions:
            column = optional_column(connection, table, candidates)

            if column is None:
                print(
                    f"Index ignoré pour bronze.{table} : "
                    "colonne de rattachement introuvable."
                )
                continue

            print(
                f"Création/vérification de l'index "
                f"{index_name} sur bronze.{table}({column})..."
            )
            cursor.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {} ON bronze.{} ({})"
                ).format(
                    sql.Identifier(index_name),
                    sql.Identifier(table),
                    sql.Identifier(column),
                )
            )

    connection.commit()


def create_company_views(connection: psycopg.Connection) -> dict[str, str]:
    entity_number = require_column(
        connection,
        "enterprise",
        ["enterprise_number", "entity_number"],
    )
    denomination_entity = require_column(
        connection,
        "denomination",
        ["entity_number", "enterprise_number"],
    )
    address_entity = require_column(
        connection,
        "address",
        ["entity_number", "enterprise_number"],
    )
    contact_entity = require_column(
        connection,
        "contact",
        ["entity_number", "enterprise_number"],
    )
    activity_entity = require_column(
        connection,
        "activity",
        ["entity_number", "enterprise_number"],
    )
    establishment_entity = require_column(
        connection,
        "establishment",
        ["enterprise_number", "entity_number"],
    )

    mapping = {
        "enterprise_number": entity_number,
        "denomination_entity": denomination_entity,
        "address_entity": address_entity,
        "contact_entity": contact_entity,
        "activity_entity": activity_entity,
        "establishment_entity": establishment_entity,
    }

    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS company")

        cursor.execute("DROP VIEW IF EXISTS company.denomination CASCADE")
        cursor.execute(
            sql.SQL(
                """
                CREATE VIEW company.denomination AS
                SELECT
                    {} AS entity_number,
                    type_of_denomination,
                    language,
                    denomination
                FROM bronze.denomination
                """
            ).format(
                normalize_number_expression(denomination_entity)
            )
        )

        cursor.execute("DROP VIEW IF EXISTS company.address CASCADE")
        cursor.execute(
            sql.SQL(
                """
                CREATE VIEW company.address AS
                SELECT
                    {} AS entity_number,
                    type_of_address,
                    country_nl,
                    country_fr,
                    zipcode,
                    municipality_nl,
                    municipality_fr,
                    street_nl,
                    street_fr,
                    house_number,
                    box,
                    extra_address_info,
                    date_striking_off
                FROM bronze.address
                """
            ).format(
                normalize_number_expression(address_entity)
            )
        )

        cursor.execute("DROP VIEW IF EXISTS company.contact CASCADE")
        cursor.execute(
            sql.SQL(
                """
                CREATE VIEW company.contact AS
                SELECT
                    {} AS entity_number,
                    entity_contact,
                    contact_type,
                    value
                FROM bronze.contact
                """
            ).format(
                normalize_number_expression(contact_entity)
            )
        )

        cursor.execute("DROP VIEW IF EXISTS company.activity CASCADE")
        cursor.execute(
            sql.SQL(
                """
                CREATE VIEW company.activity AS
                SELECT
                    {} AS entity_number,
                    activity_group,
                    nace_version,
                    nace_code,
                    classification
                FROM bronze.activity
                """
            ).format(
                normalize_number_expression(activity_entity)
            )
        )

        establishment_number = require_column(
            connection,
            "establishment",
            ["establishment_number"],
        )
        establishment_start_date = optional_column(
            connection,
            "establishment",
            ["start_date"],
        )

        start_select = (
            sql.SQL("{} AS start_date").format(
                sql.Identifier(establishment_start_date)
            )
            if establishment_start_date
            else sql.SQL("NULL::TEXT AS start_date")
        )

        cursor.execute(
            "DROP VIEW IF EXISTS company.establishment CASCADE"
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE VIEW company.establishment AS
                SELECT
                    {} AS enterprise_number,
                    LPAD(
                        REGEXP_REPLACE({}, '[^0-9]', '', 'g'),
                        10,
                        '0'
                    ) AS establishment_number,
                    {}
                FROM bronze.establishment
                """
            ).format(
                normalize_number_expression(establishment_entity),
                sql.Identifier(establishment_number),
                start_select,
            )
        )

    connection.commit()
    return mapping


def create_company_table(
    connection: psycopg.Connection,
    mapping: dict[str, str],
) -> None:
    enterprise_number = mapping["enterprise_number"]

    status = require_column(
        connection,
        "enterprise",
        ["status"],
    )
    juridical_situation = require_column(
        connection,
        "enterprise",
        ["juridical_situation"],
    )
    juridical_form = require_column(
        connection,
        "enterprise",
        ["juridical_form"],
    )
    enterprise_type = require_column(
        connection,
        "enterprise",
        ["type_of_enterprise"],
    )
    start_date = require_column(
        connection,
        "enterprise",
        ["start_date"],
    )

    with connection.cursor() as cursor:
        print("Construction de company.company...")
        cursor.execute("DROP TABLE IF EXISTS company.company")

        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE company.company AS
                WITH official_name AS (
                    SELECT entity_number, denomination
                    FROM (
                        SELECT
                            entity_number,
                            denomination,
                            ROW_NUMBER() OVER (
                                PARTITION BY entity_number
                                ORDER BY
                                    CASE
                                        WHEN type_of_denomination = '001'
                                        THEN 0 ELSE 1
                                    END,
                                    CASE
                                        WHEN language = '1'
                                        THEN 0 ELSE 1
                                    END,
                                    denomination
                            ) AS rank_number
                        FROM company.denomination
                    ) ranked
                    WHERE rank_number = 1
                ),
                main_address AS (
                    SELECT
                        entity_number,
                        type_of_address,
                        country_nl,
                        country_fr,
                        zipcode,
                        municipality_nl,
                        municipality_fr,
                        street_nl,
                        street_fr,
                        house_number,
                        box,
                        extra_address_info
                    FROM (
                        SELECT
                            address.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY entity_number
                                ORDER BY
                                    CASE
                                        WHEN type_of_address IN (
                                            'REGO',
                                            '001'
                                        )
                                        THEN 0 ELSE 1
                                    END,
                                    date_striking_off NULLS FIRST,
                                    zipcode,
                                    municipality_fr,
                                    municipality_nl
                            ) AS rank_number
                        FROM company.address AS address
                    ) ranked
                    WHERE rank_number = 1
                ),
                contact_summary AS (
                    SELECT
                        entity_number,
                        STRING_AGG(
                            DISTINCT value,
                            ', '
                        ) FILTER (
                            WHERE contact_type = 'TEL'
                        ) AS telephone,
                        STRING_AGG(
                            DISTINCT value,
                            ', '
                        ) FILTER (
                            WHERE contact_type = 'EMAIL'
                        ) AS email,
                        STRING_AGG(
                            DISTINCT value,
                            ', '
                        ) FILTER (
                            WHERE contact_type IN (
                                'WEB',
                                'WEBSITE'
                            )
                        ) AS website
                    FROM company.contact
                    GROUP BY entity_number
                ),
                code_fr AS (
                    SELECT
                        category,
                        code,
                        MAX(description) AS description
                    FROM bronze.code
                    WHERE language = 'FR'
                    GROUP BY category, code
                )
                SELECT
                    {} AS entity_number,
                    name.denomination AS official_name,

                    enterprise.{} AS status_code,
                    status_code.description AS status,

                    enterprise.{} AS juridical_situation_code,
                    juridical_situation_code.description
                        AS juridical_situation,

                    enterprise.{} AS juridical_form_code,
                    juridical_form_code.description
                        AS juridical_form,

                    enterprise.{} AS enterprise_type_code,
                    enterprise_type_code.description
                        AS enterprise_type,

                    enterprise.{} AS start_date,

                    address.type_of_address,
                    COALESCE(
                        address.street_fr,
                        address.street_nl
                    ) AS street,
                    address.house_number,
                    address.box,
                    address.zipcode,
                    COALESCE(
                        address.municipality_fr,
                        address.municipality_nl
                    ) AS municipality,
                    COALESCE(
                        address.country_fr,
                        address.country_nl
                    ) AS country,
                    CONCAT_WS(
                        ' ',
                        COALESCE(
                            address.street_fr,
                            address.street_nl
                        ),
                        address.house_number,
                        CASE
                            WHEN NULLIF(address.box, '') IS NOT NULL
                            THEN 'bte ' || address.box
                        END
                    )
                    || CASE
                        WHEN NULLIF(address.zipcode, '') IS NOT NULL
                          OR NULLIF(
                                COALESCE(
                                    address.municipality_fr,
                                    address.municipality_nl
                                ),
                                ''
                             ) IS NOT NULL
                        THEN ', '
                        ELSE ''
                    END
                    || CONCAT_WS(
                        ' ',
                        address.zipcode,
                        COALESCE(
                            address.municipality_fr,
                            address.municipality_nl
                        )
                    ) AS full_address,

                    contacts.telephone,
                    contacts.email,
                    contacts.website,

                    NOW() AS built_at
                FROM bronze.enterprise AS enterprise
                LEFT JOIN official_name AS name
                    ON name.entity_number = {}
                LEFT JOIN main_address AS address
                    ON address.entity_number = {}
                LEFT JOIN contact_summary AS contacts
                    ON contacts.entity_number = {}
                LEFT JOIN code_fr AS status_code
                    ON status_code.category = 'Status'
                   AND status_code.code = enterprise.{}
                LEFT JOIN code_fr AS juridical_situation_code
                    ON juridical_situation_code.category
                        = 'JuridicalSituation'
                   AND juridical_situation_code.code
                        = enterprise.{}
                LEFT JOIN code_fr AS juridical_form_code
                    ON juridical_form_code.category
                        = 'JuridicalForm'
                   AND juridical_form_code.code
                        = enterprise.{}
                LEFT JOIN code_fr AS enterprise_type_code
                    ON enterprise_type_code.category
                        = 'TypeOfEnterprise'
                   AND enterprise_type_code.code
                        = enterprise.{}
                """
            ).format(
                normalize_number_expression(enterprise_number),
                sql.Identifier(status),
                sql.Identifier(juridical_situation),
                sql.Identifier(juridical_form),
                sql.Identifier(enterprise_type),
                sql.Identifier(start_date),
                normalize_number_expression(enterprise_number),
                normalize_number_expression(enterprise_number),
                normalize_number_expression(enterprise_number),
                sql.Identifier(status),
                sql.Identifier(juridical_situation),
                sql.Identifier(juridical_form),
                sql.Identifier(enterprise_type),
            )
        )

        cursor.execute(
            """
            ALTER TABLE company.company
            ADD PRIMARY KEY (entity_number)
            """
        )
        cursor.execute(
            """
            CREATE INDEX idx_company_official_name
            ON company.company (official_name)
            """
        )
        cursor.execute(
            """
            CREATE INDEX idx_company_status
            ON company.company (status_code)
            """
        )
        cursor.execute("ANALYZE company.company")

        cursor.execute("SELECT COUNT(*) FROM company.company")
        company_count = cursor.fetchone()[0]

    connection.commit()
    print(f"company.company : {company_count:,} entreprises")


def create_enrichment_tables(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company.external_profile (
                entity_number TEXT PRIMARY KEY,
                denomination TEXT,
                address TEXT,
                main_activity TEXT,
                employee_information TEXT,
                legal_form TEXT,
                vat_number TEXT,
                juridical_situation TEXT,
                capital TEXT,
                general_assembly TEXT,
                fiscal_year_end TEXT,
                telephone TEXT,
                email TEXT,
                website TEXT,
                raw_hdfs_path TEXT,
                source_updated_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company.director (
                entity_number TEXT NOT NULL,
                director_name TEXT NOT NULL,
                qualities JSONB NOT NULL DEFAULT '[]'::JSONB,
                source_hdfs_path TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (entity_number, director_name)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company.relationship (
                entity_number TEXT NOT NULL,
                related_entity_number TEXT NOT NULL,
                related_entity_name TEXT,
                relationship_nature TEXT,
                relationship_date TEXT,
                related_entity_status TEXT,
                source_hdfs_path TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (
                    entity_number,
                    related_entity_number,
                    relationship_nature,
                    relationship_date
                )
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company.ejustice_publication (
                entity_number TEXT NOT NULL,
                numac TEXT NOT NULL,
                publication_date DATE,
                publication_type TEXT,
                source_url TEXT,
                html_hdfs_path TEXT,
                pdf_hdfs_path TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (entity_number, numac)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company.nbb_deposit (
                entity_number TEXT NOT NULL,
                deposit_id TEXT NOT NULL,
                fiscal_year INTEGER,
                deposit_date DATE,
                period_end_date DATE,
                language TEXT,
                model_id TEXT,
                reference TEXT,
                metadata_hdfs_path TEXT,
                csv_hdfs_path TEXT,
                pdf_hdfs_path TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (entity_number, deposit_id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company.financial_indicator (
                entity_number TEXT NOT NULL,
                fiscal_year INTEGER NOT NULL,
                revenue NUMERIC,
                gross_margin NUMERIC,
                ebit NUMERIC,
                net_result NUMERIC,
                cash NUMERIC,
                financial_debt NUMERIC,
                net_financial_debt NUMERIC,
                equity NUMERIC,
                employees NUMERIC,
                payroll_cost NUMERIC,
                operating_cost NUMERIC,
                taxes NUMERIC,
                revenue_growth_pct NUMERIC,
                gross_margin_pct NUMERIC,
                net_margin_pct NUMERIC,
                revenue_per_employee NUMERIC,
                source_csv_hdfs_path TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (entity_number, fiscal_year)
            )
            """
        )

    connection.commit()


def print_validation(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS companies,
                COUNT(official_name) AS with_name,
                COUNT(full_address) AS with_address,
                COUNT(email) AS with_email,
                COUNT(telephone) AS with_telephone,
                COUNT(website) AS with_website
            FROM company.company
            """
        )
        row = cursor.fetchone()

        print("\nValidation company.company")
        print(f"- Entreprises       : {row[0]:,}")
        print(f"- Avec dénomination : {row[1]:,}")
        print(f"- Avec adresse      : {row[2]:,}")
        print(f"- Avec e-mail       : {row[3]:,}")
        print(f"- Avec téléphone    : {row[4]:,}")
        print(f"- Avec site web     : {row[5]:,}")

        cursor.execute(
            """
            SELECT
                entity_number,
                official_name,
                status,
                juridical_form,
                start_date,
                full_address,
                email,
                telephone,
                website
            FROM company.company
            WHERE entity_number IN (
                '0878065378',
                '0836157420',
                '0203430576'
            )
            ORDER BY entity_number
            """
        )

        print("\nContrôle Google / Apple / SNCB")
        for item in cursor.fetchall():
            print(item)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    with connect() as connection:
        print("1/5 Création des index...")
        create_indexes(connection)

        print("2/5 Création des vues détaillées...")
        mapping = create_company_views(connection)

        print("3/5 Construction de la table entreprise...")
        create_company_table(connection, mapping)

        print("4/5 Création des tables d'enrichissement...")
        create_enrichment_tables(connection)

        print("5/5 Validation...")
        print_validation(connection)

    print("\nModèle entreprise PostgreSQL terminé.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
