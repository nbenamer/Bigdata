"""
Ingestion brute des fichiers BCE/KBO dans la couche Bronze.

Les fichiers sources doivent être conservés sans transformation métier.
"""

from pathlib import Path


SOURCE_DIRECTORY = Path("data")
EXPECTED_FILES = [
    "activity.csv",
    "address.csv",
    "branch.csv",
    "code.csv",
    "contact.csv",
    "denomination.csv",
    "enterprise.csv",
    "establishment.csv",
    "meta.csv",
]


def verify_source_files() -> list[Path]:
    """Vérifie la présence des fichiers sources attendus."""
    missing_files: list[Path] = []
    existing_files: list[Path] = []

    for filename in EXPECTED_FILES:
        filepath = SOURCE_DIRECTORY / filename

        if filepath.exists():
            existing_files.append(filepath)
        else:
            missing_files.append(filepath)

    if missing_files:
        formatted_files = "\n".join(
            f"- {filepath}" for filepath in missing_files
        )
        raise FileNotFoundError(
            f"Fichiers sources absents :\n{formatted_files}"
        )

    return existing_files


def main() -> None:
    """Point d'entrée du script d'ingestion Bronze."""
    files = verify_source_files()

    print("Fichiers prêts pour l'ingestion Bronze :")

    for filepath in files:
        size_mb = filepath.stat().st_size / (1024 * 1024)
        print(f"- {filepath}: {size_mb:.2f} Mo")


if __name__ == "__main__":
    main()
