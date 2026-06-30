import json
from datetime import datetime
from pathlib import Path
from typing import Any


def save_raw_json(
    source: str,
    entity_number: str,
    data: Any,
    name: str = "response",
) -> Path:
    """
    Sauvegarde une donnée brute du notebook dans data/landing.

    Exemple :
        save_raw_json("notaire", "0878065378", brut)
    """

    ingestion_date = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_directory = (
        Path("data")
        / "landing"
        / source
        / f"ingestion_date={ingestion_date}"
        / f"entity_number={entity_number}"
    )

    output_directory.mkdir(parents=True, exist_ok=True)

    output_file = output_directory / f"{name}_{timestamp}.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

    print(f"Donnée brute sauvegardée : {output_file}")

    return output_file
