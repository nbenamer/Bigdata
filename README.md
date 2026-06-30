# Projet Data Big — BCE/KBO

Pipeline de données consacré à l'ingestion et à l'analyse des données de la
Banque-Carrefour des Entreprises belge.

## Architecture prévue

- Bronze : conservation des données sources brutes
- Silver : nettoyage, typage et normalisation
- Gold : indicateurs métier et analyses

## Sources actuelles

- Open Data BCE/KBO
- Données NBB/CBSO
- Données BCE Public Search
- Publications eJustice
- Documents juridiques et notariaux

## Organisation

- `BCE_final_Propre.ipynb` : notebook d'exploration actuel
- `src/bronze/` : code d'ingestion Bronze
- `config/` : configuration du projet
- `manifests/` : manifestes d'ingestion
- `tests/` : tests automatisés
- `data/` : données locales non versionnées
