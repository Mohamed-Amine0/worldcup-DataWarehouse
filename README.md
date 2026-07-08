# World Cup Data Warehouse

Projet de mise en place d'un Data Warehouse pour analyser les données historiques des Coupes du Monde de la FIFA et le calendrier 2026.

## Structure du projet
- `data/source/` : Fichiers CSV contenant les données brutes (classements FIFA, historique des compétitions, calendrier 2026, détails des matchs de 1930 à 2022).
- `ingestion/` : Script SQL pour l'initialisation de la base de données brute (schéma public).
- `transformation/` : Script SQL pour le nettoyage et le typage des données vers la zone intermédiaire (schéma silver).
- `docker-compose.yml` : Fichier de configuration du service de base de données PostgreSQL.

## Prérequis
- Docker
- Docker Compose

## Lancement et Ingestion des données
Pour démarrer la base de données et importer automatiquement les données sources :
```bash
docker compose up -d
```

## Transformation (Zone Silver)
Pour nettoyer, filtrer et typer les données brutes :
```bash
Get-Content transformation/silver.sql -Raw | docker exec -i worldcup_db psql -U postgres -d worldcup_dw
```
Cette étape produit les tables typées suivantes dans le schéma `silver` :
- `silver.fifa_rankings` (fusion des classements 2022 et 2026)
- `silver.world_cup_history`
- `silver.schedule_2026`
- `silver.matches`
