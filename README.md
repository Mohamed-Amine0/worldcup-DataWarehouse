# World Cup Data Warehouse

Data Warehouse ETL en 3 couches (Bronze, Silver, Gold) pour analyser les données historiques des Coupes du Monde FIFA et le calendrier 2026.

## Architecture

- **Bronze** : ingestion CSV bruts → PostgreSQL (`bronze`)
- **Silver** : nettoyage, typage, déduplication via Spark → PostgreSQL (`silver`)
- **Gold** : KPIs métier via Spark → PostgreSQL (`gold`)
- **Monitoring** : Prometheus + Grafana + postgres-exporter
- **API** : FastAPI sur le port 8000

## Démarrage rapide

```bash
# Configuration légère (1 worker Spark)
docker compose -f docker-compose.1worker.yml up -d

# Attendre que PostgreSQL soit prêt, puis lancer le pipeline
./run_pipeline.sh   # Linux/Mac
# ou
.\run_pipeline.ps1  # Windows PowerShell
```

## Interfaces

| Service    | URL                      | Identifiants    |
|-----------|--------------------------|-----------------|
| Grafana   | http://localhost:3000    | admin / admin   |
| Spark UI  | http://localhost:8080    | -               |
| Prometheus| http://localhost:9090  | -               |
| API       | http://localhost:8000/docs | -             |
| PostgreSQL| localhost:5432           | user / password |

## Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

Les tests d'intégration (`test_integration_db.py`, `test_api.py`) nécessitent Docker en cours d'exécution avec le pipeline ETL exécuté.

## Structure

Voir `sujet.md` pour la documentation complète du projet.

## Prédictions (Amélioration Elo)

La logique de prédiction initiale basée sur les classements FIFA a été remplacée par un modèle basé sur les **ratings Elo historiques (1901-2026)** issus de `eloratings.net` afin d'apporter plus de précision (supérieure à 70% de réussite historique) et de nuance dans les résultats.

### Méthodologie
1. **Formule de Base** : La probabilité de victoire de l'équipe A est calculée selon l'équation :
   $$P(A) = \frac{1}{1 + 10^{\frac{Rating_B - Rating_A - H}{400}}}$$
   où $H$ représente l'avantage à domicile (+100 points Elo attribués si l'équipe joue dans son pays hôte : USA, Canada ou Mexique).
2. **Probabilité de Match Nul** : Estimée de manière dynamique par :
   $$P(\text{Nul}) = 0.25 \times e^{-\left(\frac{Rating_diff}{300}\right)^2}$$
   Le reste de la probabilité est ensuite normalisé et distribué pour obtenir des probabilités de victoire, de défaite et de nul précises.
3. **Prédiction de Buts (Score attendu)** :
   $$\text{Expected Goals}_A = \frac{\text{Buts marqués}_A}{\text{Matchs joués}_A} \times \frac{Rating_A}{Rating_B}$$

### Architecture & Tables
- **Bronze (`bronze.elo_ratings`)** : Ingestion brute de `elo_ratings_wc2026.csv` via le script `scripts/bronze/elo_to_bronze.py`.
- **Silver (`silver.match_predictions`)** : Vue SQL qui croise toutes les équipes qualifiées pour simuler tous les duels possibles à l'aide de la formule Elo.
- **Gold (`gold.predictions_2026` & `gold.tournament_predictions`)** : Tables de prédictions finalisées calculées via Spark. La table `tournament_predictions` contient les métriques détaillées (ratings, probabilités et scores attendus).
- **Grafana** : Un dashboard interactif dédié aux prédictions a été mis en place sous `dashboards/grafana/predictions_dashboard.json`.

