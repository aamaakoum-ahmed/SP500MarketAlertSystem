## SP500 Market Alert System
## Analyse de données massives & Détection d'anomalies financières avec Apache Spark
Ce projet implémente un pipeline ETL (Extract, Transform, Load) complet pour analyser les données historiques du S&P 500. 
L'objectif est d'identifier de manière statistique les entreprises présentant un risque de volatilité anormal, aidant ainsi à la prise de décision 
financière.

**Objectif du Projet:**
Développer un système capable de traiter des millions de lignes de transactions boursières pour détecter des "outliers" (anomalies). 
Le système ne se base pas sur des seuils fixes, mais calcule dynamiquement la moyenne et l'écart-type annuel pour isoler les entreprises dont le score 
de risque est statistiquement hors norme (seuil de $Moyenne + 2\sigma$).

**Stack Technique:** 
- Langage : Scala
- Moteur de calcul : Apache Spark (Spark SQL & DataFrames)
- Système : Debian 12 (Linux)
- Gestion de version : Git / GitHub

**Méthodologie:** Les 4 Sprints
Le projet a été réalisé suivant une approche Agile, découpée en quatre phases distinctes :

**-->Sprint 1 :** 
Ingestion & Nettoyage Industriel
Mise en place : Définition de schémas stricts (StructType) pour garantir l'intégrité des données.
Robustesse : Gestion des formats de dates complexes (ISO-8601 avec offsets) via la configuration spark.sql.legacy.timeParserPolicy.
Qualité : Filtrage des données corrompues (volumes nuls, prix aberrants) et suppression des colonnes superflues.

**--> Sprint 2 :** 
Feature Engineering (Métriques)
Calcul du RVI (Relative Volatility Index) : Mesure de la volatilité intra-journalière normalisée.
Log-Normalization : Transformation logarithmique des volumes de transactions pour stabiliser les calculs.
Enrichissement : Jointure (Join) avec les données de structure du capital (Shares Outstanding) pour obtenir le taux de rotation des actions.

**--> Sprint 3 :**
Intelligence Statistique
Analyse de Fenêtrage : Utilisation des Window Functions pour calculer des statistiques globales par année sans réduire le dataset.
Algorithme de Détection : Identification des anomalies via le calcul de l'écart-type.
Scoring de Sévérité : Attribution d'un indice de sévérité basé sur la distance par rapport à la moyenne.

**--> Sprint 4 :** 
Exportation & Standardisation
Optimisation : Sauvegarde des alertes au format Parquet pour des performances de lecture optimales.
Accessibilité : Génération d'un rapport final en CSV pour exploitation bureautique (Excel/BI).
Documentation : Finalisation du dépôt et mise en place des bonnes pratiques .gitignore pour la gestion des fichiers lourds.

**Structure du Répertoire**
+ src/ingestion.scala : Socle de chargement des données.
+ src/metrics.scala : Logique de calcul des indicateurs financiers.
+ src/alerts.scala : Algorithme de détection des outliers.
+ src/export.scala : Script de persistance des résultats.
+ data/ : (Non inclus sur Git) Contient les fichiers sp500_stock_price.csv et sharesOutstanding.csv.


