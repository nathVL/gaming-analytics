# Projet Steam

Ce projet permet de collecter et d'insérer des données provenant de Steam API, SteamDB et TwitchTracker dans une base de données MySQL.

## Auteurs

- Valentin Fortier
- Elias Richarme
- Nathan Villette
- Oscar Neveux

## Structure du projet

Le projet est organisé en différents dossiers pour la collecte et le traitement des données issues de Steam API, SteamDB, et TwitchTracker, avec des scripts dédiés pour l'insertion en base de données.

## Modèle de données

Notre base de données a été conçue pour stocker efficacement les différentes catégories d'informations recueillies lors de la phase de collecte des données. Sa structure relationnelle permet d'établir des liens entre les jeux Steam et les données de popularité issues de Twitch et SteamDB.

### Structure de la base de données

La base de données est organisée autour des tables principales suivantes :

- `steamgames` : Table centrale contenant les informations principales sur chaque jeu
- `developers` : Informations sur les développeurs, liés aux jeux par relation
- `categories` et `genres` : Catégorisation des jeux
- `games_categories` et `games_genres` : Tables de jonction pour les relations many-to-many
- `twitchgames` : Statistiques de streaming Twitch pour chaque jeu par date
- `players_history` : Historique du nombre de joueurs pour chaque jeu
- `price_history` : Évolution des prix des jeux au fil du temps
- `global_steam_users` : Statistiques générales des utilisateurs Steam

Le script de création utilise SqlAlchemy et un système de transactions pour garantir l'intégrité des données même en cas d'erreur pendant l'exécution des scripts. L'encodage des caractères est configuré en utf8mb4 pour supporter tous les jeux, quelle que soit leur langue d'origine.

## Processus d'insertion dans la base de données

### Configuration de la base de données

1. Utilisez le script `scripts/mysql/create_db.py` pour créer la structure initiale de la base de données
2. La connexion à la base de données est gérée par `scripts/mysql/db_connection.py`

Assurez-vous d'avoir d'abord configuré correctement les paramètres de connexion à la base de données dans le fichier `.env.local` (à créer à partir du `.env`.

### Scripts d'insertion des données

Pour insérer les données collectées dans la base, nous avons développé plusieurs scripts spécialisés :

1. **Données Steam API** : 
   - `insert_all_data_from_output()` : Traite les fichiers JSON contenant les données de base des jeux Steam
   - Les données sont collectées dans le dossier `output/steamapi/`
   - Le script `scripts/steamapi/insert_into_db.py` traite ces données et les insère dans les tables correspondantes

2. **Données SteamDB** :
   - `insert_players_history()` : Importe les données d'historique des joueurs depuis les fichiers CSV générés par le scraping de SteamDB
   - `insert_price_history()` : Importe l'historique des prix des jeux
   - Les données sont stockées dans `output/steamdb/`
   - Le script `scripts/steamdb/insert_steamdb_data_into_db.py` analyse ces données et les intègre à la base de données

3. **Données TwitchTracker** :
   - `insert_twitchData_from_file()` : Traite les données de streaming Twitch
   - Les données sont sauvegardées dans `output/twitchtracker/`
   - Le script `scripts/twitchtracker/insert_twitchData_into_bd.py` gère l'insertion de ces données dans la table `twitch_data`

### Mécanismes de gestion d'erreurs

Tous ces scripts intègrent des mécanismes robustes de gestion des erreurs :
- Utilisation de transactions pour garantir la cohérence des données
- Gestion des erreurs d'intégrité (clés étrangères, doublons, etc.)
- Conversion automatique des différents formats de date récupérés
- Suivi détaillé de la progression avec rapports d'erreurs

## Utilisation des scripts d'insertion

Ce projet utilise `UV`, pour insérer des données dans la base de données, exécutez les scripts correspondants depuis le répertoire racine du projet. Par exemple :

```bash
uv run scripts/steamapi/insert_into_db.py
uv run scripts/steamdb/insert_steamdb_data_into_db.py
uv run scripts/twitchtracker/insert_twitchData_into_bd.py
```