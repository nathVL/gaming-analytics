import json
import os
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from db_connection import get_engine


def insert_data_from_file(file_path, connection):
    """Insère les données d'un fichier JSON spécifique dans la base de données"""
    print(f"Traitement du fichier: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        games = json.load(f)
    games_count = len(games)
    success_count = 0

    for index, game in enumerate(games, 1):
        try:
            developers = game.get('developers', [])
            developer_id = None
            if developers:
                developer_name = developers[0]
                result = connection.execute(
                    text("""
                        INSERT IGNORE INTO developers (name)
                        VALUES (:developer)
                    """), {'developer': developer_name}
                )
                if result.rowcount == 1:
                    developer_id = result.lastrowid
                else:
                    developer_id = connection.execute(
                        text("SELECT id FROM developers WHERE name = :developer"),
                        {'developer': developer_name}
                    ).scalar()
            connection.execute(text("""
                INSERT INTO steamgames (appid, name, developer_id, release_date, short_description, 
                price, currency, windows, linux, mac, metacritic, recommendations, header_image)
                VALUES (:appid, :name, :developer_id, STR_TO_DATE(:release_date,'%d/%m/%Y'), :short_description,
                :price, :currency, :windows, :linux, :mac, :metacritic, :recommendations, :header_image)
            """), {
                'appid': game['appid'],
                'name': game['name'],
                'developer_id': developer_id,
                'release_date': game.get('release_date'),
                'short_description': game.get('short_description', ''),
                'price': game.get('price', 0),
                'currency': game.get('currency'),
                'windows': game.get('windows', False),
                'linux': game.get('linux', False),
                'mac': game.get('mac', False),
                'metacritic': game.get('metacritic'),
                'recommendations': game.get('recommendations', 0),
                'header_image': game.get('header_image', '')
            })

            insert_categories(connection, game)
            insert_genres(connection, game)
            success_count += 1
            print(f"Progression du fichier {os.path.basename(file_path)}: {index}/{games_count} jeux traités")

        except IntegrityError:
            print(f"Erreur d'intégrité pour le jeu {game['name']} (ID: {game['appid']})")
            continue
        except Exception as e:
            print(f"Erreur pour le jeu {game['name']} (ID: {game['appid']}): {str(e)}")
            continue

    print(f"Fichier {os.path.basename(file_path)} terminé: {success_count}/{games_count} jeux insérés avec succès")
    return success_count, games_count


def insert_categories(connection, game):
    """Insère les catégories d'un jeu"""
    categories = game.get('categories', [])
    for category in categories:
        result = connection.execute(
            text("""
                INSERT IGNORE INTO categories (description)
                VALUES (:category)
            """), {'category': category}
        )
        if result.rowcount == 1:
            category_id = result.lastrowid
        else:
            category_id = connection.execute(
                text("SELECT id FROM categories WHERE description = :category"),
                {'category': category}
            ).scalar()
        try:
            connection.execute(text("""
                INSERT INTO games_categories (game_appid, category_id)
                VALUES (:appid, :category_id)
            """), {
                'appid': game['appid'],
                'category_id': category_id
            })
        except IntegrityError:
            continue


def insert_genres(connection, game):
    """Insère les genres d'un jeu"""
    genres = game.get('genre', [])
    for genre in genres:
        result = connection.execute(
            text("""
                INSERT IGNORE INTO genres (description)
                VALUES (:genre)
            """), {'genre': genre}
        )
        if result.rowcount == 1:
            genre_id = result.lastrowid
        else:
            genre_id = connection.execute(
                text("SELECT id FROM genres WHERE description = :genre"),
                {'genre': genre}
            ).scalar()
        try:
            connection.execute(text("""
                INSERT INTO games_genres (game_appid, genre_id)
                VALUES (:appid, :genre_id)
            """), {
                'appid': game['appid'],
                'genre_id': genre_id
            })
        except IntegrityError:
            continue


def insert_all_data_from_output():
    """Traite tous les fichiers JSON du dossier output et les insère dans la base de données"""
    engine = get_engine()
    connection = engine.connect()

    if not os.path.exists('output'):
        print("Le dossier 'output' n'existe pas!")
        return

    json_files = []
    for root, dirs, files in os.walk('output'):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))

    json_files = sorted(json_files)

    if not json_files:
        print("Aucun fichier JSON trouvé dans le dossier 'output'!")
        return

    print(f"Début du traitement de {len(json_files)} fichiers JSON...")

    total_games = 0
    total_success = 0

    for file_path in json_files:
        transaction = connection.begin()
        try:
            success_count, games_count = insert_data_from_file(file_path, connection)
            transaction.commit()
            total_games += games_count
            total_success += success_count
            print(f"Transaction validée pour le fichier {os.path.basename(file_path)}")
        except Exception as e:
            transaction.rollback()
            print(f"Erreur lors du traitement du fichier {file_path}, transaction annulée: {str(e)}")

    connection.close()
    print(f"\nTraitement terminé! {total_success}/{total_games} jeux insérés avec succès dans la base de données")

if __name__ == '__main__':
    insert_all_data_from_output()
