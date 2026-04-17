import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from scripts.mysql.db_connection import get_engine


def insert_csv_data(file_path, connection):
    """Insère les données du fichier CSV dans la table GlobalSteamUsers"""
    print(f"Traitement du fichier : {file_path}")

    # Charger le CSV avec le bon délimiteur
    df = pd.read_csv(file_path, delimiter=";")

    # Conversion du champ DateTime en type datetime de pandas
    df['DateTime'] = pd.to_datetime(df['DateTime'])

    total_rows = len(df)
    success_count = 0

    for index, row in df.iterrows():
        try:
            connection.execute(text("""
                INSERT INTO global_steam_users (DateTime, Users, AverageUsers, InGame)
                VALUES (:DateTime, :Users, :AverageUsers, :InGame)
            """), {
                'DateTime': row['DateTime'],
                'Users': int(row['Users']) if not pd.isna(row['Users']) else None,
                'AverageUsers': int(row['Average Users']) if not pd.isna(row['Average Users']) else None,
                'InGame': int(row['In-Game']) if not pd.isna(row['In-Game']) else None
            })
            success_count += 1
            print(f"Progression : {index + 1}/{total_rows} enregistrements insérés")
        except IntegrityError:
            print(f"Erreur d'intégrité pour la ligne {index + 1} (probablement une clé dupliquée)")
            continue
        except Exception as e:
            print(f"Erreur pour la ligne {index + 1} : {str(e)}")
            continue

    print(
        f"Fichier {os.path.basename(file_path)} terminé : {success_count}/{total_rows} enregistrements insérés avec succès")
    return success_count, total_rows


def insert_all_csv_data():
    """Traite le fichier CSV et insère les données dans la base de données"""
    engine = get_engine()
    connection = engine.connect()

    # Chemin vers le fichier CSV
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(current_dir, "globalSteamUsers.csv")

    transaction = connection.begin()
    try:
        success_count, total_rows = insert_csv_data(csv_file_path, connection)
        transaction.commit()
        print(f"Transaction validée : {success_count}/{total_rows} enregistrements insérés")
    except Exception as e:
        transaction.rollback()
        print(f"Erreur lors du traitement du fichier {csv_file_path}, transaction annulée : {str(e)}")
    finally:
        connection.close()


if __name__ == '__main__':
    insert_all_csv_data()
