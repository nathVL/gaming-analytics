import csv
import os
import sys
import dateparser
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mysql.db_connection import get_engine

def convert_any_date(date_string):
    """Convertit n'importe quel format de date en MM/YYYY"""
    parsed_date = dateparser.parse(date_string)
    if parsed_date:
        return parsed_date.strftime("%d/%m/%Y")
    return None


def insert_players_history():
    """
    Insère les données d'historique des joueurs dans la base de données.
    Cette fonction supprime et recrée la table player_history,
    puis insère les données des fichiers CSV dans la table.
    """
    print("Début de l'insertion des données d'historique des joueurs...")

    engine = get_engine()
    connection = engine.connect()


    try:
        with connection.begin() as transaction:
            connection.execute(text("DROP TABLE IF EXISTS players_history"))
            connection.execute(text("""
                CREATE TABLE players_history (
                    appid INT NOT NULL,
                    month DATE NOT NULL,
                    peak INT,
                    gain INT,
                    percent_gain FLOAT,
                    PRIMARY KEY (appid, month),
                    FOREIGN KEY (appid) REFERENCES steamgames(appid)
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """))
        print("Table players_history recréée avec succès.")
    except Exception as e:
        print(f"Erreur lors de la recréation de la table players_history: {e}")
        connection.close()
        return
    base_dir = 'scripts/steamdb/output'
    total_inserted = 0
    total_rows = 0

    print(os.listdir(base_dir))
    for instance_dir in [d for d in os.listdir(base_dir) if
                         d.startswith('instance_') and os.path.isdir(os.path.join(base_dir, d))]:
        players_dir = os.path.join(base_dir, instance_dir, 'players')

        if not os.path.exists(players_dir):
            print(f"Le dossier {players_dir} n'existe pas, passage au suivant.")
            continue

        for file in os.listdir(players_dir):
            if file.endswith('_players.csv'):
                appid = int(file.split('_')[0])
                file_path = os.path.join(players_dir, file)
                print(f"Traitement du fichier: {file_path}")
                transaction = connection.begin()
                try:
                    with open(file_path, 'r', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        file_rows = list(reader)
                    success_count = 0
                    for row in file_rows:
                        try:
                            date = convert_any_date(row['Month'])
                            connection.execute(text("""
                                INSERT INTO players_history (appid, month, peak, gain, percent_gain)
                                VALUES (:appid, STR_TO_DATE(:date, '%d/%m/%Y'), :peak, :gain, :percent_gain)
                            """), {
                                'appid': appid,
                                'date': date,
                                'peak': int(row['Peak']) if row['Peak'] and row['Peak'] != '-' else None,
                                'gain': int(row['Gain']) if row['Gain'] and row['Gain'] != '-' else None,
                                'percent_gain': float(row['Percent_Gain'].replace(',', '')) if row['Percent_Gain'] and row['Percent_Gain'] != '-' else None
                            })
                            success_count += 1
                        except IntegrityError:
                            print(f"Erreur d'intégrité pour l'appid: {appid}, date: {row['Month']}")
                            continue
                        except Exception as e:
                            print(f"Erreur pour l'appid: {appid}, date: {row['Month']}: {str(e)}")
                            continue
                    transaction.commit()
                    total_inserted += success_count
                    total_rows += len(file_rows)
                    print(f"Fichier {file} terminé: {success_count}/{len(file_rows)} lignes insérées avec succès")
                except Exception as e:
                    transaction.rollback()
                    print(f"Erreur lors du traitement du fichier {file_path}, transaction annulée: {str(e)}")
    connection.close()
    print(f"\nTraitement terminé! {total_inserted}/{total_rows} entrées d'historique de joueurs insérées avec succès")


def insert_price_history():
    """
    Insère les données d'historique des prix dans la base de données.
    Cette fonction supprime et recrée la table price_history,
    puis insère les données des fichiers CSV dans la table.
    """
    print("Début de l'insertion des données d'historique des prix...")
    engine = get_engine()
    connection = engine.connect()
    try:
        with connection.begin() as transaction:
            connection.execute(text("DROP TABLE IF EXISTS price_history"))
            connection.execute(text("""
                CREATE TABLE price_history (
                    appid INT NOT NULL,
                    date DATE NOT NULL,
                    final_price FLOAT,
                    two_year_low FLOAT,
                    PRIMARY KEY (appid, date),
                    FOREIGN KEY (appid) REFERENCES steamgames(appid)
                )
            """))
        print("Table price_history recréée avec succès.")
    except Exception as e:
        print(f"Erreur lors de la recréation de la table price_history: {e}")
        connection.close()
        return
    base_dir = 'scripts/steamdb/output'
    total_inserted = 0
    total_rows = 0
    for instance_dir in [d for d in os.listdir(base_dir) if
                         d.startswith('instance_') and os.path.isdir(os.path.join(base_dir, d))]:
        price_dir = os.path.join(base_dir, instance_dir, 'price')
        if not os.path.exists(price_dir):
            print(f"Le dossier {price_dir} n'existe pas, passage au suivant.")
            continue
        for file in os.listdir(price_dir):
            if file.endswith('_price.csv'):
                appid = int(file.split('_')[0])
                file_path = os.path.join(price_dir, file)
                print(f"Traitement du fichier: {file_path}")
                transaction = connection.begin()
                try:
                    with open(file_path, 'r', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f, delimiter=';')
                        file_rows = list(reader)
                        if file_rows:
                            file_rows = file_rows[:-1]
                    success_count = 0
                    for row in file_rows:
                        try:
                            date = convert_any_date(row['DateTime'])
                            final_price = float(row['Final price'].replace(',', '.')) if row['Final price'] else None
                            two_year_low = float(row['2-year low'].replace(',', '.')) if row['2-year low'] else None
                            connection.execute(text("""
                                INSERT INTO price_history (appid, date, final_price, two_year_low)
                                VALUES (:appid, STR_TO_DATE(:date, '%d/%m/%Y'), :final_price, :two_year_low)
                            """), {
                                'appid': appid,
                                'date': date,
                                'final_price': final_price,
                                'two_year_low': two_year_low
                            })
                            success_count += 1
                        except IntegrityError:
                            print(f"Erreur d'intégrité pour l'appid: {appid}, date: {row['DateTime']}")
                            continue
                        except Exception as e:
                            print(f"Erreur pour l'appid: {appid}, date: {row['DateTime']}: {str(e)}")
                            continue
                    transaction.commit()
                    total_inserted += success_count
                    total_rows += len(file_rows)
                    print(f"Fichier {file} terminé: {success_count}/{len(file_rows)} lignes insérées avec succès")
                except Exception as e:
                    transaction.rollback()
                    print(f"Erreur lors du traitement du fichier {file_path}, transaction annulée: {str(e)}")
    connection.close()
    print(f"\nTraitement terminé! {total_inserted}/{total_rows} entrées d'historique de prix insérées avec succès")

if __name__ == '__main__':
    insert_price_history()
    insert_players_history()