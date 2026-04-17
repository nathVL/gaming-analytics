import csv
import os
import sys
import dateparser
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mysql.db_connection import get_engine

def convert_any_date(date_string):
    """Convertit n'importe quel format de date en DD/MM/YYYY"""
    parsed_date = dateparser.parse(date_string)
    if parsed_date:
        return parsed_date.strftime("%m/%Y")
    return None


def insert_twitchData_from_file(file_path, connection):
    """Insère les données d'un fichier CSV spécifique dans la base de données"""
    print(f"Traitement du fichier: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        twitch_data = list(reader)

    data_count = len(twitch_data)
    success_count = 0

    for index, row in enumerate(twitch_data, 1):
        try:
            # Formater les données et gérer les valeurs manquantes
            steam_id = int(row['steam_id']) if row['steam_id'] != '-' else None
            date = row['Month']
            avg_viewers = int(float(row['AvgViewers'].replace(',', ''))) if row['AvgViewers'] != '-' else None
            avg_viewers_gain = int(float(row['AvgViewersGain'].replace(',', ''))) if row['AvgViewersGain'] != '-' and row['AvgViewersGain'] != '' else None
            avg_viewers_gain_percent = float(row['AvgViewers%Gain'].replace(',', '')) if row['AvgViewers%Gain'] != '-' and row['AvgViewers%Gain'] != '' else None
            peak_viewers = int(float(row['PeakViewers'].replace(',', ''))) if row['PeakViewers'] != '-' else None
            avg_streams = int(float(row['AvgStreams'].replace(',', ''))) if row['AvgStreams'] != '-' else None
            avg_streams_gain = int(float(row['AvgStreamsGain'].replace(',', ''))) if row['AvgStreamsGain'] != '-' and row['AvgStreamsGain'] != '' else None
            avg_streams_gain_percent = float(row['AvgStreamsùGain'].replace(',', '')) if row['AvgStreamsùGain'] != '-' and row['AvgStreamsùGain'] != '' else None
            peak_streams = int(float(row['PeakStreams'].replace(',', ''))) if row['PeakStreams'] != '-' else None
            hours_watched = row['HoursWatched'] if row['HoursWatched'] != '-' else None

            # Insérer les données dans la base de données
            connection.execute(text("""
                INSERT INTO twitchgames (steam_id, date, avg_viewers,
                                        avg_viewers_gain, avg_viewers_gain_percent,
                                        peak_viewers, avg_streams,
                                        avg_streams_gain, avg_streams_gain_percent,
                                        peak_streams, hours_watched)
                VALUES (:steam_id, STR_TO_DATE(:date,'%d/%m/%Y'), :avg_viewers, :avg_viewers_gain, :avg_viewers_gain_percent,
                        :peak_viewers, :avg_streams, :avg_streams_gain, :avg_streams_gain_percent,
                        :peak_streams, :hours_watched)
            """), {
                'steam_id': steam_id,
                'date': date,
                'avg_viewers': avg_viewers,
                'avg_viewers_gain': avg_viewers_gain,
                'avg_viewers_gain_percent': avg_viewers_gain_percent,
                'peak_viewers': peak_viewers,
                'avg_streams': avg_streams,
                'avg_streams_gain': avg_streams_gain,
                'avg_streams_gain_percent': avg_streams_gain_percent,
                'peak_streams': peak_streams,
                'hours_watched': hours_watched
            })

            success_count += 1
            print(f"Progression du fichier {os.path.basename(file_path)}: {index}/{data_count} lignes traitées")

        except IntegrityError:
            print(f"Erreur d'intégrité pour la ligne {index} (steam_id: {row['steam_id']}, date: {row['Month']})")
            continue
        except Exception as e:
            print(f"Erreur pour la ligne {index} (steam_id: {row['steam_id']}, date: {row['Month']}): {str(e)}")
            continue

    print(f"Fichier {os.path.basename(file_path)} terminé: {success_count}/{data_count} lignes insérées avec succès")
    return success_count, data_count


def insert_all_data_from_output():
    engine = get_engine()
    connection = engine.connect()

    if not os.path.exists('output'):
        print("Le dossier 'output' n'existe pas!")
        return

    csv_files = []
    for root, dirs, files in os.walk('output'):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))

    csv_files = sorted(csv_files)

    print(f"Début du traitement de {len(csv_files)} fichiers CSV...")

    total_games = 0
    total_success = 0

    for file_path in csv_files:
        transaction = connection.begin()
        try:
            success_count, games_count = insert_twitchData_from_file(file_path, connection)
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
