import pandas as pd
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from mysql.db_connection import get_engine
from sqlalchemy import text


def extract_steam_games_to_csv(output_csv="steam_games.csv"):
    """Récupère la liste des jeux Steam depuis la base de données MySQL et les sauvegarde dans un CSV"""
    try:
        print("Connexion à la base de données...")
        engine = get_engine()
        query = text("SELECT appid, name FROM games")

        print("Exécution de la requête...")
        with engine.connect() as connection:
            result = connection.execute(query)
            games = [(row[0], row[1]) for row in result]

        print(f"{len(games)} jeux récupérés de la base de données.")

        # Conversion en DataFrame et sauvegarde en CSV
        df = pd.DataFrame(games, columns=['appid', 'name'])
        df.to_csv(output_csv, index=False)
        print(f"Jeux sauvegardés dans {output_csv}")
        return True
    except Exception as e:
        print(f"Erreur lors de l'extraction des jeux: {str(e)}")
        return False


if __name__ == "__main__":
    # On peut spécifier un nom de fichier en argument ou utiliser la valeur par défaut
    output_file = sys.argv[1] if len(sys.argv) > 1 else "scripts/twitchtracker/steamgames/steam_games.csv"
    extract_steam_games_to_csv(output_file)