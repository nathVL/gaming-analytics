from sqlalchemy import create_engine, text
from db_connection import get_engine

def create_tables():
    engine = get_engine()
    with engine.begin() as connection:
        try:
            connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))

            connection.execute(text("DROP TABLE IF EXISTS games_categories"))
            connection.execute(text("DROP TABLE IF EXISTS games_genres"))
            connection.execute(text("DROP TABLE IF EXISTS steamgames"))
            connection.execute(text("DROP TABLE IF EXISTS developers"))
            connection.execute(text("DROP TABLE IF EXISTS categories"))
            connection.execute(text("DROP TABLE IF EXISTS genres"))
            connection.execute(text("DROP TABLE IF EXISTS twitchgames"))

            connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        except Exception as e:
            print(f"Erreur lors de la suppression des tables: {e}")

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS developers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) UNIQUE
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS steamgames (
                appid INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                developer_id INT,
                release_date DATE,
                short_description TEXT,
                price INT,
                header_image TEXT,
                currency VARCHAR(10),
                windows BOOLEAN,
                linux BOOLEAN,
                mac BOOLEAN,
                metacritic INT,
                recommendations INT,
                FOREIGN KEY (developer_id) REFERENCES developers(id)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """))

        connection.execute(text("""
            ALTER TABLE steamgames CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS categories (
                id INT AUTO_INCREMENT PRIMARY KEY,
                description VARCHAR(255) UNIQUE
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS genres (
                id INT AUTO_INCREMENT PRIMARY KEY,
                description VARCHAR(255) UNIQUE
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS games_categories (
                game_appid INT,
                category_id INT,
                PRIMARY KEY (game_appid, category_id),
                FOREIGN KEY (game_appid) REFERENCES steamgames(appid),
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS games_genres (
                game_appid INT,
                genre_id INT,
                PRIMARY KEY (game_appid, genre_id),
                FOREIGN KEY (game_appid) REFERENCES steamgames(appid),
                FOREIGN KEY (genre_id) REFERENCES genres(id)
            )
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS twitchgames (
                steam_id int NOT NULL,
                date date NOT NULL,
                avg_viewers INT,
                avg_viewers_gain INT,
                avg_viewers_gain_percent FLOAT,
                peak_viewers INT,
                avg_streams INT,
                avg_streams_gain INT,
                avg_streams_gain_percent FLOAT,
                peak_streams INT,
                hours_watched VARCHAR(50),
                PRIMARY KEY (steam_id, date),
                FOREIGN KEY (steam_id) REFERENCES steamgames(appid)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS global_steam_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                DateTime DATETIME NOT NULL,
                Users INT,
                AverageUsers INT,
                InGame INT
            )
        """))

if __name__ == '__main__':
    create_tables()