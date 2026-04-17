from seleniumbase import Driver
import os
import shutil
import pandas as pd
import time
import random
import sys
import traceback
from selenium.webdriver.common.by import By
from dotenv import load_dotenv
from stem import Signal
from stem.control import Controller
import requests
import re
from bs4 import BeautifulSoup
import dateparser

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from mysql.db_connection import get_engine
    from sqlalchemy import text
except ImportError:
    print("Module de connexion à la base de données introuvable, certaines fonctionnalités seront désactivées")

if os.path.exists('.env.local'):
    load_dotenv('.env.local')
else:
    load_dotenv('.env')

destination_folder = "scripts/steamdb/output"

def setup_output_folders(instance_number=None):
    base_folder = destination_folder
    if instance_number:
        base_folder = f"{destination_folder}/instance_{instance_number}"

    os.makedirs(base_folder, exist_ok=True)
    os.makedirs(os.path.join(base_folder, "players"), exist_ok=True)
    os.makedirs(os.path.join(base_folder, "price"), exist_ok=True)
    return base_folder

def convert_any_date(date_string):
    """Convertit n'importe quel format de date en DD/MM/YYYY"""
    parsed_date = dateparser.parse(date_string)
    if parsed_date:
        return parsed_date.strftime("%d/%m/%Y")
    return None

def renew_tor_ip(password, control_port, driver=None):
    if driver:
        try:
            while check_ip_banned(driver):
                with Controller.from_port(port=control_port) as controller:
                    controller.authenticate(password=password)
                    controller.signal(Signal.NEWNYM)
                    print(f"Nouvelle identité demandée à Tor")
                    time.sleep(2)
                    current_ip = get_current_ip(access_port)
                    is_banned = check_ip_banned(driver)
        except Exception as e:
            print(f"Erreur lors du renouvellement de l'IP: {e}")
    else:
        with Controller.from_port(port=control_port) as controller:
            controller.authenticate(password=password)
            controller.signal(Signal.NEWNYM)
            print(f"Nouvelle identité demandée à Tor")
            time.sleep(1)
            current_ip = get_current_ip(access_port)



def get_current_ip(access_port):
    """Récupère et affiche l'IP actuelle utilisée via le proxy Tor."""
    proxies = {
        'http': f'socks5://127.0.0.1:{access_port}',
        'https': f'socks5://127.0.0.1:{access_port}'
    }
    try:
        response = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=10)
        if response.ok:
            ip_info = response.json()
            print("IP actuelle via Tor :", ip_info.get("origin"))
            return ip_info.get("origin")
        else:
            print("Impossible de récupérer l'IP actuelle.")
    except Exception as e:
        print("Erreur lors de la récupération de l'IP actuelle :", e)
    return None


def scroll(driver):
    """Fait défiler la page pour charger tous les éléments"""
    current_position = 0
    while current_position < driver.execute_script("return document.body.scrollHeight"):
        driver.execute_script(f"window.scrollBy(0, 500);")
        current_position += 500
        time.sleep(0.07)
        page_height = driver.execute_script("return document.body.scrollHeight")

def is_rate_limited(driver):
    """Vérifie si la page affiche un message de limitation de taux"""
    try:
        rate_limit_message = driver.find_elements(By.XPATH, "//h1[contains(text(), 'You have been temporarily rate limited on SteamDB')]")
        if rate_limit_message:
            print("Détection de limitation de taux sur SteamDB")
            return True
        return False
    except Exception:
        return False

def wait_for_rate_limit(driver):
    """Attend que la limitation de taux soit levée"""
    while is_rate_limited(driver):
            print(f"Limitation de taux détectée. Attente de 60 secondes")
            renew_tor_ip("butinfo", control_port, driver)
            get_current_ip(access_port)
            time.sleep(60)
            driver.refresh()


def check_ip_banned(driver):
    try:
        page_source = driver.get_page_source().lower()
        if "banned" in page_source:
            time.sleep(20)
            return True
        ban_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Your network has been banned on SteamDB')]")
        if ban_elements:
            for element in ban_elements:
                if "banned" in element.text.lower():
                    time.sleep(20)
                    return True
        return False
    except Exception as e:
        print(f"Erreur lors de la vérification du bannissement de l'IP: {e}")
        time.sleep(20)
        return True


def download_charts(driver, steamid, output_folder):
    """Extrait les données de joueurs et critiques depuis le tableau HTML, et télécharge le graphique de prix de SteamDB"""
    results = {
        "players": False,
        "reviews": False,
        "price": False
    }
    try:
        driver.uc_open_with_reconnect(f"https://steamdb.info/app/{steamid}/charts/", 8)
        driver.uc_gui_handle_captcha()
        time.sleep(0.2)
        scroll(driver)
        if is_rate_limited(driver):
            wait_for_rate_limit(driver)
            if is_rate_limited(driver):
                print(f"Limitation de taux persistante pour {steamid}, passage au jeu suivant")
                return results
        page_source = driver.get_page_source()
        soup = BeautifulSoup(page_source, 'html.parser')
        tables = soup.select("table#chart-month-table")
        if len(tables) > 0:
            players_table = tables[0]
            headers = ["Month", "Peak", "Gain", "Percent_Gain"]
            rows = []
            for tr in players_table.select("tbody tr"):
                cells = tr.select("td")
                if len(cells) >= 4:
                    if cells[0].text.strip() == "Last 30 days":
                        m = "March 2025"
                    else:
                        m = cells[0].text.strip()
                    month = convert_any_date(m)
                    peak = cells[1].text.strip().replace(',', '')
                    gain = cells[2].text.strip().replace(',', '').replace('+', '').replace('-', '-')
                    percent_gain = cells[3].text.strip().replace('%', '').replace('+', '').replace('-', '-')
                    rows.append([month, peak, gain, percent_gain])
            if rows:
                players_df = pd.DataFrame(rows, columns=headers)
                players_file = os.path.join(output_folder, "players", f"{steamid}_players.csv")
                players_df.to_csv(players_file, index=False)
                print(f"{len(rows)} lignes trouvées pour les joueurs du jeu {steamid}")
                results["players"] = True
            else:
                print(f"Aucune donnée de joueurs trouvée pour le jeu {steamid}")

    except Exception as e:
        print(f"Erreur lors de l'extraction des données des tableaux pour {steamid}: {e}")
        traceback.print_exc()
    try:
        time.sleep(0.2)
        driver.uc_open_with_reconnect(f"https://steamdb.info/app/{steamid}/", 8)
        driver.uc_gui_handle_captcha()
        if is_rate_limited(driver):
            wait_for_rate_limit(driver, 3, 60)
            if is_rate_limited(driver):
                print(f"Limitation de taux persistante pour {steamid}, passage au jeu suivant")
                return results

        scroll(driver)
        charts = driver.find_elements(By.CSS_SELECTOR, ".chart-container")
        c2 = []
        for chart in charts:
            if chart.find_elements(By.CSS_SELECTOR, ".highcharts-button-symbol"):
                c2 += [chart]
        if len(c2) > 0:
            chart = c2[0]
            button = chart.find_element(By.CSS_SELECTOR, ".highcharts-button-symbol")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)
            driver.execute_script(
                "arguments[0].dispatchEvent(new MouseEvent('click', {'bubbles': true, 'cancelable': true}));", button)
            menu_items = chart.find_elements(By.CSS_SELECTOR, ".highcharts-menu-item")
            if menu_items:
                driver.execute_script(
                    "arguments[0].dispatchEvent(new MouseEvent('click', {'bubbles': true, 'cancelable': true}));",
                    menu_items[0])
                download_attempts = 0
                while download_attempts < 3:
                    download_attempts += 1
                    time.sleep(0.5)
                    source_file = find_downloaded_csv()
                    if source_file:
                        target_file = os.path.join(output_folder, "price", f"{steamid}_price.csv")
                        shutil.move(source_file, target_file)
                        print(f"Fichier price récupéré et déplacé vers {target_file}")
                        results["price"] = True
                        break
    except Exception as e:
        print(f"Erreur lors du téléchargement du graphique price pour {steamid}: {e}")
        traceback.print_exc()

    return results


def find_downloaded_csv():
    """Trouve le fichier CSV téléchargé dans le dossier de téléchargement"""
    download_folder = "downloaded_files"
    if not os.path.exists(download_folder):
        print(f"Dossier de téléchargement {download_folder} introuvable")
        return None

    files = os.listdir(download_folder)
    csv_files = [f for f in files if f.endswith('.csv') and ('chart' in f.lower() or 'charts' in f.lower())]

    if csv_files:
        return os.path.join(download_folder, csv_files[0])
    return None

def setup_driver(control_port, access_port):
    print("Initialisation du navigateur")
    driver = Driver(uc=True, proxy=f"socks5://127.0.0.1:{access_port}")
    driver.uc_open_with_reconnect("https://steamdb.info", 8)
    driver.uc_gui_handle_captcha()
    time.sleep(2)
    driver.uc_gui_handle_captcha()

    if is_rate_limited(driver):
        print("Limitation de taux détectée lors de l'initialisation")
        if not wait_for_rate_limit(driver):
            print("Limitation de taux persistante, redémarrage du navigateur...")
            driver.quit()
            time.sleep(5)
            return setup_driver(control_port, access_port)
    print("Navigateur initialisé !")
    return driver


def fetch_games_batch(start_id, batch_size):
    try:
        print("Connexion à la base de données...")
        engine = get_engine()
        query = text(
            "SELECT appid, name FROM steamgames WHERE recommendations > 1000 LIMIT :batch_size OFFSET :start_id")
        with engine.connect() as connection:
            result = connection.execute(query, {"start_id": start_id, "batch_size": batch_size})
            games = [(row[0], row[1]) for row in result]
        print(f"{len(games)} jeux récupérés de la base de données.")
        return games
    except Exception as e:
        print(f"Erreur lors de la récupération des jeux: {str(e)}")
        return []


def fetch_total_games():
    try:
        print("Connexion à la base de données...")
        engine = get_engine()
        query = text("SELECT COUNT(*) FROM steamgames WHERE recommendations > 1000")
        with engine.connect() as connection:
            result = connection.execute(query)
            total_games = result.scalar()
        print(f"{total_games} jeux au total dans la base de données.")
        return total_games
    except Exception as e:
        print(f"Erreur lors de la récupération du nombre total de jeux: {str(e)}")
        return 0



def save_games_data_to_csv(data, output_dir, filename):
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    data.to_csv(full_path, index=False)
    print(f"Données sauvegardées dans {full_path}")

def main(instance_number=1, total_instances=1, access_port=9050, control_port=9051):
    print(f"Démarrage du processus pour l'instance {instance_number}/{total_instances}")
    print(f"Ports Tor: accès={access_port}, contrôle={control_port}")
    output_folder = setup_output_folders(instance_number)
    total_games = fetch_total_games()
    renew_tor_ip("butinfo", control_port)
    games_per_instance = total_games // total_instances
    if instance_number == total_instances:
        games_per_instance += total_games % total_instances
    start_id = (instance_number - 1) * (total_games // total_instances)

    print(f"Plage de jeux : {start_id} à {start_id + games_per_instance}")

    block_dir = os.path.join(output_folder, "blocks")
    os.makedirs(block_dir, exist_ok=True)
    block_files = [f for f in os.listdir(block_dir) if f.startswith(f"block_games_{instance_number}_") and f.endswith(".csv")] if os.path.exists(block_dir) else []

    last_processed = 0
    if block_files:
        last_file = sorted(block_files, key=lambda x: int(x.split('_')[-1].split('.')[0]))[-1]
        last_processed = int(last_file.split('_')[-1].split('.')[0])
        print(f"Reprise du traitement à partir de {last_processed} jeux déjà traités")

    adjusted_start_id = start_id + last_processed
    remaining_games = games_per_instance - last_processed
    BLOCK_SIZE = 10
    all_results = []
    driver = setup_driver(control_port, access_port)
    renew_tor_ip("butinfo", control_port, driver)
    get_current_ip(access_port)

    time.sleep(1400)
    # Your network has been banned on SteamDB
    try:
        for block_start in range(0, remaining_games, BLOCK_SIZE):
            block_end = min(block_start + BLOCK_SIZE, remaining_games)
            block_size = block_end - block_start
            block_start_id = adjusted_start_id + block_start
            print(f"\n=== Début du bloc {block_start + 1}-{block_end} (sur {remaining_games}) ===")
            print(block_start_id)
            steam_games = fetch_games_batch(block_start_id, block_size)
            if not steam_games:
                print("Aucun jeu récupéré pour ce bloc. Passage au bloc suivant.")
                continue
            block_results = []
            for i, (steam_id, game_name) in enumerate(steam_games, 1):
                global_index = last_processed + block_start + i
                time.sleep(random.uniform(0.5, 1.5))
                if i % 5 == 0 and i > 0:
                    renew_tor_ip("butinfo", control_port, driver)
                    get_current_ip(access_port)
                    time.sleep(1)

                print(f"\nTraitement du jeu {global_index}/{games_per_instance}: {game_name} (Steam ID: {steam_id})")
                try:
                    chart_results = download_charts(driver, steam_id, output_folder)
                    result_row = {
                        'steam_id': steam_id,
                        'game_name': game_name,
                        'players_chart': chart_results['players'],
                        'reviews_chart': chart_results['reviews'],
                        'price_chart': chart_results['price'],
                        'processed_time': time.strftime('%Y-%m-%d %H:%M:%S')
                    }

                    block_results.append(result_row)
                    all_results.append(result_row)

                except Exception as e:
                    print(f"Erreur lors du traitement du jeu {game_name}: {e}")
                    traceback.print_exc()
                    continue

                time.sleep(random.uniform(24,28))
            if i % 10 == 0:
                try:
                    driver.delete_all_cookies()
                    driver.execute_script("localStorage.clear();")
                    driver.execute_script("sessionStorage.clear();")
                except Exception as e:
                    print(f"Erreur lors du vidage du cache: {e}")

            block_end_index = last_processed + block_end
            if block_results:
                df_block = pd.DataFrame(block_results)
                block_filename = f"block_games_{instance_number}_{block_end_index}.csv"
                save_games_data_to_csv(df_block, block_dir, block_filename)
                print(f"Bloc sauvegardé: {block_filename}")

            print(f"Fin du bloc. Pause de 60/70 secondes avant le prochain bloc...")
            time.sleep(random.uniform(80, 90))
        try:
            driver.quit()
        except:
            pass
    except Exception as e:
        print(f"Erreur critique lors du traitement: {e}")
        traceback.print_exc()
        try:
            driver.quit()
        except:
            pass
    if all_results:
        df_results = pd.DataFrame(all_results)
        results_filename = f"steamdb_results_{instance_number}.csv"
        save_games_data_to_csv(df_results, output_folder, results_filename)
        print(f"\nToutes les données ont été sauvegardées dans {output_folder}/{results_filename}")
        print(f"Nombre total de jeux traités: {len(all_results)}")
    else:
        print("Aucune donnée n'a été récupérée.")

if __name__ == "__main__":
    ports_config = {
        1: (9050, 9051),
        2: (10050, 10051),
        3: (11050, 11051),
        4: (12050, 12051),
        5: (13050, 13051),
        6: (14050, 14051),
        7: (15050, 15051),
        8: (16050, 16051),
        9: (17050, 17051),
        10: (18050, 18051),
        11: (19050, 19051),
        12: (20050, 20051),
        13: (21050, 21051),
        14: (22050, 22051),
        15: (23050, 23051),
        16: (24050, 24051),
        17: (25050, 25051),
        18: (26050, 26051),
        19: (27050, 27051),
        20: (28050, 28051),
        21: (29050, 29051),
        22: (30050, 30051),
        23: (31050, 31051),
        24: (32050, 32051)
    }

    if len(sys.argv) == 3:
        try:
            instance_number = int(sys.argv[1])
            total_instances = int(sys.argv[2])
            if instance_number < 1 or total_instances < 1 or instance_number > total_instances:
                print("Les arguments doivent être des entiers positifs valides.")
                print("Syntaxe : script.py {numéro_instance} {nombre_total_instances}")
                sys.exit(1)

            access_port, control_port = ports_config.get(instance_number, (9050, 9051))
            main(instance_number, total_instances, access_port, control_port)
        except ValueError:
            print("Les arguments doivent être des entiers valides.")
            print("Syntaxe : script.py {numéro_instance} {nombre_total_instances}")
            sys.exit(1)
    else:
        print("Exécution en mode instance unique...")
        main()