from seleniumbase import Driver
import pandas as pd
import time
import os
import random
import re
import sys
import dateparser
from bs4 import BeautifulSoup
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mysql.db_connection import get_engine
from sqlalchemy import text
import requests
from stem import Signal
from stem.control import Controller
from dotenv import load_dotenv

if os.path.exists('.env.local'):
    load_dotenv('.env.local')
else:
    load_dotenv('.env')

def renew_tor_ip(password, control_port):
    """Renew Tor IP address"""
    try:
        with Controller.from_port(port=control_port) as controller:
            controller.authenticate(password=password)
            controller.signal(Signal.NEWNYM)
            print("Nouvelle identité demandée à Tor.")
    except Exception as e:
        print(f"Erreur lors du renouvellement de l'IP: {e}")


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

def clean_game_name(name):
    """Nettoie le nom du jeu pour faciliter la comparaison"""
    if not name or not isinstance(name, str):
        return ""
    name = name.lower()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', '-', name)
    return name

def find_game_on_twitchtracker(driver, game_name, max_retries=3):
    """Cherche un jeu sur TwitchTracker et récupère son ID avec gestion d'erreurs améliorée"""
    for retry in range(max_retries):
        try:
            print("Bypass Cloudflare")
            base_url = "https://twitchtracker.com/search?q=" + game_name
            driver.uc_open_with_reconnect(base_url)
            driver.uc_gui_click_captcha()
            try:
                driver.wait_for_element('a.navbar-brand', timeout=60)
            except:
                if driver.is_element_visible("iframe[title*='challenge']", by="css selector"):
                    print("Captcha détecté, tentative de résolution...")
                    driver.switch_to_frame("iframe[title*='challenge']")
                    if driver.is_element_visible("input[type='checkbox']", by="css selector"):
                        driver.click("input[type='checkbox']")
                    driver.switch_to_default_content()
                    time.sleep(5)
                    driver.wait_for_element('a.navbar-brand', timeout=60)

            page_source = driver.get_page_source()
            soup = BeautifulSoup(page_source, 'html.parser')
            categories_header = soup.find('h3', string='Categories')
            if not categories_header:
                print("Cloudflare a bloqué la requête ou structure de page inattendue")
                time.sleep(random.uniform(5, 10))
                continue

            try:
                games_table = categories_header.find_next('tbody')
                if not games_table:
                    print(f"Pas de tableau de jeux trouvé pour '{game_name}'")
                    return None
                games = games_table.select('tr')
                if not games:
                    print(f"Aucun jeu trouvé dans la liste pour '{game_name}'")
                    return None
            except AttributeError:
                print(f"Structure du DOM inattendue pour '{game_name}'")
                return None

            cleaned_game_name = clean_game_name(game_name)
            for row in games:
                try:
                    game_link = row.select_one("a.item-title")
                    if not game_link:
                        continue
                    found_game_name = game_link.text.strip()
                    href = game_link.get('href')
                    if clean_game_name(found_game_name) == cleaned_game_name:
                        match = re.search(r'/games/(\d+)', href)
                        if match:
                            twitch_tracker_id = match.group(1)
                            print(f"Correspondance trouvée pour '{game_name}': ID TwitchTracker = {twitch_tracker_id}")
                            return twitch_tracker_id
                except Exception as e:
                    print(f"Erreur en analysant un résultat pour '{game_name}': {str(e)}")
                    continue

            print(f"Aucune correspondance trouvée pour '{game_name}'")
            return None

        except Exception as e:
            print(f"Erreur lors de la recherche pour '{game_name}' (tentative {retry + 1}/{max_retries}): {str(e)}")
            traceback.print_exc()
            if retry < max_retries - 1:
                print(f"Nouvelle tentative dans 10 secondes...")
                time.sleep(10)
            else:
                print(f"Échec après {max_retries} tentatives")
                return None

    return None


def scrape_twitch_tracker_game_data(driver, twitch_tracker_id, max_retries=3):
    """Scrape les données d'un jeu sur TwitchTracker avec SeleniumBase Driver"""
    for attempt in range(max_retries):
        try:
            url = f"https://twitchtracker.com/games/{twitch_tracker_id}"

            driver.uc_open_with_reconnect(url)
            driver.uc_gui_click_captcha()
            try:
                driver.wait_for_element('a.navbar-brand', timeout=120)
            except:
                if driver.is_element_visible("iframe[title*='challenge']", by="css selector"):
                    print("Captcha détecté, tentative de résolution...")
                    driver.switch_to_frame("iframe[title*='challenge']")
                    if driver.is_element_visible("input[type='checkbox']", by="css selector"):
                        driver.click("input[type='checkbox']")
                    driver.switch_to_default_content()
                    time.sleep(5)
                    driver.wait_for_element('a.navbar-brand', timeout=120)

            page_source = driver.get_page_source()
            soup = BeautifulSoup(page_source, 'html.parser')
            table_selector = "div.dataTables_scrollBody"
            div_element = soup.select_one(table_selector)

            if div_element:
                table = div_element.find('table')
                if table:
                    headers = [th.text.strip() for th in table.select("thead th")]
                    headers[2]="AvgViewersGain"
                    headers[3]="AvgViewers%Gain"
                    headers[6]="AvgStreamsGain"
                    headers[7]="AvgStreamsùGain"
                    rows = [
                        [td.text.strip() for td in tr.select("td")]
                        for tr in table.select("tbody tr")
                    ]

                    if rows:
                        print(f"{len(rows)} lignes trouvées pour le jeu {twitch_tracker_id}")
                        return headers, rows
                    else:
                        print(f"Aucune ligne trouvée pour le jeu {twitch_tracker_id}")
            else:
                print(f"Élément de tableau non trouvé pour le jeu {twitch_tracker_id}")

        except Exception as e:
            print(f"Tentative {attempt + 1} échouée pour le jeu {twitch_tracker_id}: {str(e)}")
            traceback.print_exc()
            time.sleep(random.uniform(5, 10))

    print(f"Échec de la récupération des données pour le jeu {twitch_tracker_id} après {max_retries} tentatives")
    return None, None


def fetch_games_batch(start_id, batch_size):
    """Récupère un lot de jeux à partir d'un ID de départ"""
    try:
        print("Connexion à la base de données...")
        engine = get_engine()
        query = text("SELECT appid, name FROM steamgames LIMIT :batch_size OFFSET :start_id")

        with engine.connect() as connection:
            result = connection.execute(query, {"start_id": start_id, "batch_size": batch_size})
            games = [(row[0], row[1]) for row in result]

        print(f"{len(games)} jeux récupérés de la base de données.")
        return games

    except Exception as e:
        print(f"Erreur lors de la récupération des jeux: {str(e)}")
        return []


def fetch_total_games():
    """Récupère le nombre total de jeux dans la base de données"""
    try:
        print("Connexion à la base de données...")
        engine = get_engine()
        query = text("SELECT COUNT(*) FROM steamgames")

        with engine.connect() as connection:
            result = connection.execute(query)
            total_games = result.scalar()

        print(f"{total_games} jeux au total dans la base de données.")
        return total_games

    except Exception as e:
        print(f"Erreur lors de la récupération du nombre total de jeux: {str(e)}")
        return 0


def save_games_data_to_csv(data, output_dir, filename):
    """Sauvegarde les données des jeux dans un fichier CSV"""
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    data.to_csv(full_path, index=False)
    print(f"Données sauvegardées dans {full_path}")


def main(instance_number, total_instances, access_port, control_port):
    total_games = fetch_total_games()
    games_per_instance = total_games // total_instances
    if instance_number == total_instances:
        games_per_instance += total_games % total_instances
    start_id = (instance_number - 1) * (total_games // total_instances)
    print(f"Démarrage du processus pour l'instance {instance_number}/{total_instances}")
    print(f"Plage de jeux : {start_id} à {start_id + games_per_instance}")

    output_dir = f"output/output_{instance_number}"
    os.makedirs(output_dir, exist_ok=True)
    output_csv = f"twitch_tracker_games_{instance_number}.csv"

    block_files = [f for f in os.listdir(output_dir) if
                   f.startswith(f"block_games_{instance_number}_") and f.endswith(".csv")] if os.path.exists(
        output_dir) else []

    last_processed = 0
    if block_files:
        last_file = sorted(block_files, key=lambda x: int(x.split('_')[-1].split('.')[0]))[-1]
        last_processed = int(last_file.split('_')[-1].split('.')[0])
        print(f"Reprise du traitement à partir de {last_processed} jeux déjà traités")

    adjusted_start_id = start_id + last_processed
    remaining_games = games_per_instance - last_processed
    BLOCK_SIZE = 30
    all_games_data = []
    headers = None

    if block_files:
        block_files = sorted(block_files, key=lambda x: int(x.split('_')[-1].split('.')[0]))
        for block_file in block_files:
            try:
                block_path = os.path.join(output_dir, block_file)
                block_df = pd.read_csv(block_path)

                for _, row in block_df.iterrows():
                    all_games_data.append(row.tolist())

                print(f"Chargé le bloc {block_file} avec {len(block_df)} entrées")
            except Exception as e:
                print(f"Erreur lors du chargement du bloc {block_file}: {e}")

    print(f"Total de {len(all_games_data)} entrées chargées depuis les blocs existants")

    proxy_server = f"socks5://127.0.0.1:{access_port}"
    driver_args = {
        "uc": True,
        "proxy": proxy_server
    }

    driver = Driver(**driver_args)
    try:
        driver.uc_open_with_reconnect("https://twitchtracker.com")
        driver.uc_gui_click_captcha()
        try:
            driver.wait_for_element('a.navbar-brand', timeout=120)
        except:
            if driver.is_element_visible("iframe[title*='challenge']", by="css selector"):
                print("Captcha détecté, tentative de résolution...")
                driver.switch_to_frame("iframe[title*='challenge']")
                if driver.is_element_visible("input[type='checkbox']", by="css selector"):
                    driver.click("input[type='checkbox']")
                driver.switch_to_default_content()
                time.sleep(5)
        driver.uc_open_with_reconnect("https://twitchtracker.com/games/")
        driver.uc_gui_click_captcha()
        try:
            driver.wait_for_element('a.navbar-brand', timeout=120)
        except:
            if driver.is_element_visible("iframe[title*='challenge']", by="css selector"):
                print("Captcha détecté, tentative de résolution...")
                driver.switch_to_frame("iframe[title*='challenge']")
                if driver.is_element_visible("input[type='checkbox']", by="css selector"):
                    driver.click("input[type='checkbox']")
                driver.switch_to_default_content()
                time.sleep(5)
        time.sleep(500)

        for block_start in range(0, remaining_games, BLOCK_SIZE):
            block_end = min(block_start + BLOCK_SIZE, remaining_games)
            block_size = block_end - block_start
            block_start_id = adjusted_start_id + block_start
            print(f"\n=== Début du bloc {block_start + 1}-{block_end} (sur {remaining_games}) ===")
            steam_games = fetch_games_batch(block_start_id, block_size)
            if not steam_games:
                print("Aucun jeu récupéré pour ce bloc. Passage au bloc suivant.")
                continue

            block_data = []
            for i, (steam_id, game_name) in enumerate(steam_games, 1):
                global_index = last_processed + block_start + i
                time.sleep(random.uniform(0, 1))

                if i % 3 == 0 and i > 0:
                    renew_tor_ip("butinfo", control_port)
                    get_current_ip(access_port)
                    time.sleep(2)

                if not game_name:
                    print(f"Jeu {global_index}/{games_per_instance}: ID Steam {steam_id} - Pas de nom, ignoré")
                    continue

                print(f"\nTraitement du jeu {global_index}/{games_per_instance}: {game_name} (Steam ID: {steam_id})")

                try:
                    twitch_tracker_id = find_game_on_twitchtracker(driver, game_name)
                    if twitch_tracker_id:
                        game_headers, rows = scrape_twitch_tracker_game_data(driver, twitch_tracker_id)
                        if game_headers and rows:
                            if headers is None:
                                headers = game_headers
                            for row in rows:
                                processed_row = []
                                if row and len(row) > 0:
                                    date_str = row[0]
                                    parsed_date = dateparser.parse(date_str)
                                    if parsed_date:
                                        formatted_date = parsed_date.replace(day=1).strftime("%d/%m/%Y")
                                        processed_row.append(formatted_date)
                                    else:
                                        processed_row.append(None)
                                for i in range(1, len(row)):
                                    value = row[i]
                                    if value == '-':
                                        processed_row.append(None)
                                    else:
                                        if isinstance(value, str):
                                            value = value.replace('+', '').replace('%', '').replace(',', '')
                                            try:
                                                if '.' in value:
                                                    value = float(value)
                                                else:
                                                    value = int(value)
                                            except ValueError:
                                                pass
                                            if isinstance(value, str):
                                                if value.endswith('M'):
                                                    value = int(float(value[:-1]) * 1000000)
                                                elif value.endswith('K'):
                                                    value = int(float(value[:-1]) * 1000)
                                        processed_row.append(value)
                                full_row = [steam_id] + processed_row
                                all_games_data.append(full_row)
                                block_data.append(full_row)

                except Exception as e:
                    print(f"Erreur lors du traitement du jeu {game_name}: {e}")
                    traceback.print_exc()
                    continue
                time.sleep(random.uniform(1, 3))
            if i % 10 == 0:
                try:
                    driver.delete_all_cookies()
                    driver.execute_script("localStorage.clear();")
                    driver.execute_script("sessionStorage.clear();")
                except Exception as e:
                    print(f"Erreur lors du vidage du cache: {e}")
            block_end_index = last_processed + block_end
            if block_data and headers:
                all_headers = ['steam_id'] + headers
                print(headers)
                df_block = pd.DataFrame(block_data, columns=all_headers)
                block_filename = f"block_games_{instance_number}_{block_end_index}.csv"
                save_games_data_to_csv(df_block, output_dir, block_filename)
                print(f"Bloc sauvegardé: {block_filename}")
            print(f"Fin du bloc. Pause de 30 secondes avant le prochain bloc...")
            time.sleep(30)
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

    if all_games_data and headers:
        all_headers = ['steam_id'] + headers
        df_games = pd.DataFrame(all_games_data, columns=all_headers)
        save_games_data_to_csv(df_games, output_dir, output_csv)
        print(f"\nToutes les données ont été sauvegardées dans {output_dir}/{output_csv}")
        print(f"Nombre total de jeux traités: {len(all_games_data)}")
    else:
        print("Aucune donnée n'a été récupérée.")


if __name__ == "__main__":
    access_port = 9050
    control_port = 9051
    if len(sys.argv) == 3:
        try:
            instance_number = int(sys.argv[1])
            total_instances = int(sys.argv[2])
            if instance_number < 1 or total_instances < 1 or instance_number > total_instances:
                print("Les arguments doivent être des entiers positifs valides.")
                print("Syntaxe : script.py {numéro_instance} {nombre_total_instances}")
                sys.exit(1)

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
                12: (20050, 20051)
            }

            if instance_number in ports_config:
                access_port, control_port = ports_config[instance_number]

            main(instance_number, total_instances, access_port, control_port)
        except ValueError:
            print("Les arguments doivent être des entiers valides.")
            print("Syntaxe : script.py {numéro_instance} {nombre_total_instances}")
            sys.exit(1)
    else:
        print("Syntaxe incorrect. Utilisez : script.py {numéro_instance} {nombre_total_instances}")
        print("Exemple : python script.py 1 10  # Pour lancer l'instance 1 sur un total de 10 instances")
