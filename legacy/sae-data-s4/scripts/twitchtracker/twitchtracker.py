import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import random
from pyvirtualdisplay import Display


def bypass_cloudflare(driver):
    """
    Naviguer sur les pages principales pour contourner Cloudflare
    """
    try:
        # Naviguer d'abord sur la page principale
        print("Navigation sur la page principale...")
        driver.get("https://twitchtracker.com/")
        time.sleep(random.uniform(2, 4))

        # Ensuite rechercher un terme générique
        print("Navigation sur la page de recherche...")
        driver.get("https://twitchtracker.com/search?q=aaa")
        time.sleep(random.uniform(2, 4))

        # Vérifier si la page de recherche est chargée
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
        print("Contournement de Cloudflare réussi!")
        return True
    except Exception as e:
        print(f"Erreur lors du contournement de Cloudflare: {e}")
        return False


def batch_scrape_games(games, base_url="https://twitchtracker.com/games/",
                       table_selector="div.dataTables_scrollBody"):
    # Démarrage du virtual display
    display = Display(visible=0, size=(1920, 1080))
    display.start()

    chrome_profile_path = os.path.join(os.path.expanduser("~"), "chrome_selenium_profile")
    os.makedirs(chrome_profile_path, exist_ok=True)
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={chrome_profile_path}")
    options.add_argument("--disable-extensions")
    options.add_argument("--user-data-dir=chromiumprofile")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.binary_location = "/var/lib/flatpak/exports/bin/org.chromium.Chromium"

    driver = None
    results = {}

    try:
        print("Initialisation du navigateur...")
        driver = uc.Chrome(options=options)

        # Étape de contournement de Cloudflare
        if not bypass_cloudflare(driver):
            print("Impossible de contourner Cloudflare")
            return None

        for game in games:
            url = f"{base_url}{game}"
            print(f"\nTraitement du jeu ID: {game}")

            try:
                driver.get(url)
                time.sleep(random.uniform(2, 4))

                # Attente dynamique avec gestion des erreurs
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, table_selector))
                    )
                except Exception as wait_error:
                    print(f"Erreur d'attente pour le jeu {game}: {wait_error}")
                    continue

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                div_element = soup.select_one(table_selector)

                if div_element:
                    table = div_element.find('table')
                    if table:
                        headers = [th.text.strip() for th in table.select("thead th")]

                        rows = []
                        for tr in table.select("tbody tr"):
                            row = [td.text.strip() for td in tr.select("td")]
                            if row:
                                rows.append(row)

                        if rows:
                            df = pd.DataFrame(rows, columns=headers)
                            df['game'] = game
                            results[game] = df
                            print(f"Données extraites avec succès pour le jeu {game}!")
                        else:
                            print(f"Aucune ligne trouvée pour le jeu {game}")
                    else:
                        print(f"Aucun tableau trouvé pour le jeu {game}")
                else:
                    print(f"Élément '{table_selector}' non trouvé pour le jeu {game}")

            except Exception as e:
                print(f"Erreur lors du traitement du jeu {game}: {e}")

        if results:
            all_data = pd.concat(list(results.values()), ignore_index=True)
            return all_data
        else:
            print("Aucune donnée n'a été récupérée")
            return None

    except Exception as e:
        print(f"Une erreur globale est survenue: {str(e)}")
        return None

    finally:
        if driver is not None:
            driver.quit()
        display.stop()


if __name__ == "__main__":
    game_ids = [
        "130942",
        "33214",
        "516575"
    ]

    all_games_data = batch_scrape_games(game_ids)

    if all_games_data is not None:
        all_games_data.to_csv("donnees_jeux_twitch.csv", index=False)
        print("\nToutes les données ont été sauvegardées dans donnees_jeux_twitch.csv")

        for game_id in game_ids:
            game_data = all_games_data[all_games_data['game'] == game_id]
            if not game_data.empty:
                game_data.to_csv(f"jeu_{game_id}.csv", index=False)
                print(f"Données du jeu {game_id} sauvegardées dans jeu_{game_id}.csv")