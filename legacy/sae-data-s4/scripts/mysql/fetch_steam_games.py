import requests
import json
import re
import time
import dateparser
import os
import sys
from datetime import datetime
from stem import Signal
from stem.control import Controller

proxies = {
    'http': 'socks5://127.0.0.1:9050',
    'https': 'socks5://127.0.0.1:9050'
}


def renew_tor_ip(password, control_port=9051):
    try:
        with Controller.from_port(port=control_port) as controller:
            controller.authenticate(password=password)
            controller.signal(Signal.NEWNYM)
            print("Nouvelle identité demandée à Tor.")
    except Exception as e:
        print(f"Erreur lors du renouvellement de l'IP: {e}")


def get_current_ip():
    """Récupère et affiche l'IP actuelle utilisée via le proxy Tor."""
    try:
        response = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=5)
        if response.ok:
            ip_info = response.json()
            print("IP actuelle via Tor :", ip_info.get("origin"))
            return ip_info.get("origin")
        else:
            print("Impossible de récupérer l'IP actuelle.")
    except Exception as e:
        print("Erreur lors de la récupération de l'IP actuelle :", e)
    return None


def convert_any_date(date_string):
    """Convertit n'importe quel format de date en DD/MM/YYYY"""
    parsed_date = dateparser.parse(date_string)
    if parsed_date:
        return parsed_date.strftime("%d/%m/%Y")
    return None


def fetch_games_batch(last_appid, max_count=1000):
    """Récupère un lot de jeux à partir du dernier appid"""
    url = f"https://api.steampowered.com/IStoreService/GetAppList/v0001/?key=083D9D762D28A915AF30059FF3043610&include_games=true&include_dlc=false&include_software=false&include_videos=false&include_hardware=false&max_results={max_count}&last_appid={last_appid}"
    print(f"Récupération des jeux depuis l'API (last_appid={last_appid})...")
    response = requests.get(url)
    if not response.ok:
        print(f"Erreur lors de la récupération de la liste d'applications: {response.status_code}")
        return [], None
    data = response.json()["response"]
    apps = data.get("apps", [])
    new_last_appid = data.get("last_appid")
    print(f"Récupéré {len(apps)} jeux, dernier appid: {new_last_appid}")
    return apps, new_last_appid


def process_games_batch(apps, batch_number, compteur_global, total_saved, total_target):
    """Traite un lot de jeux et extrait les informations détaillées."""
    games_data = []
    total_in_batch = len(apps)

    for index, app in enumerate(apps, start=1):
        appid = app["appid"]
        name = app["name"]
        compteur_global += 1

        progress_text = f"Batch {batch_number}: {index}/{total_in_batch}"
        progress_text += f" (Total: {compteur_global}/{total_target})"

        if index % 10 == 0:
            renew_tor_ip("butinfo")
            get_current_ip()

        max_retries = 3
        for attempt in range(max_retries):
            try:
                store_url = f"https://store.steampowered.com/api/appdetails?l=fr&appids={appid}&cc=fr"
                response = requests.get(store_url, proxies=proxies)

                if not response.ok:
                    print(f'{progress_text} Problème d\'API pour {name} (tentative {attempt + 1}/{max_retries})')
                    if attempt < max_retries - 1:
                        print("Attente de 10 secondes avant nouvel essai...")
                        time.sleep(10)
                        continue
                    else:
                        print(f"Échec après {max_retries} tentatives, passage au jeu suivant.")
                        break

                data = response.json()
                app_details = data.get(str(appid), {})
                if not app_details.get("success") or "data" not in app_details:
                    break
                app_data = app_details["data"]

                release_date = app_data.get("release_date", {})
                date_str = release_date.get("date", "")

                if not date_str or any(
                        term in date_str.lower() for term in ["coming", "soon", "tba", "to be announced", "announced"]
                ):
                    print(f"{progress_text} Jeu non sorti : {name}: {date_str}")
                    break

                is_released = release_date.get("coming_soon", True) == False
                if not is_released:
                    break

                date = convert_any_date(date_str)

                if app_data.get("price_overview"):
                    price = app_data.get("price_overview").get("initial", 0)
                    currency = app_data.get("price_overview").get("currency", None)
                else:
                    price = 0
                    currency = None

                score = app_data.get("metacritic", {}).get("score", None)

                game_info = {
                    "appid": appid,
                    "name": app_data.get("name", ""),
                    "release_date": date,
                    "short_description": app_data.get("short_description", ""),
                    "developers": app_data.get("developers", []),
                    "categories": [category["description"] for category in app_data.get("categories", [])],
                    "genre": [genre["description"] for genre in app_data.get("genres", [])],
                    "header_image": app_data.get("header_image", ""),
                    "price": price,
                    "currency": currency,
                    "windows": app_data.get("platforms", {}).get("windows", None),
                    "linux": app_data.get("platforms", {}).get("linux", None),
                    "mac": app_data.get("platforms", {}).get("mac", None),
                    "metacritic": score,
                    "recommendations": app_data.get("recommendations", {}).get("total", 0)
                }
                games_data.append(game_info)
                print(f"{progress_text} {currency} Jeu n°{appid} ajouté : {name}")
                break

            except Exception as e:
                print(f'{progress_text} Erreur pour {name}: {str(e)} (tentative {attempt + 1}/{max_retries})')
                if attempt < max_retries - 1:
                    print("Attente de 10 secondes avant nouvel essai...")
                    time.sleep(10)
                else:
                    print(f"Échec après {max_retries} tentatives, passage au jeu suivant.")

    return games_data, compteur_global


def save_batch(games_data, batch_number, instance_number):
    """Sauvegarde un lot de jeux dans un fichier JSON"""
    output_dir = f"output/output_{instance_number}"
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{output_dir}/steam_data_{batch_number:03d}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(games_data, f, ensure_ascii=False, indent=2)
    print(f"Sauvegarde de {len(games_data)} jeux dans {filename}")


def main(instance_number):
    total_target = 22000
    start_points = {
        1: 0,
        2: 827330,
        3: 1385670,
        4: 1922500,
        5: 2485470,
        6: 3074230
    }

    compteur_global = 0
    total_saved = 0
    batch_number = 1
    last_appid = start_points.get(instance_number, 0)

    while compteur_global < total_target:
        print(f"\n=== Traitement du batch {batch_number} ===")
        apps, last_appid = fetch_games_batch(last_appid)

        if not apps:
            print("Plus aucun jeu à récupérer ou erreur dans l'API. Arrêt du processus.")
            break

        games_data, compteur_global = process_games_batch(apps, batch_number, compteur_global, total_saved,
                                                          total_target)
        if games_data:
            save_batch(games_data, batch_number, instance_number)
            total_saved += len(games_data)

        print(
            f"\nProgression: {compteur_global}/{total_target} jeux traités ({(compteur_global / total_target) * 100:.2f}%)")
        print(f"Total de jeux sauvegardés: {total_saved}")

        batch_number += 1

        if not last_appid:
            print("Plus aucun jeu disponible. Fin du traitement.")
            break

    print(
        f"\nTraitement terminé! {total_saved} jeux ont été récupérés et sauvegardés dans {batch_number - 1} fichiers.")
    print(f"Total de jeux traités: {compteur_global}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            instance_number = int(sys.argv[1])
            if 1 <= instance_number <= 6:
                main(instance_number)
            else:
                print("Le numéro d'instance doit être entre 1 et 6.")
        except ValueError:
            print("Veuillez fournir un numéro d'instance valide.")
    else:
        print("Veuillez fournir un numéro d'instance (1-6) en argument.")
