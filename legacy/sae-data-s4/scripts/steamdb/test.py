from seleniumbase import Driver
import os
import shutil
from dotenv import load_dotenv
import time
from selenium.webdriver.common.by import By

if os.path.exists('.env.local'):
    load_dotenv('.env.local')
else:
    load_dotenv('.env')

steamid = 3164500
destination_folder = "scripts/steamdb/output"

os.makedirs(os.path.join(destination_folder, "players"), exist_ok=True)
os.makedirs(os.path.join(destination_folder, "reviews"), exist_ok=True)
os.makedirs(os.path.join(destination_folder, "price"), exist_ok=True)

driver = Driver(uc=True, proxy="socks5://127.0.0.1:11050")
driver.uc_open_with_reconnect("https://steamdb.info", 4)
driver.uc_gui_handle_captcha()
cookie = {"name": "__Host-steamdb", "value": "7725226-bc1fa8b83f225d4aeaaa961c2959fde2be73e309", "domain": "",
          "path": "/", "expires": 1750952369, "httpOnly": True, "secure": True, "sameSite": "Lax"}
driver.add_cookie(cookie)
driver.uc_open_with_reconnect(f"https://steamdb.info/app/{steamid}/charts/", 4)
driver.uc_gui_handle_captcha()

def scroll():
    current_position = 0
    while current_position < driver.execute_script("return document.body.scrollHeight"):
        driver.execute_script(f"window.scrollBy(0, 1000);")
        current_position += 1000
        time.sleep(0.05)
        page_height = driver.execute_script("return document.body.scrollHeight")

scroll()
print("Scroll terminé")
time.sleep(1)

charts = driver.find_elements(By.CSS_SELECTOR, ".chart-container")
c2 = []
for chart in charts:
    if chart.find_elements(By.CSS_SELECTOR, ".highcharts-button-symbol"):
        c2 += [chart]
print(f"Nombre de graphiques trouvés: {len(c2)}")

for i, chart in enumerate(c2):
    if i == 0 or i == 2:
        print(f"Traitement du graphique {i}")
        button = chart.find_element(By.CSS_SELECTOR, ".highcharts-button-symbol")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        time.sleep(0.5)
        driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {'bubbles': true, 'cancelable': true}));", button)
        menu_items = chart.find_elements(By.CSS_SELECTOR, ".highcharts-menu-item")
        if menu_items:
            driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {'bubbles': true, 'cancelable': true}));", menu_items[0])
            time.sleep(0.5)
            if i == 0:
                downloaded_file = os.path.join("downloaded_files", "charts.csv")
                target_file = os.path.join(destination_folder, "players", f"{steamid}_players.csv")
            elif i == 2:
                downloaded_file = os.path.join("downloaded_files", "charts (1).csv")
                target_file = os.path.join(destination_folder, "reviews", f"{steamid}_reviews.csv")

            shutil.move("downloaded_files/chart.csv", target_file)
            print(f"Fichier déplacé vers {target_file}")

driver.uc_open(f"https://steamdb.info/app/{steamid}/")
driver.uc_gui_click_captcha()
time.sleep(0.5)

charts = driver.find_elements(By.CSS_SELECTOR, ".chart-container")
if len(charts) > 0:
    scroll()
    print("Scroll terminé")
    c2 = []
    for chart in charts:
        if chart.find_elements(By.CSS_SELECTOR, ".highcharts-button-symbol"):
            c2 += [chart]
    print(f"Nombre de graphiques trouvés: {len(c2)}")
    for i, chart in enumerate(c2):
        button = chart.find_element(By.CSS_SELECTOR, ".highcharts-button-symbol")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {'bubbles': true, 'cancelable': true}));", button)
        menu_items = chart.find_elements(By.CSS_SELECTOR, ".highcharts-menu-item")
        if menu_items:
            driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {'bubbles': true, 'cancelable': true}));", menu_items[0])
            time.sleep(0.5)
            downloaded_file = os.path.join("downloaded_files", "charts.csv")
            target_file = os.path.join(destination_folder, "price", f"{steamid}_price.csv")
            shutil.move("downloaded_files/chart.csv", target_file)
            print(f"Fichier déplacé vers {target_file}")

driver.quit()