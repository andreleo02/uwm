import schedule
import time
import requests

def read_data_api():
    # Costruisci l'URL con il parametro per ottenere solo i dati relativi al 2024
    url = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/netvox-r718x-bin-sensor/records?order_by=time%20DESC&limit=20&timezone=Australia%2FMelbourne"


# Send GET request to API with parameters
    response = requests.get(url)
  
    if response.status_code == 200:
        data = response.json()
        # Processa i dati come necessario
        print(data)
    else:
        print("Errore:", response.status_code)

# Definisci la pianificazione per eseguire read_data_api() ogni 2 minuti
schedule.every(1).minutes.do(read_data_api)

# Loop principale per eseguire la pianificazione
while True:
    schedule.run_pending()
    time.sleep(1)