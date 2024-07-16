import requests
import time
from utils import *
import json

api_url = "http://localhost:1234"

while True:
        event = generate_event()
        purchased_items = generate_purchased_items()

        try:
                send_event_response = requests.put(f"{api_url}/SendEvent/", json=event.model_dump())
                send_event_response.raise_for_status() 

                purchased_items_response = requests.post(f"{api_url}/PurchasedItems/", json=purchased_items.model_dump())
                purchased_items_response.raise_for_status()  

                print("Istekler basariyla tamamlandi.")

        except requests.exceptions.RequestException as e:
                print("Istek basarisiz oldu:", e)

        time.sleep(1)