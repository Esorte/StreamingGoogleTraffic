import requests
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utc+7')
)

# Google Maps API details
API_KEY = 'mai mee'
URL = "https://maps.googleapis.com/maps/api/distancematrix/json"

# Locations
origin1 = "long, lat"  # bkk
destination1 = "long, lat"  # muic

while True:
    response = requests.get(URL, params={
        "origin1": origins,
        "destination1": destinations,
        "departure_time": "now",
        "key": API_KEY
    })
    
    if response.status_code == 200:
        data = response.json()
        producer.send('traffic_data', value=data)
        print("Data sent to Kafka:", data)
    else:
        print("Failed to get data:", response.status_code, response.text)
    
    time.sleep(60)  # run every minute
