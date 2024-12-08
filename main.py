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

location_map = {
    (???, ???): (???, ???),  # ori -> dest
    (???, ???): (???, ???)

}
# origin1 = "long, lat"  # bkk
# destination1 = "long, lat"  # muic

while True:
    for (origin_lat, origin_long), (dest_lat, dest_long) in location_map.items():
        origins = f"{origin_lat},{origin_long}"
        destinations = f"{dest_lat},{dest_long}"
        
        response = requests.get(URL, params={
            "origins": origins,
            "destinations": destinations,
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
