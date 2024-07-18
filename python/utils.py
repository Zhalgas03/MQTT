import math
import random
import json
import paho.mqtt.client as mqtt

def generate_sensor_data():
    air_quality = round(random.uniform(0, 100), 2)
    temperature = round(random.uniform(-20, 40), 2)
    humidity = round(random.uniform(0, 100), 2)
    
    
    if math.isnan(air_quality):
        air_quality=0.0
    if math.isnan(temperature):
        temperature=0.0
    if math.isnan(humidity):
        humidity=0.0
    
    sensor_data = {
        "air_quality": air_quality,
        "temperature": temperature,
        "humidity": humidity
    }
    
    return sensor_data
