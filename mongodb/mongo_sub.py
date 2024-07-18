
import json
import paho.mqtt.client as mqtt
from pymongo import MongoClient
sys.path.append('../python')
from settings import MQTT_BROKER, MQTT_PORT, MQTT_TOPICS, MONGO_URI, MONGO_DB, MONGO_COLLECTIONS

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]

def on_message(client, userdata, msg):
    message = msg.payload.decode()
    print(f"Received message on topic {msg.topic}: {message}")
    

    collection_name = None
    for key, topic in MQTT_TOPICS.items():
        if msg.topic == topic:
            collection_name = MONGO_COLLECTIONS[key]
            break
    
    if collection_name:
        collection = db[collection_name]
        message_document = {
            "topic": msg.topic,
            "message": message
        }
        collection.insert_one(message_document)
        print(f"Message saved to MongoDB collection '{collection_name}'")
    else:
        print("No matching collection found for topic")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        for topic in MQTT_TOPICS.values():
            client.subscribe(topic)
            print(f"Subscribed to topic {topic}")
    else:
        print(f"Failed to connect to MQTT broker with code {rc}")


mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

try:

    while True:
        pass

except KeyboardInterrupt:
    print("Exiting...")

finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("MQTT connection closed")

    mongo_client.close()
    print("MongoDB connection closed")
