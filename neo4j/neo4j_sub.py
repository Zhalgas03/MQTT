
import json
import paho.mqtt.client as mqtt
from neo4j import GraphDatabase
sys.path.append('../python')
from settings import MQTT_BROKER, MQTT_PORT, MQTT_TOPICS, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def on_message(client, userdata, msg):
    message = msg.payload.decode()
    print(f"Received message on topic {msg.topic}: {message}")

    with neo4j_driver.session() as session:
        session.run(
            "CREATE (s:SensorData {topic: $topic, message: $message})",
            topic=msg.topic,
            message=message
        )
    print("Message saved to Neo4j")

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

    neo4j_driver.close()
    print("Neo4j connection closed")
