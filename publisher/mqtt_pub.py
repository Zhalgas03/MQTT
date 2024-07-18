
import time
import json
import paho.mqtt.client as mqtt
sys.path.append('../python')
from settings import MQTT_BROKER, MQTT_PORT, MQTT_TOPICS
from utils import generate_sensor_data


mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

try:
    while True:
        sensor_data = generate_sensor_data()
        print(f"Generated sensor data: {sensor_data}")
        
        for key, topic in MQTT_TOPICS.items():
            message = json.dumps(sensor_data[key])
            result = mqtt_client.publish(topic, message)
            status = result.rc
            if status == 0:
                print(f"Published {key}: {message} to topic {topic}")
            else:
                print(f"Failed to send message to topic {topic}")

        time.sleep(5)

except KeyboardInterrupt:
    print("Exiting...")

finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("MQTT connection closed")
