
import json
import paho.mqtt.client as mqtt
import mysql.connector
sys.path.append('../python')
from settings import MQTT_BROKER, MQTT_PORT, MQTT_TOPICS, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_TABLE

try:
    mysql_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    mysql_cursor = mysql_conn.cursor()
    print("Connected to MySQL database")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {MYSQL_TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        topic VARCHAR(255) NOT NULL,
        message TEXT NOT NULL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    mysql_cursor.execute(create_table_query)
    print(f"Table '{MYSQL_TABLE}' created successfully")

except mysql.connector.Error as err:
    print(f"Error connecting to MySQL: {err}")
    exit(1)

def on_message(client, userdata, msg):
    message = msg.payload.decode()
    print(f"Received message on topic {msg.topic}: {message}")
    
    insert_query = f"INSERT INTO {MYSQL_TABLE} (topic, message) VALUES (%s, %s)"
    insert_values = (msg.topic, message)
    
    try:
        mysql_cursor.execute(insert_query, insert_values)
        mysql_conn.commit()
        print("Message saved to MySQL")
    
    except mysql.connector.Error as err:
        print(f"Error inserting into MySQL: {err}")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        for topic in MQTT_TOPICS.values():
            client.subscribe(topic)
            print(f"Subscribed to topic {topic}")
    else:
        print(f"Failed to connect to MQTT broker with code {rc}")


mqtt_client = mqtt.Client()
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

    if mysql_conn.is_connected():
        mysql_cursor.close()
        mysql_conn.close()
        print("MySQL connection closed")
