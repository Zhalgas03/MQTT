# MQTT settings
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPICS = {
    "air_quality": "sensor/air_quality",
    "temperature": "sensor/temperature",
    "humidity": "sensor/humidity"
}

# MySQL settings
MYSQL_HOST = "localhost"
MYSQL_PORT = 3310
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "mqtt_db"
MYSQL_TABLE = "sensor_data"

# MongoDB settings
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "mqtt_db"
MONGO_COLLECTIONS = {
    "air_quality": "air_quality_data",
    "temperature": "temperature_data",
    "humidity": "humidity_data"
}

# Neo4j settings
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "zhalgas03"
