import pika
import paho.mqtt.client as mqtt
import logging
# RabbitMQ connection details
credentials = pika.PlainCredentials("guest", "guest")
logging.basicConfig(level=logging.INFO)
print(logging.basicConfig(level=logging.INFO))
# MQTT broker details
mqtt_broker = "MQTT_BROKER_IP"
mqtt_topic = "TOPIC_NAME"
exchange_name = "EXCHANGE_NAME"
routing_key = "ROUTING_KEY"

#Mongodb details
mongo_uri = "mongodb://localhost:27017/"  # Replace with your actual connection URI
mongo_db_name = "DATABASE_NAME"
mongo_collection_name = "COLLECTION_NAME"


def on_message(channel, method, properties, body):
    try:
        print("**********")
        client = MongoClient(mongo_uri)
        db = client[mongo_db_name]
        collection = db[mongo_collection_name]

        # Forward message to MQTT broker
        mqtt_client.publish(mqtt_topic, body)
        logging.info(f"Forwarded message to MQTT topic: {mqtt_topic}")

        # Store message in MongoDB
        data = {"message": body.decode(), "timestamp": method.timestamp}
        collection.insert_many(data)
        logging.info(f"Stored message in MongoDB: {data}")
       
        channel.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message consumption

    except Exception as e:
        logging.error(f"Error processing message: {e}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT Broker!")
    else:
        logging.error(f"Failed to connect to MQTT Broker: {rc}")

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
channel = connection.channel()

result = channel.queue_declare(queue="QUEUE_NAME")

queue_name = result.method.queue

channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

channel.basic_consume(queue=queue_name, on_message_callback=on_message)

# Connect to MQTT broker
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.connect(mqtt_broker)

# Start consuming messages
print("Waiting for messages...")
channel.start_consuming()
