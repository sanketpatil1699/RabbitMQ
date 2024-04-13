import pika
import json
import random

credentials = pika.PlainCredentials("guest", "guest")

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
channel = connection.channel()

# Define exchange and routing key 
exchange_name = "EXCHANGE_NAME"
routing_key = "ROUTING_KEY"
queue_name = "QUEUE_NAME"

# Message to send
temp = round(random.uniform(17.0, 24.0), 2)
hum = random.uniform(50.0, 59.3)
data = {"Temperature": temp, "Humidity": hum}

# Encode message as JSON
message_body = json.dumps(data).encode()

# Publish message using the exchange and routing key
channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message_body)

connection.close()

print(f"Message sent: {data}")
