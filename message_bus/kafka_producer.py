import json 
from kafka import KafkaProducer


# Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x: 
                        json.dumps(x).encode('utf-8'))

def send_message(topic, message):
    producer.send(topic, message)

