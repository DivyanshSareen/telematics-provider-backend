import json
import os
from kafka import KafkaProducer
from typing import Optional

class GPSKafkaProducer:
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.topic = "gps-topic"
        
    def connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            return True
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            return False
    
    def publish_gps_data(self, vehicle_data: dict) -> bool:
        if not self.producer:
            if not self.connect():
                return False
                
        try:
            future = self.producer.send(
                self.topic,
                value=vehicle_data,
                key=vehicle_data.get('vehicleId')
            )
            future.get(timeout=10)
            return True
        except Exception as e:
            print(f"Failed to publish message to Kafka: {e}")
            return False
    
    def close(self):
        if self.producer:
            self.producer.close()