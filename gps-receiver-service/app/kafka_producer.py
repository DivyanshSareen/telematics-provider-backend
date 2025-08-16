import json
import os
from aiokafka import AIOKafkaProducer
from typing import Optional

class GPSKafkaProducer:
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.topic = "gps-topic"
        
    async def connect(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            await self.producer.start()
            return True
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            return False
    
    async def publish_gps_data(self, vehicle_data: dict) -> bool:
        if not self.producer:
            if not await self.connect():
                return False
                
        try:
            composite_key = f"{vehicle_data.get('vehicleId')}:{vehicle_data.get('driverId')}"
            await self.producer.send(
                self.topic,
                value=vehicle_data,
                key=composite_key
            )
            return True
        except Exception as e:
            print(f"Failed to publish message to Kafka: {e}")
            return False
    
    async def close(self):
        if self.producer:
            await self.producer.stop()