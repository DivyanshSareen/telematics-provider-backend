from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from app.connection_manager import ConnectionManager
from app.models import VehicleLocation
from app.kafka_producer import GPSKafkaProducer

router = APIRouter()

manager = ConnectionManager()
kafka_producer = GPSKafkaProducer()

@router.websocket("/ws/vehicle-location")
async def vehicle_location_websocket(websocket: WebSocket):
    if not await manager.connect(websocket):
        # Connection limit reached
        return
    try:
        while True:
            data_text = await websocket.receive_text()

            try:
                location = VehicleLocation.parse_raw(data_text)
            except ValidationError as e:
                await websocket.send_text(f"400 Bad Request: {e.errors()}")
                continue

            print(f"✅ Received valid data: {location}")
            
            # Publish to Kafka
            location_dict = location.dict()
            if kafka_producer.publish_gps_data(location_dict):
                print(f"📤 Published to Kafka topic 'gps-topic': {location.vehicleId}")
                await websocket.send_text("200 OK: Data received, validated and published to Kafka")
            else:
                print(f"❌ Failed to publish to Kafka: {location.vehicleId}")
                await websocket.send_text("202 Accepted: Data received and validated, but Kafka publishing failed")

    except WebSocketDisconnect:
        manager.disconnect(websocket)
