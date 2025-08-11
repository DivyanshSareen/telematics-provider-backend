from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from app.connection_manager import ConnectionManager
from app.models import VehicleLocation

router = APIRouter()

manager = ConnectionManager()

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
            await websocket.send_text("200 OK: Data received and validated")

    except WebSocketDisconnect:
        manager.disconnect(websocket)
