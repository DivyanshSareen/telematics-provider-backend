from pydantic import BaseModel

class VehicleLocation(BaseModel):
    vehicleId: str
    driverId: str
    latitude: float
    longitude: float
    timestamp: str  # You can add a stricter datetime validation if needed
