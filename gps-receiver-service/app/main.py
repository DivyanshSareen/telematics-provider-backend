from fastapi import FastAPI
from app.endpoints import websocket_routes

app = FastAPI(title="Vehicle Tracking WebSocket API")

# Include WebSocket routes
app.include_router(websocket_routes.router)

# Simple health check endpoint
@app.get("/health")
def health_check():
    return {"status": "ok"}
