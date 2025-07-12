from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Sender Service", description="Service to send chat messages to Receiver")

# Receiver service configuration
RECEIVER_URL = "http://localhost:8001"

class ChatMessage(BaseModel):
    message: str

@app.post("/send")
async def send_message(chat_message: ChatMessage):
    """
    Accept chat messages and send them directly to receiver service
    """
    try:
        # Send message directly to receiver service
        response = requests.post(
            f"{RECEIVER_URL}/receive",
            json={"message": chat_message.message},
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        
        if response.status_code == 200:
            logger.info(f"Message sent successfully to receiver: {chat_message.message}")
            return {
                "status": "success",
                "message": "Message sent to receiver",
                "receiver_response": response.json()
            }
        else:
            logger.error(f"Receiver service returned error: {response.status_code}")
            raise HTTPException(status_code=500, detail=f"Receiver service error: {response.text}")
        
    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to receiver service")
        raise HTTPException(status_code=500, detail="Receiver service not available")
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send message: {str(e)}")

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy", "service": "sender"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 