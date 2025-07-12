from fastapi import FastAPI
from pydantic import BaseModel
import json
import logging
import time
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
from fastapi import HTTPException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Receiver Service", description="Service to receive chat messages")

# In-memory storage for messages
messages = []

# Prometheus metrics
messages_received = Counter('messages_received_total', 'Total number of messages received')
message_processing_time = Histogram('message_processing_seconds', 'Time spent processing messages')
errors_total = Counter('errors_total', 'Total number of errors')

class ChatMessage(BaseModel):
    message: str

@app.post("/receive")
async def receive_message(chat_message: ChatMessage):
    """
    Receive messages directly from sender service
    """
    start_time = time.time()
    
    try:
        # Store message with timestamp
        message_entry = {
            'message': chat_message.message,
         #   'timestamp': time.time(),
        #   'source': 'http'
        }
        
        messages.append(message_entry)
        
        # Update metrics
        messages_received.inc()
        message_processing_time.observe(time.time() - start_time)
        
        logger.info(f"Received message: {chat_message.message}")
        
        return {
            "status": "success",
            "message": "Message received and stored",
            "total_messages": len(messages)
        }
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        errors_total.inc()
        raise HTTPException(status_code=500, detail=f"Failed to process message: {str(e)}")

@app.get("/messages")
async def get_messages():
    """
    Retrieve all received messages
    """
    return {
        "messages": messages,
        "total_count": len(messages)
    }

@app.get("/metrics")
async def get_metrics():
    """
    Expose Prometheus metrics
    """
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy", 
        "service": "receiver",
        "messages_count": len(messages)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8001) 