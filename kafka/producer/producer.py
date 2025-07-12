from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Sender Service", description="Service to send chat messages to Kafka")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = [os.getenv('KAFKA_BOOTSTRAP_SERVERS')]
TOPIC_NAME = 'chat-devops'
# TOPIC_NAME = 'chat-devops

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    logger.info("Kafka producer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    producer = None

class ChatMessage(BaseModel):
    message: str

@app.post("/send")
async def send_message(chat_message: ChatMessage):
    """
    Accept chat messages and publish them to Kafka topic
    """
    if not producer:
        raise HTTPException(status_code=500, detail="Kafka producer not available")
    
    try:
        # Publish message to Kafka
        future = producer.send(
            TOPIC_NAME,
            value={"message": chat_message.message},
            key="chat-message"    
        )
        
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Message sent successfully to topic {record_metadata.topic} "
                   f"partition {record_metadata.partition} offset {record_metadata.offset}")
        
        return {
            "status": "success",
            "message": "Message sent to Kafka",
            "topic": record_metadata.topic,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset
        }
        
    except Exception as e:
        logger.error(f"Error sending message to Kafka: {e}")
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

                                      