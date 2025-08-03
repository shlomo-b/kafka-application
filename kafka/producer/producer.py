from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import logging
import os
import uuid
from fastapi.responses import Response
from fastapi.responses import JSONResponse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Sender Service", description="Service to send chat messages to Kafka")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = [os.getenv('KAFKA_BOOTSTRAP_SERVERS')]
TOPIC_NAME = 'chat-devops'
ORDERS_TOPIC_NAME = 'orders'

def initialize_kafka_producer():
    """
    Initialize Kafka producer with retry logic
    """
    global producer
    
    max_retries = 10
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka producer (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info("Kafka producer initialized successfully")
            break
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Kafka producer initialization failed.")
                producer = None

# Initialize producer
initialize_kafka_producer()

class ChatMessage(BaseModel):
    message: str

class Order(BaseModel):
    name: str
    order: str

@app.post("/send")
async def send_message(chat_message: ChatMessage):
    """
    Accept chat messages and publish them to Kafka topic
    """
    if not producer:
        raise HTTPException(status_code=500, detail="Kafka producer not available")
    
    try:
        # Generate unique message ID
        message_id = str(uuid.uuid4())
        
        # Publish message to Kafka
        future = producer.send(
            TOPIC_NAME,
            value={"message": chat_message.message, "id": message_id},
            key="chat-message"    
        )
        
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Message sent successfully to topic {record_metadata.topic} "
                   f"partition {record_metadata.partition} offset {record_metadata.offset}")
        
        import json as json_lib
        return Response(
            content=json_lib.dumps({
                "status": "success",
                "message": "Message sent to Kafka",
                "message_id": message_id,
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset
            }, indent=2),
            media_type="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error sending message to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send message: {str(e)}")

@app.post("/send-order")
async def send_order(order: Order):
    """
    Accept orders and publish them to Kafka orders topic
    """
    if not producer:
        raise HTTPException(status_code=500, detail="Kafka producer not available")
    
    try:
        # Generate unique order ID
        order_id = str(uuid.uuid4())
        
        # Publish order to Kafka
        future = producer.send(
            ORDERS_TOPIC_NAME,
            value={"name": order.name, "order": order.order, "id": order_id},
            key="order"    
        )
        
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Order sent successfully to topic {record_metadata.topic} "
                   f"partition {record_metadata.partition} offset {record_metadata.offset}")
        
        import json as json_lib
        return Response(
            content=json_lib.dumps({
                "status": "success",
                "message": "Order sent to Kafka",
                "order_id": order_id,
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
                "name": order.name
            }, indent=2),
            media_type="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error sending order to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send order: {str(e)}")

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    import json as json_lib
    return Response(
        content=json_lib.dumps({
            "status": "healthy", 
            "service": "sender",
            "kafka_connected": producer is not None
        }, indent=2),
        media_type="application/json"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)  

                                      