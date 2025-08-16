from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import logging
import os
import uuid
import threading
import time
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

# Global variables
producer = None
is_connected = False
message_buffer = []
buffer_lock = threading.Lock()

def try_connect_kafka():
    """Try to connect to Kafka - simple and direct"""
    global producer, is_connected
    
    try:
        logger.info("Trying to connect to Kafka...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        # Test connection
        producer.metrics()
        is_connected = True
        logger.info("✅ Connected to Kafka!")
        return True
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        is_connected = False
        return False

def connection_monitor():
    """Simple background thread that always tries to connect"""
    global producer, is_connected
    
    while True:
        if not is_connected:
            try_connect_kafka()
            if is_connected:
                # Send buffered messages
                send_buffered_messages()
        time.sleep(5)  # Try every 5 seconds

def send_buffered_messages():
    """Send any buffered messages"""
    global message_buffer
    
    with buffer_lock:
        if not message_buffer:
            return
        
        logger.info(f"Sending {len(message_buffer)} buffered messages...")
        messages_to_send = message_buffer.copy()
        message_buffer.clear()
    
    for msg in messages_to_send:
        try:
            send_message_internal(msg['topic'], msg['value'], msg['key'])
            logger.info(f"Sent buffered message to {msg['topic']}")
        except Exception as e:
            logger.error(f"Failed to send buffered message: {e}")
            # Put back in buffer
            with buffer_lock:
                message_buffer.append(msg)

def send_message_internal(topic, value, key):
    """Send message to Kafka"""
    if not producer or not is_connected:
        raise Exception("Not connected to Kafka")
    
    future = producer.send(topic, value=value, key=key)
    return future.get(timeout=10)

# Start connection monitor
connection_thread = threading.Thread(target=connection_monitor, daemon=True)
connection_thread.start()

class ChatMessage(BaseModel):
    message: str

class Order(BaseModel):
    name: str
    order: str

@app.post("/send")
async def send_message(chat_message: ChatMessage):
    """Send chat message"""
    message_id = str(uuid.uuid4())
    
    try:
        if is_connected:
            record_metadata = send_message_internal(
                TOPIC_NAME,
                {"message": chat_message.message, "id": message_id},
                "chat-message"
            )
            
            return Response(
                content=json.dumps({
                    "status": "success",
                    "message": "Message sent to Kafka",
                    "message_id": message_id,
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset
                }, indent=2),
                media_type="application/json"
            )
        else:
            # Buffer message
            with buffer_lock:
                message_buffer.append({
                    'topic': TOPIC_NAME,
                    'value': {"message": chat_message.message, "id": message_id},
                    'key': "chat-message"
                })
            
            return Response(
                content=json.dumps({
                    "status": "buffered",
                    "message": "Message buffered - will send when connected",
                    "message_id": message_id,
                    "buffered_count": len(message_buffer)
                }, indent=2),
                media_type="application/json"
            )
            
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/send-order")
async def send_order(order: Order):
    """Send order"""
    order_id = str(uuid.uuid4())
    
    try:
        if is_connected:
            record_metadata = send_message_internal(
                ORDERS_TOPIC_NAME,
                {"name": order.name, "order": order.order, "id": order_id},
                "order"
            )
            
            return Response(
                content=json.dumps({
                    "status": "success",
                    "message": "Order sent to Kafka",
                    "order_id": order_id,
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset
                }, indent=2),
                media_type="application/json"
            )
        else:
            # Buffer order
            with buffer_lock:
                message_buffer.append({
                    'topic': ORDERS_TOPIC_NAME,
                    'value': {"name": order.name, "order": order.order, "id": order_id},
                    'key': "order"
                })
            
            return Response(
                content=json.dumps({
                    "status": "buffered",
                    "message": "Order buffered - will send when connected",
                    "order_id": order_id,
                    "buffered_count": len(message_buffer)
                }, indent=2),
                media_type="application/json"
            )
            
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check"""
    return Response(
        content=json.dumps({
            "status": "healthy",
            "kafka_connected": is_connected,
            "buffered_messages": len(message_buffer)
        }, indent=2),
        media_type="application/json"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)  

                                      