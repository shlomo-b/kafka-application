from fastapi import FastAPI
from kafka import KafkaConsumer
import json
import logging
import threading
import time
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Receiver Service", description="Service to receive chat messages from Kafka")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = [os.getenv('KAFKA_BOOTSTRAP_SERVERS')]
TOPIC_NAME = 'chat-devops'
ORDERS_TOPIC_NAME = 'orders'
GROUP_ID = 'chat-receiver-group'
ORDERS_GROUP_ID = 'orders-receiver-group'

# In-memory storage for messages
messages = []
orders = []
messages_lock = threading.Lock()
orders_lock = threading.Lock()

# Prometheus metrics
messages_received = Counter('messages_received_total', 'Total number of messages received')
orders_received = Counter('orders_received_total', 'Total number of orders received')
message_processing_time = Histogram('message_processing_seconds', 'Time spent processing messages')
order_processing_time = Histogram('order_processing_seconds', 'Time spent processing orders')
errors_total = Counter('errors_total', 'Total number of errors')

# Initialize Kafka consumers
try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info("Kafka consumer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka consumer: {e}")
    consumer = None

try:
    orders_consumer = KafkaConsumer(
        ORDERS_TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=ORDERS_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info("Kafka orders consumer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka orders consumer: {e}")
    orders_consumer = None

def consume_messages():
    """
    Background thread to consume messages from Kafka
    """
    if not consumer:
        logger.error("Kafka consumer not available")
        return
    
    logger.info("Starting to consume messages from Kafka...")
    
    try:
        for message in consumer:
            start_time = time.time()
            
            try:
                # Extract message data
                message_data = message.value
                message_text = message_data.get('message', 'Unknown message')
                
                # Store message with timestamp
                message_entry = {
                    'message': message_text,
                    'timestamp': time.time(),
                    'offset': message.offset,
                    'partition': message.partition
                }
                
                # Thread-safe message storage
                with messages_lock:
                    messages.append(message_entry)
                
                # Update metrics
                messages_received.inc()
                message_processing_time.observe(time.time() - start_time)
                
                logger.info(f"Received message: {message_text} (offset: {message.offset})")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                errors_total.inc()
                
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
        errors_total.inc()

def consume_orders():
    """
    Background thread to consume orders from Kafka
    """
    if not orders_consumer:
        logger.error("Kafka orders consumer not available")
        return
    
    logger.info("Starting to consume orders from Kafka...")
    
    try:
        for message in orders_consumer:
            start_time = time.time()
            
            try:
                # Extract order data
                order_data = message.value
                name = order_data.get('name', 'Unknown name')
                order_text = order_data.get('order', 'Unknown order')
                
                # Store order with timestamp
                order_entry = {
                    'name': name,
                    'order': order_text,
                    'timestamp': time.time(),
                    'offset': message.offset,
                    'partition': message.partition
                }
                
                # Thread-safe order storage
                with orders_lock:
                    orders.append(order_entry)
                
                # Update metrics
                orders_received.inc()
                order_processing_time.observe(time.time() - start_time)
                
                logger.info(f"Received order from {name}: {order_text} (offset: {message.offset})")
                
            except Exception as e:
                logger.error(f"Error processing order: {e}")
                errors_total.inc()
                
    except Exception as e:
        logger.error(f"Error in orders consumer loop: {e}")
        errors_total.inc()

# Start consumer thread
if consumer:
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

if orders_consumer:
    orders_thread = threading.Thread(target=consume_orders, daemon=True)
    orders_thread.start()

@app.get("/messages")
async def get_messages():
    """
    Retrieve all received messages
    """
    with messages_lock:
        return {
            "messages": messages,
            "total_count": len(messages)
        }

@app.get("/orders")
async def get_orders():
    """
    Retrieve all received orders
    """
    with orders_lock:
        return {
            "orders": orders,
            "total_count": len(orders)
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
        "kafka_connected": consumer is not None,
        "orders_kafka_connected": orders_consumer is not None,
        "messages_count": len(messages),
        "orders_count": len(orders)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 