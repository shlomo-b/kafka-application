from fastapi import FastAPI
from kafka import KafkaConsumer
import json
import logging
import threading
import time
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response, JSONResponse
import os
import redis
from fastapi import HTTPException

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

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# In-memory storage for messages
messages = []
orders = []
messages_lock = threading.Lock()
orders_lock = threading.Lock()

# In-memory counters
total_messages_received = 0
total_orders_received = 0

# Prometheus metrics
messages_received = Counter('messages_received_total', 'Total number of messages received')
orders_received = Counter('orders_received_total', 'Total number of orders received')
message_processing_time = Histogram('message_processing_seconds', 'Time spent processing messages')
order_processing_time = Histogram('order_processing_seconds', 'Time spent processing orders')
errors_total = Counter('errors_total', 'Total number of errors')

def initialize_kafka_consumers():
    """
    Initialize Kafka consumers with retry logic
    """
    global consumer, orders_consumer
    
    # Initialize main consumer with retry
    consumer = None
    max_retries = 10
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Kafka consumer initialized successfully")
            break
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Kafka consumer initialization failed.")
                consumer = None
    
    # Initialize orders consumer with retry
    orders_consumer = None
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka orders (attempt {attempt + 1}/{max_retries})...")
            orders_consumer = KafkaConsumer(
                ORDERS_TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=ORDERS_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Kafka orders consumer initialized successfully")
            break
        except Exception as e:
            logger.error(f"Failed to initialize Kafka orders consumer (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Kafka orders consumer initialization failed.")
                orders_consumer = None

def try_reconnect_kafka():
    """Try to reconnect to Kafka - simple and direct"""
    global consumer, orders_consumer
    
    # Try to reconnect main consumer
    if not consumer:
        try:
            logger.info("Trying to reconnect to Kafka...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("✅ Reconnected to Kafka!")
            # Start consumer thread
            consumer_thread = threading.Thread(target=consume_messages, daemon=True)
            consumer_thread.start()
        except Exception as e:
            logger.error(f"Failed to reconnect to Kafka: {e}")
    
    # Try to reconnect orders consumer
    if not orders_consumer:
        try:
            logger.info("Trying to reconnect to Kafka for orders...")
            orders_consumer = KafkaConsumer(
                ORDERS_TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=ORDERS_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("✅ Reconnected to Kafka for orders!")
            # Start orders consumer thread
            orders_thread = threading.Thread(target=consume_orders, daemon=True)
            orders_thread.start()
        except Exception as e:
            logger.error(f"Failed to reconnect to Kafka for orders: {e}")

def connection_monitor():
    """Background thread that always tries to reconnect"""
    while True:
        if not consumer or not orders_consumer:
            try_reconnect_kafka()
        time.sleep(5)  # Try every 5 seconds

# Initialize consumers
initialize_kafka_consumers()

# Start connection monitor
connection_thread = threading.Thread(target=connection_monitor, daemon=True)
connection_thread.start()

# Initialize Redis client
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5
    )
    # Test Redis connection
    redis_client.ping()
    logger.info("Redis client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Redis client: {e}")
    redis_client = None

def store_message_in_redis(message_data: dict, offset: int, partition: int):
    """
    Store received message in Redis
    """
    if not redis_client:
        logger.warning("Redis not available, skipping message storage")
        return
    
    try:
        message_id = message_data.get('id', f"msg_{int(time.time())}_{offset}")
        message_key = f"received_message:{message_id}"
        
        from datetime import datetime
        message_metadata = {
            'message': message_data.get('message', ''),
            'topic': TOPIC_NAME,
            'partition': partition,
            'offset': offset,
            'timestamp': datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Store in Redis with 24-hour expiration
        redis_client.hset(message_key, mapping=message_metadata)
        redis_client.expire(message_key, 86400)  # 24 hours
        
        # Add to received messages list (keep last 100 messages)
        redis_client.lpush('received_messages', message_id)
        redis_client.ltrim('received_messages', 0, 99)
        
        # Increment received message counter
        redis_client.incr('total_messages_received')
        
        logger.info(f"Message {message_id} stored in Redis")
        
    except Exception as e:
        logger.error(f"Error storing message in Redis: {e}")

def store_order_in_redis(order_data: dict, offset: int, partition: int):
    """
    Store received order in Redis
    """
    if not redis_client:
        logger.warning("Redis not available, skipping order storage")
        return
    
    try:
        order_id = order_data.get('id', f"order_{int(time.time())}_{offset}")
        order_key = f"received_order:{order_id}"
        
        from datetime import datetime
        order_metadata = {
            'name': order_data.get('name', ''),
            'order': order_data.get('order', ''),
            'topic': ORDERS_TOPIC_NAME,
            'partition': partition,
            'offset': offset,
            'timestamp': datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Store in Redis with 24-hour expiration
        redis_client.hset(order_key, mapping=order_metadata)
        redis_client.expire(order_key, 86400)  # 24 hours
        
        # Add to received orders list (keep last 100 orders)
        redis_client.lpush('received_orders', order_id)
        redis_client.ltrim('received_orders', 0, 99)
        
        # Increment received order counter
        redis_client.incr('total_orders_received')
        
        logger.info(f"Order {order_id} stored in Redis")
        
    except Exception as e:
        logger.error(f"Error storing order in Redis: {e}")

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
                
                from datetime import datetime
                # Store message with timestamp
                message_entry = {
                    'message': message_text,
                    'topic': TOPIC_NAME,
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp': datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Thread-safe message storage
                with messages_lock:
                    messages.append(message_entry)
                    global total_messages_received
                    total_messages_received += 1
                
                # Store in Redis
                store_message_in_redis(message_data, message.offset, message.partition)
                
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
                
                from datetime import datetime
                # Store order with timestamp
                order_entry = {
                    'name': name,
                    'order': order_text,
                    'topic': ORDERS_TOPIC_NAME,
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp': datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Thread-safe order storage
                with orders_lock:
                    orders.append(order_entry)
                    global total_orders_received
                    total_orders_received += 1
                
                # Store in Redis
                store_order_in_redis(order_data, message.offset, message.partition)
                
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
    Retrieve all received messages from Redis
    """
    if not redis_client:
        # Fallback to in-memory storage if Redis is not available
        logger.info("Redis not available, using in-memory storage for messages")
        with messages_lock:
            import json as json_lib
            return Response(
                content=json_lib.dumps({
                    "messages": messages,
                    "total_count": len(messages),
                    "total_received": total_messages_received
                }, indent=2),
                media_type="application/json"
            )
    
    try:
        logger.info("Retrieving messages from Redis...")
        # Get received message IDs from Redis
        received_message_ids = redis_client.lrange('received_messages', 0, -1)
        messages_from_redis = []
        
        for message_id in received_message_ids:
            message_data = redis_client.hgetall(f"received_message:{message_id}")
            if message_data:
                messages_from_redis.append(message_data)
        
        total_received = redis_client.get('total_messages_received') or 0
        
        logger.info(f"Successfully retrieved {len(messages_from_redis)} messages from Redis")
        
        import json as json_lib
        return Response(
            content=json_lib.dumps({
                "messages": messages_from_redis,
                "total_count": len(messages_from_redis),
                "total_received": int(total_received),
                "source": "redis"
            }, indent=2),
            media_type="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error retrieving messages from Redis: {e}")
        # Fallback to in-memory storage
        logger.info("Redis error occurred, using in-memory storage for messages")
        with messages_lock:
            import json as json_lib
            return Response(
                content=json_lib.dumps({
                    "messages": messages,
                    "total_count": len(messages),
                    "total_received": total_messages_received
                }, indent=2),
                media_type="application/json"
            )

@app.get("/orders")
async def get_orders():
    """
    Retrieve all received orders from Redis
    """
    if not redis_client:
        # Fallback to in-memory storage if Redis is not available
        logger.info("Redis not available, using in-memory storage for orders")
        with orders_lock:
            import json as json_lib
            return Response(
                content=json_lib.dumps({
                    "orders": orders,
                    "total_count": len(orders),
                    "total_received": total_orders_received
                }, indent=2),
                media_type="application/json"
            )
    
    try:
        logger.info("Retrieving orders from Redis...")
        # Get received order IDs from Redis
        received_order_ids = redis_client.lrange('received_orders', 0, -1)
        orders_from_redis = []
        
        for order_id in received_order_ids:
            order_data = redis_client.hgetall(f"received_order:{order_id}")
            if order_data:
                orders_from_redis.append(order_data)
        
        total_received = redis_client.get('total_orders_received') or 0
        
        logger.info(f"Successfully retrieved {len(orders_from_redis)} orders from Redis")
        
        import json as json_lib
        return Response(
            content=json_lib.dumps({
                "orders": orders_from_redis,
                "total_count": len(orders_from_redis),
                "total_received": int(total_received),
                "source": "redis"
            }, indent=2),
            media_type="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error retrieving orders from Redis: {e}")
        # Fallback to in-memory storage
        logger.info("Redis error occurred, using in-memory storage for orders")
        with orders_lock:
            import json as json_lib
            return Response(
                content=json_lib.dumps({
                    "orders": orders,
                    "total_count": len(orders),
                    "total_received": total_orders_received
                }, indent=2),
                media_type="application/json"
            )

@app.get("/messages/redis")
async def get_redis_messages():
    """
    Get received messages from Redis
    """
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis not available")
    
    try:
        # Get received message IDs
        received_message_ids = redis_client.lrange('received_messages', 0, -1)
        messages = []
        
        for message_id in received_message_ids:
            message_data = redis_client.hgetall(f"received_message:{message_id}")
            if message_data:
                messages.append(message_data)
        
        total_received = redis_client.get('total_messages_received') or 0
        
        return {
            "messages": messages,
            "total_received": int(total_received),
            "recent_count": len(messages)
        }
        
    except Exception as e:
        logger.error(f"Error retrieving messages from Redis: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve messages: {str(e)}")

@app.get("/orders/redis")
async def get_redis_orders():
    """
    Get received orders from Redis
    """
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis not available")
    
    try:
        # Get received order IDs
        received_order_ids = redis_client.lrange('received_orders', 0, -1)
        orders = []
        
        for order_id in received_order_ids:
            order_data = redis_client.hgetall(f"received_order:{order_id}")
            if order_data:
                orders.append(order_data)
        
        total_received = redis_client.get('total_orders_received') or 0
        
        return {
            "orders": orders,
            "total_received": int(total_received),
            "recent_count": len(orders)
        }
        
    except Exception as e:
        logger.error(f"Error retrieving orders from Redis: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve orders: {str(e)}")

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
    redis_status = "connected" if redis_client and redis_client.ping() else "disconnected"
    
    import json as json_lib
    return Response(
        content=json_lib.dumps({
            "status": "healthy", 
            "service": "receiver",
            "kafka_connected": consumer is not None,
            "orders_kafka_connected": orders_consumer is not None,
            "redis_connected": redis_status,
            "messages_count": len(messages),
            "orders_count": len(orders)
        }, indent=2),
        media_type="application/json"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 