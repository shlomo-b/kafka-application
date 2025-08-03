# Kafka + Redis Application

**Official repository:** [https://github.com/shlomo-b/kafka-application](https://github.com/shlomo-b/kafka-application)

A robust Python-based Kafka + Redis microservices demo with FastAPI, featuring Redis caching and both Zookeeper-based and KRaft Kafka clusters.  
Includes a producer, consumer with Redis integration, and a web UI for Kafka management.

---

## Requirements
- Docker & Docker Compose
- (Optional) Python 3.9+ for local development

---

## Features

- **Producer**: FastAPI service to send messages to Kafka topics via REST (`/send`, `/send-order`).
- **Consumer**: FastAPI service that consumes messages from Kafka, stores them in Redis, and exposes REST endpoints for message history and Prometheus metrics.
- **Redis Integration**: Caching and persistence layer for message storage with 24-hour TTL.

- **Kafka UI**: Web interface for monitoring topics, consumers, and messages.
- **CI/CD**: GitHub Actions for building, scanning, and pushing Docker images for both producer and consumer.

---

## Project Structure

```
kafka-application/
│
├── kafka/
│   ├── docker-compose.yaml          # Kafka + Redis + Services
│   ├── producer/
│   │   ├── producer.py              # FastAPI producer with retry logic
│   │   ├── requirements.txt         # Dependencies
│   │   ├── Dockerfile               # Container configuration
│   │   └── image-tag.txt            # CI/CD image tag
│   └── consumer/
│       ├── consumer.py              # FastAPI consumer with Redis + retry logic
│       ├── requirements.txt         # Dependencies (includes Redis)
│       ├── Dockerfile               # Container configuration
│       └── image-tag.txt            # CI/CD image tag
│
│
└── .github/workflows/
    ├── producer.yml
    └── consumer.yml
```

---

## Architecture

### Data Flow
```
Producer → Kafka → Consumer → Redis
```

### Services
- **Producer**: Sends messages to Kafka topics (`chat-devops`, `orders`)
- **Consumer**: Receives from Kafka, stores in Redis, serves via REST API
- **Redis**: Caching and persistence layer with automatic expiration
- **Kafka**: Message broker with KRaft mode (no Zookeeper)
- **Kafka UI**: Web interface for monitoring

---

## Kafka Cluster Options

### 1. KRaft-based Kafka (default, active)

- Uses `bitnami/kafka` in KRaft mode (no Zookeeper).
- More modern and efficient setup.

### 2. Zookeeper-based Kafka (commented out)

- Uses `confluentinc/cp-kafka` and `zookeeper`.
- Available in commented section of `docker-compose.yaml`.

**To switch between modes:**  
- Comment/uncomment the relevant `services:` block in `kafka/docker-compose.yaml`.

---

## How to Run (Local Development)

### 1. Clone the repository

```bash
git clone https://github.com/shlomo-b/kafka-application.git
cd kafka-application/kafka
```

### 2. Build and start all services

```bash
docker-compose up --build
```

### 3. Access the services

- **Producer API:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Consumer API:** [http://localhost:8001/docs](http://localhost:8001/docs)
- **Kafka UI:** [http://localhost:8080](http://localhost:8080)
- **Redis:** localhost:6379

---

## API Usage

### Producer Endpoints
- `POST /send` - Send chat messages to Kafka
- `POST /send-order` - Send orders to Kafka
- `GET /health` - Health check

### Consumer Endpoints
- `GET /messages` - Get messages from Redis
- `GET /orders` - Get orders from Redis
- `GET /messages/redis` - Get messages directly from Redis
- `GET /orders/redis` - Get orders directly from Redis
- `GET /metrics` - Prometheus metrics
- `GET /health` - Health check with Redis status

---

## Redis Integration

### Features
- **Message Storage**: All received messages stored in Redis with 24-hour TTL
- **Order Storage**: All received orders stored in Redis with 24-hour TTL
- **Automatic Cleanup**: Messages expire after 24 hours
- **Recent History**: Last 100 messages/orders kept in Redis lists
- **Counters**: Total sent/received counters maintained in Redis

### Redis Data Structure
```
Messages: received_message:{message_id} (Hash)
Orders: received_order:{order_id} (Hash)
Recent Lists: received_messages, received_orders (Lists)
Counters: total_messages_received, total_orders_received
```

## CI/CD

- **GitHub Actions** build, scan (Trivy), and push Docker images for both producer and consumer.
- Image tags are read from `image-tag.txt` in each service folder.

---

## Switching Kafka Modes

- **KRaft-based (default):**  
  Active, uses `bitnami/kafka` in KRaft mode.
- **Zookeeper-based:**  
  Comment out KRaft section and uncomment Zookeeper+Confluent section in `docker-compose.yaml`.

---

## Troubleshooting

- **Kafka UI shows no clusters:**  
  Make sure the `KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS` matches your Kafka service name and port.
- **Producer/Consumer can't connect:**  
  Check `KAFKA_BOOTSTRAP_SERVERS` in environment and Docker Compose.
- **Redis connection issues:**  
  Verify Redis service is running and accessible on port 6379.
- **CI/CD fails to find `image-tag.txt`:**  
  Ensure the file exists and the path is correct in the workflow.

---

## Evolution

### Before Kafka (HTTP-based)
- Direct HTTP communication between services
- Synchronous communication
- No message persistence

### Before Redis (Kafka-only)
- Kafka message broker
- In-memory storage only
- No data persistence

### Current (Kafka + Redis)
- Asynchronous Kafka communication
- Redis caching and persistence
- Production-ready architecture