# Kafka Application

**Official repository:** [https://github.com/shlomo-b/kafka-application](https://github.com/shlomo-b/kafka-application)

A simple Python-based Kafka microservices demo with FastAPI, supporting both Zookeeper-based and KRaft (KRaft mode, no Zookeeper) Kafka clusters.  
Includes a producer, a consumer, and a web UI for Kafka management.

---

## Features

- **Producer**: FastAPI service to send messages to a Kafka topic via REST (`/send`).
- **Consumer**: FastAPI service that consumes messages from Kafka and exposes REST endpoints for message history and Prometheus metrics.
- **Kafka UI**: Web interface for monitoring topics, consumers, and messages.
- **CI/CD**: GitHub Actions for building, scanning, and pushing Docker images for both producer and consumer.

---

## Project Structure

```
kafka-application/
│
├── kafka/
│   ├── docker-compose.yaml
│   ├── producer/
│   │   ├── producer.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── consumer/
│       ├── consumer.py
│       ├── requirements.txt
│       └── Dockerfile
│
└── .github/workflows/
    ├── producer.yml
    └── consumer.yml
```

---

## Kafka Cluster Options

### 1. Zookeeper-based Kafka (default, uncommented)

- Uses `confluentinc/cp-kafka` and `zookeeper`.
- Suitable for legacy and most production setups.

### 2. KRaft-based Kafka (no Zookeeper, commented out)

- Uses `bitnami/kafka` in KRaft mode (no Zookeeper).
- Uncomment the relevant section in `docker-compose.yaml` to use.

**To switch between modes:**  
- Comment/uncomment the relevant `services:` block in `kafka/docker-compose.yaml`.

---

## How to Run (Local Development)

### 1. Clone the repository

```bash
git clone https://github.com/shlomo-b/kafka-application.git
cd kafka-application/kafka
```

### 2. Choose your Kafka mode

- **Zookeeper-based (default):**  
  No changes needed.
- **KRaft-based:**  
  Comment out the Zookeeper+Confluent section and uncomment the Bitnami KRaft section in `docker-compose.yaml`.

### 3. Build and start all services

```bash
docker-compose up --build
```

### 4. Access the services

- **Producer API:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Consumer API:** [http://localhost:8001/docs](http://localhost:8001/docs)
- **Kafka UI:** [http://localhost:8080](http://localhost:8080)

---

## API Usage

### Producer

- **POST /send**
  - Body: `{ "message": "Hello World" }`
  - Publishes the message to Kafka topic (`chat-devops`).

- **GET /health**
  - Health check endpoint.

### Consumer

- **GET /messages**
  - Returns all received messages.

- **GET /metrics**
  - Prometheus metrics.

- **GET /health**
  - Health check endpoint.

---

## Requirements

- Docker & Docker Compose
- (Optional) Python 3.9+ for local development

---

## Manual Local Development

You can run each service locally (outside Docker):

```bash
# Producer
cd kafka/producer
pip install -r requirements.txt
python producer.py

# Consumer
cd kafka/consumer
pip install -r requirements.txt
python consumer.py
```
> Make sure Kafka is running and accessible at the address in `KAFKA_BOOTSTRAP_SERVERS`.

---

## CI/CD

- **GitHub Actions** build, scan (Trivy), and push Docker images for both producer and consumer.
- Image tags are read from `image-tag.txt` in each service folder.

---

## Switching Kafka Modes

- **Zookeeper-based:**  
  Default, uses `confluentinc/cp-kafka` and `zookeeper`.
- **KRaft-based:**  
  Uncomment the Bitnami section and comment out the Zookeeper+Confluent section in `docker-compose.yaml`.

---

## Troubleshooting

- **Kafka UI shows no clusters:**  
  Make sure the `KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS` matches your Kafka service name and port.
- **Producer/Consumer can't connect:**  
  Check `KAFKA_BOOTSTRAP_SERVERS` in environment and Docker Compose.
- **CI/CD fails to find `image-tag.txt`:**  
  Ensure the file exists and the path is correct in the workflow.

---

## Contributing

Contributions, issues, and feature requests are welcome! Feel free to open an issue or submit a pull request.

## Contact

For questions or support, open an issue on the [GitHub repository](https://github.com/shlomo-b/kafka-application).