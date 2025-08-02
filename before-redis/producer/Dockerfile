FROM python:3.9-slim

WORKDIR /app

# for CI github actions
COPY /kafka/producer/requirements.txt /app

# for local development
# COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends \
        curl \
        iputils-ping \
        net-tools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# for CI github actions
COPY /kafka/producer/producer.py /app

# for local development
# COPY producer.py /app

EXPOSE 8000

CMD ["python", "producer.py"] 


