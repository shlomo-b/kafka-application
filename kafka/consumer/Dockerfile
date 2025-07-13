FROM python:3.12-slim

WORKDIR /app

# for CI github actionsג
COPY /kafka/consumer/requirements.txt /app

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
COPY /kafka/consumer/consumer.py /app

# for local developmentt
# COPY consumer.py /app

EXPOSE 8001

CMD ["python", "consumer.py"] 


