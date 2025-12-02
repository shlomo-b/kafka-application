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

# ------------ NEW (security best practice) -------------
# Create a non-root user
RUN adduser --disabled-password --gecos "" appuser --uid 1000

# Make sure the user owns the files
RUN chown -R appuser:appuser /app

# Switch to the new user
USER appuser

EXPOSE 8000

CMD ["python", "producer.py"] 


