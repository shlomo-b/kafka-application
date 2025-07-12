FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY receiver_service.py .

EXPOSE 8001

CMD ["python", "receiver_service.py"] 