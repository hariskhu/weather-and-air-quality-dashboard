FROM python:3.12.3-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY data/ ./data/

WORKDIR /app

CMD ["python3", "-i", "weather_module.py"]