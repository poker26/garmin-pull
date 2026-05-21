FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata curl \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Moscow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY garmin_pull.py health.py ./
COPY pullers/ ./pullers/

RUN mkdir -p /app/tokens /app/logs

EXPOSE 8080

# Long-running mode по умолчанию (без аргументов)
CMD ["python", "-u", "garmin_pull.py"]
