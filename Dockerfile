# base image
FROM --platform=linux/amd64 python:3.12 AS base

WORKDIR /app

COPY . /app
COPY ./requirements.txt /app

# Set environment variables
ENV FD_SERVICE_NAME=fd-reader-heartbeat-blob

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FD_LOG_LEVEL=INFO \
    FD_METRICS_PORT=9090 \
    PYTHONPATH=/app 

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENV PYTHONPATH=/app

# Expose metrics port and standard web port
EXPOSE ${FD_METRICS_PORT}

CMD ["python", "app/main.py"]
