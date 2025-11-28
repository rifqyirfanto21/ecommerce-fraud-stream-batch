FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

COPY docker/loader/requirements-loader.txt /tmp/requirements-loader.txt
RUN pip install --no-cache-dir -r /tmp/requirements-loader.txt

COPY src /app/src

ENV PYTHONPATH=/app

CMD ["python", "-u", "-m", "src.streaming.loader"]