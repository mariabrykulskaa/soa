FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY aggregator/requirements.txt .
RUN pip install -r requirements.txt
COPY aggregator/app ./app
EXPOSE 8010
CMD ["python", "-m", "app.main"]
