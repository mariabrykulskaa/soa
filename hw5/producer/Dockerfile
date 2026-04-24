FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY producer/requirements.txt .
RUN pip install -r requirements.txt
COPY producer/app ./app
COPY schemas/movie_event.avsc ./app/movie_event.avsc
EXPOSE 8000
CMD ["python", "-m", "app.main"]
