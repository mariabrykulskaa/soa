FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /tests
COPY tests/requirements.txt .
RUN pip install -r requirements.txt
COPY tests/ .
CMD ["pytest", "-v", "--color=yes"]
