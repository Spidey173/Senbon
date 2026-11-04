FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir fastapi uvicorn pydantic

COPY config/ ./config/
COPY scripts/mock_api_server.py ./scripts/mock_api_server.py

ENV PYTHONPATH="/app"
ENV MOCK_API_HOST="0.0.0.0"
ENV MOCK_API_PORT="8000"

EXPOSE 8000

CMD ["python", "scripts/mock_api_server.py"]

