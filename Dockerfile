FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY score_terrain.py .
COPY terrain_worker.py .

# Set environment
ENV PYTHONUNBUFFERED=1

# Run worker
CMD ["python", "terrain_worker.py"]
