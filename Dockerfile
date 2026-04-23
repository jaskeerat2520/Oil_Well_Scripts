FROM python:3.11-slim

  WORKDIR /app

  RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc \
      postgresql-client \
      ca-certificates \
      && rm -rf /var/lib/apt/lists/*

  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt

  COPY score_terrain.py .
  COPY score_emissions.py .
  COPY detect_surface_anomalies.py .

  COPY terrain_worker.py .
  COPY emissions_worker.py .
  COPY surface_anomalies_worker.py .

  ENV PYTHONUNBUFFERED=1
  ENV WORKER_SCRIPT=terrain_worker.py

  CMD python ${WORKER_SCRIPT}
