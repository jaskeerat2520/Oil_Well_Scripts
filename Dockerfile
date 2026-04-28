FROM python:3.11-slim

  WORKDIR /app

  RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc \
      postgresql-client \
      ca-certificates \
      && rm -rf /var/lib/apt/lists/*

  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt

# All scoring scripts that workers might subprocess-invoke
COPY score_terrain.py .
COPY score_emissions.py .
COPY score_population.py .
COPY score_pad_detection.py .
COPY detect_surface_anomalies.py .

# All worker wrappers — the right one is selected at runtime via WORKER_SCRIPT
COPY terrain_worker.py .
COPY emissions_worker.py .
COPY population_worker.py .
COPY pad_detection_worker.py .
COPY surface_anomalies_worker.py .

  ENV PYTHONUNBUFFERED=1
  ENV WORKER_SCRIPT=terrain_worker.py

  CMD python ${WORKER_SCRIPT}
