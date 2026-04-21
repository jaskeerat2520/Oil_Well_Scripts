"""
queue_counties.py

Pushes all Ohio counties to Cloud Tasks queue for parallel processing.
Each task calls terrain-worker service with a county name.
"""

import os
import psycopg2
from google.cloud import tasks_v2
from dotenv import load_dotenv
import json

load_dotenv()

# Database config
DB_HOST = os.getenv("SUPABASE_DB_HOST")
DB_NAME = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT = int(os.getenv("SUPABASE_DB_PORT", 5432))

# Cloud Tasks config
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "gen-lang-client-0658746801")
QUEUE_NAME = "terrain-queue"
REGION = "us-central1"
SERVICE_URL = "https://terrain-worker-XXXXX.run.app"  # Update after deployment


def get_counties_needing_processing():
    """Fetch counties that haven't completed terrain scoring yet."""
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT
    )
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT w.county
            FROM wells w
            JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
            LEFT JOIN well_remote_sensing wrs2 ON w.api_no = wrs2.api_no
            WHERE w.lat IS NOT NULL AND w.lng IS NOT NULL
              AND wrs2.terrain_processed_at IS NULL
            ORDER BY w.county
        """)
        return [r[0] for r in cur.fetchall()]
    conn.close()


def create_task(client, county: str):
    """Create a Cloud Task to process a single county."""
    parent = client.queue_path(PROJECT_ID, REGION, QUEUE_NAME)

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{SERVICE_URL}/process-county",
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"county": county}).encode(),
        }
    }

    response = client.create_task(request={"parent": parent, "task": task})
    print(f"✓ Queued: {county}")
    return response


def main():
    print("📍 Fetching counties needing terrain processing…")
    counties = get_counties_needing_processing()
    print(f"Found {len(counties)} counties\n")

    client = tasks_v2.CloudTasksClient()

    for county in counties:
        create_task(client, county)

    print(f"\n✅ All {len(counties)} counties queued for processing!")


if __name__ == "__main__":
    main()
