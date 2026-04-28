"""
population_worker.py

Cloud Run worker that processes one county at a time via HTTP.
Receives county name via POST /process-county, runs:
    python score_population.py --county <COUNTY>

Parallel to emissions_worker.py — same contract, different scoring script.
"""

import os
import subprocess
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)


@app.route("/process-county", methods=["POST"])
def process_county():
    """
    Receives JSON: {"county": "HOCKING"}
    Runs: python score_population.py --county HOCKING
    """
    try:
        data = request.get_json(force=True) or {}
        county = (data.get("county") or "").strip().upper()

        if not county:
            return jsonify({"error": "county field required"}), 400

        print(f"Starting population scoring for {county}…", flush=True)

        result = subprocess.run(
            ["python", "score_population.py", "--county", county],
            capture_output=True,
            text=True,
            timeout=3600,
        )

        if result.returncode != 0:
            return jsonify({
                "error": f"score_population failed for {county}",
                "stderr": result.stderr[-4000:],
            }), 500

        return jsonify({
            "success": True,
            "county": county,
            "output_tail": result.stdout[-2000:],
        }), 200

    except subprocess.TimeoutExpired:
        return jsonify({"error": f"Timeout processing {county}"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "worker": "population"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
