"""
pad_detection_worker.py

Cloud Run worker that processes one county at a time via Cloud Tasks.
Receives county name via HTTP POST, runs score_pad_detection.py --county <COUNTY>.

Mirrors terrain_worker.py — same deployment shape, same enqueue script,
same env contract.
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
    Receives JSON: {"county": "HANCOCK"}
    Runs:           python score_pad_detection.py --county HANCOCK
    """
    try:
        data = request.get_json() or {}
        county = data.get("county", "").strip().upper()
        if not county:
            return jsonify({"error": "county field required"}), 400

        print(f"Starting pad detection for {county}…", flush=True)

        result = subprocess.run(
            ["python", "score_pad_detection.py", "--county", county],
            capture_output=True,
            text=True,
            timeout=7200,  # 2 hours per county — NAIP+OSIP fetches are slower than DEM
        )

        if result.returncode != 0:
            return jsonify({
                "error": f"score_pad_detection failed for {county}",
                "stderr": result.stderr,
            }), 500

        return jsonify({
            "success": True,
            "county":  county,
            "output":  result.stdout,
        }), 200

    except subprocess.TimeoutExpired:
        return jsonify({"error": f"Timeout processing {county}"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
