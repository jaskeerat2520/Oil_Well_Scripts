"""
terrain_worker.py

Cloud Run worker that processes one county at a time via Cloud Tasks.
Receives county name via HTTP POST, runs score_terrain.py --county <COUNTY>
"""

import os
import sys
import subprocess
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

@app.route("/process-county", methods=["POST"])
def process_county():
    """
    Receives JSON: {"county": "ATHENS"}
    Runs: python score_terrain.py --county ATHENS
    Returns result/error
    """
    try:
        data = request.get_json()
        county = data.get("county", "").strip().upper()

        if not county:
            return jsonify({"error": "county field required"}), 400

        print(f"Starting terrain scoring for {county}…", flush=True)

        # Run the scoring script
        result = subprocess.run(
            ["python", "score_terrain.py", "--county", county],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout per county
        )

        if result.returncode != 0:
            return jsonify({
                "error": f"Score terrain failed for {county}",
                "stderr": result.stderr
            }), 500

        return jsonify({
            "success": True,
            "county": county,
            "output": result.stdout
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
