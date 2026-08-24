"""
AlphaCandle - Dashboard (Flask). Run as an independent process:
    python dashboard.py

Pure read-only view over state.py - never places orders, can run even if
main.py isn't running yet (shows empty/placeholder state).
"""
from flask import Flask, jsonify, render_template
import config
import state

app = Flask(__name__, template_folder="templates", static_folder="static")


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/state")
def api_state():
    snapshot = state.snapshot()
    snapshot.update({
        "max_loss_per_day": config.MAX_LOSS_PER_DAY,
        "max_trades_per_day": config.MAX_TRADES_PER_DAY,
        "max_open_positions": config.MAX_OPEN_POSITIONS,
    })
    return jsonify(snapshot)


if __name__ == "__main__":
    app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT, debug=False)
