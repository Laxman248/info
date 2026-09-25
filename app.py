import os
import time
import json
from flask import Flask, Response, render_template, stream_with_context

app = Flask(__name__)

# Path to the log file to watch. Override with env var in production, e.g.:
#   LOG_FILE=/var/log/myapp/app.log
LOG_FILE = os.environ.get("LOG_FILE", os.path.join(os.path.dirname(__file__), "app.log"))
MAX_LINES = 200  # how many historical lines to send when a client first connects


def read_last_lines(path, n=MAX_LINES):
    """Return the last n lines of the log file (for initial page load)."""
    if not os.path.exists(path):
        return []
    with open(path, "r", errors="ignore") as f:
        lines = f.readlines()
    return [line.rstrip("\n") for line in lines[-n:]]


def tail_file(path):
    """Generator that behaves like `tail -f` -- yields new lines as they're written."""
    while not os.path.exists(path):
        time.sleep(1)

    with open(path, "r", errors="ignore") as f:
        f.seek(0, os.SEEK_END)  # start at the end, only new lines from here
        while True:
            line = f.readline()
            if line:
                yield line.rstrip("\n")
            else:
                time.sleep(0.5)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/stream")
def stream():
    """SSE endpoint: sends historical lines first, then live new lines forever."""

    def event_stream():
        for line in read_last_lines(LOG_FILE):
            yield f"data: {json.dumps(line)}\n\n"
        for line in tail_file(LOG_FILE):
            yield f"data: {json.dumps(line)}\n\n"

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # tells Nginx not to buffer this response
        },
    )


@app.route("/health")
def health():
    return {"status": "ok", "log_file": LOG_FILE, "exists": os.path.exists(LOG_FILE)}


if __name__ == "__main__":
    # Local development only. In production run with gunicorn (see DEPLOY.md).
    app.run(host="0.0.0.0", port=5000, debug=True)
