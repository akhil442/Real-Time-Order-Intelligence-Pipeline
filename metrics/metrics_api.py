from flask import Flask, Response
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
import json
import os

app = Flask(__name__)

METRICS_FILE = "/shared/metrics/metrics.json"

raw_record_count_gauge = Gauge("raw_record_count", "Raw records seen in latest batch")
unique_event_id_count_gauge = Gauge("unique_event_id_count", "Unique event ids in latest batch")
unique_order_id_count_gauge = Gauge("unique_order_id_count", "Unique order ids in latest batch")
duplicate_events_count_gauge = Gauge("duplicate_events_count", "Duplicate events in latest batch")
bad_record_count_gauge = Gauge("bad_record_count", "Bad records in latest batch")
invalid_total_amount_count_gauge = Gauge("invalid_total_amount_count", "Invalid total amount count in latest batch")
invalid_quantity_count_gauge = Gauge("invalid_quantity_count", "Invalid quantity count in latest batch")
invalid_payment_status_count_gauge = Gauge("invalid_payment_status_count", "Invalid payment status count in latest batch")
late_event_count_gauge = Gauge("late_event_count", "Late events in latest batch")

def load_metrics():
    if not os.path.exists(METRICS_FILE):
        return

    try:
        with open(METRICS_FILE, "r") as f:
            data = json.load(f)

        raw_record_count_gauge.set(data.get("raw_record_count", 0))
        unique_event_id_count_gauge.set(data.get("unique_event_id_count", 0))
        unique_order_id_count_gauge.set(data.get("unique_order_id_count", 0))
        duplicate_events_count_gauge.set(data.get("duplicate_events_count", 0))
        bad_record_count_gauge.set(data.get("bad_record_count", 0))
        invalid_total_amount_count_gauge.set(data.get("invalid_total_amount_count", 0))
        invalid_quantity_count_gauge.set(data.get("invalid_quantity_count", 0))
        invalid_payment_status_count_gauge.set(data.get("invalid_payment_status_count", 0))
        late_event_count_gauge.set(data.get("late_event_count", 0))

    except Exception as e:
        print(f"Error loading metrics: {e}")

@app.route("/metrics")
def metrics():
    load_metrics()
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

@app.route("/")
def home():
    return "Metrics exporter is running. Visit /metrics"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)