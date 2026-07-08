from http.server import BaseHTTPRequestHandler, HTTPServer
import mysql.connector
import time

MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DB = "uts_basisdataterdistribusi"

MASTER = {
    "host": "host.docker.internal",
    "port": 3310,
}

SHARDS = {
    1: {"host": "host.docker.internal", "port": 3311},
    2: {"host": "host.docker.internal", "port": 3312},
    3: {"host": "host.docker.internal", "port": 3313},
    4: {"host": "host.docker.internal", "port": 3314},
    5: {"host": "host.docker.internal", "port": 3315},
}


def connect_mysql(host, port):
    return mysql.connector.connect(
        host=host,
        port=port,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        connection_timeout=2,
        autocommit=True,
    )


def fetch_count(host, port, query):
    try:
        conn = connect_mysql(host, port)
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        return result, 1
    except Exception:
        return 0, 0


def build_metrics():
    lines = []

    master_total, master_up = fetch_count(
        MASTER["host"], MASTER["port"], "SELECT COUNT(*) FROM master_data"
    )

    lines.append("# HELP uts_master_up Status master database, 1 berarti hidup dan 0 berarti mati")
    lines.append("# TYPE uts_master_up gauge")
    lines.append(f"uts_master_up {master_up}")

    lines.append("# HELP uts_master_total_rows Total data pada tabel master_data")
    lines.append("# TYPE uts_master_total_rows gauge")
    lines.append(f"uts_master_total_rows {master_total}")

    pending_total, _ = fetch_count(
        MASTER["host"], MASTER["port"], "SELECT COUNT(*) FROM pending_sync WHERE synced = 0"
    )

    lines.append("# HELP uts_pending_sync_unsynced Total pending sync yang belum selesai")
    lines.append("# TYPE uts_pending_sync_unsynced gauge")
    lines.append(f"uts_pending_sync_unsynced {pending_total}")

    event_total, _ = fetch_count(
        MASTER["host"], MASTER["port"], "SELECT COUNT(*) FROM events"
    )

    lines.append("# HELP uts_events_total Total event yang tercatat")
    lines.append("# TYPE uts_events_total gauge")
    lines.append(f"uts_events_total {event_total}")

    lines.append("# HELP uts_shard_up Status shard database, 1 berarti hidup dan 0 berarti mati")
    lines.append("# TYPE uts_shard_up gauge")

    lines.append("# HELP uts_shard_total_rows Total data pada setiap shard")
    lines.append("# TYPE uts_shard_total_rows gauge")

    for shard_id, info in SHARDS.items():
        shard_total, shard_up = fetch_count(
            info["host"], info["port"], "SELECT COUNT(*) FROM shard_data"
        )
        lines.append(f'uts_shard_up{{shard_id="{shard_id}"}} {shard_up}')
        lines.append(f'uts_shard_total_rows{{shard_id="{shard_id}"}} {shard_total}')

    lines.append("# HELP uts_exporter_last_scrape_timestamp Waktu terakhir exporter dibaca")
    lines.append("# TYPE uts_exporter_last_scrape_timestamp gauge")
    lines.append(f"uts_exporter_last_scrape_timestamp {int(time.time())}")

    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return

        metrics = build_metrics()

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(metrics.encode("utf-8"))


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8000), MetricsHandler)
    print("UTS DB Metrics Exporter jalan di port 8000...")
    server.serve_forever()