# -*- coding: utf-8 -*-
import os
import sys
import time
import threading
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from pymongo import MongoClient
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:_QSgzb9P7hxAy9vrQFzUyNvmmSWYiMf8SUgDnSvd3OhCDUcr@b03d8326-922f-445c-bd0e-98551c2b5ac3.asia-southeast1.firestore.goog:443/fb-saiflow?loadBalanced=true&tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false")
DB_NAME = os.getenv("DB_NAME", "fb-saiflow")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "runoob")

PROJECT_ID = os.getenv("PROJECT_ID", "saiflow-509205")
DATASET_ID = os.getenv("DATASET_ID", "firestore_dw")
TABLE_ID = os.getenv("TABLE_ID", "runoob")
PORT = int(os.getenv("PORT", "8080"))

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass

def start_health_check_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
    logging.info(f"Health check server listening on port {PORT}...")
    server.serve_forever()

def run_sync():
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    logging.info("Connecting to database...")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}
    ]

    while True:
        try:
            logging.info(f"Connecting to Change Stream on {DB_NAME}.{COLLECTION_NAME}...")
            with collection.watch(pipeline=pipeline, full_document='updateLookup') as stream:
                logging.info("Change Stream connected successfully. Waiting for events...")
                for change in stream:
                    doc = change.get("fullDocument", {})
                    name = doc.get("name")
                    age = doc.get("age")

                    if age is not None:
                        try:
                            age = int(age)
                        except (ValueError, TypeError):
                            age = None

                    row_to_insert = {
                        "name": str(name) if name is not None else None,
                        "age": age
                    }

                    errors = bq_client.insert_rows_json(table_ref, [row_to_insert])
                    if errors:
                        logging.error(f"Failed to insert row: {errors}")
                    else:
                        logging.info(f"Synced document successfully: {row_to_insert}")

        except Exception as e:
            logging.warning(f"Change Stream interrupted/not active yet: {e}. Retrying in 15 seconds...")
            time.sleep(15)

if __name__ == "__main__":
    health_thread = threading.Thread(target=start_health_check_server, daemon=True)
    health_thread.start()
    run_sync()
