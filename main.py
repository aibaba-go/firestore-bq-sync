import os
import sys
import logging
from pymongo import MongoClient
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 配置项：通过环境变量读取，或直接填写
MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:_QSgzb9P7hxAy9vrQFzUyNvmmSWYiMf8SUgDnSvd3OhCDUcr@b03d8326-922f-445c-bd0e-98551c2b5ac3.asia-southeast1.firestore.goog:443/fb-saiflow?loadBalanced=true&tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false")
DB_NAME = os.getenv("DB_NAME", "fb-saiflow")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "runoob")

PROJECT_ID = os.getenv("PROJECT_ID", "saiflow-509205")
DATASET_ID = os.getenv("DATASET_ID", "firestore_dw")
TABLE_ID = os.getenv("TABLE_ID", "runoob")

def run_sync():
    # 初始化 BigQuery 客户端
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # 连接兼容 MongoDB 的 Firestore 实例
    logging.info("Connecting to database...")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    logging.info(f"Start watching Change Stream on {DB_NAME}.{COLLECTION_NAME}...")

    # 监听集合级别的变更（支持 insert、update、replace）
    pipeline = [
        {"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}
    ]

    try:
        with collection.watch(pipeline=pipeline, full_document='updateLookup') as stream:
            for change in stream:
                doc = change.get("fullDocument", {})
                
                # 提取目标字段
                name = doc.get("name")
                age = doc.get("age")

                # 类型安全转换
                if age is not None:
                    try:
                        age = int(age)
                    except (ValueError, TypeError):
                        age = None

                row_to_insert = {
                    "name": str(name) if name is not None else None,
                    "age": age
                }

                # 流式写入 BigQuery
                errors = bq_client.insert_rows_json(table_ref, [row_to_insert])
                if errors:
                    logging.error(f"Failed to insert row into BigQuery: {errors}")
                else:
                    logging.info(f"Synced document successfully: {row_to_insert}")

    except Exception as e:
        logging.error(f"Error in Change Stream loop: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run_sync()
