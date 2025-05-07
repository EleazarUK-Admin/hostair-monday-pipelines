#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hospitable → BigQuery
Borra y recrea la tabla properties.messagess_ss en cada ejecución,
luego ingesta todas las conversaciones (streaming + retry 404).
"""

import json, random, time
from datetime import datetime, timezone
from typing import List

from google.api_core.exceptions import NotFound, TooManyRequests
from google.cloud import bigquery, storage
from google.cloud.bigquery import (
    SchemaField, TimePartitioning, TimePartitioningType
)

# ─────────── CONFIG ───────────
BQ_PROJECT  = "hostair-test-data"
BQ_DATASET  = "properties"
BQ_TABLE    = "messagess_ss"
GCS_BUCKET  = "historic_info"
PREFIX      = "messages/"
BATCH_SIZE  = 500
MAX_RETRIES = 5
# ──────────────────────────────

SCHEMA: List[SchemaField] = [
    SchemaField("platform"       , "STRING"),
    SchemaField("platform_id"    , "STRING"),
    SchemaField("conversation_id", "STRING"),
    SchemaField("reservation_id" , "STRING"),
    SchemaField("content_type"   , "STRING"),
    SchemaField("body"           , "STRING"),
    SchemaField("attachments"    , "STRING"),  # JSON serializado
    SchemaField("sender_type"    , "STRING"),
    SchemaField("sender_role"    , "STRING"),
    SchemaField("sender"         , "STRING"),  # JSON serializado
    SchemaField("created_at"     , "TIMESTAMP"),
    SchemaField("ingested_at"    , "TIMESTAMP"),
]

# ────────── UTILIDADES BQ ──────────
def drop_table(client: bigquery.Client) -> None:
    tid = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    client.delete_table(tid, not_found_ok=True)        # idempotente
    print(f"✓ Tabla {tid} eliminada (si existía)")

def create_table(client: bigquery.Client) -> None:
    tid = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    tbl = bigquery.Table(tid, schema=SCHEMA)
    tbl.time_partitioning = TimePartitioning(
        type_=TimePartitioningType.DAY,
        field="created_at",
        require_partition_filter=True,
    )
    tbl.clustering_fields = ["platform"]
    client.create_table(tbl)
    print(f"✓ Tabla creada: {tid}")

def to_row(m: dict) -> dict:
    return {
        "platform"       : m.get("platform"),
        "platform_id"    : m.get("platform_id"),
        "conversation_id": m.get("conversation_id"),
        "reservation_id" : m.get("reservation_id"),
        "content_type"   : m.get("content_type"),
        "body"           : m.get("body"),
        "attachments"    : json.dumps(m.get("attachments", []), ensure_ascii=False),
        "sender_type"    : m.get("sender_type"),
        "sender_role"    : m.get("sender_role"),
        "sender"         : json.dumps(m.get("sender", {}), ensure_ascii=False),
        "created_at"     : m.get("created_at"),
        "ingested_at"    : datetime.now(timezone.utc).isoformat()
    }

def insert_with_retry(bq: bigquery.Client, table_id: str, rows: List[dict]) -> None:
    delay = 1.5
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            errs = bq.insert_rows_json(table_id, rows)
            if errs:
                raise RuntimeError(errs)
            return
        except (NotFound, TooManyRequests) as e:
            if attempt == MAX_RETRIES:
                raise
            sleep = delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
            print(f"⚠️  {e.__class__.__name__} → retry {attempt}/{MAX_RETRIES} en {sleep:.1f}s")
            time.sleep(sleep)

# ─────────────── MAIN ───────────────
def main() -> None:
    print("=== Ingesta iniciada", datetime.now(timezone.utc).isoformat(), "===")

    bq_client = bigquery.Client(project=BQ_PROJECT)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # DROP + CREATE
    drop_table(bq_client)
    create_table(bq_client)

    rows, total = [], 0
    blobs = [b for b in bucket.list_blobs(prefix=PREFIX) if b.name.endswith(".json")]
    print(f"Encontrados {len(blobs):,} archivos de conversación en GCS")

    for blob in blobs:
        data = json.loads(blob.download_as_bytes()).get("data", [])
        for msg in data:
            rows.append(to_row(msg))
            if len(rows) >= BATCH_SIZE:
                insert_with_retry(bq_client, table_id, rows)
                total += len(rows)
                print(f"  • {total:,} filas insertadas…")
                rows.clear()

    if rows:
        insert_with_retry(bq_client, table_id, rows)
        total += len(rows)

    print(f"=== Ingesta completa: {total:,} mensajes cargados ===")

if __name__ == "__main__":
    main()
