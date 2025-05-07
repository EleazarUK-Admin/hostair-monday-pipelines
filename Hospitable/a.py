#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import unicodedata
import concurrent.futures
from datetime import datetime
from google.cloud import storage, bigquery
from google.api_core.exceptions import BadRequest

# === CONFIGURACIÓN ===
GCS_BUCKET  = "historic_info"
BQ_PROJECT  = "hostair-test-data"
LOG_DATASET = "project_settings"
LOG_TABLE   = "historic_ingest_log"

# Cantidad de hilos para paralelizar lectura de GCS
MAX_THREADS = 10


# ---------- utilidades ----------
def now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def sanitize(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    s = s.replace(" ", "_").replace("/", "_")
    return "".join(c for c in s if c.isalnum() or c in "._-")


def list_paths(bucket, prefix: str):
    """Devuelve todas las rutas .json bajo un prefijo."""
    return [b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".json")]


# ---------- fase 1: metadata de reservations ----------
def build_metadata(bucket):
    """
    Recorre reservations/ y devuelve un dict con claves (year, month, day, code)
    y la metadata necesaria para el merge posterior.
    """
    meta = {}
    for path in list_paths(bucket, "reservations/"):
        parts = path.split("/")  # ['reservations','YYYY','MM','DD','code.json']
        if len(parts) != 5:
            continue
        year, month, day, fn = parts[1], parts[2], parts[3], parts[4]
        code = fn.rsplit(".", 1)[0].lower()

        data = json.loads(bucket.blob(path).download_as_string())

        reservation_id = data.get("id")
        checkin_iso    = data.get("check_in") or data.get("arrival_date")
        checkin_date   = checkin_iso[:10] if checkin_iso else None

        prop_id = None
        props = data.get("properties") or []
        if props and isinstance(props, list):
            prop_id = props[0].get("id")

        meta[(year, month, day, code)] = {
            "reservation_id": reservation_id,
            "property_id":    str(prop_id) if prop_id is not None else None,
            "checkin":        checkin_date,
            "reservation_path": path,
        }
    print(f"[{now()}] → Metadata de {len(meta)} reservas cargada")
    return meta


# ---------- fase 2: procesar conversations ----------
def process_conversation(path, bucket, meta, out_rows):
    parts = path.split("/")  # ['messages','YYYY','MM','DD','code.json']
    if len(parts) != 5:
        return
    year, month, day, fn = parts[1], parts[2], parts[3], parts[4]
    code = fn.rsplit(".", 1)[0].lower()
    key  = (year, month, day, code)
    if key not in meta:
        return

    data = json.loads(bucket.blob(path).download_as_string())
    conv_id = data.get("conversation_id") or data.get("conversationId")
    msgs    = data.get("data") or data.get("messages") or []
    no_msgs = len(msgs)

    info = meta[key]
    out_rows.append({
        "property_id":       info["property_id"],
        "checkin":           info["checkin"],
        "code":              code.upper(),
        "reservation_path":  info["reservation_path"],
        "conversation_id":   conv_id,
        "no_messages":       no_msgs,
        "conversation_path": path,
    })
    print(f"[{now()}]   • {code.upper()}: conv={conv_id}, msgs={no_msgs}")


# ---------- fase 3: upsert en BigQuery ----------
def upsert_bigquery(rows):
    client   = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{LOG_DATASET}.{LOG_TABLE}"
    tmp_id   = f"{BQ_PROJECT}.{LOG_DATASET}.__tmp_ingest"

    # Asegurar que property_id sea STRING en la tabla destino
    try:
        client.query(
            f"ALTER TABLE `{table_id}` "
            f"ALTER COLUMN property_id SET DATA TYPE STRING"
        ).result()
        print(f"[{now()}] → Columna property_id convertida a STRING en {table_id}")
    except BadRequest as e:
        # Si ya es STRING u otro motivo aceptable, se ignora
        if "No column to alter" not in str(e):
            print(f"[{now()}] → ALTER TABLE omitido: {e.message or e}")

    # Crear tabla temporal
    schema = [
        bigquery.SchemaField("property_id",       "STRING"),
        bigquery.SchemaField("checkin",           "DATE"),
        bigquery.SchemaField("code",              "STRING"),
        bigquery.SchemaField("reservation_path",  "STRING"),
        bigquery.SchemaField("conversation_id",   "STRING"),
        bigquery.SchemaField("no_messages",       "INT64"),
        bigquery.SchemaField("conversation_path", "STRING"),
    ]

    client.delete_table(tmp_id, not_found_ok=True)
    client.create_table(bigquery.Table(tmp_id, schema=schema))
    client.insert_rows_json(tmp_id, rows)
    print(f"[{now()}] → {len(rows)} filas cargadas en {tmp_id}")

    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{tmp_id}`  S
    ON   T.reservation_path = S.reservation_path
    WHEN MATCHED THEN UPDATE SET
        property_id       = S.property_id,
        checkin           = S.checkin,
        conversation_id   = S.conversation_id,
        no_messages       = S.no_messages,
        conversation_path = S.conversation_path
    WHEN NOT MATCHED THEN INSERT (
        property_id, checkin, code, reservation_path,
        conversation_id, no_messages, conversation_path
    ) VALUES (
        S.property_id, S.checkin, S.code, S.reservation_path,
        S.conversation_id, S.no_messages, S.conversation_path
    )
    """
    client.query(merge_sql).result()
    print(f"[{now()}] → MERGE completado en {table_id}")

    client.delete_table(tmp_id)
    print(f"[{now()}] → Tabla temporal {tmp_id} eliminada")


# ---------- main ----------
def main():
    print(f"[{now()}] === INICIO ACTUALIZACIÓN LOGS ===")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    # 1) Metadata de reservations
    meta = build_metadata(bucket)

    # 2) Procesar conversations
    out_rows = []
    paths = list_paths(bucket, "messages/")
    print(f"[{now()}] → Encontrados {len(paths)} archivos de conversación")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        futures = [pool.submit(process_conversation, p, bucket, meta, out_rows) for p in paths]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    print(f"[{now()}] → Total filas a upsert: {len(out_rows)}")

    # 3) Upsert en BigQuery
    if out_rows:
        upsert_bigquery(out_rows)

    print(f"[{now()}] === PROCESO FINALIZADO ===")


if __name__ == "__main__":
    main()
