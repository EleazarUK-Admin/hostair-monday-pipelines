#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import sys
import unicodedata
import concurrent.futures
from datetime import datetime
import requests
from google.cloud import storage, bigquery

# === CONFIGURACIÓN ===
API_TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9."
    "eyJhdWQiOiI5YTYyNGRmMC0xMmYxLTQ0OGUtYjg4NC00MzY3ODBhNWQzY2QiLCJqdGkiOiJkMGQyNWJlMTRkMTMwNDBkYjA0YThmM2ZmYmM4YzkzMzBiY2RlMTY1ZmVkNjNhNTVkMzUwZmEwOWNjMzI3OTAxMDZlMTliN2M3ODJkZTgwMCIsImlhdCI6MTcxNDczMTA2Ny43MTE5MzQsIm5iZiI6MTcxNDczMTA2Ny43MTE5MzcsImV4cCI6MTc0NjI2NzA2Ny43MDcyODEsInN1YiI6IjI3NDYyIiwic2NvcGVzIjpbInBhdDpyZWFkIiwicGF0OndyaXRlIl19."
    "UQv1WCXiuJ20n5aE7UvmNcLTYTuGANThzpUvg5N8zqWJGX803-8auXWWoYuxygH41GiHZy6XXvQS8BVoiFFcPMahYj45rO3s7lIdKhD5CQAvDFVaMv33u96W9I7ISrfRvGO9Uo3_LqsHOauZX7QICErejrF2zcs7hpayDBV2tfg-ZZN0qnynQLhIqHiS1nV34mxk3currsrXNJIUydCKc20gQHHeiC-AKS4f9jnwvHlX2D-7_xcW1po2SJ53oBdNln6wMozmk6pp7Qdft6dFuPFoVxuOv4crExuGMC1tfX5r9RShg5c3g5z7NzSDQNz_ETgNQhOoPwPntFfprtZSloiASluWaqcJPwVxJo4C_G5pOIeer4aiTOytOVCvjhaXkNhMFqcTE9-Q2DnqxlhX1aIZ4H69-V_p5F4C_UYu2N6RbsqkVPlzBQBYKJCV5FK2mF0oQZREdGT8HsgVdtRn5V-Wm9bP1r6maBugIipfd87566jkZGPCXIjHjZDZBDMnyPnH7ntsspnzM7RUfT3rrlXKnk1ZxqWl0RgakgvCZ8GTKS77PxelGqUZu38GKNpxrI0aC--_DR2_wY31HbnRc6zhSIsviC9cn5w7NDqBUSdFuNv9EFi1XjFb3ayRqpmT4p5eVGFjY-34962Ln60txgAY9MaYYuy2WVJ-a8CHK6Q"
)
GCS_BUCKET      = "historic_info"
BQ_PROJECT      = "hostair-test-data"
LOG_DATASET     = "project_settings"
LOG_TABLE       = "historic_ingest_log"
API_BASE_URL    = "https://public.api.hospitable.com/v2"

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {API_TOKEN}"
}

MAX_THREADS = 10

# === FUNCIONES AUXILIARES ===
def now():
    return datetime.utcnow().isoformat(timespec="seconds")

def sanitize(name: str) -> str:
    """Normaliza cadenas para rutas de GCS."""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    s = s.replace(" ", "_").replace("/", "_")
    return "".join(c for c in s if c.isalnum() or c in "._-")

def list_reservations(bucket) -> list:
    """Recorre el bucket y devuelve la lista de paths de reservas."""
    blobs = bucket.list_blobs(prefix="reservations/")
    paths = [b.name for b in blobs if b.name.endswith(".json")]
    print(f"[{now()}] → Encontradas {len(paths)} reservas en GCS")
    return paths

def fetch_save_messages(res_path: str, bucket, detail_rows: list):
    """
    Para cada reserva:
     1. Descarga el JSON de la reserva, extrae el UUID y el código.
     2. Consulta el API de mensajes usando el UUID.
     3. Guarda el JSON de mensajes en messages/YYYY/MM/DD/{code}.json.
     4. Registra los datos para BigQuery.
    """
    # 1. Leer y parsear el JSON de la reserva
    blob = bucket.blob(res_path)
    raw = blob.download_as_string()
    res_json = json.loads(raw)
    reservation_id = res_json.get("id")              # UUID requerido
    code           = res_json.get("code") or "unknown"

    # Extraer fecha desde la ruta: reservations/YYYY/MM/DD/code.json
    parts = res_path.split('/')
    if len(parts) < 5:
        print(f"[{now()}] ⚠️ Ruta inesperada: {res_path}", file=sys.stderr)
        return
    year, month, day = parts[1], parts[2], parts[3]

    print(f"[{now()}] → Reserva {code} (UUID: {reservation_id}) → obteniendo mensajes")

    # 2. Consulta al API de mensajes con el UUID
    url = f"{API_BASE_URL}/reservations/{reservation_id}/messages"
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    # 3. Guardar JSON en GCS bajo la ruta paralela en 'messages'
    conv_path = f"messages/{year}/{month}/{day}/{sanitize(code)}.json"
    bucket.blob(conv_path).upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json; charset=utf-8"
    )

    # Conteo y metadata
    messages     = data.get("data") or data.get("messages") or []
    no_messages  = len(messages)
    conversation_id = data.get("conversation_id") or data.get("conversationId")

    # 4. Agregar fila al detalle de logs
    detail_rows.append({
        "property_id":      None,
        "checkin":          None,
        "code":             code,
        "reservation_path": res_path,
        "conversation_id":  conversation_id,
        "no_messages":      no_messages,
        "conversation_path": conv_path
    })

    print(f"[{now()}]   • Guardados {no_messages} mensajes en '{conv_path}'")

def insert_logs(bq_client, rows: list):
    """Inserta todas las filas de log en BigQuery."""
    table_ref = f"{BQ_PROJECT}.{LOG_DATASET}.{LOG_TABLE}"
    print(f"[{now()}] → Insertando {len(rows)} filas de log en {table_ref}")
    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"[{now()}] ⚠️ Errores al insertar logs: {errors}", file=sys.stderr)
    else:
        print(f"[{now()}] ✓ Logs insertados correctamente")

# === FLUJO PRINCIPAL ===
def main():
    print(f"[{now()}] === INICIO DEL PROCESO DE MENSAJES ===")

    storage_client = storage.Client()
    bq_client      = bigquery.Client(project=BQ_PROJECT)
    bucket         = storage_client.bucket(GCS_BUCKET)

    reservation_paths = list_reservations(bucket)
    detail_rows = []

    print(f"[{now()}] → Procesando mensajes (threads={MAX_THREADS})")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [
            executor.submit(fetch_save_messages, path, bucket, detail_rows)
            for path in reservation_paths
        ]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                fut.result()
            except Exception as e:
                print(f"[{now()}] ⚠️ Error en {reservation_paths[i-1]}: {e}", file=sys.stderr)
            print(f"[{now()}]   • Completadas {i}/{len(futures)} reservas")

    insert_logs(bq_client, detail_rows)
    print(f"[{now()}] === PROCESO FINALIZADO: {len(detail_rows)} entradas de log ===")

if __name__ == "__main__":
    main()
