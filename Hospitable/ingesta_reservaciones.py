#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json, unicodedata, concurrent.futures, sys
from datetime import datetime
from typing import List, Dict
import requests
from google.cloud import storage, bigquery

API_TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9."
    "eyJhdWQiOiI5YTYyNGRmMC0xMmYxLTQ0OGUtYjg4NC00MzY3ODBhNWQzY2QiLCJqdGkiOiJkMGQyNWJlMTRkMTMwNDBkYjA0YThmM2ZmYmM4YzkzMzBiY2RlMTY1ZmVkNjNhNTVkMzUwZmEwOWNjMzI3OTAxMDZlMTliN2M3ODJkZTgwMCIsImlhdCI6MTcxNDczMTA2Ny43MTE5MzQsIm5iZiI6MTcxNDczMTA2Ny43MTE5MzcsImV4cCI6MTc0NjI2NzA2Ny43MDcyODEsInN1YiI6IjI3NDYyIiwic2NvcGVzIjpbInBhdDpyZWFkIiwicGF0OndyaXRlIl19."
    "UQv1WCXiuJ20n5aE7UvmNcLTYTuGANThzpUvg5N8zqWJGX803-8auXWWoYuxygH41GiHZy6XXvQS8BVoiFFcPMahYj45rO3s7lIdKhD5CQAvDFVaMv33u96W9I7ISrfRvGO9Uo3_LqsHOauZX7QICErejrF2zcs7hpayDBV2tfg-ZZN0qnynQLhIqHiS1nV34mxk3currsrXNJIUydCKc20gQHHeiC-AKS4f9jnwvHlX2D-7_xcW1po2SJ53oBdNln6wMozmk6pp7Qdft6dFuPFoVxuOv4crExuGMC1tfX5r9RShg5c3g5z7NzSDQNz_ETgNQhOoPwPntFfprtZSloiASluWaqcJPwVxJo4C_G5pOIeer4aiTOytOVCvjhaXkNhMFqcTE9-Q2DnqxlhX1aIZ4H69-V_p5F4C_UYu2N6RbsqkVPlzBQBYKJCV5FK2mF0oQZREdGT8HsgVdtRn5V-Wm9bP1r6maBugIipfd87566jkZGPCXIjHjZDZBDMnyPnH7ntsspnzM7RUfT3rrlXKnk1ZxqWl0RgakgvCZ8GTKS77PxelGqUZu38GKNpxrI0aC--_DR2_wY31HbnRc6zhSIsviC9cn5w7NDqBUSdFuNv9EFi1XjFb3ayRqpmT4p5eVGFjY-34962Ln60txgAY9MaYYuy2WVJ-a8CHK6Q"
)
GCS_BUCKET       = "historic_info"
BQ_PROJECT       = "hostair-test-data"
BQ_DATASET       = "properties"
LOG_DATASET      = "project_settings"
LOG_TABLE        = "historic_ingest_log"
LOG_SUM_TABLE    = "historic_ingest_log_sum"
API_URL          = "https://public.api.hospitable.com/v2/reservations"
DATE_START, DATE_END = "2016-01-01", "2024-12-31"

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {API_TOKEN}"
}

def sanitize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    s = s.replace(" ", "_").replace("/", "_")
    return "".join(c for c in s if c.isalnum() or c in "._-")

def now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")

def get_property_ids(client: bigquery.Client) -> List[str]:
    sql = (
        "SELECT DISTINCT id FROM "
        f"`{BQ_PROJECT}.{BQ_DATASET}.historic_properties_json_ss`"
    )
    rows = list(client.query(sql).result())
    print(f"[{now()}] → Recibidos {len(rows)} property_id de BigQuery")
    return [r.id for r in rows]

def fetch_save_reservations(prop_id: str,
                            bucket: storage.bucket.Bucket,
                            detail_rows: List[Dict],
                            summary_rows: List[Dict]) -> None:
    print(f"[{now()}] → Empieza descarga de property {prop_id}")
    page, total_prop = 1, 0
    params = {
        "per_page": "100",
        "properties[]": prop_id,
        "start_date": DATE_START,
        "end_date": DATE_END,
        "include": "financials,properties,listings",
        "date_query": "checkin",
    }
    while True:
        params["page"] = page
        r = requests.get(API_URL, headers=HEADERS, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        batch = 0
        for res in data.get("data", []):
            code    = res.get("code")
            arrival = res.get("arrival_date")
            if not code or not arrival:
                continue

            prop_from_json = (
                res.get("property_id")
                or res.get("property", {}).get("id")
                or prop_id
            )
            dt = datetime.fromisoformat(arrival)
            gcs_path = f"reservations/{dt:%Y/%m/%d}/{sanitize(code)}.json"
            bucket.blob(gcs_path).upload_from_string(
                json.dumps(res, ensure_ascii=False),
                content_type="application/json; charset=utf-8"
            )
            detail_rows.append({
                "property_id": prop_from_json,
                "checkin": dt.date().isoformat(),
                "code": code,
                "reservation_path": gcs_path,
                "conversation_id": res.get("conversation_id"),
                "no_messages": None,
                "conversation_path": None
            })
            total_prop += 1
            batch += 1

        print(f"[{now()}]   • Property {prop_id} página {page} → {batch} reservas")
        if not data.get("links", {}).get("next"):
            break
        page += 1

    summary_rows.append({
        "type": "property",
        "id": prop_id,
        "processed_type": "reservations",
        "processed": total_prop
    })
    print(f"[{now()}] ← Termina property {prop_id} ({total_prop} reservas)")

def insert_logs(bq: bigquery.Client,
                detail: List[Dict],
                summary: List[Dict]) -> None:
    print(f"[{now()}] → Insertando {len(detail)} filas detalle y {len(summary)} de resumen")
    bq.insert_rows_json(f"{BQ_PROJECT}.{LOG_DATASET}.{LOG_TABLE}", detail)
    bq.insert_rows_json(f"{BQ_PROJECT}.{LOG_DATASET}.{LOG_SUM_TABLE}", summary)

def main():
    print(f"[{now()}] === INICIO DEL PROCESO ===")
    storage_client = storage.Client()
    bq_client      = bigquery.Client(project=BQ_PROJECT)
    bucket         = storage_client.bucket(GCS_BUCKET)

    property_ids = get_property_ids(bq_client)
    detail_rows, summary_rows = [], []

    print(f"[{now()}] → Descargando reservas (max_threads=5)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = [
            pool.submit(fetch_save_reservations, pid, bucket, detail_rows, summary_rows)
            for pid in property_ids
        ]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            fut.result()
            print(f"[{now()}]   • Propiedades completadas: {i}/{len(futures)}")

    insert_logs(bq_client, detail_rows, summary_rows)
    print(f"[{now()}] === FIN DEL PROCESO === Reservas guardadas: {len(detail_rows)}, Propiedades procesadas: {len(summary_rows)}")

if __name__ == "__main__":
    main()
