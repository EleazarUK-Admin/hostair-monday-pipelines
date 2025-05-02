# main.py
import json
import unicodedata
import pathlib
import concurrent.futures
from datetime import datetime
from typing import List

import requests
from google.api_core.exceptions import NotFound
from google.cloud import storage, bigquery

# ──────────────── CONFIGURACIÓN ────────────────
API_URL = "https://public.api.hospitable.com/v2/properties"
API_TOKEN = (
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9."
        "eyJhdWQiOiI5YTYyNGRmMC0xMmYxLTQ0OGUtYjg4NC00MzY3ODBhNWQzY2QiLCJqdGkiOiJkMGQyNWJlMTRkMTMwNDBkYjA0YThmM2ZmYmM4YzkzMzBiY2RlMTY1ZmVkNjNhNTVkMzUwZmEwOWNjMzI3OTAxMDZlMTliN2M3ODJkZTgwMCIsImlhdCI6MTcxNDczMTA2Ny43MTE5MzQsIm5iZiI6MTcxNDczMTA2Ny43MTE5MzcsImV4cCI6MTc0NjI2NzA2Ny43MDcyODEsInN1YiI6IjI3NDYyIiwic2NvcGVzIjpbInBhdDpyZWFkIiwicGF0OndyaXRlIl19."
        "UQv1WCXiuJ20n5aE7UvmNcLTYTuGANThzpUvg5N8zqWJGX803-8auXWWoYuxygH41GiHZy6XXvQS8BVoiFFcPMahYj45rO3s7lIdKhD5CQAvDFVaMv33u96W9I7ISrfRvGO9Uo3_LqsHOauZX7QICErejrF2zcs7hpayDBV2tfg-ZZN0qnynQLhIqHiS1nV34mxk3currsrXNJIUydCKc20gQHHeiC-AKS4f9jnwvHlX2D-7_xcW1po2SJ53oBdNln6wMozmk6pp7Qdft6dFuPFoVxuOv4crExuGMC1tfX5r9RShg5c3g5z7NzSDQNz_ETgNQhOoPwPntFfprtZSloiASluWaqcJPwVxJo4C_G5pOIeer4aiTOytOVCvjhaXkNhMFqcTE9-Q2DnqxlhX1aIZ4H69-V_p5F4C_UYu2N6RbsqkVPlzBQBYKJCV5FK2mF0oQZREdGT8HsgVdtRn5V-Wm9bP1r6maBugIipfd87566jkZGPCXIjHjZDZBDMnyPnH7ntsspnzM7RUfT3rrlXKnk1ZxqWl0RgakgvCZ8GTKS77PxelGqUZu38GKNpxrI0aC--_DR2_wY31HbnRc6zhSIsviC9cn5w7NDqBUSdFuNv9EFi1XjFb3ayRqpmT4p5eVGFjY-34962Ln60txgAY9MaYYuy2WVJ-a8CHK6Q"
    )

BUCKET_NAME = "historic_info"                    # bucket destino en GCS
DEST_PATH = "properties"                         # carpeta dentro del bucket
BQ_PROJECT = "hostair-test-data"                 # ID de proyecto GCP
BQ_DATASET = "properties"
BQ_TABLE   = "historic_properties_json_ss"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {API_TOKEN}",
}

# ────────────────  UTILIDADES ────────────────
def sanitize_filename(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().replace(" ", "_").replace("/", "_")
    return "".join(c for c in name if c.isalnum() or c in "._-")

def build_schema() -> List[bigquery.SchemaField]:
    """Esquema EXACTO pedido, generado a mano."""
    return [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("public_name", "STRING"),
        bigquery.SchemaField("picture", "STRING"),
        bigquery.SchemaField(
            "address",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("number", "STRING"),
                bigquery.SchemaField("street", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("postcode", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("country_name", "STRING"),
                bigquery.SchemaField(
                    "coordinates",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("latitude", "STRING"),
                        bigquery.SchemaField("longitude", "STRING"),
                    ],
                ),
                bigquery.SchemaField("display", "STRING"),
            ],
        ),
        bigquery.SchemaField("timezone", "STRING"),
        bigquery.SchemaField("listed", "BOOL"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("summary", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("checkin", "STRING"),
        bigquery.SchemaField("checkout", "STRING"),
        bigquery.SchemaField("amenities", "STRING", mode="REPEATED"),
        bigquery.SchemaField(
            "capacity",
            "RECORD",
            fields=[
                bigquery.SchemaField("max", "INT64"),
                bigquery.SchemaField("bedrooms", "INT64"),
                bigquery.SchemaField("beds", "INT64"),
                bigquery.SchemaField("bathrooms", "FLOAT64"),
            ],
        ),
        bigquery.SchemaField(
            "room_details",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(
                    "beds",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("type", "STRING"),
                        bigquery.SchemaField("quantity", "INT64"),
                    ],
                )
            ],
        ),
        bigquery.SchemaField("property_type", "STRING"),
        bigquery.SchemaField("room_type", "STRING"),
        bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
        bigquery.SchemaField(
            "house_rules",
            "RECORD",
            fields=[
                bigquery.SchemaField("pets_allowed", "BOOL"),
                bigquery.SchemaField("smoking_allowed", "BOOL"),
                bigquery.SchemaField("events_allowed", "BOOL"),
            ],
        ),
        bigquery.SchemaField("calendar_restricted", "BOOL"),
        bigquery.SchemaField(
            "parent_child",
            "RECORD",
            fields=[
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("parent", "STRING"),
                bigquery.SchemaField("children", "STRING", mode="REPEATED"),
                bigquery.SchemaField("siblings", "STRING", mode="REPEATED"),
            ],
        ),
        # bookings sub-estructura (solo nivel 1: lo relevante)
        bigquery.SchemaField(
            "bookings",
            "RECORD",
            fields=[
                bigquery.SchemaField(
                    "booking_policies",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("cancellation", "STRING", mode="REPEATED"),
                        bigquery.SchemaField(
                            "payment_terms",
                            "RECORD",
                            fields=[
                                bigquery.SchemaField("status", "STRING"),
                                bigquery.SchemaField("description", "STRING", mode="REPEATED"),
                                bigquery.SchemaField("grace_period", "STRING"),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField(
                    "listing_markups",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("platform", "STRING"),
                        bigquery.SchemaField("type", "STRING"),
                        bigquery.SchemaField("markup", "FLOAT64"),
                    ],
                ),
                bigquery.SchemaField(
                    "security_deposits",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("name", "STRING"),
                        bigquery.SchemaField("type", "STRING"),
                        bigquery.SchemaField(
                            "value",
                            "RECORD",
                            fields=[
                                bigquery.SchemaField("amount", "INT64"),
                                bigquery.SchemaField("formatted", "STRING"),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField(
                    "occupancy_based_rules",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("guests_included", "INT64"),
                        bigquery.SchemaField(
                            "extra_guest_fee",
                            "RECORD",
                            fields=[
                                bigquery.SchemaField("type", "STRING"),
                                bigquery.SchemaField(
                                    "value",
                                    "RECORD",
                                    fields=[
                                        bigquery.SchemaField("amount", "INT64"),
                                        bigquery.SchemaField("formatted", "STRING"),
                                    ],
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "pet_fee",
                            "RECORD",
                            mode="NULLABLE",
                            fields=[
                                bigquery.SchemaField("type", "STRING"),
                                bigquery.SchemaField(
                                    "value",
                                    "RECORD",
                                    fields=[
                                        bigquery.SchemaField("amount", "INT64"),
                                        bigquery.SchemaField("formatted", "STRING"),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField(
                    "fees",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("name", "STRING"),
                        bigquery.SchemaField("type", "STRING"),
                        bigquery.SchemaField(
                            "value",
                            "RECORD",
                            fields=[
                                bigquery.SchemaField("amount", "INT64"),
                                bigquery.SchemaField("formatted", "STRING"),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField(
                    "discounts",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("name", "STRING"),
                        bigquery.SchemaField("type", "STRING"),
                        bigquery.SchemaField("value", "FLOAT64"),
                    ],
                ),
            ],
        ),
    ]

# ────────────────  PASO 1. DESCARGAR Y SUBIR JSON ────────────────
def dump_all_properties() -> int:
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    page, total = 1, 0
    while True:
        resp = requests.get(API_URL, headers=HEADERS, params={"per_page": 100, "page": page, "include": "bookings"})
        resp.raise_for_status()
        data = resp.json()

        # Subir en paralelo para acelerar un poco
        with concurrent.futures.ThreadPoolExecutor() as pool:
            futures = []
            for prop in data.get("data", []):
                safe = sanitize_filename(prop.get("name") or f"sin_nombre_{prop['id']}")
                json_str = json.dumps(prop, ensure_ascii=False)
                blob_path = f"{DEST_PATH}/{safe}.json"
                futures.append(
                    pool.submit(bucket.blob(blob_path).upload_from_string, json_str.encode("utf-8"), content_type="application/json; charset=utf-8")
                )
            for _ in concurrent.futures.as_completed(futures):
                total += 1

        if not data.get("links", {}).get("next"):
            break
        page += 1
    print(f"✓ {total} archivos subidos al bucket gs://{BUCKET_NAME}/{DEST_PATH}")
    return total

# ────────────────  PASO 2. RECREAR TABLA BQ ────────────────
def recreate_table(schema: List[bigquery.SchemaField]) -> None:
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # Borrar si existe
    try:
        client.delete_table(table_id, not_found_ok=False)
        print(f"✎ Tabla {table_id} eliminada")
    except NotFound:
        print(f"ℹ︎ Tabla {table_id} no existía")

    # Crear tabla vacía
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(f"✓ Tabla {table_id} creada")

# ────────────────  PASO 3. CARGAR JSON A LA TABLA ────────────────
def load_json_into_table(schema: List[bigquery.SchemaField]) -> None:
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    uri = f"gs://{BUCKET_NAME}/{DEST_PATH}/*.json"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Esperar a que termine
    dest_table = client.get_table(table_id)
    print(f"✓ {dest_table.num_rows:,} filas cargadas en {table_id}")

# ────────────────  PROGRAMA PRINCIPAL ────────────────
def main():
    print("\n🚀 Inicio de proceso", datetime.utcnow().isoformat(), "UTC")
    total = dump_all_properties()

    if total == 0:
        print("No se descargaron propiedades. Proceso terminado.")
        return

    schema = build_schema()
    recreate_table(schema)
    load_json_into_table(schema)
    print("🏁 Proceso completado con éxito.\n")

if __name__ == "__main__":
    main()
