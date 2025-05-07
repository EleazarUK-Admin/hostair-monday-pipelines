#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga todas las reservas NDJSON de
gs://historic_info/reservations/**  →  hostair-test-data.properties.reservaciones_ss
• DROP + CREATE table cada ejecución
• Partición diaria (arrival_date) y clustering (code, platform)
• Tolera hasta 5 000 registros inválidos por sub-job
"""

from google.cloud import bigquery, storage
from datetime import datetime, timezone
import concurrent.futures as cf
import time, sys

# -------- CONFIGURACIÓN ------------------------------------------------------
PROJECT_ID   = "hostair-test-data"
DATASET_ID   = "properties"
TABLE_ID     = "reservaciones_ss"
BUCKET       = "historic_info"
PREFIX       = "reservations/"
MAX_WORKERS  = 16      # hilos para enumerar blobs
CHUNK        = 5_000   # URIs por sub-job (≤10 000)
MAX_BAD      = 5_000   # filas “malas” permitidas por sub-job
LOCATION     = "US"
# -----------------------------------------------------------------------------


def _c(code): return f"\033[{code}m" if sys.stdout.isatty() else ""
CLR, RST = {"hdr":"95","ok":"92","warn":"93","err":"91","dim":"2"}, _c("0")
ts  = lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
log = lambda m,l="hdr": print(f"{_c(CLR[l])}[{ts()}] {m}{RST}", flush=True)


def list_json_uris() -> list[str]:
    log(f"Enumerando gs://{BUCKET}/{PREFIX}…", "dim")
    bucket = storage.Client().bucket(BUCKET)
    uris = []
    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        list(pool.map(lambda b: uris.append(f"gs://{BUCKET}/{b.name}") if b.name.endswith(".json") else None,
                      bucket.list_blobs(prefix=PREFIX)))
    log(f"→ {len(uris):,} archivos JSON encontrados", "ok")
    return uris


def prep_bq(bq: bigquery.Client):
    ds = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    ds.location = LOCATION
    bq.create_dataset(ds, exists_ok=True)
    bq.delete_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", not_found_ok=True)
    log(f"Tabla anterior {TABLE_ID} eliminada (si existía)", "warn")


def load_to_bq(uris: list[str]):
    bq = bigquery.Client(project=PROJECT_ID)
    prep_bq(bq)

    cfg = bigquery.LoadJobConfig(
        source_format         = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect            = True,
        write_disposition     = "WRITE_TRUNCATE",
        ignore_unknown_values = True,
        max_bad_records       = MAX_BAD,
        time_partitioning     = bigquery.TimePartitioning(
                                    type_=bigquery.TimePartitioningType.DAY,
                                    field="arrival_date"),
        clustering_fields     = ["code", "platform"]
    )

    total, t0 = 0, time.time()
    for i in range(0, len(uris), CHUNK):
        batch = uris[i:i+CHUNK]; part = i//CHUNK + 1
        log(f"Sub-job {part}: cargando {len(batch):,} archivos…", "warn")
        job = bq.load_table_from_uri(batch, f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", job_config=cfg)
        job.result()
        total += job.output_rows or 0
        log(f"✔  Sub-job {part} OK – filas: {job.output_rows:,}", "ok")

    table = bq.get_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    log(f"✅  Tabla {table.full_table_id} creada con {table.num_rows:,} filas "
        f"en {time.time()-t0:,.1f}s", "ok")


def main():
    log("=== INICIO DEL PROCESO ===")
    uris = list_json_uris()
    if not uris:
        log("No se encontraron JSON. Abortando.", "err"); return
    load_to_bq(uris)
    log("=== FIN DEL PROCESO ===")


if __name__ == "__main__":
    main()
