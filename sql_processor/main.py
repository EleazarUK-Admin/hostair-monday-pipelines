import os
from google.cloud import storage, bigquery

BUCKET_NAME = "limpieza-test"
SQL_PREFIX = "sql_por_procesar/"
SQL_PROCESADOS_PREFIX = "sql_ejecutados"
SQL_FALLIDOS_PREFIX = "sql_fallidos"
JSON_PREFIX = "procesando/"
JSON_PROCESADOS_PREFIX = "procesados"
LOGS_PREFIX = "logs"

def main():
    sc = storage.Client()
    bc = bigquery.Client(project="hostair-test-data")
    b = sc.bucket(BUCKET_NAME)
    for blob in b.list_blobs(prefix=SQL_PREFIX):
        if blob.name.endswith(".sql"):
            fsql = os.path.basename(blob.name)
            sql_content = blob.download_as_text(encoding="utf-8").strip()
            try:
                bc.query(sql_content).result()
                b.rename_blob(blob, f"{SQL_PROCESADOS_PREFIX}/{fsql}")
                fjson = fsql.replace(".sql", ".json")
                jblob = b.blob(f"{JSON_PREFIX}{fjson}")
                if jblob.exists():
                    b.rename_blob(jblob, f"{JSON_PROCESADOS_PREFIX}/{fjson}")
            except Exception as e:
                b.rename_blob(blob, f"{SQL_FALLIDOS_PREFIX}/{fsql}")
                b.blob(f"{LOGS_PREFIX}/{fsql}.log").upload_from_string(str(e), content_type="text/plain")

if __name__ == "__main__":
    main()
