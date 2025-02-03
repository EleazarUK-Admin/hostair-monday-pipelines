import os
from google.cloud import storage, bigquery

SQL_PREFIX = "sql_por_procesar/"
SQL_PROCESADOS_PREFIX = "sql_ejecutados"
SQL_FALLIDOS_PREFIX = "sql_fallidos"
JSON_PREFIX = "procesando/"
JSON_PROCESADOS_PREFIX = "procesados"
LOGS_PREFIX = "logs"

def get_buckets():
    bq = bigquery.Client()
    query = """
    SELECT DISTINCT bucket FROM `project_settings.board_settings`
    WHERE bucket IS NOT NULL
    """
    query_job = bq.query(query)
    return [row.bucket for row in query_job]

def main():
    sc = storage.Client()
    bc = bigquery.Client()
    for bucket_name in get_buckets():
        bucket = sc.bucket(bucket_name)
        for blob in bucket.list_blobs(prefix=SQL_PREFIX):
            if blob.name.endswith(".sql"):
                sql_filename = os.path.basename(blob.name)
                sql_content = blob.download_as_text(encoding="utf-8").strip()
                try:
                    bc.query(sql_content).result()
                    bucket.rename_blob(blob, f"{SQL_PROCESADOS_PREFIX}/{sql_filename}")
                    json_filename = sql_filename.replace(".sql", ".json")
                    json_blob = bucket.blob(f"{JSON_PREFIX}{json_filename}")
                    if json_blob.exists():
                        bucket.rename_blob(json_blob, f"{JSON_PROCESADOS_PREFIX}/{json_filename}")
                except Exception as e:
                    bucket.rename_blob(blob, f"{SQL_FALLIDOS_PREFIX}/{sql_filename}")
                    log_blob = bucket.blob(f"{LOGS_PREFIX}/{sql_filename}.log")
                    log_blob.upload_from_string(str(e), content_type="text/plain")

if __name__ == "__main__":
    main()

#gcloud builds submit --tag gcr.io/hostair-test-data/ejecutor_sql:latest 
#gcloud run jobs create ejecutor-sql-job --image gcr.io/hostair-test-data/ejecutor_sql:latest --region us-central1
#gcloud run jobs execute ejecutor-sql-job --region us-central1
