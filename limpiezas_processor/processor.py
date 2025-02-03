import os
import json
from datetime import datetime
from google.cloud import storage, bigquery

def parse_value_for_bq(value):
    if value is None:
        return None
    if isinstance(value, dict):
        if "checked" in value:
            return value["checked"]
        if "rating" in value and isinstance(value["rating"], (int, float)):
            return value["rating"]
        if "hour" in value and "minute" in value:
            return f"{value['hour']:02d}:{value['minute']:02d}:00"
        if "date" in value and value["date"] is not None:
            return value["date"]
        if "from" in value and "to" in value:
            return f"{value['from']} - {value['to']}"
        if "personsAndTeams" in value and isinstance(value["personsAndTeams"], list):
            names = [str(person.get("name", "")) for person in value["personsAndTeams"]]
            return ", ".join(names)
        if "files" in value and isinstance(value["files"], list):
            urls = [file.get("url", "") for file in value["files"] if "url" in file]
            return ", ".join(urls)
        if "label" in value:
            return value["label"]
        if "value" in value:
            val = value["value"]
            if val is None:
                return None
            if isinstance(val, (int, float, bool)):
                return val
            return str(val)
        if "url" in value:
            return value["url"]
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            return int(value)
        except (ValueError, TypeError):
            pass
        return value
    return str(value)

def escape_sql_string(s):
    escaped = s.replace("'", "''")
    return f"'{escaped}'"

def generate_insert_sql(pulse_id, pulse_name, column_values, table_fqn):
    cols = ["id", "name"] + list(column_values.keys())
    vals = []
    vals.append(str(pulse_id))
    if pulse_name is None:
        vals.append("NULL")
    else:
        vals.append(escape_sql_string(pulse_name))
    for c in column_values:
        parsed_val = parse_value_for_bq(column_values[c])
        if parsed_val is None:
            vals.append("NULL")
        elif isinstance(parsed_val, (int, float, bool)):
            vals.append(str(parsed_val))
        else:
            vals.append(escape_sql_string(parsed_val))
    col_string = ", ".join(f"`{c}`" for c in cols)
    val_string = ", ".join(vals)
    return f"INSERT INTO `{table_fqn}` ({col_string}) VALUES ({val_string});"

def generate_update_sql(pulse_id, column_id, new_value, table_fqn):
    parsed_val = parse_value_for_bq(new_value)
    if parsed_val is None:
        set_expr = "NULL"
    elif isinstance(parsed_val, (int, float, bool)):
        set_expr = str(parsed_val)
    else:
        set_expr = escape_sql_string(parsed_val)
    return f"UPDATE `{table_fqn}` SET `{column_id}` = {set_expr} WHERE `id` = {pulse_id};"

def generate_delete_sql(pulse_id, table_fqn):
    return f"DELETE FROM `{table_fqn}` WHERE `id` = {pulse_id};"

def get_board_settings(board_id):
    client = bigquery.Client()
    query = """
    SELECT bucket, dataset, `table`
    FROM `project_settings.board_settings`
    WHERE board_id = @board_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("board_id", "INT64", board_id)]
    )
    query_job = client.query(query, job_config=job_config)
    rows = list(query_job)
    if not rows:
        raise Exception("Configuración no encontrada para board_id")
    row = rows[0]
    return {"bucket": row.bucket, "dataset": row.dataset, "table": row.table}

def get_bucket(bucket_name):
    client = storage.Client()
    return client.bucket(bucket_name)

def download_json(blob):
    content = blob.download_as_text(encoding="utf-8")
    return json.loads(content)

def upload_file_to_bucket(bucket_name, destination_path, content):
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_string(content, content_type="text/plain")

def move_blob(blob, new_prefix):
    filename = os.path.basename(blob.name)
    destination_name = f"{new_prefix}/{filename}"
    new_blob = blob.bucket.rename_blob(blob, destination_name)
    return new_blob.name

def process_webhook_json(data, table_fqn):
    e = data.get("event", {})
    t = e.get("type", "")
    if t == "create_pulse":
        pid = e.get("pulseId")
        pname = e.get("pulseName")
        cv = e.get("columnValues", {})
        return generate_insert_sql(pid, pname, cv, table_fqn)
    elif t == "update_column_value":
        pid = e.get("pulseId")
        cid = e.get("columnId")
        nv = e.get("value", {})
        return generate_update_sql(pid, cid, nv, table_fqn)
    elif t == "delete_pulse":
        pid = e.get("itemId")
        return generate_delete_sql(pid, table_fqn)
    elif t == "update_name":
        pid = e.get("pulseId")
        nv = e.get("value", {}).get("name")
        return generate_update_sql(pid, "name", nv, table_fqn)
    return None

def process_blob_in_por_procesar(blob):
    data = download_json(blob)
    board_id = None
    if "event" in data and "boardId" in data["event"]:
        board_id = data["event"]["boardId"]
    elif "boardId" in data:
        board_id = data["boardId"]
    if board_id is None:
        raise Exception("boardId no encontrado en el JSON")
    settings = get_board_settings(board_id)
    table_fqn = f"{settings['dataset']}.{settings['table']}"
    stmt = process_webhook_json(data, table_fqn)
    if stmt:
        base_name = os.path.splitext(os.path.basename(blob.name))[0]
        sql_name = f"{base_name}.sql"
        destination_sql_path = f"sql_por_procesar/{sql_name}"
        upload_file_to_bucket(settings["bucket"], destination_sql_path, stmt)
    move_blob(blob, "procesando")

def get_processing_buckets():
    client = bigquery.Client()
    query = """
    SELECT DISTINCT bucket
    FROM `project_settings.board_settings`
    WHERE bucket IS NOT NULL
    """
    query_job = client.query(query)
    return [row.bucket for row in query_job]

def main():
    sc = storage.Client()
    for bucket_name in get_processing_buckets():
        bucket = sc.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="por_procesar/")
        for blob in blobs:
            if blob.name.endswith(".json"):
                process_blob_in_por_procesar(blob)

if __name__ == "__main__":
    main()



#gcloud builds submit --tag gcr.io/hostair-test-data/generador_sql2:latest 
#gcloud run jobs create generador-sql-test-job --image gcr.io/hostair-test-data/generador_sql2:latest --region us-central1
#gcloud run jobs execute generador-sql-test-job --region us-central1
