import requests
import json
import datetime
import time
import uuid

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"

BOARD_ID = 4045377615
COLUMN_ID_FECHA = "pulse_log_mkqatmxw"

BIGQUERY_TABLE_ID = "properties.monday_properties_equipment"
LOGS_TABLE_ID = "project_settings.logs"

hoy = datetime.date.today()
ayer = hoy - datetime.timedelta(days=1)

START_DATE = ayer
END_DATE = hoy
DELTA = datetime.timedelta(days=1)

COLUMN_TYPE_MAP = {
    "name": "STRING",
    "subelementos": "STRING",
    "estado": "STRING",
    "n_meros": "FLOAT64",
    "texto_1": "STRING",
    "texto72": "STRING",
    "archivo_1": "STRING",
    "estado1": "STRING",
    "texto97": "STRING",
    "archivo_16": "STRING",
    "estado5": "STRING",
    "texto91": "STRING",
    "archivo_15": "STRING",
    "estado15": "STRING",
    "archivo84": "STRING",
    "estado3": "STRING",
    "archivo2": "STRING",
    "estado8": "STRING",
    "texto792": "STRING",
    "archivo": "STRING",
    "estado54": "STRING",
    "texto29": "STRING",
    "archivo5": "STRING",
    "estado0": "STRING",
    "archivo31": "STRING",
    "estado17": "STRING",
    "archivo96": "STRING",
    "estado03": "STRING",
    "archivo28": "STRING",
    "estado2": "STRING",
    "texto58": "STRING",
    "archivo_14": "STRING",
    "estado9": "STRING",
    "texto0": "STRING",
    "archivo27": "STRING",
    "texto4": "STRING",
    "archivo82": "STRING",
    "text_mkqed5gj": "STRING",
    "estado6": "STRING",
    "texto79": "STRING",
    "archivo7": "STRING",
    "estado7": "STRING",
    "texto75": "STRING",
    "archivo51": "STRING",
    "color": "STRING",
    "archivo24": "STRING",
    "estado39": "STRING",
    "texto5": "STRING",
    "archivo04": "STRING",
    "estado31": "STRING",
    "board_relation3": "STRING",
    "pulse_id_mkqak00g": "INT64",
    "pulse_log_mkqatmxw": "TIMESTAMP",
    "pulse_updated_mkqae7xe": "TIMESTAMP",
}

def try_parse_json_string(s):
    if not isinstance(s, str):
        return None
    try:
        parsed = json.loads(s)
        return parsed if isinstance(parsed, dict) else None
    except (ValueError, TypeError):
        pass
    s_fixed = s.replace("''", '"').replace("'", '"')
    try:
        parsed = json.loads(s_fixed)
        return parsed if isinstance(parsed, dict) else None
    except (ValueError, TypeError):
        return None

def parse_value_for_bq(value):
    try:
        if value is None:
            return None
        if isinstance(value, dict):
            if "files" in value:
                return ", ".join(
                    f.get("url") or f.get("public_url") or f.get("preview_url") or f.get("name", "")
                    for f in value["files"]
                )
            if "label" in value:
                return value["label"].get("text")
            if "text" in value:
                return value["text"]
            if "checked" in value:
                return value["checked"]
            if "rating" in value:
                return value["rating"]
            if "hour" in value and "minute" in value:
                return f"{value['hour']:02d}:{value['minute']:02d}:00"
            if "dateTime" in value:
                return value["dateTime"]
            if "date" in value:
                return value["date"]
            if "from" in value and "to" in value:
                return f"{value['from']} - {value['to']}"
            if "personsAndTeams" in value:
                return ", ".join(str(p.get("id", "")) for p in value["personsAndTeams"])
            if "linkedPulseIds" in value:
                return ", ".join(str(x.get("linkedPulseId", "")) for x in value["linkedPulseIds"])
            if "labels" in value:
                return ", ".join(l.get("name", "") for l in value["labels"])
            if "created_at" in value:
                return value["created_at"]
            if "updated_at" in value:
                return value["updated_at"]
            if "button" in value:
                return str(value["button"])
            if "url" in value:
                return value["url"]
            if "phone" in value:
                return value["phone"]
            if "countryName" in value:
                return value["countryName"]
            if "color" in value and len(value) == 1:
                return value["color"]
            if "item_id" in value:
                return str(value["item_id"])
            if "running" in value and "duration" in value:
                return str(value["duration"])
            if "ids" in value:
                return ", ".join(str(x) for x in value["ids"])
            if "value" in value:
                inner_val = value["value"]
                if isinstance(inner_val, (int, float, bool)):
                    return inner_val
                parsed_dict = try_parse_json_string(inner_val)
                if parsed_dict:
                    return parse_value_for_bq(parsed_dict)
                return str(inner_val)
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, (int, float, bool)):
            return value
        if isinstance(value, str):
            parsed_dict = try_parse_json_string(value)
            if parsed_dict:
                return parse_value_for_bq(parsed_dict)
            try:
                return float(value) if "." in value else int(value)
            except (ValueError, TypeError):
                return value
        return str(value)
    except Exception as e:
        print(f"[parse_value_for_bq] Error: {e}")
        return None

def parse_monday_column_value(text_val, json_val, bq_type):
    try:
        parsed_from_json_val = None
        if json_val:
            try:
                as_dict = json.loads(json_val)
                if isinstance(as_dict, dict):
                    parsed_from_json_val = parse_value_for_bq(as_dict)
            except:
                pass
        parsed_from_text = parse_value_for_bq(text_val) if parsed_from_json_val is None else None
        raw_val = parsed_from_json_val if parsed_from_json_val is not None else parsed_from_text
        if bq_type == "STRING":
            return "" if raw_val is None else str(raw_val)
        if raw_val is None:
            return None
        if bq_type == "BOOL":
            if isinstance(raw_val, bool):
                return raw_val
            return str(raw_val).lower().strip() in ("true", "checked", "1", "sí", "yes", "verdadero")
        if bq_type in ("NUMERIC", "FLOAT64"):
            try:
                return float(raw_val)
            except:
                return None
        if bq_type in ("INT64", "INTEGER"):
            try:
                return int(float(raw_val))
            except:
                return None
        if bq_type == "DATE":
            try:
                return datetime.datetime.strptime(str(raw_val), "%Y-%m-%d").date()
            except:
                return None
        if bq_type == "TIME":
            for fmt in ("%H:%M:%S", "%H:%M"):
                try:
                    return datetime.datetime.strptime(str(raw_val), fmt).time()
                except ValueError:
                    continue
            return None
        if bq_type == "TIMESTAMP":
            try:
                return datetime.datetime.fromisoformat(str(raw_val).replace("Z", "+00:00"))
            except:
                return None
        return str(raw_val)
    except Exception as e:
        print(f"[parse_monday_column_value] Error: {e}")
        return None

def build_monday_query():
    return f"""
        query {{
          boards(ids: {BOARD_ID}) {{
            items_page(
              limit: 500
              query_params: {{
                rules: [{{
                  column_id: "{COLUMN_ID_FECHA}",
                  operator: any_of,
                  compare_value: ["YESTERDAY"],
                  compare_attribute: "UPDATED_AT"
                }}]
              }}
            ) {{
              items {{
                id
                name
                created_at
                column_values {{
                  id
                  text
                  value
                }}
              }}
            }}
          }}
        }}
    """

def fetch_items_from_monday():
    try:
        query = build_monday_query()
        headers = {"Authorization": API_TOKEN, "Content-Type": "application/json"}
        response = requests.post(API_URL, json={"query": query}, headers=headers)
        data = response.json()
        if "errors" in data:
            raise Exception(data["errors"])
        boards_data = data.get("data", {}).get("boards", [])
        return boards_data[0].get("items_page", {}).get("items", []) if boards_data else []
    except Exception as e:
        print(f"[fetch_items_from_monday] Error: {e}")
        raise

def transform_items_to_rows(items):
    try:
        rows = []
        for item in items:
            row = {}
            row["pulse_id_mkqak00g"] = int(item["id"]) if item["id"] else None
            row["name"] = item.get("name", "") or ""
            for col_val in item.get("column_values", []):
                col_id = col_val["id"]
                if col_id in COLUMN_TYPE_MAP:
                    bq_type = COLUMN_TYPE_MAP[col_id]
                    row[col_id] = parse_monday_column_value(col_val["text"], col_val["value"], bq_type)
            rows.append(row)
        return rows
    except Exception as e:
        print(f"[transform_items_to_rows] Error: {e}")
        raise

def to_json_serializable(row):
    new_row = {}
    for k, v in row.items():
        if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
            new_row[k] = v.isoformat()
        else:
            new_row[k] = v
    return new_row

def ensure_dataset_exists(dataset_id):
    client = bigquery.Client()
    dataset_ref = bigquery.Dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' ya existe.")
    except NotFound:
        client.create_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' creado.")

def upsert_rows_into_bq(rows):
    if not rows:
        print("No hay filas que insertar/actualizar.")
        return 0, 0
    client = bigquery.Client()
    dataset_id = f"{client.project}.temp_dataset"
    ensure_dataset_exists(dataset_id)
    temp_table_id = f"{dataset_id}.tmp_{uuid.uuid4().hex}"

    schema = [bigquery.SchemaField("pulse_id_mkqak00g", "INT64"), bigquery.SchemaField("name", "STRING")]
    for col_id, bq_type in COLUMN_TYPE_MAP.items():
        if col_id not in ("pulse_id_mkqak00g", "name"):
            schema.append(bigquery.SchemaField(col_id, bq_type))
    client.create_table(bigquery.Table(temp_table_id, schema=schema))
    print(f"Tabla temporal creada: {temp_table_id}")

    client.insert_rows_json(temp_table_id, [to_json_serializable(r) for r in rows])
    print(f"{len(rows)} filas insertadas en la tabla temporal.")

    all_cols = [f"`{f.name}`" for f in schema]
    update_exprs = [f"T.{c} = S.{c}" for c in all_cols if c != "`pulse_id_mkqak00g`"]
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join([f"S.{c}" for c in all_cols])

    merge_sql = f"""
        MERGE `{BIGQUERY_TABLE_ID}` T
        USING `{temp_table_id}` S
        ON T.pulse_id_mkqak00g = S.pulse_id_mkqak00g
        WHEN MATCHED THEN UPDATE SET {', '.join(update_exprs)}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    merge_job = client.query(merge_sql)
    merge_job.result()
    dml_stats = merge_job.dml_stats or {}
    inserted = dml_stats.get("inserted_row_count", 0)
    updated = dml_stats.get("updated_row_count", 0)
    print(f"MERGE completado: {inserted} insertadas, {updated} actualizadas.")
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f"Tabla temporal eliminada: {temp_table_id}")
    return inserted, updated

def insert_log_record(database_name, process_name, records_created,
                      records_updated, execution_time, status,
                      primero, ultimo):
    try:
        client = bigquery.Client()
        query = f"""
        INSERT INTO `{LOGS_TABLE_ID}` (
          log_timestamp,
          database_name,
          process_name,
          records_created,
          records_updated,
          execution_time,
          status,
          primero,
          ultimo
        )
        VALUES (
          CURRENT_TIMESTAMP(),
          '{database_name}',
          '{process_name}',
          {records_created},
          {records_updated},
          {execution_time},
          '{status}',
          '{primero}',
          '{ultimo}'
        )
        """
        client.query(query).result()
        print("Log insertado correctamente.")
    except Exception as e:
        print(f"[insert_log_record] Error: {e}")

def main():
    print(f"Iniciando ingestión desde {START_DATE} hasta {END_DATE}")
    overall_inserted = 0
    overall_updated = 0
    start_time_global = time.time()
    current_date = START_DATE
    while current_date <= END_DATE:
        end_chunk = min(current_date + DELTA - datetime.timedelta(days=1), END_DATE)
        try:
            print(f"Extrayendo items del {current_date} al {end_chunk}...")
            items = fetch_items_from_monday()
            print(f"Items obtenidos: {len(items)}")
            rows = transform_items_to_rows(items)
            inserted, updated = upsert_rows_into_bq(rows)
            overall_inserted += inserted
            overall_updated += updated
        except Exception as e:
            print(f"[main] Error en ciclo {current_date}-{end_chunk}: {e}")
            insert_log_record(
                database_name="monday_properties_equipment",
                process_name="ingesta_diaria_monday_properties_equipment",
                records_created=0,
                records_updated=0,
                execution_time=0,
                status="ERROR",
                primero=str(current_date),
                ultimo=str(end_chunk)
            )
        current_date += DELTA
    total_time = time.time() - start_time_global
    insert_log_record(
        database_name="monday_properties_equipment",
        process_name="ingesta_diaria_monday_properties_equipment",
        records_created=overall_inserted + overall_updated,
        records_updated=overall_updated,
        execution_time=total_time,
        status="SUCCESS",
        primero=str(START_DATE),
        ultimo=str(END_DATE)
    )
    print("Proceso completado con éxito.")

if __name__ == "__main__":
    main()
