import requests
import json
import datetime
import time
import uuid

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ─────────── Configuración ───────────
API_TOKEN = (
    "eyJhbGciOiJIUzI1NiJ9."
    "eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6"
    "d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ."
    "ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
)
API_URL  = "https://api.monday.com/v2"

BOARD_ID        = 6311489730
COLUMN_ID_FECHA = "pulse_log_mkqa2a71"

BIGQUERY_TABLE_ID = "properties.monday_properties_checkin_guide"
LOGS_TABLE_ID     = "project_settings.logs"

hoy  = datetime.date.today()
ayer = hoy - datetime.timedelta(days=1)

hoy  = datetime.date.today()
ayer = hoy - datetime.timedelta(days=1)

START_DATE = datetime.date.today() - datetime.timedelta(days=1)
END_DATE   = datetime.date.today() - datetime.timedelta(days=1)
DELTA      = datetime.timedelta(days=1)

# ─────────── Mapeo de columnas → tipos BQ ───────────
COLUMN_TYPE_MAP = {
    "subelementos__1": "STRING",
    "archivo97__1": "STRING",
    "archivo": "STRING",
    "texto_largo__1": "STRING",
    "long_text__1": "STRING",
    "archivo__1": "STRING",
    "long_text2__1": "STRING",
    "long_text0__1": "STRING",
    "archivo3__1": "STRING",
    "dup__of_paso_2___ingl_s__1": "STRING",
    "long_text3__1": "STRING",
    "archivo5__1": "STRING",
    "long_text1__1": "STRING",
    "dup__of_paso_3___ingl_s__1": "STRING",
    "archivo7__1": "STRING",
    "long_text08__1": "STRING",
    "long_text29__1": "STRING",
    "archivo36__1": "STRING",
    "dup__of_paso_5___ingl_s__1": "STRING",
    "long_text7__1": "STRING",
    "archivo0__1": "STRING",
    "dup__of_paso_6___ingl_s__1": "STRING",
    "long_text8__1": "STRING",
    "archivo9__1": "STRING",
    "dup__of_paso_7___ingl_s__1": "STRING",
    "long_text84__1": "STRING",
    "archivo91__1": "STRING",
    "texto_largo7__1": "STRING",
    "long_text16__1": "STRING",
    "archivo77__1": "STRING",
    "dup__of_paso_9___ingl_s__1": "STRING",
    "long_text39__1": "STRING",
    "archivo6__1": "STRING",
    "texto_largo4__1": "STRING",
    "long_text9__1": "STRING",
    "archivo4__1": "STRING",
    "long_text6__1": "STRING",
    "long_text4__1": "STRING",
    "dup__of_paso_12___foto__1": "STRING",
    "paso_13___espa_ol__1": "STRING",
    "paso_13__ingl_s__1": "STRING",
    "dup__of_paso_13___foto__1": "STRING",
    "long_text48__1": "STRING",
    "dup__of_paso_13__ingl_s__1": "STRING",
    "archivo8__1": "STRING",
    "texto_largo2__1": "STRING",
    "long_text88__1": "STRING",
    "archivo82__1": "STRING",
    "texto_largo1__1": "STRING",
    "long_text07__1": "STRING",
    "archivo57__1": "STRING",
    "texto_largo5__1": "STRING",
    "dup__of_texto_largo__1": "STRING",
    "archivo2__1": "STRING",
    "texto_largo9__1": "STRING",
    "dup__of_texto_largo5__1": "STRING",
    "archivo66__1": "STRING",
    "texto_largo43__1": "STRING",
    "dup__of_texto_largo6__1": "STRING",
    "archivo88__1": "STRING",
    "texto_largo74__1": "STRING",
    "dup__of_texto_largo0__1": "STRING",
    "n_meros__1": "FLOAT64",
    "estado__1": "STRING",
    "link_to_propiedades__1": "STRING",
    "reflejo__1": "STRING",
    "reflejo7__1": "STRING",
    "pulse_log_mkqa2a71": "TIMESTAMP",
    "pulse_updated_mkqacfva": "TIMESTAMP",
}

# ───────────────────────────────────────── helpers ─────────────────────────────────────────
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
                    f.get("url")
                    or f.get("public_url")
                    or f.get("preview_url")
                    or f.get("name", "")
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
            except Exception:
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
            return str(raw_val).lower().strip() in (
                "true",
                "checked",
                "1",
                "sí",
                "yes",
                "verdadero",
            )
        if bq_type in ("NUMERIC", "FLOAT64"):
            try:
                return float(raw_val)
            except Exception:
                return None
        if bq_type in ("INT64", "INTEGER"):
            try:
                return int(float(raw_val))
            except Exception:
                return None
        if bq_type == "DATE":
            try:
                return datetime.datetime.strptime(str(raw_val), "%Y-%m-%d").date()
            except Exception:
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
            except Exception:
                return None
        return str(raw_val)
    except Exception as e:
        print(f"[parse_monday_column_value] Error: {e}")
        return None


# ───────────────── Monday.com ─────────────────
def build_monday_query(start_str, end_str):
    return f"""
        query {{
          boards(ids: {BOARD_ID}) {{
            items_page(
              limit: 500
              query_params: {{
                rules: [{{
                  column_id: "{COLUMN_ID_FECHA}",
                  operator: between,
                  compare_value: [            "2019-01-16",            "2025-04-29"          ],
                  compare_attribute: "CREATED_AT"
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


def fetch_items_from_monday(a, b):
    try:
        query = build_monday_query(a, b)
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


# ───────────────── Transformación ─────────────────
def transform_items_to_rows(items):
    try:
        rows = []
        for item in items:
            row = {
                "pulse_id_mkqa2ewa": int(item["id"]) if item["id"] else None,
                "name": item.get("name", "") or "",
            }
            for col_val in item.get("column_values", []):
                col_id = col_val["id"]
                if col_id in COLUMN_TYPE_MAP:
                    bq_type = COLUMN_TYPE_MAP[col_id]
                    row[col_id] = parse_monday_column_value(
                        col_val["text"], col_val["value"], bq_type
                    )
            rows.append(row)
        return rows
    except Exception as e:
        print(f"[transform_items_to_rows] Error: {e}")
        raise


def to_json_serializable(row):
    out = {}
    for k, v in row.items():
        out[k] = v.isoformat() if isinstance(v, (datetime.datetime, datetime.date, datetime.time)) else v
    return out


# ───────────────── BigQuery helpers ─────────────────
def ensure_dataset_exists(dataset_id):
    client = bigquery.Client()
    ref = bigquery.Dataset(dataset_id)
    try:
        client.get_dataset(ref)
    except NotFound:
        client.create_dataset(ref)


def upsert_rows_into_bq(rows):
    if not rows:
        print("No hay filas para insertar/actualizar.")
        return 0, 0

    client = bigquery.Client()

    # 1️⃣ tabla temporal
    tmp_ds  = f"{client.project}.temp_dataset"
    ensure_dataset_exists(tmp_ds)
    tmp_tbl = f"{tmp_ds}.tmp_{uuid.uuid4().hex}"

    schema = [
        bigquery.SchemaField("pulse_id_mkqa2ewa", "INT64"),
        bigquery.SchemaField("name", "STRING"),
    ] + [
        bigquery.SchemaField(col_id, bq_type)
        for col_id, bq_type in COLUMN_TYPE_MAP.items()
    ]

    client.create_table(bigquery.Table(tmp_tbl, schema=schema))
    client.insert_rows_json(tmp_tbl, [to_json_serializable(r) for r in rows])

    # 2️⃣ merge
    cols = [f"`{f.name}`" for f in schema]
    updates = [f"T.{c} = S.{c}" for c in cols if c != "`pulse_id_mkqa2ewa`"]
    merge_sql = f"""
        MERGE `{BIGQUERY_TABLE_ID}` T
        USING `{tmp_tbl}` S
        ON T.pulse_id_mkqa2ewa = S.pulse_id_mkqa2ewa
        WHEN MATCHED THEN UPDATE SET {', '.join(updates)}
        WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join(f"S.{c}" for c in cols)})
    """

    job = client.query(merge_sql)
    job.result()

    stats    = getattr(job, "dml_statistics", None)
    inserted = stats.inserted_row_count if stats else 0
    updated  = stats.updated_row_count  if stats else 0
    print(f"MERGE completado: {inserted} insertadas, {updated} actualizadas.")

    client.delete_table(tmp_tbl, not_found_ok=True)
    return inserted, updated


# ───────────────── Logging ─────────────────
def insert_log_record(db, proc, created, updated, exec_time, status, primero, ultimo):
    try:
        client = bigquery.Client()
        client.query(
            f"""
            INSERT INTO `{LOGS_TABLE_ID}` (
              log_timestamp, database_name, process_name,
              records_created, records_updated, execution_time,
              status, primero, ultimo
            )
            VALUES (
              CURRENT_TIMESTAMP(), '{db}', '{proc}',
              {created}, {updated}, {exec_time},
              '{status}', '{primero}', '{ultimo}'
            )
        """
        ).result()
    except Exception as e:
        print(f"[insert_log_record] Error: {e}")


# ───────────────── Main ─────────────────
def main():
    print(f"Iniciando ingestión {START_DATE} → {END_DATE}")
    total_ins, total_upd = 0, 0
    t0 = time.time()

    current_date = START_DATE
    while current_date <= END_DATE:
        end_chunk = min(current_date + DELTA - datetime.timedelta(days=1), END_DATE)
        try:
            print(f"   ▸ Extrayendo items del {current_date} al {end_chunk}...")
            items = fetch_items_from_monday(current_date,end_chunk)
            rows  = transform_items_to_rows(items)
            ins, upd = upsert_rows_into_bq(rows)
            total_ins += ins
            total_upd += upd
        except Exception as e:
            print(f"[main] Error {current_date}-{end_chunk}: {e}")
            insert_log_record(
                "monday_properties_checkin_guide",
                "ingesta_diaria_monday_properties_checkin_guide",
                0,
                0,
                0,
                "ERROR",
                str(current_date),
                str(end_chunk),
            )
        current_date += DELTA

    insert_log_record(
        "monday_properties_checkin_guide",
        "ingesta_diaria_monday_properties_checkin_guide",
        total_ins + total_upd,
        total_upd,
        time.time() - t0,
        "SUCCESS",
        str(START_DATE),
        str(END_DATE),
    )
    print("Proceso completado.")


if __name__ == "__main__":
    main()
