import requests
import json
import datetime
import time
import uuid

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ─────────────────── CONFIG ───────────────────
API_TOKEN = (
    "eyJhbGciOiJIUzI1NiJ9."
    "eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6"
    "d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ."
    "ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
)
API_URL = "https://api.monday.com/v2"

BOARD_ID        = 4045551102
COLUMN_ID_FECHA = "pulse_log_mkqar56e"

BIGQUERY_TABLE_ID = "properties.monday_properties_services"
LOGS_TABLE_ID     = "project_settings.logs"

START_DATE = datetime.date.today() - datetime.timedelta(days=1)
END_DATE   = datetime.date.today() - datetime.timedelta(days=1)
DELTA      = datetime.timedelta(days=1)

# ─────── Columnas → tipos BQ ───────
COLUMN_TYPE_MAP = {
    "texto": "STRING",
    "archivo5": "STRING",
    "texto04": "STRING",
    "texto85": "STRING",
    "texto7": "STRING",
    "archivo2": "STRING",
    "texto9": "STRING",
    "texto1": "STRING",
    "texto0": "STRING",
    "texto4": "STRING",
    "texto2": "STRING",
    "long_text1__1": "STRING",
    "texto12": "STRING",
    "texto8": "STRING",
    "texto5": "STRING",
    "texto18": "STRING",
    "texto27": "STRING",
    "archivo_1__1": "STRING",
    "texto_mkmy84yn": "STRING",
    "texto6": "STRING",
    "texto89": "STRING",
    "text4": "STRING",
    "texto817": "STRING",
    "archivo": "STRING",
    "fecha_de_pago___cfe__1": "STRING",
    "texto81": "STRING",
    "texto19": "STRING",
    "archivo6": "STRING",
    "fecha_de_pago___agua__1": "STRING",
    "texto__1": "STRING",
    "texto9__1": "STRING",
    "estado": "STRING",
    "board_relation3": "STRING",
    "reflejo__1": "STRING",
    "estado_1__1": "STRING",
    "pulse_log_mkqar56e": "TIMESTAMP",
    "pulse_updated_mkqa74hg": "TIMESTAMP",
}

# ───────────────────────── helpers ─────────────────────────
def try_parse_json_string(s):
    if not isinstance(s, str):
        return None
    try:
        parsed = json.loads(s)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    try:
        parsed = json.loads(s.replace("''", '"').replace("'", '"'))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
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
            if value.get("dateTime"):
                return value["dateTime"]
            if value.get("date"):
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
                inner = value["value"]
                if isinstance(inner, (int, float, bool)):
                    return inner
                parsed = try_parse_json_string(inner)
                return parse_value_for_bq(parsed) if parsed else str(inner)
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, (int, float, bool)):
            return value
        if isinstance(value, str):
            parsed = try_parse_json_string(value)
            if parsed:
                return parse_value_for_bq(parsed)
            try:
                return float(value) if "." in value else int(value)
            except Exception:
                return value
        return str(value)
    except Exception as e:
        print(f"[parse_value_for_bq] Error: {e}")
        return None


def parse_monday_column_value(text_val, json_val, bq_type):
    parsed_json_val = None
    if json_val:
        try:
            jd = json.loads(json_val)
            if isinstance(jd, dict):
                parsed_json_val = parse_value_for_bq(jd)
        except Exception:
            pass
    raw_val = parsed_json_val if parsed_json_val is not None else parse_value_for_bq(text_val)

    try:
        if bq_type == "STRING":
            return "" if raw_val is None else str(raw_val)
        if raw_val is None:
            return None
        if bq_type == "BOOL":
            return (
                raw_val
                if isinstance(raw_val, bool)
                else str(raw_val).lower().strip() in ("true", "checked", "1", "sí", "yes", "verdadero")
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


# ────────────── Monday.com ──────────────
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
                  compare_value: [            "2019-01-16",            "2026-04-29"          ],
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
    headers = {"Authorization": API_TOKEN, "Content-Type": "application/json"}
    data = requests.post(API_URL, json={"query": build_monday_query(a,b)}, headers=headers).json()
    if "errors" in data:
        raise Exception(data["errors"])
    boards = data.get("data", {}).get("boards", [])
    return boards[0].get("items_page", {}).get("items", []) if boards else []


# ───────── Transformación ─────────
def transform_items_to_rows(items):
    rows = []
    for item in items:
        row = {
            "pulse_id_mkqaxmpc": int(item["id"]) if item["id"] else None,
            "name": item.get("name") or "",
        }
        for cv in item.get("column_values", []):
            cid = cv["id"]
            if cid in COLUMN_TYPE_MAP:
                row[cid] = parse_monday_column_value(cv["text"], cv["value"], COLUMN_TYPE_MAP[cid])
        rows.append(row)
    return rows


def to_json(row):
    return {k: (v.isoformat() if isinstance(v, (datetime.datetime, datetime.date, datetime.time)) else v)
            for k, v in row.items()}


# ───────── BigQuery helpers ─────────
def ensure_dataset_exists(ds_id):
    c = bigquery.Client()
    try:
        c.get_dataset(ds_id)
    except NotFound:
        c.create_dataset(ds_id)


def upsert_rows_into_bq(rows):
    if not rows:
        print("No hay filas para insertar/actualizar.")
        return 0, 0

    client = bigquery.Client()
    tmp_ds  = f"{client.project}.temp_dataset"
    ensure_dataset_exists(tmp_ds)
    tmp_tbl = f"{tmp_ds}.tmp_{uuid.uuid4().hex}"

    schema = [
        bigquery.SchemaField("pulse_id_mkqaxmpc", "INT64"),
        bigquery.SchemaField("name", "STRING"),
    ] + [bigquery.SchemaField(cid, tp) for cid, tp in COLUMN_TYPE_MAP.items()]

    client.create_table(bigquery.Table(tmp_tbl, schema=schema))
    client.insert_rows_json(tmp_tbl, [to_json(r) for r in rows])

    cols    = [f"`{f.name}`" for f in schema]
    updates = [f"T.{c} = S.{c}" for c in cols if c != "`pulse_id_mkqaxmpc`"]

    merge_sql = f"""
        MERGE `{BIGQUERY_TABLE_ID}` T
        USING `{tmp_tbl}` S
        ON T.pulse_id_mkqaxmpc = S.pulse_id_mkqaxmpc
        WHEN MATCHED THEN
          UPDATE SET {', '.join(updates)}
        WHEN NOT MATCHED THEN
          INSERT ({', '.join(cols)}) VALUES ({', '.join(f"S.{c}" for c in cols)})
    """

    job = client.query(merge_sql)
    job.result()

    stats    = getattr(job, "dml_statistics", None)
    inserted = stats.inserted_row_count if stats else 0
    updated  = stats.updated_row_count  if stats else 0
    print(f"MERGE completado: {inserted} insertadas, {updated} actualizadas.")

    client.delete_table(tmp_tbl, not_found_ok=True)
    return inserted, updated


# ───────── Logging ─────────
def insert_log(db, proc, created, updated, secs, status, first, last):
    try:
        bigquery.Client().query(
            f"""
            INSERT INTO `{LOGS_TABLE_ID}` (
              log_timestamp, database_name, process_name,
              records_created, records_updated, execution_time,
              status, primero, ultimo
            )
            VALUES (
              CURRENT_TIMESTAMP(), '{db}', '{proc}',
              {created}, {updated}, {secs},
              '{status}', '{first}', '{last}'
            )
        """
        ).result()
    except Exception as e:
        print(f"[insert_log] Error: {e}")


# ───────── Main ─────────
def main():
    print(f"▶ Ingesta {START_DATE} → {END_DATE}")
    tot_ins, tot_upd = 0, 0
    tic = time.time()

    current_date = START_DATE
    while current_date <= END_DATE:
        end_chunk = min(current_date + DELTA - datetime.timedelta(days=1), END_DATE)
        try:
            print(f"   ▸ {current_date} – {end_chunk}")
            rows = transform_items_to_rows(fetch_items_from_monday(current_date,end_chunk))
            ins, upd = upsert_rows_into_bq(rows)
            tot_ins += ins
            tot_upd += upd
        except Exception as e:
            print(f"[main] Error {current_date}-{end_chunk}: {e}")
            insert_log(
                "monday_properties_services",
                "ingesta_diaria_monday_properties_services",
                0, 0, 0, "ERROR",
                str(current_date), str(end_chunk),
            )
        current_date += DELTA

    insert_log(
        "monday_properties_services",
        "ingesta_diaria_monday_properties_services",
        tot_ins + tot_upd,
        tot_upd,
        time.time() - tic,
        "SUCCESS",
        str(START_DATE),
        str(END_DATE),
    )
    print("✔ Proceso completado.")


if __name__ == "__main__":
    main()
