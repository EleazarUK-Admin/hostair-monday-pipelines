import requests
import json
import datetime
import time
import uuid

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ───────── Config ─────────
API_TOKEN = (
    "eyJhbGciOiJIUzI1NiJ9."
    "eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6"
    "d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ."
    "ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
)
API_URL = "https://api.monday.com/v2"

BOARD_ID        = 4045465610
COLUMN_ID_FECHA = "pulse_log"

BIGQUERY_TABLE_ID = "properties.monday_properties_general_information"
LOGS_TABLE_ID     = "project_settings.logs"

START_DATE = datetime.date.today() - datetime.timedelta(days=1)

END_DATE   = datetime.date.today() - datetime.timedelta(days=1)
DELTA      = datetime.timedelta(days=1)

# ───────── Columnas → tipos BQ ─────────
COLUMN_TYPE_MAP = {
    "name": "STRING",
    "subelementos__1": "STRING",
    "texto16": "STRING",
    "men__desplegable": "STRING",
    "men__desplegable6": "STRING",
    "estado": "STRING",
    "dup__of_piso": "STRING",
    "texto47": "STRING",
    "archivo3": "STRING",
    "reflejo": "STRING",
    "texto10": "STRING",
    "estado38": "STRING",
    "texto5": "STRING",
    "texto259": "STRING",
    "texto74": "STRING",
    "texto69": "STRING",
    "text_mkq731zq": "STRING",
    "men__desplegable1": "STRING",
    "texto0": "STRING",
    "archivo": "STRING",
    "n_meros": "FLOAT64",
    "dup__of_rec_mara_1": "STRING",
    "estado_13": "STRING",
    "camas__r1_": "FLOAT64",
    "texto28": "STRING",
    "estado1": "STRING",
    "color": "STRING",
    "camas__r2_": "FLOAT64",
    "texto24": "STRING",
    "estado9": "STRING",
    "color4": "STRING",
    "camas__r3_": "FLOAT64",
    "texto7": "STRING",
    "estado11": "STRING",
    "color0": "STRING",
    "numeric": "FLOAT64",
    "numeric5": "FLOAT64",
    "texto09": "STRING",
    "texto44": "STRING",
    "estado3": "STRING",
    "texto3": "STRING",
    "texto60": "STRING",
    "texto_largo": "STRING",
    "archivo6": "STRING",
    "texto_largo3": "STRING",
    "long_text": "STRING",
    "archivo35": "STRING",
    "color5": "STRING",
    "estado20": "STRING",
    "texto_largo31": "STRING",
    "archivo7": "STRING",
    "estado_1": "STRING",
    "texto36": "STRING",
    "estado25": "STRING",
    "texto_largo58": "STRING",
    "texto_largo__1": "STRING",
    "color46": "STRING",
    "reflejo0": "STRING",
    "reflejo6": "STRING",
    "estado2": "STRING",
    "board_relation4": "STRING",
    "f_rmula": "STRING",
    "formula": "STRING",
    "pulse_log": "TIMESTAMP",
    "conectar_tableros7": "STRING",
    "reflejo_mkkcctfd": "STRING",
    "reflejo00": "STRING",
    "board_relation": "STRING",
    "reflejo65": "STRING",
    "reflejo__1": "STRING",
    "reflejo7__1": "STRING",
    "estado_1__1": "STRING",
    "personas_mkkay0ry": "STRING",
    "conectar_tableros_mkkc9qch": "STRING",
    "reflejo_mkkccnt8": "STRING",
    "board_relation_mkkc7vq0": "STRING",
    "reflejo_mkkcemtk": "STRING",
    "id_de_elemento_mkmr9rw9": "INT64",
    "pulse_updated_mkqa1962": "TIMESTAMP",
}

# ───────────────────────────────────────── utilidades ─────────────────────────────────────────
def try_parse_json_string(s):
    if not isinstance(s, str):
        return None
    try:
        return json.loads(s) if isinstance(json.loads(s), dict) else None
    except (ValueError, TypeError):
        pass
    try:
        return json.loads(s.replace("''", '"').replace("'", '"'))
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
            except (ValueError, TypeError):
                return value
        return str(value)
    except Exception as e:
        print(f"[parse_value_for_bq] Error: {e}")
        return None


def parse_monday_column_value(text_val, json_val, bq_type):
    try:
        parsed_json = None
        if json_val:
            try:
                tmp = json.loads(json_val)
                if isinstance(tmp, dict):
                    parsed_json = parse_value_for_bq(tmp)
            except Exception:
                pass
        raw_val = parsed_json if parsed_json is not None else parse_value_for_bq(text_val)
        if bq_type == "STRING":
            return "" if raw_val is None else str(raw_val)
        if raw_val is None:
            return None
        if bq_type == "BOOL":
            return str(raw_val).lower().strip() in ("true", "checked", "1", "sí", "yes", "verdadero") \
                   if not isinstance(raw_val, bool) else raw_val
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


# ───────── Monday.com ─────────
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
        headers = {"Authorization": API_TOKEN, "Content-Type": "application/json"}
        data = requests.post(API_URL, json={"query": build_monday_query(a,b)}, headers=headers).json()
        if "errors" in data:
            raise Exception(data["errors"])
        boards = data.get("data", {}).get("boards", [])
        return boards[0].get("items_page", {}).get("items", []) if boards else []
    except Exception as e:
        print(f"[fetch_items_from_monday] Error: {e}")
        raise


# ───────── Transformación ─────────
def transform_items_to_rows(items):
    rows = []
    for item in items:
        row = {
            "id_de_elemento_mkmr9rw9": int(item["id"]) if item["id"] else None,
            "name": item.get("name", "") or "",
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
    client = bigquery.Client()
    try:
        client.get_dataset(ds_id)
    except NotFound:
        client.create_dataset(ds_id)


def upsert_rows_into_bq(rows):
    if not rows:
        print("No hay filas para insertar/actualizar.")
        return 0, 0

    client  = bigquery.Client()
    tmp_ds  = f"{client.project}.temp_dataset"
    ensure_dataset_exists(tmp_ds)
    tmp_tbl = f"{tmp_ds}.tmp_{uuid.uuid4().hex}"

    schema = [
        bigquery.SchemaField("id_de_elemento_mkmr9rw9", "INT64"),
        bigquery.SchemaField("name", "STRING"),
    ] + [
        bigquery.SchemaField(cid, t)
        for cid, t in COLUMN_TYPE_MAP.items() if cid not in ("id_de_elemento_mkmr9rw9", "name")
    ]

    client.create_table(bigquery.Table(tmp_tbl, schema=schema))
    client.insert_rows_json(tmp_tbl, [to_json(r) for r in rows])

    cols    = [f"`{f.name}`" for f in schema]
    updates = [f"T.{c} = S.{c}" for c in cols if c != "`id_de_elemento_mkmr9rw9`"]

    merge_sql = f"""
        MERGE `{BIGQUERY_TABLE_ID}` T
        USING `{tmp_tbl}` S
        ON T.id_de_elemento_mkmr9rw9 = S.id_de_elemento_mkmr9rw9
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
    total_ins, total_upd = 0, 0
    tic = time.time()

    current_date = START_DATE
    while current_date <= END_DATE:
        end_chunk = min(current_date + DELTA - datetime.timedelta(days=1), END_DATE)
        try:
            print(f"   ▸ {current_date} – {end_chunk}")
            rows = transform_items_to_rows(fetch_items_from_monday(current_date,end_chunk))
            ins, upd = upsert_rows_into_bq(rows)
            total_ins += ins
            total_upd += upd
        except Exception as e:
            print(f"[main] Error {current_date}-{end_chunk}: {e}")
            insert_log(
                "monday_properties_general_information",
                "ingesta_diaria_monday_properties_general_information",
                0, 0, 0, "ERROR",
                str(current_date), str(end_chunk),
            )
        current_date += DELTA

    insert_log(
        "monday_properties_general_information",
        "ingesta_diaria_monday_properties_general_information",
        total_ins + total_upd,
        total_upd,
        time.time() - tic,
        "SUCCESS",
        str(START_DATE),
        str(END_DATE),
    )
    print("✔ Proceso completado.")


if __name__ == "__main__":
    main()
