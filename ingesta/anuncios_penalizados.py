import requests
import json
import datetime
from google.cloud import bigquery

API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"
BOARD_ID = 7019548313
COLUMN_ID_FECHA = "__last_updated__"
BIGQUERY_TABLE_ID = "operations.anuncios_penalizados"
START_DATE = datetime.date(2024, 7, 11)
END_DATE = datetime.date(2027, 1, 1)
DELTA = datetime.timedelta(days=1)
COLUMN_TYPE_MAP = {
    "id": "NUMERIC",
    "name": "STRING",
    "subelementos_mkkqhsz7": "STRING",
    "date": "DATE",
    "status": "STRING",
    "estado_1__1": "STRING",
    "dup__of_fecha_del_aviso__1": "DATE",
    "texto_largo__1": "STRING",
    "dup__of_evaluaciones8__1": "STRING",
    "bot_n__1": "STRING",
    "long_text__1": "STRING",
    "dup__of_apelaci_n__1": "STRING",
    "archivo__1": "STRING",
    "conectar_tableros__1": "STRING",
    "n_meros__1": "NUMERIC",
    "link_to____acciones_mkkq4aj1": "STRING",
    "workdoc_de_monday_mkm016kp": "STRING"
}

def try_parse_json_string(s):
    if not isinstance(s, str):
        return None
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            return parsed
        return None
    except (ValueError, TypeError):
        pass
    s_fixed = s.replace("''", '"').replace("'", '"')
    try:
        parsed = json.loads(s_fixed)
        if isinstance(parsed, dict):
            return parsed
        return None
    except (ValueError, TypeError):
        return None

def parse_value_for_bq(value):
    if value is None:
        return None
    if isinstance(value, dict):
        if "files" in value and isinstance(value["files"], list):
            file_info_list = []
            for f in value["files"]:
                fname = f.get("name", "")
                asset_id = f.get("assetId")
                f_url = f.get("url") or f.get("public_url") or f.get("preview_url")
                if fname and f_url:
                    file_info_list.append(f"{fname} => {f_url}")
                elif fname and asset_id:
                    file_info_list.append(f"{fname} (assetId={asset_id})")
                elif fname:
                    file_info_list.append(fname)
                elif f_url:
                    file_info_list.append(f_url)
                else:
                    file_info_list.append(str(f))
            return ", ".join(file_info_list)
        if "label" in value and isinstance(value["label"], dict):
            status_label_text = value["label"].get("text")
            if status_label_text:
                return status_label_text
            return json.dumps(value["label"], ensure_ascii=False)
        if "text" in value:
            return value["text"]
        if "checked" in value:
            return value["checked"]
        if "rating" in value and isinstance(value["rating"], (int, float)):
            return value["rating"]
        if "hour" in value and "minute" in value:
            h = value["hour"]
            m = value["minute"]
            return f"{h:02d}:{m:02d}:00"
        if "date" in value and value["date"]:
            return value["date"]
        if "dateTime" in value and value["dateTime"]:
            return value["dateTime"]
        if "from" in value and "to" in value:
            return f"{value['from']} - {value['to']}"
        if "personsAndTeams" in value and isinstance(value["personsAndTeams"], list):
            ids = [str(person.get("id", "")) for person in value["personsAndTeams"]]
            return ", ".join(ids)
        if "linkedPulseIds" in value and isinstance(value["linkedPulseIds"], list):
            linked_ids = [str(x.get("linkedPulseId", "")) for x in value["linkedPulseIds"]]
            return ", ".join(linked_ids)
        if "labels" in value and isinstance(value["labels"], list):
            label_names = [l.get("name", "") for l in value["labels"]]
            return ", ".join(label_names)
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
        if "ids" in value and isinstance(value["ids"], list):
            ids_str = [str(x) for x in value["ids"]]
            return ", ".join(ids_str)
        if "value" in value:
            inner_val = value["value"]
            if isinstance(inner_val, (int, float, bool)):
                return inner_val
            parsed_dict = try_parse_json_string(inner_val)
            if parsed_dict:
                if "text" in parsed_dict:
                    return parsed_dict["text"]
                if "label" in parsed_dict:
                    lb = parsed_dict["label"]
                    if isinstance(lb, dict) and "text" in lb:
                        return lb["text"]
                    return str(lb)
                if "checked" in parsed_dict:
                    return parsed_dict["checked"]
                if "ids" in parsed_dict and isinstance(parsed_dict["ids"], list):
                    ids_str = [str(x) for x in parsed_dict["ids"]]
                    return ", ".join(ids_str)
                return json.dumps(parsed_dict, ensure_ascii=False)
            else:
                return str(inner_val)
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        parsed_dict = try_parse_json_string(value)
        if parsed_dict:
            if "text" in parsed_dict:
                return parsed_dict["text"]
            if "label" in parsed_dict:
                lb = parsed_dict["label"]
                if isinstance(lb, dict) and "text" in lb:
                    return lb["text"]
                return str(lb)
            if "checked" in parsed_dict:
                return parsed_dict["checked"]
            return json.dumps(parsed_dict, ensure_ascii=False)
        try:
            if "." in value:
                return float(value)
            else:
                return int(value)
        except (ValueError, TypeError):
            pass
        return value
    return str(value)

def parse_monday_column_value(text_val, json_val, bq_type):
    parsed_from_json_val = None
    if json_val:
        try:
            as_dict = json.loads(json_val)
            if isinstance(as_dict, dict):
                parsed_from_json_val = parse_value_for_bq(as_dict)
        except:
            pass
    if parsed_from_json_val is None or parsed_from_json_val == "":
        parsed_from_text = parse_value_for_bq(text_val)
    else:
        parsed_from_text = None
    raw_val = parsed_from_json_val if parsed_from_json_val else parsed_from_text
    if bq_type == "STRING":
        return "" if raw_val is None else str(raw_val)
    if raw_val is None:
        return None
    if bq_type == "BOOL":
        if isinstance(raw_val, bool):
            return raw_val
        val_str = str(raw_val).lower().strip()
        return val_str in ("true", "checked", "1", "sí", "yes", "false")
    if bq_type == "NUMERIC":
        try:
            return float(raw_val)
        except:
            return None
    if bq_type == "DATE":
        val_str = str(raw_val)
        try:
            return datetime.datetime.strptime(val_str, "%Y-%m-%d").date()
        except:
            return None
    if bq_type == "TIME":
        val_str = str(raw_val)
        fmts = ["%H:%M:%S", "%H:%M"]
        for f in fmts:
            try:
                return datetime.datetime.strptime(val_str, f).time()
            except ValueError:
                pass
        return None
    if bq_type == "TIMESTAMP":
        val_str = str(raw_val).replace("Z", "+00:00")
        try:
            return datetime.datetime.fromisoformat(val_str)
        except:
            return None
    return str(raw_val)

def build_monday_query(date_str):
    query = f"""
    query {{
      boards(ids: {BOARD_ID}) {{
        items_page(
          query_params: {{
            rules: [{{
              column_id: "{COLUMN_ID_FECHA}",
              compare_value: ["EXACT", "{date_str}"],
              operator: any_of,
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
    return query

def fetch_items_from_monday(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    query = build_monday_query(date_str)
    headers = {"Authorization": API_TOKEN, "Content-Type": "application/json"}
    response = requests.post(API_URL, json={"query": query}, headers=headers)
    data = response.json()
    if "errors" in data:
        raise Exception(f"Error al consultar items: {data['errors']}")
    boards_data = data.get("data", {}).get("boards", [])
    if not boards_data:
        return []
    items_page_data = boards_data[0].get("items_page", {})
    items_list = items_page_data.get("items", [])
    return items_list

def transform_items_to_rows(items):
    rows = []
    for item in items:
        row = {}
        row["id"] = float(item["id"]) if item["id"] else None
        row["name"] = item.get("name", "") or ""
        for col_val in item.get("column_values", []):
            col_id = col_val["id"]
            text_val = col_val["text"]
            json_val = col_val["value"]
            if col_id in COLUMN_TYPE_MAP:
                bq_type = COLUMN_TYPE_MAP[col_id]
                parsed_val = parse_monday_column_value(text_val, json_val, bq_type)
                row[col_id] = parsed_val
        rows.append(row)
    return rows

def to_json_serializable(row):
    new_row = {}
    for key, value in row.items():
        if isinstance(value, datetime.datetime):
            new_row[key] = value.isoformat()
        elif isinstance(value, datetime.date):
            new_row[key] = value.isoformat()
        elif isinstance(value, datetime.time):
            new_row[key] = value.isoformat()
        else:
            new_row[key] = value
    return new_row

def insert_rows_into_bq(rows):
    if not rows:
        print("No hay filas que insertar en este lote.")
        return
    rows_serializable = [to_json_serializable(r) for r in rows]
    client = bigquery.Client()
    errors = client.insert_rows_json(BIGQUERY_TABLE_ID, rows_serializable)
    if errors:
        print("Errores al insertar en BigQuery:", errors)
    else:
        print(f"Se han insertado {len(rows_serializable)} filas en {BIGQUERY_TABLE_ID}.")

def main():
    current_date = START_DATE
    while current_date <= END_DATE:
        print(f"\nObteniendo items del {current_date}...")
        items = fetch_items_from_monday(current_date)
        print(f"   Se encontraron {len(items)} items en ese rango.")
        rows = transform_items_to_rows(items)
        insert_rows_into_bq(rows)
        current_date += DELTA

if __name__ == "__main__":
    main()
