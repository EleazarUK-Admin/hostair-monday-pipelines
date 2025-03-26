import requests
import json
import datetime
from google.cloud import bigquery

# -------------------------------------------------------------
# CONFIGURACIONES GLOBALES
# -------------------------------------------------------------
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"

# Ajustado a tu nuevo board y tabla
BOARD_ID = 7072610932           # ID del board
COLUMN_ID_FECHA = "date"        # Columna DATE en tu board
BIGQUERY_TABLE_ID = "finance.agente_soporte_campo"

# Fechas de inicio y fin
START_DATE = datetime.date(2020, 1, 1)
END_DATE   = datetime.date(2026, 1, 1)

# Incremento de 2 días
DELTA = datetime.timedelta(days=2)

# Mapeo de columnas de Monday -> tipo en BigQuery,
# según tu CREATE TABLE `finance.agente_soporte_campo`
COLUMN_TYPE_MAP = {
    "id": "INT64",
    "name": "STRING",
    "subelementos__1": "STRING",
    "date": "DATE",
    "n_meros9__1": "FLOAT64",
    "horas_extras5__1": "FLOAT64",
    "label__1": "STRING",
    "horas_extras__1": "STRING",
    "archivo__1": "STRING",
    "personas__1": "STRING",
    "f_rmula__1": "STRING",
    "estado__1": "STRING",
    "texto1__1": "STRING",
    "f_rmula0__1": "STRING",
    "conectar_tableros__1": "STRING"
}

# --------------------------------------------------------------------
# FUNCIONES AUXILIARES PARA PARSEAR VALORES
# --------------------------------------------------------------------
def try_parse_json_string(s):
    """Intenta convertir una cadena a JSON de forma robusta."""
    if not isinstance(s, str):
        return None
    try:
        return json.loads(s)
    except (ValueError, TypeError):
        pass

    # Segundo intento: reemplazando comillas simples
    s_fixed = s.replace("''", '"').replace("'", '"')
    try:
        return json.loads(s_fixed)
    except (ValueError, TypeError):
        return None


def parse_value_for_bq(value):
    """
    Extrae un valor 'limpio' (string, int, float, bool, etc.)
    a partir de las estructuras típicas que envía Monday.
    """
    if value is None:
        return None

    if isinstance(value, dict):
        # Casos comunes en JSON de Monday
        if "files" in value and isinstance(value["files"], list):
            file_info = []
            for f in value["files"]:
                fname = f.get("name", "")
                asset_id = f.get("assetId")
                f_url = f.get("url") or f.get("public_url") or f.get("preview_url")
                if fname and f_url:
                    file_info.append(f"{fname} => {f_url}")
                elif fname and asset_id:
                    file_info.append(f"{fname} (assetId={asset_id})")
                elif fname:
                    file_info.append(fname)
                elif f_url:
                    file_info.append(f_url)
                else:
                    file_info.append(str(f))
            return ", ".join(file_info)

        if "label" in value and isinstance(value["label"], dict):
            label_txt = value["label"].get("text")
            if label_txt:
                return label_txt
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
            ids = [str(p.get("id", "")) for p in value["personsAndTeams"]]
            return ", ".join(ids)

        if "linkedPulseIds" in value and isinstance(value["linkedPulseIds"], list):
            linked = [str(x.get("linkedPulseId", "")) for x in value["linkedPulseIds"]]
            return ", ".join(linked)

        if "labels" in value and isinstance(value["labels"], list):
            labels = [l.get("name", "") for l in value["labels"]]
            return ", ".join(labels)

        if "button" in value:
            return str(value["button"])

        if "phone" in value:
            return value["phone"]

        if "url" in value:
            return value["url"]

        if "item_id" in value:
            return str(value["item_id"])

        if "running" in value and "duration" in value:
            return str(value["duration"])

        if "ids" in value and isinstance(value["ids"], list):
            return ", ".join(str(x) for x in value["ids"])

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
                    return ", ".join(str(x) for x in parsed_dict["ids"])
                return json.dumps(parsed_dict, ensure_ascii=False)
            else:
                return str(inner_val)

        return json.dumps(value, ensure_ascii=False)

    # Si es número o bool
    if isinstance(value, (int, float, bool)):
        return value

    # Si es string, tratar de parsear JSON o número
    if isinstance(value, str):
        parsed = try_parse_json_string(value)
        if isinstance(parsed, dict):
            if "text" in parsed:
                return parsed["text"]
            if "label" in parsed:
                lb = parsed["label"]
                if isinstance(lb, dict) and "text" in lb:
                    return lb["text"]
                return str(lb)
            if "checked" in parsed:
                return parsed["checked"]
            return json.dumps(parsed, ensure_ascii=False)

        # Intentar convertir a número
        try:
            if "." in value:
                return float(value)
            else:
                return int(value)
        except (ValueError, TypeError):
            pass

        # Si no es JSON ni numérico, se devuelve tal cual
        return value

    # Si es lista / otro => string
    return str(value)


def parse_monday_column_value(text_val, json_val, bq_type):
    """
    Usa parse_value_for_bq para extraer la 'información importante'
    y luego convertir al tipo de BQ.
    """
    parsed_from_json = None
    if json_val:
        try:
            as_dict = json.loads(json_val)
            if isinstance(as_dict, dict):
                if "index" in as_dict:
                    pass
                else:
                    parsed_from_json = parse_value_for_bq(as_dict)
        except:
            pass

    if not parsed_from_json:
        parsed_from_text = parse_value_for_bq(text_val)
    else:
        parsed_from_text = None

    raw_val = parsed_from_json if parsed_from_json else parsed_from_text

    # Conversiones según tipo
    if bq_type == "STRING":
        return "" if raw_val is None else str(raw_val)

    if raw_val is None:
        return None

    if bq_type == "BOOL":
        if isinstance(raw_val, bool):
            return raw_val
        val_str = str(raw_val).lower().strip()
        return val_str in ("true", "checked", "1", "sí", "yes")

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
        val_str = str(raw_val)
        try:
            return datetime.datetime.strptime(val_str, "%Y-%m-%d").date()
        except:
            return None

    if bq_type == "TIME":
        val_str = str(raw_val)
        for fmt in ["%H:%M:%S", "%H:%M"]:
            try:
                return datetime.datetime.strptime(val_str, fmt).time()
            except:
                pass
        return None

    if bq_type == "TIMESTAMP":
        val_str = str(raw_val).replace("Z", "+00:00")
        try:
            return datetime.datetime.fromisoformat(val_str)
        except:
            return None

    return str(raw_val)

# --------------------------------------------------------------------
# CONSULTA A MONDAY Y CARGA A BIGQUERY
# --------------------------------------------------------------------
def build_monday_query(start_str, end_str):
    """
    Construye la query GraphQL para filtrar items por la columna "date".
    """
    query = f"""
    query {{
      boards(ids: {BOARD_ID}) {{
        items_page(
          query_params: {{
            rules: [{{
              column_id: "{COLUMN_ID_FECHA}",
              compare_value: ["{start_str}", "{end_str}"],
              operator: between
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


def fetch_items_from_monday(start_date, end_date):
    """Consulta la API de Monday para items cuya fecha esté entre start_date y end_date."""
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    query = build_monday_query(start_str, end_str)
    headers = {
        "Authorization": API_TOKEN,
        "Content-Type": "application/json"
    }
    response = requests.post(API_URL, json={"query": query}, headers=headers)
    
    data = response.json()
    if "errors" in data:
        raise Exception(f"Error al consultar items: {data['errors']}")
    
    boards_data = data.get("data", {}).get("boards", [])
    if not boards_data:
        return []
    items_page = boards_data[0].get("items_page", {})
    items_list = items_page.get("items", [])
    return items_list


def transform_items_to_rows(items):
    """
    Transforma la lista de items en filas (diccionarios) listos para BigQuery.
    """
    rows = []
    
    for item in items:
        row = {}
        # id
        row["id"] = int(item["id"]) if item["id"] else None
        # name
        row["name"] = item.get("name", "") or ""

        # Recorremos column_values
        for col_val in item.get("column_values", []):
            col_id   = col_val["id"]
            text_val = col_val["text"]
            json_val = col_val["value"]

            if col_id in COLUMN_TYPE_MAP:
                bq_type = COLUMN_TYPE_MAP[col_id]
                
                # Si la columna es "label__1", forzamos a tomar el text:
                if col_id == "label__1":
                    parsed_val = parse_monday_column_value(text_val, None, bq_type)
                else:
                    parsed_val = parse_monday_column_value(text_val, json_val, bq_type)

                row[col_id] = parsed_val

        rows.append(row)

    return rows


def to_json_serializable(row):
    """
    Convierte date/time/datetime a string ISO-8601 para insert_rows_json.
    """
    new_row = {}
    for k, v in row.items():
        if isinstance(v, datetime.datetime):
            new_row[k] = v.isoformat()
        elif isinstance(v, datetime.date):
            new_row[k] = v.isoformat()
        elif isinstance(v, datetime.time):
            new_row[k] = v.isoformat()
        else:
            new_row[k] = v
    return new_row


def insert_rows_into_bq(rows):
    """Inserta las filas en la tabla finance.agente_soporte_campo."""
    if not rows:
        print("No hay filas para insertar en este lote.")
        return

    rows_serializable = [to_json_serializable(r) for r in rows]
    
    client = bigquery.Client()
    errors = client.insert_rows_json(BIGQUERY_TABLE_ID, rows_serializable)
    if errors:
        print("Errores al insertar en BigQuery:", errors)
    else:
        print(f"Se han insertado {len(rows_serializable)} filas en {BIGQUERY_TABLE_ID}.")


def main():
    print(f"Procesando datos desde {START_DATE} hasta {END_DATE}, en intervalos de 2 días...")
    current_date = START_DATE

    while current_date <= END_DATE:
        end_chunk = current_date + DELTA - datetime.timedelta(days=1)
        if end_chunk > END_DATE:
            end_chunk = END_DATE

        print(f"\nObteniendo items del {current_date} al {end_chunk}...")
        items = fetch_items_from_monday(current_date, end_chunk)
        print(f"   Se encontraron {len(items)} items en ese rango.")

        rows = transform_items_to_rows(items)
        insert_rows_into_bq(rows)

        current_date += DELTA


if __name__ == "__main__":
    main()
