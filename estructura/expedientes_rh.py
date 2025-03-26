import requests
import json
import datetime
from google.cloud import bigquery


# -------------------------------------------------------------
# CONFIGURACIONES GLOBALES
# -------------------------------------------------------------
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"

# Ajustado según tu requerimiento:
BOARD_ID = 3983935560          # ID del board
COLUMN_ID_FECHA = "date_1"     # Es la columna date de Monday que filtras
BIGQUERY_TABLE_ID = "operations.expedientes_rh"

# Rango de fechas para la consulta:
START_DATE = datetime.date(2019, 2, 10)
END_DATE   = datetime.date(2026, 1, 1)

# Incremento de 2 días
DELTA = datetime.timedelta(days=2)


# Mapeo de las columnas (id de Monday) -> tipo BigQuery, 
# en base a tu CREATE TABLE `operations.expedientes_rh`
COLUMN_TYPE_MAP = {
    "id": "INT64",
    "name": "STRING",
    "nickname1": "STRING",
    "monday_user_id6": "STRING",
    "board_relation__1": "STRING",
    "status_1": "STRING",
    "date_1": "DATE",
    "date_18": "DATE",
    "personas__1": "STRING",
    "phone": "STRING",
    "correo_electr_nico": "STRING",
    "texto9": "STRING",
    "label__1": "STRING",
    "dup__of_modalidad__1": "STRING",
    "archivo": "STRING",
    "archivo1": "STRING",
    "dup__of_csf__1": "STRING",
    "texto": "STRING",
    "texto3": "STRING",
    "files": "STRING",
    "numbers": "FLOAT64",
    "numeric__1": "FLOAT64",
    "texto_corto__1": "STRING",
    "texto_corto1__1": "STRING",
    "status": "STRING",
    "date_13": "DATE",
    "dup__of_contrato__1": "STRING",
    "fecha2__1": "DATE",
    "___puesto__1": "STRING",
}

# --------------------------------------------------------------------
# FUNCIONES AUXILIARES DE PARSEO (igual que en tu webhook original)
# --------------------------------------------------------------------
def try_parse_json_string(s):
    """
    Intenta convertir una cadena a JSON de forma robusta.
    """
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
    """
    Extrae un valor 'limpio' (string, int, float, bool, NULL)
    a partir de las estructuras típicas que envía Monday.
    """
    if value is None:
        return None

    # Si es diccionario
    if isinstance(value, dict):
        # Varios casos especiales
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
        
        # Time tracking:
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
                    lab = parsed_dict["label"]
                    if isinstance(lab, dict) and "text" in lab:
                        return lab["text"]
                    return str(lab)
                if "checked" in parsed_dict:
                    return parsed_dict["checked"]
                if "ids" in value and isinstance(value["ids"], list):
                    ids_str = [str(x) for x in value["ids"]]
                    return ", ".join(ids_str)
                return json.dumps(parsed_dict, ensure_ascii=False)
            else:
                return str(inner_val)

        # Si no coinciden los casos, devolvemos el dict JSON
        return json.dumps(value, ensure_ascii=False)

    # Si es numérico o bool
    if isinstance(value, (int, float, bool)):
        return value

    # Si es string, intentamos parsear JSON, luego numérico
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
            if "files" in parsed_dict:
                return json.dumps(parsed_dict, ensure_ascii=False)
            return json.dumps(parsed_dict, ensure_ascii=False)

        # Intentar convertir a número
        try:
            if "." in value:
                return float(value)
            else:
                return int(value)
        except (ValueError, TypeError):
            pass

        # Si no es ni JSON ni numérico, devolvemos tal cual
        return value

    # Lista/tupla/otro => str
    return str(value)


def parse_monday_column_value(text_val, json_val, bq_type):
    """
    1. Usa parse_value_for_bq para extraer la 'información importante'
    2. Dependiendo de bq_type, convierte el resultado a date/time/etc.
    """
    parsed_from_json_val = None
    
    # Intentamos primero con json_val
    if json_val:
        try:
            as_dict = json.loads(json_val)
            if isinstance(as_dict, dict):
                if "index" in as_dict:
                    pass
                else:
                    parsed_from_json_val = parse_value_for_bq(as_dict)
        except:
            pass

    # Si no sacamos nada útil, usamos text_val
    if not parsed_from_json_val:
        parsed_from_text = parse_value_for_bq(text_val)
    else:
        parsed_from_text = None
   
    # Decidimos cuál usar
    raw_val = parsed_from_json_val if parsed_from_json_val else parsed_from_text

    # Convertimos según bq_type
    if bq_type == "STRING":
        if raw_val is None:
            return ""
        return str(raw_val)

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

    # Si no coincidió, devolvemos string
    return str(raw_val)

# --------------------------------------------------------------------
# LÓGICA PARA EXTRAER ITEMS DE MONDAY Y SUBIR A BIGQUERY
# --------------------------------------------------------------------
def build_monday_query(start_str, end_str):
    """
    Construye la Query GraphQL para filtrar items
    según la columna date_1 (entre start_str y end_str).
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
    """
    Llama a la API de Monday para traer items 
    donde la columna date_1 esté entre start_date y end_date.
    """
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
    items_page_data = boards_data[0].get("items_page", {})
    items_list = items_page_data.get("items", [])
    
    return items_list


def transform_items_to_rows(items):
    """
    Transforma la lista de items en filas que BigQuery pueda insertar.
    """
    rows = []
    
    for item in items:
        row = {}
        
        # id en INT64
        row["id"] = int(item["id"]) if item["id"] else None
        
        # name (STRING)
        row["name"] = item.get("name", "") or ""

        # Recorremos las column_values
        for col_val in item.get("column_values", []):
            col_id  = col_val["id"]      # p.ej. "date_1"
            text_val = col_val["text"]   # p.ej. "2023-01-05"
            json_val = col_val["value"]  # p.ej. '{"changed_at":"2025-02-01T20:06:49Z"}'
            
            if col_id in COLUMN_TYPE_MAP:
                bq_type = COLUMN_TYPE_MAP[col_id]
                
                # Aquí forzamos que si es dup__of_modalidad__1 o status, 
                # usemos el text (ignorando json_val).
                if col_id in ["dup__of_modalidad__1", "status"]:
                    parsed_val = parse_monday_column_value(text_val, None, bq_type)
                else:
                    parsed_val = parse_monday_column_value(text_val, json_val, bq_type)
                
                row[col_id] = parsed_val
        
        rows.append(row)
    
    return rows


def to_json_serializable(row):
    """
    Convierte date/time/datetime a string ISO-8601 
    para que insert_rows_json no falle.
    """
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
    """
    Inserta (append) las filas en la tabla operations.expedientes_rh.
    """
    if not rows:
        print("No hay filas que insertar en este lote.")
        return
    
    # Convertir cada row a algo serializable
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
