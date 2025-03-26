import requests
import json
import datetime
from google.cloud import bigquery


# -------------------------------------------------------------
# CONFIGURACIONES GLOBALES
# -------------------------------------------------------------
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"

BOARD_ID = 4460406422  # ID del board "Limpiezas"
COLUMN_ID_FECHA = "fecha_1"      # Es la columna date de Monday que filtras
BIGQUERY_TABLE_ID = "housekeeping.prod_monday_limpiezas"

# Fecha de inicio y fin
START_DATE = datetime.date(2022, 12, 30)
END_DATE   = datetime.date(2027, 1, 1)  # Ajusta según necesites

# Incremento de 2 días
DELTA = datetime.timedelta(days=1)

# Mapeo de las columnas (id de Monday) -> tipo BigQuery (basado en tu CREATE TABLE)
COLUMN_TYPE_MAP = {
    "id": "NUMERIC",
    "name": "STRING",
    "subelementos": "STRING",
    "fecha_1": "DATE",
    "person": "STRING",
    "estado_14": "STRING",
    "bot_n6": "STRING",
    "reflejo197": "STRING",
    "color60": "STRING",
    "estado5": "STRING",
    "archivo61": "STRING",
    "texto03": "STRING",
    "enlace_1__1": "STRING",
    "file8": "STRING",
    "archivo6": "STRING",
    "verificar78": "BOOL",
    "verificar7": "BOOL",
    "boolean": "BOOL",
    "boolean5": "BOOL",
    "boolean0": "BOOL",
    "boolean3": "BOOL",
    "boolean33": "BOOL",
    "boolean2": "BOOL",
    "boolean1": "BOOL",
    "boolean4": "BOOL",
    "boolean9": "BOOL",
    "boolean6": "BOOL",
    "boolean8": "BOOL",
    "boolean59": "BOOL",
    "boolean94": "BOOL",
    "boolean7": "BOOL",
    "boolean34": "BOOL",
    "boolean20": "BOOL",
    "boolean30": "BOOL",
    "boolean79": "BOOL",
    "boolean95": "BOOL",
    "boolean85": "BOOL",
    "boolean65": "BOOL",
    "boolean29": "BOOL",
    "archivo__1": "STRING",
    "conectar_tableros1": "STRING",
    "n_meros9": "NUMERIC",
    "dup__of_noches": "NUMERIC",
    "board_relation": "STRING",
    "reflejo1": "STRING",
    "conectar_tableros7": "STRING",
    "f_rmula5": "STRING",
    "n_meros1": "NUMERIC",
    "dup__of_recibo2": "STRING",
    "label": "STRING",
    "n_meros0": "NUMERIC",
    "texto_largo7": "STRING",
    "archivo74": "STRING",
    "dup__of___reembolso": "NUMERIC",
    "dup__of_extras9": "STRING",
    "f_rmula0": "STRING",
    "board_relation9": "STRING",
    "reflejo45": "STRING",
    "reflejo97": "STRING",
    "reflejo839": "STRING",
    "reflejo26": "STRING",
    "reflejo48": "STRING",
    "reflejo855": "STRING",
    "f_rmula1": "STRING",
    "n_meros4": "NUMERIC",
    "dup__of_checkout_id9": "STRING",
    "text__1": "STRING",
    "texto02": "STRING",
    "estado_17": "STRING",
    "conectar_tableros__1": "STRING",
    "reflejo_18__1": "STRING",
    "lookup__1": "STRING",
    "lookup8__1": "STRING",
    "lookup6__1": "STRING",
    "estado_15": "STRING",
    "estado_10": "STRING",
    "bot_n7": "STRING",
    "estado_1993": "STRING",
    "fecha_12": "DATE",
    "pulse_log": "TIMESTAMP",
    "pulse_updated": "TIMESTAMP",
    "texto6": "STRING",
    "bot_n2": "STRING",
    "personas": "STRING",
    "link_to_agenda_vero": "STRING",
    "board_relation26": "STRING",
    "hora": "TIME",
    "hour": "TIME",
    "f_rmula9": "STRING",
    "men__desplegable5": "STRING",
    "enlace__1": "STRING",
    "board_relation__1": "STRING",
    "reflejo__1": "STRING",
    "n_meros__1": "NUMERIC",
    "label_1__1": "STRING",
    "id_de_elemento_mkm572bd": "STRING",
}

# --------------------------------------------------------------------
# FUNCIONES AUXILIARES DE PARSEO ROBUSTO (similares a tu webhook)
# --------------------------------------------------------------------
def try_parse_json_string(s):
    """
    Intenta convertir una cadena a JSON de forma robusta.
    """
    if not isinstance(s, str):
        return None
    
    # 1) Intento normal
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            return parsed
        return None
    except (ValueError, TypeError):
        pass

    # 2) Reemplazar comillas simples por dobles
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
    a partir de las estructuras típicas que envía Monday en la columna 'value'.
    
    Adaptado de tu ejemplo para el webhook.
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
            # status con "label": {"text": "..."}
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
        
        # "running" / time tracking
        if "running" in value and "duration" in value:
            return str(value["duration"])
         # [AGREGADO] Si existe "ids": [3, 4], devolvemos "3,4"
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

        # Si no coincidió, devolvemos todo el dict como JSON
        return json.dumps(value, ensure_ascii=False)

    # Si es numérico o bool
    if isinstance(value, (int, float, bool)):
        return value

    # Si es string
    if isinstance(value, str):
        # 1) Intentar parsear como JSON
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

        # 2) Intentar parsear como numérico
        try:
            if "." in value:
                return float(value)
            else:
                return int(value)
        except (ValueError, TypeError):
            pass

        # 3) Si no es numérico ni JSON, devolvemos la cadena tal cual
        return value

    # Si es lista/tupla u otro => str
    return str(value)

# --------------------------------------------------------------------
# FUNCIÓN PRINCIPAL PARA PARSEAR: 
#   Usa parse_value_for_bq para "limpiar" y luego 
#   aplica la lógica de conversión de tipo 
#   (DATE, TIME, TIMESTAMP, BOOL, etc.) 
# --------------------------------------------------------------------
def parse_monday_column_value(text_val, json_val, bq_type):
    """
    1. Usa parse_value_for_bq para extraer la 'información importante'
       del valor JSON de Monday.
    2. Dependiendo de bq_type, convierte el resultado final a date/time/timestamp/bool/float/int...
    """
    # parse_value_for_bq asume que le pasas "algo" (dict, str, etc.)
    # Monday te da 'text_val' (string) y 'json_val' (posible JSON)
    # A veces 'json_val' ya es un string JSON, a veces es None, etc.

    # Revisamos primero si json_val es parseable:
    parsed_from_json_val = None
    


    if json_val:
        as_dict = json.loads(json_val)

        if "index" in as_dict:
            parsed_from_text = parse_value_for_bq(as_dict)
        else:
        # Intenta cargarlo como JSON (Monday usualmente envía un JSON en string)
            try:
                if isinstance(as_dict, dict):
                    # parseamos el dict con parse_value_for_bq
                    parsed_from_json_val = parse_value_for_bq(as_dict)
            except:
                pass

    # Si no sacamos nada de json_val, intentamos con text_val
    if parsed_from_json_val is None or parsed_from_json_val == "":
        parsed_from_text = parse_value_for_bq(text_val)
    else:
        parsed_from_text = None

    # Decidimos cuál usar
    raw_val = parsed_from_json_val if parsed_from_json_val else parsed_from_text

    # Ahora, si bq_type es "STRING", "DATE", etc., convertimos
    if bq_type == "STRING":
        if raw_val is None:
            return ""
        return str(raw_val)

    if raw_val is None:
        return None

    # Para BOOL
    if bq_type == "BOOL":
        # parse_value_for_bq podría devolver "true"/"false"/"checked", etc.
        # hacemos parse manual:
        if isinstance(raw_val, bool):
            return raw_val
        val_str = str(raw_val).lower().strip()
        return val_str in ("true", "checked", "1", "sí", "yes","false")

    # Para NUMERIC
    if bq_type == "NUMERIC":
        # parse_value_for_bq pudo devolver float, int, str
        # convertimos a float
        try:
            return float(raw_val)
        except:
            return None

    # Para DATE (YYYY-MM-DD)
    if bq_type == "DATE":
        # raw_val podría ser "2023-02-07"
        val_str = str(raw_val)
        try:
            return datetime.datetime.strptime(val_str, "%Y-%m-%d").date()
        except:
            return None

    # Para TIME (HH:MM:SS)
    if bq_type == "TIME":
        val_str = str(raw_val)
        fmts = ["%H:%M:%S", "%H:%M"]
        for f in fmts:
            try:
                return datetime.datetime.strptime(val_str, f).time()
            except ValueError:
                pass
        return None

    # Para TIMESTAMP
    if bq_type == "TIMESTAMP":
        # parse_value_for_bq a veces devuelve "2023-02-07T08:00:00Z"
        # o "2023-02-07T08:00:00+00:00"
        val_str = str(raw_val)
        # quitamos la Z si está
        val_str = val_str.replace("Z", "+00:00")
        try:
            return datetime.datetime.fromisoformat(val_str)
        except:
            return None

    # Si no matcheó nada, lo devolvemos como string
    return str(raw_val)

# --------------------------------------------------------------------
# LÓGICA PARA EXTRAER ITEMS DE MONDAY Y SUBIR A BIGQUERY
# --------------------------------------------------------------------
def build_monday_query(start_str, end_str):
    """
    Construye la Query GraphQL para Monday usando la operación
    items_page con una regla 'between' en la columna 'fecha_1'.
    """
    query = f"""
    query {{
      boards(ids: {BOARD_ID}) {{
        items_page(
          limit: 300
          query_params: {{
            rules: [{{
              column_id: "{COLUMN_ID_FECHA}",
              operator: between,
              compare_value: ["{start_str}", "{end_str}"]
              
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
    Llama a la API de Monday para traer items cuya columna fecha_1
    esté entre start_date y end_date (inclusive).
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
    Transforma la lista de items de Monday (con column_values)
    en filas que BigQuery pueda insertar (diccionarios).
    """
    rows = []
    
    for item in items:
        row = {}
        
        # id (numérico en BQ):
        row["id"] = float(item["id"]) if item["id"] else None
        
        # name (STRING)
        row["name"] = item.get("name", "") or ""

        # Recorremos las column_values
        for col_val in item.get("column_values", []):
            col_id = col_val["id"]         # p. ej. "fecha_1"
            text_val = col_val["text"]     # p. ej. "2023-01-05"
            json_val = col_val["value"]    # p. ej. '{"changed_at":"2025-02-01T20:06:49Z"}'
            
            # Si col_id está en el schema:
            if col_id in COLUMN_TYPE_MAP:
                bq_type = COLUMN_TYPE_MAP[col_id]
                # Usamos parse_monday_column_value (que internamente se basa en parse_value_for_bq).
                parsed_val = parse_monday_column_value(text_val, json_val, bq_type)
                row[col_id] = parsed_val
        
        rows.append(row)
    
    return rows


def to_json_serializable(row):
    """
    Convierte date/time/datetime a string ISO-8601
    para que insert_rows_json no falle al serializar.
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
    Inserta (append) las filas en la tabla housekeeping.stage_monday_limpiezas.
    Usa insert_rows_json de la librería google-cloud-bigquery.
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




