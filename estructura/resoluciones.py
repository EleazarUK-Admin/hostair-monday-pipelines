import requests
import json
import datetime
from google.cloud import bigquery

# -------------------------------------------------------------
# CONFIGURACIONES GLOBALES
# -------------------------------------------------------------
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL   = "https://api.monday.com/v2"

BOARD_ID           = 6908297780
COLUMN_ID_FECHA    = "registro_de_creaci_n_mkkdqxf2"  # Filtraremos por esta columna tipo TIMESTAMP
BIGQUERY_TABLE_ID  = "operations.resoluciones"

# Ajusta el rango de fechas según requieras
START_DATE = datetime.date(2024, 1, 1)
END_DATE   = datetime.date(2027, 1, 1)

# Incremento de 1 día para iterar
DELTA = datetime.timedelta(days=1)


# --------------------------------------------------------------------
# MAPEO DE COLUMNAS A TIPOS DE BIGQUERY
# (basado en CREATE TABLE operations.resoluciones)
# --------------------------------------------------------------------
COLUMN_TYPE_MAP = {
    "id": "NUMERIC",
    "name": "STRING",
    "subelementos__1": "STRING",
    "conectar_tableros__1": "STRING",
    "reflejo_1__1": "STRING",
    "date": "DATE",
    "cobrar___1": "NUMERIC",
    "texto_largo__1": "STRING",
    "reflejo0__1": "STRING",
    "label__1": "STRING",
    "fecha_1__1": "DATE",
    "enlace__1": "STRING",
    "status": "STRING",
    "reflejo__1": "STRING",
    "conectar_tableros2__1": "STRING",
    "texto7__1": "STRING",
    "conectar_tableros9__1": "STRING",
    "reflejo7__1": "STRING",
    "conectar_tableros25__1": "STRING",
    "n_meros7__1": "NUMERIC",
    "f_rmula__1": "STRING",
    "fecha2__1": "DATE",
    "dup__of_fecha_del_cobro__1": "DATE",
    "archivo3__1": "STRING",
    "texto9__1": "STRING",
    "n_meros5__1": "NUMERIC",
    "reflejo_17__1": "STRING",
    "registro_de_creaci_n_mkkdqxf2": "TIMESTAMP",
    "_ltima_actualizaci_n_mkkvfxaz": "TIMESTAMP",
    "archivo_mkm2973p": "STRING"
}

# --------------------------------------------------------------------
# FUNCIONES AUXILIARES DE PARSEO ROBUSTO
# --------------------------------------------------------------------
def try_parse_json_string(s):
    """
    Intenta parsear una cadena como JSON.
    Devuelve el diccionario resultante o None si falla.
    """
    if not isinstance(s, str):
        return None

    # 1) Intento parseo normal
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
    Intenta contemplar la mayoría de los tipos de columnas de Monday:
    - Text, Long Text, Status, Dropdown, People, Timeline, Date, 
      Tags, Link, Numbers, Checkbox, Phone, Email, World Clock, 
      Country, Week, Progress Tracking, Rating, Color Picker,
      Creation Log, Last Updated, Board Relationship, Subitems, 
      Dependency, Formula, Mirror, Time Tracking, Auto Number,
      Item ID, Vote, File, Connect Boards, Button, etc.

    NOTA: Dependiendo de la configuración del board, 
    puede que ciertos campos no lleguen exactamente como se listan aquí. 
    Ajusta según tus necesidades reales.
    """
    # Si es None => NULL
    if value is None:
        return None

    # --------------------------------------------------------------------------
    # CASO 1: el valor es un diccionario
    # --------------------------------------------------------------------------
    if isinstance(value, dict):
        # ----------------------------------------------------------------------
        # 1) Archivos (File) => "files": [...]
        #    Ejemplo:
        #    {
        #      "files": [
        #        { "name": "example.jpg", "assetId": 123456, "url": "...", ... }
        #      ]
        #    }
        # ----------------------------------------------------------------------
        if "files" in value and isinstance(value["files"], list):
            file_info_list = []
            for f in value["files"]:
                # Monday a veces manda "url", otras "public_url", otras "name", etc.
                fname = f.get("name", "")
                asset_id = f.get("assetId")
                f_url = f.get("url") or f.get("public_url") or f.get("preview_url")
                # Construimos un string con la info que sí exista
                if fname and f_url:
                    file_info_list.append(f"{fname} => {f_url}")
                elif fname and asset_id:
                    file_info_list.append(f"{fname} (assetId={asset_id})")
                elif fname:
                    file_info_list.append(fname)
                elif f_url:
                    file_info_list.append(f_url)
                else:
                    # Si no viene nada, devolvemos el dict completo
                    file_info_list.append(str(f))
            return ", ".join(file_info_list)
        if "duration" in value:
            return str(value["duration"])
        # ----------------------------------------------------------------------
        # 2) Status (en la nueva forma con "label": { "text": "...", ... })
        #    Ejemplo:
        #    {
        #      "label": {
        #        "index": 0,
        #        "text": "En Proceso",
        #        "style": {...},
        #        "is_done": false
        #      }
        #    }
        # ----------------------------------------------------------------------
        if "index" in value:
            return str(value["text"])
        
        if "label" in value and isinstance(value["label"], dict):
            # Tomar el "text" si existe
            status_label_text = value["label"].get("text")
            if status_label_text:
                return status_label_text
            # Si no hay "text", devolver el dict como string
            return json.dumps(value["label"], ensure_ascii=False)

        # ----------------------------------------------------------------------
        # 3) "text" directo en la raíz:
        #    Ejemplo: { "text": "valor X" }
        # ----------------------------------------------------------------------
        if "text" in value:
            return value["text"]

        # ----------------------------------------------------------------------
        # 4) Checkbox
        #    Ejemplo: { "checked": "true" } o { "checked": "false" }
        # ----------------------------------------------------------------------
        if "checked" in value:
            return value["checked"]

        # ----------------------------------------------------------------------
        # 5) Rating
        #    Ejemplo: { "rating": 4 }
        # ----------------------------------------------------------------------
        if "rating" in value and isinstance(value["rating"], (int, float)):
            return value["rating"]

        # ----------------------------------------------------------------------
        # 6) Hora y minuto => { "hour": 14, "minute": 30 }
        # ----------------------------------------------------------------------
        if "hour" in value and "minute" in value:
            h = value["hour"]
            m = value["minute"]
            return f"{h:02d}:{m:02d}:00"

        # ----------------------------------------------------------------------
        # 7) Fecha => { "date": "YYYY-MM-DD" } o { "dateTime": "YYYY-MM-DDTHH:mm:ssZ" }
        # ----------------------------------------------------------------------
        if "date" in value and value["date"] is not None:
            return value["date"]
        if "dateTime" in value and value["dateTime"] is not None:
            return value["dateTime"]

        # ----------------------------------------------------------------------
        # 8) Timeline => { "from": "2023-01-01", "to": "2023-01-31" }
        # ----------------------------------------------------------------------
        if "from" in value and "to" in value:
            return f"{value['from']} - {value['to']}"

        # ----------------------------------------------------------------------
        # 9) Personas/equipos => { "personsAndTeams": [ { "name": "...", ... }, ... ] }
        # ----------------------------------------------------------------------
        if "personsAndTeams" in value and isinstance(value["personsAndTeams"], list):
            ids = [str(person.get("id", "")) for person in value["personsAndTeams"]]
            return ", ".join(ids)

        # ----------------------------------------------------------------------
        # 10) Mirror (Board Relationship) => { "linkedPulseIds": [...] }
        # ----------------------------------------------------------------------
        if "linkedPulseIds" in value and isinstance(value["linkedPulseIds"], list):
            linked_ids = [str(item.get("linkedPulseId", "")) for item in value["linkedPulseIds"]]
            return ", ".join(linked_ids)

        if "from" in value and "to" in value:
            return f"{value['from']} - {value['to']}"
        # ----------------------------------------------------------------------
        # 11) Dropdown => { "labels": [ {"id":..., "name":"..."}, ... ] }
        # ----------------------------------------------------------------------
        if "labels" in value and isinstance(value["labels"], list):
            label_names = [l.get("name", "") for l in value["labels"]]
            return ", ".join(label_names)

        # ----------------------------------------------------------------------
        # 12) creation_log => { "created_at": "2023-01-25T08:00:00Z", "created_by": {...}, ... }
        # ----------------------------------------------------------------------
        if "created_at" in value:
            return value["created_at"]

        # ----------------------------------------------------------------------
        # 13) last_updated => { "updated_at": "2023-01-25T09:00:00Z", "updated_by": {...}, ... }
        # ----------------------------------------------------------------------
        if "updated_at" in value:
            return value["updated_at"]

        # ----------------------------------------------------------------------
        # 14) Button => { "button": "algún valor" } (puede variar)
        # ----------------------------------------------------------------------
        if "button" in value:
            return str(value["button"])

        # ----------------------------------------------------------------------
        # 15) Link => a veces como { "url": "http://...", "text": "nombre link" }
        # ----------------------------------------------------------------------
        if "url" in value:
            # Si hay "text", podríamos armar "text => url"
            link_url = value["url"]
            return link_url

        # ----------------------------------------------------------------------
        # 16) Phone => a veces { "phone": "...", "countryShortName": "...", ... }
        # ----------------------------------------------------------------------
        if "phone" in value:
            # devolvemos phone directo
            return value["phone"]

        # ----------------------------------------------------------------------
        # 17) Country => { "countryName": "Mexico", "code": "MX" }
        # ----------------------------------------------------------------------
        if "countryName" in value:
            return value["countryName"]

        # ----------------------------------------------------------------------
        # 18) Color Picker => { "color": "#FFFFFF" }
        # ----------------------------------------------------------------------
        if "color" in value and len(value) == 1:
            return value["color"]

        # ----------------------------------------------------------------------
        # 19) Progress Tracking => a veces { "changed_at": "...", "value": "X" }
        #     Monday a veces lo maneja internamente.
        # ----------------------------------------------------------------------
       
        # ----------------------------------------------------------------------
        # 20) Time Tracking => { "running": false, "additional_value": "12345", ... }
        #     Suele requerir un parseo más complejo. Si solo quieres un string, devuélvelo.
        # -----------
        #-----------------------------------------------------------
        if "running" in value:
            return str(value["duration"])
        
        if "ids" in value and isinstance(value["ids"], list):
            ids_str = [str(x) for x in value["ids"]]
            return ", ".join(ids_str)
        # ----------------------------------------------------------------------
        # 21) formula, subitems, dependency, mirror, etc. que guardan JSON en "value"
        #     p.ej.: { "value": "{\"text\":\"algo\",\"extra\":...}" }
        # ----------------------------------------------------------------------
        if "value" in value:
            val = value["value"]
            if val is None:
                return None
            if isinstance(val, (int, float, bool)):
                return val

            # Si es un string, intentamos parsear
            parsed_dict = try_parse_json_string(val)
            if parsed_dict:
                # Repetimos la priorización interna
                if "text" in parsed_dict:
                    return parsed_dict["text"]
                if "label" in parsed_dict:
                    # si label es dict con text
                    if isinstance(parsed_dict["label"], dict) and "text" in parsed_dict["label"]:
                        return parsed_dict["label"]["text"]
                    return str(parsed_dict["label"])
                if "checked" in parsed_dict:
                    return parsed_dict["checked"]
                if "files" in parsed_dict:
                    return json.dumps(parsed_dict, ensure_ascii=False)
                # etc.
                return json.dumps(parsed_dict, ensure_ascii=False)
            else:
                # Si no se pudo parsear como JSON, devuelvo la cadena pura
                return str(val)

        # ----------------------------------------------------------------------
        # Si no entró en ningún caso, devolvemos todo el dict como JSON
        # ----------------------------------------------------------------------
        return json.dumps(value, ensure_ascii=False)

    # --------------------------------------------------------------------------
    # CASO 2: Si es un número o booleano
    # --------------------------------------------------------------------------
    if isinstance(value, (int, float, bool)):
        return value

    # --------------------------------------------------------------------------
    # CASO 3: Si es un string
    # --------------------------------------------------------------------------
    if isinstance(value, str):
        # 1) Intentar parsear como JSON
        parsed_dict = try_parse_json_string(value)
        if parsed_dict:
            # Revisamos si al parsearlo como dict, encontramos algo interesante
            if "text" in parsed_dict:
                return parsed_dict["text"]
            if "label" in parsed_dict:
                if isinstance(parsed_dict["label"], dict) and "text" in parsed_dict["label"]:
                    return parsed_dict["label"]["text"]
                return str(parsed_dict["label"])
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

    # --------------------------------------------------------------------------
    # CASO 4: Si es algo más raro (listas, tuplas, etc.), lo convertimos a string
    # --------------------------------------------------------------------------
    return str(value)


def parse_monday_column_value(text_val, json_val, bq_type):
    """
    Usa parse_value_for_bq para extraer la 'información importante'
    y luego la convierte al tipo de dato correspondiente en BigQuery.
    """
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

    # Ajustes según el tipo BigQuery
    if bq_type == "STRING":
        return str(raw_val) if raw_val is not None else ""

    if raw_val is None:
        return None

    if bq_type == "BOOL":
        if isinstance(raw_val, bool):
            return raw_val
        val_str = str(raw_val).lower().strip()
        return val_str in ("true", "checked", "1", "sí", "yes", "verdadero")

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

    # Si no coincide con un tipo específico, devolver string
    return str(raw_val)

# --------------------------------------------------------------------
# LÓGICA PARA EXTRAER ITEMS DE MONDAY Y SUBIR A BIGQUERY
# --------------------------------------------------------------------
def build_monday_query(start_str, end_str):
    """
    Filtra por fecha EXACT 'start_str' en la columna COLUMN_ID_FECHA.
    La API con 'items_page' y 'query_params' no soporta directamente rangos,
    así que hacemos una consulta EXACT para cada día.
    """
    query = f"""
    query {{
      boards(ids: {BOARD_ID}) {{
        items_page(
          query_params: {{
            rules: [{{
              column_id: "{COLUMN_ID_FECHA}",
              compare_value: ["EXACT", "{start_str}"],
              operator: any_of,
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
    return query


def fetch_items_from_monday(date_to_fetch):
    """
    Consulta todos los items de Monday que tengan CREATED_AT == date_to_fetch (EXACT).
    """
    day_str = date_to_fetch.strftime("%Y-%m-%d")
    query   = build_monday_query(day_str, day_str)

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
    items_list      = items_page_data.get("items", [])
    
    return items_list


def transform_items_to_rows(items):
    """
    Transforma la lista de items de Monday (con column_values)
    en filas (diccionarios) que BigQuery pueda insertar.
    """
    rows = []
    for item in items:
        row = {}
        
        # id
        row["id"] = float(item["id"]) if item["id"] else None
        
        # name
        row["name"] = item.get("name", "") or ""

        # Recorremos las column_values
        for col_val in item.get("column_values", []):
            col_id   = col_val["id"]
            text_val = col_val["text"]
            json_val = col_val["value"]
            
            if col_id in COLUMN_TYPE_MAP:
                bq_type    = COLUMN_TYPE_MAP[col_id]
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
    Inserta (append) las filas en la tabla operations.resoluciones.
    """
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
    print(f"Iniciando proceso desde {START_DATE} hasta {END_DATE}, día a día...\n")
    current_date = START_DATE
    
    while current_date <= END_DATE:
        print(f"Consultando items del {current_date} (EXACT CREATED_AT)...")
        items = fetch_items_from_monday(current_date)
        
        print(f"   Se encontraron {len(items)} items para esa fecha.")
        
        rows = transform_items_to_rows(items)
        insert_rows_into_bq(rows)
        
        current_date += DELTA


if __name__ == "__main__":
    main()
