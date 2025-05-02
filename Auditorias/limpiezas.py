import requests
import json
import datetime
import time
import uuid

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ============================================================
# Configuración y Constantes
# ============================================================
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"
BOARD_ID = 4460406422
COLUMN_ID_FECHA = "pulse_log"

# Tabla destino en BigQuery (donde se encuentran los datos)
BIGQUERY_TABLE_ID = "housekeeping.prod_monday_limpiezas"

# Tabla de auditoría (según el schema proporcionado)
AUDIT_TABLE_ID = "project_settings.auditoria"

# Tabla de logs
LOGS_TABLE_ID = "project_settings.logs"

# Fechas y ventana de consulta
# Para auditar “los datos de ayer” se podría ajustar START_DATE y END_DATE o el DELTA.
START_DATE = datetime.date.today() - datetime.timedelta(days=2)
END_DATE = datetime.date.today() - datetime.timedelta(days=2)
DELTA = datetime.timedelta(days=1)  

# Mapeo de columnas Monday -> Tipos de BigQuery
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
    "id_de_elemento_mkm572bd": "STRING"
}

# ============================================================
# Funciones de Parsing y Transformación
# ============================================================
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
    """Parsea un valor de Monday para que sea compatible con BigQuery (tipos simples)."""
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
                    return ", ".join(str(x) for x in parsed_dict["ids"])
                return json.dumps(parsed_dict, ensure_ascii=False)
            else:
                return str(inner_val)
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (int, float, bool)):
        return value
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
    """Dado el text y value de Monday, parsearlo al tipo correcto de BigQuery."""
    parsed_from_json_val = None
    if json_val:
        try:
            as_dict = json.loads(json_val)
            if isinstance(as_dict, dict):
                if "index" in as_dict:
                    parsed_from_json_val = None
                else:
                    parsed_from_json_val = parse_value_for_bq(as_dict)
        except:
            pass

    if not parsed_from_json_val:
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

def build_monday_query(start_str, end_str):
    """
    Construye la consulta GraphQL para filtrar items según la fecha (columna 'fecha_1')
    entre start_str y end_str (formato YYYY-MM-DD).
    """
    query = f"""
    query {{
      boards(ids: {BOARD_ID}) {{
        items_page(
          limit: 500
          query_params: {{
            rules: [{{
              column_id: "{COLUMN_ID_FECHA}",
              compare_value: ["{start_str}", "{end_str}"],
              operator: between,
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

def fetch_items_from_monday(start_date, end_date):
    """
    Consulta la API de Monday para obtener los items dentro del rango de fechas.
    """
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    query = build_monday_query(start_str, end_str)
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
    """
    Convierte los items de Monday a filas (dict) con los campos y tipos correctos según COLUMN_TYPE_MAP.
    """
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
    """
    Convierte objetos datetime/date/time a string ISO para insert_rows_json.
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

def ensure_dataset_exists(dataset_id):
    """
    Verifica si un dataset existe en BigQuery; de lo contrario, lo crea.
    dataset_id debe ser "project_id.dataset_name", por ejemplo: "myproject.temp_dataset".
    """
    client = bigquery.Client()
    dataset_ref = bigquery.Dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"El dataset '{dataset_id}' ya existe.")
    except NotFound:
        client.create_dataset(dataset_ref)
        print(f"Se ha creado el dataset '{dataset_id}' correctamente.")

# ============================================================
# Funciones de Auditoría y Logging
# ============================================================
def escape_sql(value):
    """
    Escapa los caracteres problemáticos en una cadena para incluirla en una sentencia SQL.
    Reemplaza saltos de línea por espacios y escapa las comillas simples.
    """
    if isinstance(value, str):
        return value.replace("\n", " ").replace("'", "''")
    return value

def insert_log_record(database_name, process_name, records_created, records_updated,
                      execution_time, status, primero, ultimo):
    """
    Inserta un registro en la tabla de logs.
    """
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
      '{escape_sql(database_name)}',
      '{escape_sql(process_name)}',
      {records_created},
      {records_updated},
      {execution_time},
      '{escape_sql(status)}',
      '{escape_sql(primero)}',
      '{escape_sql(ultimo)}'
    )
    """
    job = client.query(query)
    job.result()
    print("Registro de log insertado correctamente.")

def insert_audit_record(audit_id, fecha_auditoria, database_name, audit_type, start_date, end_date,
                        total_monday, total_bigquery, newest_record_id_monday, oldest_record_id_monday,
                        newest_record_id_bigquery, oldest_record_id_bigquery, difference, status):
    """
    Inserta un registro en la tabla de auditoría (schema proporcionado):
      - audit_date es de tipo DATE (se obtiene de fecha_auditoria.date()).
      - Los IDs se insertan como números (o NULL si no son válidos).
    """
    client = bigquery.Client()
    
    # Convertir los campos de ID a valores numéricos o NULL
    def numeric_or_null(val):
        return val if val and str(val).isdigit() else "NULL"
    
    newest_monday_val = numeric_or_null(newest_record_id_monday)
    oldest_monday_val = numeric_or_null(oldest_record_id_monday)
    newest_bigquery_val = numeric_or_null(newest_record_id_bigquery)
    oldest_bigquery_val = numeric_or_null(oldest_record_id_bigquery)
    
    query = f"""
    INSERT INTO `{AUDIT_TABLE_ID}` (
      audit_id,
      audit_date,
      database_name,
      audit_type,
      start_date,
      end_date,
      total_monday,
      total_bigquery,
      newest_record_id_monday,
      oldest_record_id_monday,
      newest_record_id_bigquery,
      oldest_record_id_bigquery,
      last_updated,
      difference,
      status
    )
    VALUES (
      {audit_id},
      DATE('{fecha_auditoria.date().isoformat()}'),
      '{escape_sql(database_name)}',
      '{escape_sql(audit_type)}',
      DATE('{start_date.strftime("%Y-%m-%d")}'),
      DATE('{end_date.strftime("%Y-%m-%d")}'),
      {total_monday},
      {total_bigquery},
      {newest_monday_val},
      {oldest_monday_val},
      {newest_bigquery_val},
      {oldest_bigquery_val},
      CURRENT_TIMESTAMP(),
      {difference},
      '{escape_sql(status)}'
    )
    """
    job = client.query(query)
    job.result()
    print("Registro de auditoría insertado correctamente.")

def audit_rows_in_bq(rows, start_date, end_date, monday_query):
    """
    Compara los registros obtenidos de Monday con los existentes en BigQuery usando el campo 'id'
    y registra en la tabla de auditoría:
      - La cantidad de registros reportados por la API de Monday y en BigQuery.
      - El registro (id) más antiguo y el más nuevo de ambos orígenes.
      - La diferencia entre ambas cantidades.
      - Un mensaje detallado en 'status'.
    Nota: El query de Monday (monday_query) se utiliza solo para auditoría interna y no se inserta.
    """
    client = bigquery.Client()

    if not rows:
        print("No hay filas para auditar en este lote.")
        audit_id = int(uuid.uuid4().int % 1000000000)
        fecha_auditoria = datetime.datetime.now()
        database_name = "limpiezas"
        audit_type = "2_days_ago"
        total_monday = 0
        total_bigquery = 0
        newest_record_id_monday = None
        oldest_record_id_monday = None
        newest_record_id_bigquery = None
        oldest_record_id_bigquery = None
        difference = 0
        status_msg = "No se encontraron registros en la consulta de Monday."
        insert_audit_record(audit_id, fecha_auditoria, database_name, audit_type, start_date, end_date,
                            total_monday, total_bigquery, newest_record_id_monday, oldest_record_id_monday,
                            newest_record_id_bigquery, oldest_record_id_bigquery, difference, status_msg)
        return (0, 0)

    # Extraer los IDs de los registros de Monday (se asume que el campo "id" ya fue parseado a número)
    monday_ids = [str(int(row["id"])) for row in rows if row.get("id") is not None]
    total_monday = len(monday_ids)

    # Determinar el registro más antiguo y el más nuevo según la API de Monday
    oldest_record_id_monday = min(monday_ids, key=lambda x: int(x)) if monday_ids else None
    newest_record_id_monday = max(monday_ids, key=lambda x: int(x)) if monday_ids else None

    # Consultar en BigQuery la cantidad de registros existentes para estos IDs
    ids_numeric = ", ".join(monday_ids)
    query_count = f"""
    SELECT COUNT(*) as count
    FROM `{BIGQUERY_TABLE_ID}`
    WHERE id IN ({ids_numeric})
    """
    query_job = client.query(query_count)
    results = query_job.result()
    row_result = next(results, None)
    total_bigquery = row_result.count if row_result else 0

    # Obtener el registro más antiguo en BigQuery
    query_oldest = f"""
    SELECT id
    FROM `{BIGQUERY_TABLE_ID}`
    WHERE id IN ({ids_numeric})
    ORDER BY CAST(id AS INT64) ASC
    LIMIT 1
    """
    query_job_oldest = client.query(query_oldest)
    results_oldest = list(query_job_oldest.result())
    oldest_record_id_bigquery = str(results_oldest[0].id) if results_oldest else None

    # Obtener el registro más nuevo en BigQuery
    query_newest = f"""
    SELECT id
    FROM `{BIGQUERY_TABLE_ID}`
    WHERE id IN ({ids_numeric})
    ORDER BY CAST(id AS INT64) DESC
    LIMIT 1
    """
    query_job_newest = client.query(query_newest)
    results_newest = list(query_job_newest.result())
    newest_record_id_bigquery = str(results_newest[0].id) if results_newest else None

    difference = abs(total_monday - total_bigquery)

    if total_monday != total_bigquery:
        status_msg = f"Discrepancia: API Monday reporta {total_monday} registros, pero BigQuery tiene {total_bigquery}."
    else:
        status_msg = "Auditoría exitosa: Todos los registros de Monday existen en BigQuery."

    audit_id = int(uuid.uuid4().int % 1000000000)
    fecha_auditoria = datetime.datetime.now()
    database_name = "limpiezas"
    audit_type = "2_days_ago"

    insert_audit_record(audit_id, fecha_auditoria, database_name, audit_type, start_date, end_date,
                        total_monday, total_bigquery, newest_record_id_monday, oldest_record_id_monday,
                        newest_record_id_bigquery, oldest_record_id_bigquery, difference, status_msg)

    print(f"Auditoría completada -> Monday: {total_monday}, BigQuery: {total_bigquery}")
    return (total_monday, total_bigquery)

# ============================================================
# Función Principal
# ============================================================
def main():
    print(f"Procesando datos desde {START_DATE} hasta {END_DATE}, en intervalos de {DELTA.days} días...")
    overall_monday = 0
    overall_bigquery = 0
    start_time_global = time.time()
    current_date = START_DATE

    while current_date <= END_DATE:
        end_chunk = current_date + DELTA - datetime.timedelta(days=1)
        if end_chunk > END_DATE:
            end_chunk = END_DATE

        print(f"\nObteniendo items del {current_date} al {end_chunk}...")
        monday_query = build_monday_query(current_date.strftime("%Y-%m-%d"), end_chunk.strftime("%Y-%m-%d"))
        items = fetch_items_from_monday(current_date, end_chunk)
        print(f"   Se encontraron {len(items)} items en ese rango.")
        rows = transform_items_to_rows(items)

        # Realizar auditoría en lugar de MERGE
        cantidad_monday, cantidad_bigquery = audit_rows_in_bq(rows, current_date, end_chunk, monday_query)
        overall_monday += cantidad_monday
        overall_bigquery += cantidad_bigquery

        current_date += DELTA

    end_time_global = time.time()
    total_time = end_time_global - start_time_global

    # Insertar log en la tabla de logs
    insert_log_record(
        database_name="limpiezas",
        process_name="auditoria_diaria_limpiezas",
        records_created=overall_monday,
        records_updated=overall_bigquery,
        execution_time=total_time,
        status="SUCCESS",
        primero=str(START_DATE),
        ultimo=str(END_DATE)
    )

    print("Proceso completado con éxito.")

if __name__ == "__main__":
    main()
