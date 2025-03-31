import requests
import json
import datetime
import time
import uuid

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"
BOARD_ID = 4460406422
COLUMN_ID_FECHA = "__last_updated__"

# Tabla final de destino (donde haremos MERGE).
BIGQUERY_TABLE_ID = "housekeeping.prod_monday_limpiezas"

# Tabla de logs
LOGS_TABLE_ID = "project_settings.logs"
hoy = datetime.date.today()
ayer = hoy - datetime.timedelta(days=4)

# Fechas y ventana de consulta
START_DATE = datetime.date(2019, 1, 1)
END_DATE = datetime.date(2027, 1, 1)
# Fechas y ventana de consulta
START_DATE = ayer
END_DATE = hoy
DELTA = datetime.timedelta(days=4)  # Procesar de día en día

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
    # Intentar con un replace sencillo de comillas
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
        # Manejo de varias estructuras internas (archivos, labels, etc.)
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
        # Por defecto, convertir a JSON string
        return json.dumps(value, ensure_ascii=False)

    # Manejo si value es un tipo primitivo
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        # Intentar parsear como JSON primero
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
        # Si no es JSON, intentar convertir a número
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
                # Si es un dict y no es sólo {"index":...}
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

    # Convertir el valor crudo a su tipo BQ deseado
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

    # Por defecto, retornar string
    return str(raw_val)

def build_monday_query(start_str, end_str):
    """
    Genera un query GraphQL para filtrar por la columna `COLUMN_ID_FECHA`,
    con compare_attribute='CREATED_AT' y operator='between'.
    """
    query = f"""
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
    return query

def fetch_items_from_monday(start_date, end_date):
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
    Convierte los items de Monday a filas (dict) con los campos
    y tipos correctos según COLUMN_TYPE_MAP.
    """
    rows = []
    for item in items:
        row = {}
        # Convertir id y name
        row["id"] = float(item["id"]) if item["id"] else None
        row["name"] = item.get("name", "") or ""

        # Columnas definidas en COLUMN_TYPE_MAP
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
        client.get_dataset(dataset_ref)  # Intenta obtener el dataset
        print(f"El dataset '{dataset_id}' ya existe.")
    except NotFound:
        # Si no existe, crearlo
        client.create_dataset(dataset_ref)
        print(f"Se ha creado el dataset '{dataset_id}' correctamente.")

def upsert_rows_into_bq(rows):
    """
    Realiza el upsert en BigQuery usando una tabla de staging temporal y MERGE.
    Devuelve (num_inserted, num_updated).
    """
    if not rows:
        print("No hay filas que upsertar en este lote.")
        return (0, 0)

    client = bigquery.Client()

    # Nombre del dataset temporal y verificación/creación
    dataset_id = f"{client.project}.temp_dataset"  # Puedes cambiar 'temp_dataset' si lo deseas
    ensure_dataset_exists(dataset_id)

    # 1) Crear tabla de staging temporal
    temp_table_id = f"{dataset_id}.tmp_{uuid.uuid4().hex}"

    # Construir esquema con base en COLUMN_TYPE_MAP + id, name
    schema = []
    # Agregamos "id" de primero
    schema.append(bigquery.SchemaField("id", "NUMERIC"))
    # "name"
    schema.append(bigquery.SchemaField("name", "STRING"))
    # Ahora el resto de columnas (excepto "id" y "name")
    for col_id, bq_type in COLUMN_TYPE_MAP.items():
        if col_id not in ("id", "name"):
            schema.append(bigquery.SchemaField(col_id, bq_type))

    table = bigquery.Table(temp_table_id, schema=schema)
    table = client.create_table(table)
    print(f"Tabla temporal creada: {temp_table_id}")

    # 2) Insertar las filas en la tabla de staging
    rows_serializable = [to_json_serializable(r) for r in rows]
    errors = client.insert_rows_json(temp_table_id, rows_serializable)
    if errors:
        print("Errores al insertar en staging:", errors)
        return (0, 0)

    print(f"Se insertaron {len(rows_serializable)} filas en la tabla temporal.")

    # 3) Generar la sentencia MERGE
    all_columns = [f"`{field.name}`" for field in schema]
    update_assignments = []
    for field in schema:
        if field.name != "id":
            update_assignments.append(f"T.`{field.name}` = S.`{field.name}`")

    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join([f"S.{c}" for c in all_columns])

    merge_sql = f"""
    MERGE `{BIGQUERY_TABLE_ID}` T
    USING `{temp_table_id}` S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET {", ".join(update_assignments)}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """

    merge_job = client.query(merge_sql)
    merge_job.result()  # Esperar a que termine

    # 4) Obtener métricas de DML (inserted/updated)
    dml_stats = merge_job.dml_stats
    if dml_stats:
        num_inserted = dml_stats.inserted_row_count
        num_updated = dml_stats.updated_row_count
    else:
        num_inserted = 0
        num_updated = 0

    print(f"MERGE completado -> Filas insertadas: {num_inserted}, Filas actualizadas: {num_updated}")

    # 5) Borrar la tabla temporal
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f"Tabla temporal eliminada: {temp_table_id}")

    return (num_inserted, num_updated)

def insert_log_record(database_name, process_name, records_created, records_updated,
                      execution_time, status, primero, ultimo):
    """
    Inserta un registro en la tabla project_settings.logs.
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
    job = client.query(query)
    job.result()
    print("Registro de log insertado correctamente.")

def main():
    print(f"Procesando datos desde {START_DATE} hasta {END_DATE}, en intervalos de {DELTA.days} días...")
    overall_inserted = 0
    overall_updated = 0

    start_time_global = time.time()

    current_date = START_DATE
    while current_date <= END_DATE:
        end_chunk = current_date + DELTA - datetime.timedelta(days=1)
        if end_chunk > END_DATE:
            end_chunk = END_DATE

        print(f"\nObteniendo items del {current_date} al {end_chunk}...")
        items = fetch_items_from_monday(current_date, end_chunk)
        print(f"   Se encontraron {len(items)} items en ese rango.")
        rows = transform_items_to_rows(items)

        # Hacer upsert en BQ
        num_inserted, num_updated = upsert_rows_into_bq(rows)
        overall_inserted += num_inserted
        overall_updated += num_updated

        current_date += DELTA

    end_time_global = time.time()
    total_time = end_time_global - start_time_global

    # Al final, insertar log en project_settings.logs
    # records_created = inserted + updated
    # records_updated = total updated
    insert_log_record(
        database_name="limpiezas",
        process_name="ingesta_diaria_limpiezas",      # Ajusta si quieres otro nombre
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
