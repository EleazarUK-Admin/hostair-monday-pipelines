import requests
import json
import datetime
import time
import uuid
import random

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest

# --- Parámetros y constantes ---
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"

# Nuevo board y columna fecha
BOARD_ID = 5914627798
COLUMN_ID_FECHA = "creaci_n_de_registro"

# Tabla final de destino para MERGE (upsert)
BIGQUERY_TABLE_ID = "support.soporte_operativo"

# Tabla de logs
LOGS_TABLE_ID = "project_settings.logs"
hoy = datetime.date.today()
ayer = hoy - datetime.timedelta(days=2)

# Fechas y ventana de consulta
START_DATE = datetime.date(2019, 1, 1)
END_DATE = datetime.date(2027, 1, 1)
# Fechas y ventana de consulta
START_DATE = datetime.date(2019, 1, 1)
END_DATE = datetime.date(2027, 1, 1)
DELTA = datetime.timedelta(days=3)  # Procesar de día en día


# Mapeo de columnas Monday -> Tipos de BigQuesry
# (ajustado a lo que definiste en tu CREATE TABLE)
COLUMN_TYPE_MAP = {
    "id": "INT 64",
    "name": "STRING",
    "conectar_tableros1__1": "STRING",
    "personas__1": "STRING",
    "texto3": "STRING",
    "archivo": "STRING",
    "dup__of_estado": "STRING",
    "status": "STRING",
    "estado6__1": "STRING",
    "personas": "STRING",
    "dup__of___asignado__1": "STRING",
    "dup__of___link__1": "STRING",
    "conectar_tableros4": "STRING",
    "personas_16": "STRING",
    "creaci_n_de_registro": "TIMESTAMP",
    "bot_n8__1": "STRING",
    "fecha1__1": "DATE",
    "dup__of_prioridad__1": "STRING",
    "dup__of___clasificaci_n__1": "STRING",
    "reflejo4__1": "STRING",
    "dup__of_payout8__1": "STRING",
    "dup__of_payout__1": "STRING",
    "reflejo_1": "STRING",
    "subelementos": "STRING",
    "texto62": "STRING",
    "archivo4": "STRING",
    "clasificaci_n_1": "NUMERIC",
    "duration": "STRING",
    "dup__of____total__1": "STRING",
    "dup__of____soluci_n__1": "STRING",
    "duration__1": "STRING",
    "dup__of____en_espera__1": "STRING",
    "_ltima_actualizaci_n": "TIMESTAMP",
    "date": "DATE",
    "texto__1": "STRING",
    "f_rmula__1": "STRING",
    "conectar_tableros__1": "STRING",
    "reflejo__1": "STRING",
    "dup__of_aah_en_turno__1": "STRING",
    "estado__1": "STRING",
    "fecha__1": "DATE",
    "f_rmula1__1": "STRING",
    "formula__1": "STRING",
    "formula7__1": "STRING",
    "formula6__1": "STRING",
    "formula2__1": "STRING",
    "f_rmula4__1": "STRING",
    "texto1__1": "STRING",
    "n_meros__1": "NUMERIC",
    "dup__of____tool_connect8__1": "NUMERIC",
    "dup__of____tool_connect6__1": "STRING",
    "link_to_resoluciones__1": "STRING",
    "personas2__1": "STRING",
    "reflejo6__1": "STRING",
    "fecha12__1": "DATE",
    "hora__1": "TIME",
    "hour__1": "TIME",
    "hour2__1": "TIME",
    "date__1": "DATE",
    "date1__1": "DATE",
    "date3__1": "DATE",
    "id__de_elemento__1": "INT64",
    "conectar_tableros3__1": "STRING",
    "board_relation__1": "STRING",
    "board_relation9__1": "STRING",
    "board_relation_mkmty0nb": "STRING"
}


# --------------------------------------------------
#                 Funciones de parseo
# --------------------------------------------------
def try_parse_json_string(s):
    """Intenta parsear un string como JSON y retorna un dict si es posible."""
    if not isinstance(s, str):
        return None
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            return parsed
        return None
    except (ValueError, TypeError):
        pass
    # Reemplazar comillas simples
    s_fixed = s.replace("''", '"').replace("'", '"')
    try:
        parsed = json.loads(s_fixed)
        if isinstance(parsed, dict):
            return parsed
        return None
    except (ValueError, TypeError):
        return None


def parse_value_for_bq(value):
    """Recorre la estructura JSON devuelta por Monday y extrae un valor primitivo (str, num, bool, etc.)."""
    if value is None:
        return None

    if isinstance(value, dict):
        # Manejos especiales (archivos, labels, times, etc.)
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
            ids = [str(p.get("id", "")) for p in value["personsAndTeams"]]
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

        # A veces "value" es un JSON incrustado
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

        # Por defecto, retornar su representación JSON
        return json.dumps(value, ensure_ascii=False)

    # Si es un primitivo (int, float, bool, str)
    if isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, str):
        # Intentar parsear como JSON
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
    """Convierte (text, value) de Monday a un tipo BigQuery (STRING, DATE, TIMESTAMP, etc.)."""
    parsed_from_json_val = None
    if json_val:
        try:
            as_dict = json.loads(json_val)
            if isinstance(as_dict, dict) and "index" not in as_dict:
                parsed_from_json_val = parse_value_for_bq(as_dict)
        except:
            pass

    if parsed_from_json_val:
        raw_val = parsed_from_json_val
    else:
        raw_val = parse_value_for_bq(text_val)

    if bq_type == "STRING":
        return "" if raw_val is None else str(raw_val)

    if raw_val is None:
        return None

    if bq_type == "BOOL":
        # Ejemplo (no tienes bool en tu esquema)
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
        for fmt in ["%H:%M:%S", "%H:%M"]:
            try:
                return datetime.datetime.strptime(val_str, fmt).time()
            except ValueError:
                pass
        return None

    if bq_type == "TIMESTAMP":
        val_str = str(raw_val).replace("Z", "+00:00")
        try:
            return datetime.datetime.fromisoformat(val_str)
        except:
            return None

    # Por defecto, lo convertimos a string
    return str(raw_val)


# --------------------------------------------------
#       Construcción del query (CREATED_AT)
# --------------------------------------------------
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
              operator: between,
              compare_value: ["{start_str}", "{end_str}"],
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
    Llama a la API de Monday para traer items creados entre start_date y end_date
    (CREATED_AT), usando la columna COLUMN_ID_FECHA con 'between'.
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
    return items_page_data.get("items", [])


def transform_items_to_rows(items):
    """Convierte la lista de items de Monday a filas aptas para BigQuery, según COLUMN_TYPE_MAP."""
    rows = []
    for item in items:
        row = {}
        # Convertir ID a float/num si la tenemos
        if item["id"]:
            try:
                row["id"] = int(item["id"])
            except:
                row["id"] = None
        else:
            row["id"] = None

        # Nombre
        row["name"] = item.get("name", "") or ""

        # Columnas personalizadas
        for col_val in item.get("column_values", []):
            col_id = col_val["id"]
            text_val = col_val["text"]
            json_val = col_val["value"]
            if col_id in COLUMN_TYPE_MAP:
                bq_type = COLUMN_TYPE_MAP[col_id]
                row[col_id] = parse_monday_column_value(text_val, json_val, bq_type)

        rows.append(row)
    return rows


def to_json_serializable(row):
    """Convierte date/datetime/time a string ISO antes de insert_rows_json."""
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


# --------------------------------------------------
#  Crear dataset temporal y MERGE (upsert)
# --------------------------------------------------
def ensure_dataset_exists(dataset_id):
    """Verifica si el dataset existe en BigQuery; de lo contrario, lo crea."""
    client = bigquery.Client()
    dataset_ref = bigquery.Dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"El dataset '{dataset_id}' ya existe.")
    except NotFound:
        client.create_dataset(dataset_ref)
        print(f"Se ha creado el dataset '{dataset_id}' correctamente.")


def upsert_rows_into_bq(rows):
    """
    Realiza un MERGE (upsert) en la tabla final usando una tabla
    temporal en dataset "temp_dataset". Retorna (num_inserted, num_updated).
    """
    if not rows:
        print("No hay filas que upsertar en este lote.")
        return (0, 0)

    client = bigquery.Client()
    dataset_id = f"{client.project}.temp_dataset"
    ensure_dataset_exists(dataset_id)

    temp_table_id = f"{dataset_id}.tmp_{uuid.uuid4().hex}"

    # Construir el esquema según COLUMN_TYPE_MAP + id y name
    schema = []
    # id
    schema.append(bigquery.SchemaField("id", "INT64"))
    # name
    schema.append(bigquery.SchemaField("name", "STRING"))

    for col_id, bq_type in COLUMN_TYPE_MAP.items():
        if col_id not in ("id", "name"):
            schema.append(bigquery.SchemaField(col_id, bq_type))

    table = bigquery.Table(temp_table_id, schema=schema)
    table = client.create_table(table)
    print(f"Tabla temporal creada: {temp_table_id}")

    # Insertar filas en la tabla temporal
    rows_serializable = [to_json_serializable(r) for r in rows]
    errors = client.insert_rows_json(temp_table_id, rows_serializable)
    if errors:
        print("Errores al insertar en staging:", errors)
        return (0, 0)
    print(f"Se insertaron {len(rows_serializable)} filas en la tabla temporal.")

    # Construir SQL MERGE
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

    # Ejecutar el MERGE con reintentos en caso de concurrencia
    max_retries = 3
    num_inserted = 0
    num_updated = 0
    for attempt in range(max_retries):
        try:
            merge_job = client.query(merge_sql)
            merge_job.result()  # Esperar a que termine

            dml_stats = merge_job.dml_stats
            if dml_stats:
                num_inserted = dml_stats.inserted_row_count
                num_updated = dml_stats.updated_row_count
            print(f"MERGE completado -> Filas insertadas: {num_inserted}, Filas actualizadas: {num_updated}")
            break

        except BadRequest as e:
            if "Could not serialize access to table" in str(e):
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt + random.random()
                    print(f"Conflicto de concurrencia. Reintentando en {wait_time:.2f}s (intento {attempt+1}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    print("Se alcanzó el máximo de reintentos. Abortando MERGE.")
                    raise
            else:
                raise

    # Eliminar la tabla temporal
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f"Tabla temporal eliminada: {temp_table_id}")

    return (num_inserted, num_updated)


# --------------------------------------------------
#   Insertar LOG al terminar la ingesta
# --------------------------------------------------
def insert_log_record(database_name, process_name, records_created, records_updated,
                      execution_time, status, primero, ultimo):
    """
    Inserta un registro en la tabla project_settings.logs con la información de ejecución.
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
    print("Registro de log insertado correctamente en project_settings.logs.")


# --------------------------------------------------
#                 Función principal
# --------------------------------------------------
def main():
    print(f"Iniciando proceso desde {START_DATE} hasta {END_DATE} (incremento de {DELTA.days} día/s)\n")
    overall_inserted = 0
    overall_updated = 0

    start_time_global = time.time()
    current_date = START_DATE

    # Recorrer en intervalos de 2 días
    while current_date <= END_DATE:
        end_chunk = current_date
        if end_chunk > END_DATE:
            end_chunk = END_DATE

        print(f"Consultando items creados entre {current_date} y {end_chunk} (CREATED_AT)...")
        items = fetch_items_from_monday(current_date, end_chunk)
        print(f"   Se encontraron {len(items)} items.")

        rows = transform_items_to_rows(items)
        num_inserted, num_updated = upsert_rows_into_bq(rows)
        overall_inserted += num_inserted
        overall_updated += num_updated

        current_date += DELTA

    total_time = time.time() - start_time_global

    # Insertar log en la tabla de logs
    insert_log_record(
        database_name="support",
        process_name="ingesta_historica_soporte_operativo",      # Ajusta si quieres otro nombre
        records_created=overall_inserted + overall_updated,
        records_updated=overall_updated,
        execution_time=total_time,
        status="SUCCESS",
        primero=str(START_DATE),
        ultimo=str(END_DATE)
    )

    print("\nProceso completado con éxito.")
    print(f"Filas totales insertadas: {overall_inserted}, actualizadas: {overall_updated}.\n")


if __name__ == "__main__":
    main()
