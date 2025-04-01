import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from google.cloud.logging_v2.handlers.transports import BackgroundThreadTransport
import logging

# Configuración de Cloud Logging
client_logging = google.cloud.logging.Client()
handler = CloudLoggingHandler(client_logging, transport=BackgroundThreadTransport)
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(handler)

import requests
import json
import datetime
import time
import uuid
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

#################################
# CONFIGURACIÓN Y CONSTANTES
#################################

API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"

# El ID del board al que quieres conectarte.
BOARD_ID = 7019548313

# Columna para filtrar por fecha en Monday.
COLUMN_ID_FECHA = "__last_updated__"

# Tabla final (donde se hará el MERGE).
BIGQUERY_TABLE_ID = "operations.anuncios_penalizados"

# Tabla de logs (asegúrate de que exista con las mismas columnas definidas).
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

#################################
# FUNCIONES AUXILIARES
#################################

def try_parse_json_string(s):
    """Intenta parsear una cadena como JSON y devuelve dict o None."""
    try:
        if not isinstance(s, str):
            return None
        try:
            parsed = json.loads(s)
            if isinstance(parsed, dict):
                return parsed
            return None
        except (ValueError, TypeError):
            pass
        # Intentar un reemplazo rápido de comillas
        s_fixed = s.replace("''", '"').replace("'", '"')
        try:
            parsed = json.loads(s_fixed)
            if isinstance(parsed, dict):
                return parsed
            return None
        except (ValueError, TypeError):
            return None
    except Exception as e:
        logging.exception("Error in try_parse_json_string")
        return None

def parse_value_for_bq(value):
    """
    Parseo genérico para convertir estructuras de Monday a tipos
    básicos (str, float, bool, etc.) antes de mapear a BigQuery.
    """
    try:
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
                ids = [str(p.get("id", "")) for p in value["personsAndTeams"]]
                return ", ".join(ids)

            if "linkedPulseIds" in value and isinstance(value["linkedPulseIds"], list):
                linked_ids = [str(x.get("linkedPulseId", "")) for x in value["linkedPulseIds"]]
                return ", ".join(linked_ids)

            if "labels" in value and isinstance(value["labels"], list):
                label_names = [lbl.get("name", "") for lbl in value["labels"]]
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
    except Exception as e:
        logging.exception("Error in parse_value_for_bq")
        return None

def parse_monday_column_value(text_val, json_val, bq_type):
    """
    Dado el text y value de una columna en Monday, parsear al tipo BigQuery deseado.
    """
    try:
        parsed_from_json_val = None
        if json_val:
            try:
                as_dict = json.loads(json_val)
                if isinstance(as_dict, dict):
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
    except Exception as e:
        logging.exception("Error in parse_monday_column_value")
        return None

#################################
# CONSULTA A MONDAY
#################################

def build_monday_query(start_str, end_str):
    """
    Genera un query GraphQL para filtrar por la columna `COLUMN_ID_FECHA`,
    con compare_attribute='CREATED_AT' y operator='between'.
    """
    try:
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
    except Exception as e:
        logging.exception("Error in build_monday_query")
        raise

def fetch_items_from_monday(start_date, end_date):
    """
    Llama a la API de Monday para obtener items cuya 'date' esté entre start_date y end_date.
    """
    try:
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
        items_page_data = boards_data[0].get("items", []) or boards_data[0].get("items_page", {}).get("items", [])
        return items_page_data
    except Exception as e:
        logging.exception("Error in fetch_items_from_monday")
        raise

def transform_items_to_rows(items):
    """
    Convierte los items de Monday a un arreglo de dicts,
    mapeando valores al tipo correcto según COLUMN_TYPE_MAP.
    """
    try:
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
    except Exception as e:
        logging.exception("Error in transform_items_to_rows")
        raise

def to_json_serializable(row):
    """
    Convierte objetos datetime/date/time a str ISO-8601
    para poder insertarlos en BigQuery con insert_rows_json.
    """
    try:
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
    except Exception as e:
        logging.exception("Error in to_json_serializable")
        return row

#################################
# LÓGICA DE UPSERT (MERGE)
#################################

def ensure_dataset_exists(dataset_id):
    """
    Verifica si un dataset existe; si no, lo crea.
    dataset_id con formato "proyecto.dataset".
    """
    try:
        client = bigquery.Client()
        dataset_ref = bigquery.Dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
            logging.info(f"El dataset '{dataset_id}' ya existe.")
        except NotFound:
            client.create_dataset(dataset_ref)
            logging.info(f"Se ha creado el dataset '{dataset_id}' correctamente.")
    except Exception as e:
        logging.exception("Error in ensure_dataset_exists")
        raise

def upsert_rows_into_bq(rows):
    """
    Realiza el upsert (MERGE) en BigQuery usando:
      1) Tabla temporal de staging
      2) MERGE con la tabla final
      3) Elimina la tabla temporal
    Retorna (num_inserted, num_updated).
    """
    try:
        if not rows:
            logging.info("No hay filas que upsertar en este lote.")
            return (0, 0)

        client = bigquery.Client()
        # Nombre del dataset temporal
        dataset_id = f"{client.project}.temp_dataset"
        ensure_dataset_exists(dataset_id)

        # Crear tabla temporal con esquema basado en COLUMN_TYPE_MAP
        temp_table_id = f"{dataset_id}.tmp_{uuid.uuid4().hex}"

        # Construir esquema
        schema = []
        schema.append(bigquery.SchemaField("id", "INT64"))
        schema.append(bigquery.SchemaField("name", "STRING"))
        for col_id, bq_type in COLUMN_TYPE_MAP.items():
            if col_id not in ("id", "name"):
                schema.append(bigquery.SchemaField(col_id, bq_type))

        table = bigquery.Table(temp_table_id, schema=schema)
        table = client.create_table(table)
        logging.info(f"Tabla temporal creada: {temp_table_id}")

        # Insertar filas en la tabla temporal
        rows_serializable = [to_json_serializable(r) for r in rows]
        errors = client.insert_rows_json(temp_table_id, rows_serializable)
        if errors:
            logging.error(f"Errores al insertar en staging: {errors}")
            return (0, 0)
        logging.info(f"Se insertaron {len(rows_serializable)} filas en la tabla temporal.")

        # Generar la sentencia MERGE
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
        merge_job.result()  # Esperamos a que termine

        dml_stats = merge_job.dml_stats
        if dml_stats:
            num_inserted = dml_stats.inserted_row_count
            num_updated = dml_stats.updated_row_count
        else:
            num_inserted = 0
            num_updated = 0

        logging.info(f"MERGE completado -> Filas insertadas: {num_inserted}, Filas actualizadas: {num_updated}")

        # Borrar la tabla temporal
        client.delete_table(temp_table_id, not_found_ok=True)
        logging.info(f"Tabla temporal eliminada: {temp_table_id}")

        return (num_inserted, num_updated)
    except Exception as e:
        logging.exception("Error in upsert_rows_into_bq")
        raise

#################################
# LÓGICA DE LOGS
#################################

def insert_log_record(database_name, process_name, records_created, records_updated,
                      execution_time, status, primero, ultimo):
    """
    Inserta un registro de log en la tabla `project_settings.logs`.
    Asegúrate de que la tabla logs tenga estas columnas:
      - log_timestamp (TIMESTAMP)
      - database_name (STRING)
      - process_name (STRING)
      - records_created (INT64)
      - records_updated (INT64)
      - execution_time (FLOAT64)
      - status (STRING)
      - primero (STRING)
      - ultimo (STRING)
    """
    try:
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
        logging.info("Registro de log insertado correctamente.")
    except Exception as e:
        logging.exception("Error in insert_log_record")
        # No se interrumpe el proceso si falla el log

#################################
# MAIN
#################################

def main():
    logging.info(f"Procesando datos desde {START_DATE} hasta {END_DATE}, en intervalos de {DELTA.days} días...")
    overall_inserted = 0
    overall_updated = 0
    start_time_global = time.time()
    current_date = START_DATE

    while current_date <= END_DATE:
        end_chunk = current_date + DELTA - datetime.timedelta(days=1)
        if end_chunk > END_DATE:
            end_chunk = END_DATE

        try:
            logging.info(f"Obteniendo items del {current_date} al {end_chunk}...")
            items = fetch_items_from_monday(current_date, end_chunk)
            logging.info(f"Se encontraron {len(items)} items en ese rango.")
        except Exception as e:
            logging.exception("Error al obtener items desde Monday")
            insert_log_record(
                database_name="anuncios_penalizados",
                process_name="ingesta_diaria_anuncios_penalizados",
                records_created=0,
                records_updated=0,
                execution_time=0,
                status="ERROR en fetch_items_from_monday",
                primero=str(current_date),
                ultimo=str(end_chunk)
            )
            current_date += DELTA
            continue

        try:
            rows = transform_items_to_rows(items)
        except Exception as e:
            logging.exception("Error al transformar items a filas")
            insert_log_record(
                database_name="anuncios_penalizados",
                process_name="ingesta_diaria_anuncios_penalizados",
                records_created=0,
                records_updated=0,
                execution_time=0,
                status="ERROR en transform_items_to_rows",
                primero=str(current_date),
                ultimo=str(end_chunk)
            )
            current_date += DELTA
            continue

        try:
            num_inserted, num_updated = upsert_rows_into_bq(rows)
            overall_inserted += num_inserted
            overall_updated += num_updated
        except Exception as e:
            logging.exception("Error al realizar upsert en BigQuery")
            insert_log_record(
                database_name="anuncios_penalizados",
                process_name="ingesta_diaria_anuncios_penalizados",
                records_created=0,
                records_updated=0,
                execution_time=0,
                status="ERROR en upsert_rows_into_bq",
                primero=str(current_date),
                ultimo=str(end_chunk)
            )
            current_date += DELTA
            continue

        current_date += DELTA

    end_time_global = time.time()
    total_time = end_time_global - start_time_global

    try:
        insert_log_record(
            database_name="anuncios_penalizados",
            process_name="ingesta_diaria_anuncios_penalizados",
            records_created=overall_inserted + overall_updated,
            records_updated=overall_updated,
            execution_time=total_time,
            status="SUCCESS",
            primero=str(START_DATE),
            ultimo=str(END_DATE)
        )
    except Exception as e:
        logging.exception("Error al insertar registro de log final")

    logging.info("Proceso completado con éxito.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Error en la ejecución del proceso principal")
