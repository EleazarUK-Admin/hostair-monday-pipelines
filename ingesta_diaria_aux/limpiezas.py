import requests
import json
import datetime
import time
import uuid
import traceback

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# -----------------------------------------------------------------------------
# 1. UTILIDADES DE LOG POR CONSOLA (sustituyen a logging)
# -----------------------------------------------------------------------------
def info(msg: str) -> None:
    print(f"[INFO] {datetime.datetime.utcnow().isoformat()} - {msg}")

def error(msg: str) -> None:
    print(f"[ERROR] {datetime.datetime.utcnow().isoformat()} - {msg}")

def exception(msg: str, exc: Exception) -> None:
    print(f"[EXCEPTION] {datetime.datetime.utcnow().isoformat()} - {msg}: {exc}")
    traceback.print_exc()


# -----------------------------------------------------------------------------
# 2. CONFIGURACIÓN GENERAL
# -----------------------------------------------------------------------------
API_TOKEN = (
    "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBa"
    "IiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
)
API_URL   = "https://api.monday.com/v2"
BOARD_ID  = 4460406422
COLUMN_ID_FECHA = "__last_updated__"

BIGQUERY_TABLE_ID = "housekeeping.prod_monday_limpiezas"
LOGS_TABLE_ID     = "project_settings.logs"

hoy  = datetime.date.today()
ayer = hoy - datetime.timedelta(days=1)

START_DATE = ayer
END_DATE   = hoy
DELTA      = datetime.timedelta(days=1)   # tamaño de ventana diaria


# -----------------------------------------------------------------------------
# 3. MAPEO DE COLUMNAS MONDAY → TIPOS BIGQUERY
#    (idéntico al original: se incluye completo para no perder funcionalidad)
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 4. PARSEO DE VALORES DE MONDAY
# -----------------------------------------------------------------------------
def try_parse_json_string(s):
    try:
        if not isinstance(s, str):
            return None
        try:                            # string → JSON directo
            parsed = json.loads(s)
            return parsed if isinstance(parsed, dict) else None
        except (ValueError, TypeError):
            pass
        s_fixed = s.replace("''", '"').replace("'", '"')  # comillas simples → dobles
        try:                            # string corregido → JSON
            parsed = json.loads(s_fixed)
            return parsed if isinstance(parsed, dict) else None
        except (ValueError, TypeError):
            return None
    except Exception as e:
        exception("Error in try_parse_json_string", e)
        return None


def parse_value_for_bq(value):
    """
    Convierte un valor ‘crudo’ devuelto por Monday a un tipo primitivo
    compatible con BigQuery (str, int, float, bool, fecha, hora, etc.).
    """
    try:
        # --------------------------- NONE ---------------------------
        if value is None:
            return None

        # --------------------------- DICT ---------------------------
        if isinstance(value, dict):
            # -------- Archivos
            if "files" in value and isinstance(value["files"], list):
                lista = []
                for f in value["files"]:
                    fname   = f.get("name", "")
                    asset   = f.get("assetId")
                    url     = f.get("url") or f.get("public_url") or f.get("preview_url")
                    if fname and url:
                        lista.append(f"{fname} => {url}")
                    elif fname and asset:
                        lista.append(f"{fname} (assetId={asset})")
                    elif fname:
                        lista.append(fname)
                    elif url:
                        lista.append(url)
                    else:
                        lista.append(str(f))
                return ", ".join(lista)

            # -------- Campo de estado / label
            if "label" in value and isinstance(value["label"], dict):
                return value["label"].get("text") or json.dumps(value["label"], ensure_ascii=False)

            # -------- Campos simples
            for simple_key in (
                "text", "checked", "date", "dateTime", "created_at",
                "updated_at", "button", "url", "phone", "countryName",
                "color", "item_id",
            ):
                if simple_key in value and value[simple_key] not in (None, ""):
                    return value[simple_key]

            # Hora (hour / minute)
            if "hour" in value and "minute" in value:
                return f"{value['hour']:02d}:{value['minute']:02d}:00"

            # Rango fecha
            if "from" in value and "to" in value:
                return f"{value['from']} - {value['to']}"

            # Personas / Teams
            if "personsAndTeams" in value and isinstance(value["personsAndTeams"], list):
                ids = [str(p.get("id", "")) for p in value["personsAndTeams"]]
                return ", ".join(ids)

            # Enlaces entre tableros
            if "linkedPulseIds" in value and isinstance(value["linkedPulseIds"], list):
                ids = [str(x.get("linkedPulseId", "")) for x in value["linkedPulseIds"]]
                return ", ".join(ids)

            # Etiquetas múltiples
            if "labels" in value and isinstance(value["labels"], list):
                names = [l.get("name", "") for l in value["labels"]]
                return ", ".join(names)

            # Campo interno "value" (string que es JSON)
            if "value" in value:
                inner_val = value["value"]
                if isinstance(inner_val, (int, float, bool)):
                    return inner_val
                parsed_dict = try_parse_json_string(inner_val)
                if parsed_dict:
                    # intentamos extraer campos comunes
                    if "text"    in parsed_dict: return parsed_dict["text"]
                    if "label"   in parsed_dict: 
                        lb = parsed_dict["label"]
                        return lb["text"] if isinstance(lb, dict) and "text" in lb else str(lb)
                    if "checked" in parsed_dict: return parsed_dict["checked"]
                    if "ids"     in parsed_dict and isinstance(parsed_dict["ids"], list):
                        return ", ".join(str(x) for x in parsed_dict["ids"])
                    return json.dumps(parsed_dict, ensure_ascii=False)
                return str(inner_val)     # valor tal cual

            # Fallback dict → string JSON
            return json.dumps(value, ensure_ascii=False)

        # --------------------------- PRIMITIVOS ---------------------------
        if isinstance(value, (int, float, bool)):
            return value

        # --------------------------- STRING ---------------------------
        if isinstance(value, str):
            parsed = try_parse_json_string(value)
            if isinstance(parsed, dict):
                if "text" in parsed:
                    return parsed["text"]
                if "label" in parsed:
                    lb = parsed["label"]
                    return lb["text"] if isinstance(lb, dict) and "text" in lb else str(lb)
                if "checked" in parsed:
                    return parsed["checked"]
                return json.dumps(parsed, ensure_ascii=False)
            # intentar convertir numérico
            try:
                return float(value) if "." in value else int(value)
            except (ValueError, TypeError):
                return value   # devolver string original

        # --------------------------- OTHER ---------------------------
        return str(value)

    except Exception as e:
        exception("Error in parse_value_for_bq", e)
        return None


def parse_monday_column_value(text_val, json_val, bq_type):
    """
    Usa `text` y `value` de Monday para producir un valor
    del tipo requerido por la columna BigQuery.
    """
    try:
        parsed_json_val = None
        if json_val:
            try:
                as_dict = json.loads(json_val)
                if isinstance(as_dict, dict) and "index" not in as_dict:
                    parsed_json_val = parse_value_for_bq(as_dict)
            except Exception:
                pass

        raw_val = parsed_json_val if parsed_json_val is not None else parse_value_for_bq(text_val)

        # Conversión final por tipo destino
        if bq_type == "STRING":
            return "" if raw_val is None else str(raw_val)

        if raw_val is None:
            return None

        if bq_type == "BOOL":
            if isinstance(raw_val, bool):
                return raw_val
            return str(raw_val).lower().strip() in ("true", "1", "checked", "sí", "yes")

        if bq_type in ("NUMERIC", "FLOAT64"):
            try:  return float(raw_val)
            except Exception:  return None

        if bq_type in ("INT64", "INTEGER"):
            try:  return int(float(raw_val))
            except Exception:  return None

        if bq_type == "DATE":
            try:  return datetime.datetime.strptime(str(raw_val), "%Y-%m-%d").date()
            except Exception:  return None

        if bq_type == "TIME":
            for fmt in ("%H:%M:%S", "%H:%M"):
                try:  return datetime.datetime.strptime(str(raw_val), fmt).time()
                except Exception: pass
            return None

        if bq_type == "TIMESTAMP":
            try:  return datetime.datetime.fromisoformat(str(raw_val).replace("Z", "+00:00"))
            except Exception:  return None

        return str(raw_val)

    except Exception as e:
        exception("Error in parse_monday_column_value", e)
        return None


# -----------------------------------------------------------------------------
# 5. BUILD GRAPHQL QUERY
# -----------------------------------------------------------------------------
def build_monday_query(start_str, end_str):
    """
    Devuelve la consulta GraphQL que filtra por columna `COLUMN_ID_FECHA`
    actualizada ayer (se mantiene idéntica a la versión del usuario).
    """
    try:
        return f"""
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
    except Exception as e:
        exception("Error in build_monday_query", e)
        raise


# -----------------------------------------------------------------------------
# 6. EXTRACCIÓN DE ITEMS DE MONDAY
# -----------------------------------------------------------------------------
def fetch_items_from_monday(start_date, end_date):
    try:
        query = build_monday_query(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        headers = {"Authorization": API_TOKEN, "Content-Type": "application/json"}

        data = requests.post(API_URL, json={"query": query}, headers=headers).json()
        if "errors" in data:
            raise Exception(f"Error al consultar items: {data['errors']}")

        boards_data = data.get("data", {}).get("boards", [])
        if not boards_data:
            return []

        return boards_data[0].get("items_page", {}).get("items", [])

    except Exception as e:
        exception("Error in fetch_items_from_monday", e)
        raise


# -----------------------------------------------------------------------------
# 7. TRANSFORMACIÓN A FILAS BIGQUERY
# -----------------------------------------------------------------------------
def transform_items_to_rows(items):
    try:
        rows = []
        for it in items:
            row = {
                "id":   float(it["id"]) if it["id"] else None,
                "name": it.get("name", "") or "",
            }
            for col in it.get("column_values", []):
                cid = col["id"]
                if cid in COLUMN_TYPE_MAP:
                    row[cid] = parse_monday_column_value(
                        col["text"], col["value"], COLUMN_TYPE_MAP[cid]
                    )
            rows.append(row)
        return rows
    except Exception as e:
        exception("Error in transform_items_to_rows", e)
        raise


# -----------------------------------------------------------------------------
# 8. BIGQUERY HELPERS
# -----------------------------------------------------------------------------
def to_json_serializable(row):
    try:
        return {
            k: (
                v.isoformat()
                if isinstance(v, (datetime.datetime, datetime.date, datetime.time))
                else v
            )
            for k, v in row.items()
        }
    except Exception as e:
        exception("Error in to_json_serializable", e)
        return row


def ensure_dataset_exists(dataset_id: str):
    try:
        client = bigquery.Client()
        ref = bigquery.Dataset(dataset_id)
        try:
            client.get_dataset(ref)
            info(f"Dataset '{dataset_id}' ya existe.")
        except NotFound:
            client.create_dataset(ref)
            info(f"Se creó dataset '{dataset_id}'.")
    except Exception as e:
        exception("Error in ensure_dataset_exists", e)
        raise


def upsert_rows_into_bq(rows):
    """
    Inserta/actualiza en BigQuery usando staging + MERGE.
    Devuelve tupla (rows_inserted, rows_updated).
    """
    try:
        if not rows:
            info("No hay filas para procesar.")
            return 0, 0

        client = bigquery.Client()
        dataset_id = f"{client.project}.temp_dataset"
        ensure_dataset_exists(dataset_id)

        temp_table = f"{dataset_id}.tmp_{uuid.uuid4().hex}"

        # ---- esquema
        schema = [
            bigquery.SchemaField("id", "NUMERIC"),
            bigquery.SchemaField("name", "STRING"),
        ] + [
            bigquery.SchemaField(c, t)
            for c, t in COLUMN_TYPE_MAP.items()
            if c not in ("id", "name")
        ]

        client.create_table(bigquery.Table(temp_table, schema=schema))
        info(f"Creada tabla temporal: {temp_table}")

        # ---- inserción a staging
        errors = client.insert_rows_json(
            temp_table, [to_json_serializable(r) for r in rows]
        )
        if errors:
            error(f"Errores al insertar en staging: {errors}")
            return 0, 0
        info(f"Insertadas {len(rows)} filas en staging.")

        # ---- MERGE
        all_cols = [f"`{field.name}`" for field in schema]
        update_set = [
            f"T.`{field.name}` = S.`{field.name}`"
            for field in schema
            if field.name != "id"
        ]
        merge_sql = f"""
        MERGE `{BIGQUERY_TABLE_ID}` T
        USING `{temp_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET {', '.join(update_set)}
        WHEN NOT MATCHED THEN
          INSERT({', '.join(all_cols)}) VALUES({', '.join(['S.'+c for c in all_cols])})
        """
        job = client.query(merge_sql)
        job.result()

        stats = job.dml_stats
        inserted = stats.inserted_row_count if stats else 0
        updated  = stats.updated_row_count  if stats else 0

        info(f"MERGE completado → insertados: {inserted}  actualizados: {updated}")

        client.delete_table(temp_table, not_found_ok=True)
        info(f"Tabla temporal eliminada: {temp_table}")

        return inserted, updated

    except Exception as e:
        exception("Error in upsert_rows_into_bq", e)
        raise


def insert_log_record(db, proc, created, updated, exec_time, status, first, last):
    """
    Inserta una fila en la tabla de logs definida en LOGS_TABLE_ID.
    No detiene el flujo si falla.
    """
    try:
        client = bigquery.Client()
        q = f"""
        INSERT INTO `{LOGS_TABLE_ID}` (
          log_timestamp, database_name, process_name,
          records_created, records_updated, execution_time,
          status, primero, ultimo
        )
        VALUES (
          CURRENT_TIMESTAMP(), '{db}', '{proc}',
          {created}, {updated}, {exec_time},
          '{status}', '{first}', '{last}'
        )
        """
        client.query(q).result()
        info("Registro de log insertado.")
    except Exception as e:
        exception("Error in insert_log_record", e)


# -----------------------------------------------------------------------------
# 9. FUNCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    try:
        info(f"Iniciando proceso del {START_DATE} al {END_DATE}...")
        total_ins, total_upd = 0, 0
        t0 = time.time()

        current = START_DATE
        while current <= END_DATE:
            chunk_end = min(current + DELTA - datetime.timedelta(days=1), END_DATE)
            info(f"⇢ Ventana {current} – {chunk_end}")
            items = fetch_items_from_monday(current, chunk_end)
            info(f"   Items recibidos: {len(items)}")
            rows = transform_items_to_rows(items)
            ins, upd = upsert_rows_into_bq(rows)
            total_ins += ins
            total_upd += upd
            current += DELTA

        elapsed = time.time() - t0
        insert_log_record(
            "limpiezas",
            "ingesta_diaria_limpiezas",
            total_ins + total_upd,
            total_upd,
            elapsed,
            "SUCCESS",
            str(START_DATE),
            str(END_DATE),
        )
        info("Proceso completado correctamente.")

    except Exception as e:
        exception("Error en la ejecución del proceso principal", e)


# -----------------------------------------------------------------------------
# 10. ENTRYPOINT
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
