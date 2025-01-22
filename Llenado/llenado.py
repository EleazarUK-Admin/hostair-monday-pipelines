import os
import json
from google.cloud import storage

BUCKET_NAME = "limpieza-test"
GCS_PREFIX_JSON = "llenado/"
GCS_PREFIX_SQL = "sql_por_procesar/"
TABLE_FQN = "housekeeping.test_limpieza"

COLUMN_TYPES = {
    "id":"NUMERIC",
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

def parse_value(col_id, col_type, raw_value):
    if raw_value is None:
        return None
    try:
        parsed_json = json.loads(raw_value)
    except:
        parsed_json = raw_value
    if col_type == "BOOL":
        if isinstance(parsed_json, dict) and "checked" in parsed_json:
            return bool(parsed_json["checked"])
        return False
    elif col_type == "DATE":
        if isinstance(parsed_json, dict) and "date" in parsed_json:
            return parsed_json["date"]
        return None
    elif col_type == "NUMERIC":
        try:
            return float(parsed_json)
        except:
            return None
    elif col_type == "TIMESTAMP":
        if isinstance(parsed_json, dict):
            date_key = "created_at" if "created_at" in parsed_json else "updated_at"
            if date_key in parsed_json:
                return parsed_json[date_key]
        return None
    elif col_type == "TIME":
        if isinstance(parsed_json, dict):
            h = parsed_json.get("hour", 0)
            m = parsed_json.get("minute", 0)
            return f"{h:02}:{m:02}:00"
        return None
    else:
        if isinstance(parsed_json, dict):
            return json.dumps(parsed_json, ensure_ascii=False)
        return str(parsed_json)

def generar_inserts_sql(json_items, table_fqn):
    inserts = []
    all_columns = list(COLUMN_TYPES.keys())
    for item in json_items:
        row_values = {}
        row_values["id"] = item["id"]
        row_values["name"] = item["name"]
        for col_val in item["column_values"]:
            cid = col_val["id"]
            rval = col_val["value"]
            ctype = COLUMN_TYPES.get(cid, "STRING")
            parsed = parse_value(cid, ctype, rval)
            row_values[cid] = parsed
        cols_sql = []
        vals_sql = []
        for cid in all_columns:
            cols_sql.append(f"`{cid}`")
            val = row_values.get(cid)
            if val is None:
                vals_sql.append("NULL")
            else:
                ctype = COLUMN_TYPES[cid]
                if ctype in ("STRING", "TIME", "DATE", "TIMESTAMP"):
                    safe_val = str(val).replace("'", "\\'")
                    vals_sql.append(f"'{safe_val}'")
                elif ctype == "BOOL":
                    vals_sql.append("TRUE" if val else "FALSE")
                elif ctype == "NUMERIC":
                    vals_sql.append(str(val))
                else:
                    safe_val = str(val).replace("'", "\\'")
                    vals_sql.append(f"'{safe_val}'")
        col_str = ", ".join(cols_sql)
        val_str = ", ".join(vals_sql)
        inserts.append(f"INSERT INTO `{table_fqn}` ({col_str}) VALUES ({val_str});")
    return inserts

def main():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    for blob in bucket.list_blobs(prefix=GCS_PREFIX_JSON):
        if blob.name.endswith(".json"):
            filename = os.path.basename(blob.name)
            data_text = blob.download_as_text(encoding="utf-8")
            monday_data = json.loads(data_text)
            statements = generar_inserts_sql(monday_data, TABLE_FQN)
            sql_file_content = "\n".join(statements)
            base_name = os.path.splitext(filename)[0]
            sql_blob_name = f"{GCS_PREFIX_SQL}{base_name}.sql"
            bucket.blob(sql_blob_name).upload_from_string(sql_file_content, content_type="text/plain")

if __name__ == "__main__":
    main()
