import os
import json
import time
from datetime import datetime
from google.cloud import storage

TABLE_FQN = "housekeeping.test_limpieza"
BUCKET_NAME = "limpieza-test"

# =====================================================
# =============== Funciones de parsing ================
# =====================================================

def parse_value_for_bq(value):
    """
    Convierte el valor a un tipo de Python nativo:
      - None => None
      - dict con "value" => str, int, float o bool según corresponda
      - dict con "url" => regresa sólo la url (str)
      - dict/list genérico => regresa la serialización JSON (str)
      - int/float/bool => lo devuelve tal cual
      - str => intenta parsear a número, si no, deja el string como string
    """
    if value is None:
        return None

    # Si es dict, manejamos los casos especiales
    if isinstance(value, dict):
        # Caso 1) dict con subllave "value"
        if "value" in value:
            val = value["value"]
            if val is None:
                return None
            if isinstance(val, (int, float, bool)):
                return val
            return str(val)  # convertimos a string
        
        # Caso 2) dict con "url"
        if "url" in value:
            return value["url"]  # sólo devolvemos la URL
        
        # Caso 3) cualquier dict no contemplado => JSON serializado
        return json.dumps(value, ensure_ascii=False)

    # Si es int/float/bool => lo regresamos tal cual
    if isinstance(value, (int, float, bool)):
        return value

    # Si es string, intentamos parsear a num
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            return int(value)
        except (ValueError, TypeError):
            pass
        
        # si no es un número, regresamos la string tal cual
        return value

    # Cualquier otro caso (raro) => forzamos a string
    return str(value)

def escape_sql_string(s):
    """
    Realiza el escape estándar para un string en SQL:
    - reemplaza comillas simples ' por ''
    - envuelve en comillas simples.
    """
    # primero duplicamos las comillas simples
    escaped = s.replace("'", "''")
    # luego envolvemos en comillas simples
    return f"'{escaped}'"

def generate_insert_sql(pulse_id, pulse_name, column_values):
    cols = ["id", "name"] + list(column_values.keys())
    vals = []

    # Para ID (generalmente un int)
    vals.append(str(pulse_id))

    # Para NAME
    if pulse_name is None:
        vals.append("NULL")
    else:
        # escapamos para SQL
        vals.append(escape_sql_string(pulse_name))

    # Para columnValues
    for c in column_values:
        parsed_val = parse_value_for_bq(column_values[c])
        
        if parsed_val is None:
            vals.append("NULL")
        elif isinstance(parsed_val, (int, float, bool)):
            vals.append(str(parsed_val))  # lo metemos tal cual
        else:
            # asumimos que es string => escapamos
            vals.append(escape_sql_string(parsed_val))

    col_string = ", ".join(f"`{c}`" for c in cols)
    val_string = ", ".join(vals)
    return f"INSERT INTO `{TABLE_FQN}` ({col_string}) VALUES ({val_string});"

def generate_update_sql(pulse_id, column_id, new_value):
    parsed_val = parse_value_for_bq(new_value)

    if parsed_val is None:
        set_expr = "NULL"
    elif isinstance(parsed_val, (int, float, bool)):
        set_expr = str(parsed_val)
    else:
        # es string => escapamos
        set_expr = escape_sql_string(parsed_val)

    return f"UPDATE `{TABLE_FQN}` SET `{column_id}` = {set_expr} WHERE `id` = {pulse_id};"

def generate_delete_sql(pulse_id):
    return f"DELETE FROM `{TABLE_FQN}` WHERE `id` = {pulse_id};"

# =====================================================
# =========== Funciones de interacción GCS ============
# =====================================================

def get_bucket():
    client = storage.Client()
    return client.bucket(BUCKET_NAME)

def list_objects_with_prefix(prefix):
    bucket = get_bucket()
    return list(bucket.list_blobs(prefix=prefix))

def download_json(blob):
    content = blob.download_as_text(encoding="utf-8")
    return json.loads(content)

def upload_file_to_bucket(destination_path, content):
    bucket = get_bucket()
    blob = bucket.blob(destination_path)
    blob.upload_from_string(content, content_type="text/plain")

def move_blob(source_blob, new_prefix):
    bucket = get_bucket()
    source_name = source_blob.name
    filename = os.path.basename(source_name)
    destination_name = f"{new_prefix}/{filename}"
    new_blob = bucket.rename_blob(source_blob, destination_name)
    return new_blob.name

# =====================================================
# ============ Procesamiento de cada evento ===========
# =====================================================

def process_webhook_json(data):
    e = data.get("event", {})
    t = e.get("type", "")

    if t == "create_pulse":
        pid = e.get("pulseId")
        pname = e.get("pulseName")
        cv = e.get("columnValues", {})
        return generate_insert_sql(pid, pname, cv)

    elif t == "update_column_value":
        pid = e.get("pulseId")
        cid = e.get("columnId")
        nv = e.get("value", {})
        return generate_update_sql(pid, cid, nv)

    elif t == "delete_pulse":
        pid = e.get("itemId")
        return generate_delete_sql(pid)

    return None

def process_blob_in_por_procesar(blob):
    data = download_json(blob)
    stmt = process_webhook_json(data)
    if stmt:
        base_name = os.path.splitext(os.path.basename(blob.name))[0]
        sql_name = f"{base_name}.sql"
        destination_sql_path = f"sql_por_procesar/{sql_name}"
        upload_file_to_bucket(destination_sql_path, stmt)

    move_blob(blob, "procesando")

# =====================================================
# =================== Bucle principal =================
# =====================================================

def main():
    try:
        blobs = list_objects_with_prefix("por_procesar/")
        for b in blobs:
            if b.name.endswith(".json"):
                process_blob_in_por_procesar(b)
    except Exception as e:
        print(f"Error procesando: {e}")

if __name__ == "__main__":
    main()
