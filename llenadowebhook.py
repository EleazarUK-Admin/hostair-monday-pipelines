import os
import glob
import json

TABLE_FQN = "Prueba.test_limpieza"

def parse_value_for_bq(value):
    if value is None:
        return None
    
    if isinstance(value, (dict, list)):
        if isinstance(value, dict) and "value" in value:
            val = value["value"]
            if val is None:
                return None
            if isinstance(val, (int, float, bool)):  
                return val
            return str(val)  
        return json.dumps(value, ensure_ascii=False) 
    
    if isinstance(value, (int, float, bool)):
        return value  
    
    try:
        if "." in value:  
            return float(value)
        return int(value)  
    except (ValueError, TypeError):
        pass  
    
    return f"'{str(value).replace('\'', '\\\'')}'"

def generate_insert_sql(pulse_id, pulse_name, column_values):
    cols = ["id", "name"] + list(column_values.keys())
    vals = []
    vals.append(str(pulse_id))
    if pulse_name is None:
        vals.append("NULL")
    else:
        vals.append(f"'{pulse_name.replace('\'','\\\'')}'")
    for c in column_values:
        v = parse_value_for_bq(column_values[c])
        if v is None:
            vals.append("NULL")
        else:
            vals.append(f"'{v.replace('\'','\\\'')}'")
    col_string = ", ".join(f"`{c}`" for c in cols)
    val_string = ", ".join(vals)
    return f"INSERT INTO `{TABLE_FQN}` ({col_string}) VALUES ({val_string});"

def generate_update_sql(pulse_id, column_id, new_value):
    v = parse_value_for_bq(new_value)
    if v is None:
        set_expr = "NULL"
    elif isinstance(v, (int, float, bool)):  
        set_expr = str(v)  
    else:
        set_expr = f"'{str(v).replace('\'', '\\\'')}'"  
    return f"UPDATE `{TABLE_FQN}` SET `{column_id}` = {set_expr} WHERE `id` = {pulse_id};"

def process_webhook_json(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    e = data.get("event", {})
    t = e.get("type")
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
    return None

def main():
    files = glob.glob("webhook_data_*.json")
    with open("webhook_events.sql", "w", encoding="utf-8") as out_sql:
        for fpath in files:
            stmt = process_webhook_json(fpath)
            if stmt:
                out_sql.write(stmt + "\n")

if __name__ == "__main__":
    main()
