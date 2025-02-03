import requests
import json
import os

API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
BOARD_ID = 6908297780
API_URL = "https://api.monday.com/v2"

query = """
query ($boardIds: [ID!]!) {
  boards (ids: $boardIds) {
    id
    name
    columns {
      id
      title
      type
    }
  }
}
"""

variables = {"boardIds": [str(BOARD_ID)]}
headers = {"Authorization": API_TOKEN, "Content-Type": "application/json"}
res = requests.post(API_URL, json={"query": query, "variables": variables}, headers=headers)
data = res.json()
if "errors" in data:
    raise Exception(data["errors"])
boards = data["data"].get("boards", [])
if not boards:
    raise Exception("No se encontró el board con ese ID.")
board_info = boards[0]
columns = board_info.get("columns", [])
os.makedirs("output", exist_ok=True)
json_path = os.path.join("output", f"board_{BOARD_ID}_info.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(board_info, f, ensure_ascii=False, indent=2)

type_map = {
    "text": "STRING",
    "long_text": "STRING",
    "name": "STRING",
    "numeric": "FLOAT64",
    "numbers": "FLOAT64",
    "date": "DATE",
    "timeline": "STRING",
    "week": "STRING",
    "hour": "TIME",
    "time": "TIME",
    "duration": "STRING",
    "country": "STRING",
    "email": "STRING",
    "phone": "STRING",
    "color": "STRING",
    "link": "STRING",
    "tag": "STRING",
    "status": "STRING",
    "dropdown": "STRING",
    "button": "STRING",
    "subtasks": "STRING",
    "subitems": "STRING",
    "formula": "STRING",
    "dependency": "STRING",
    "team": "STRING",
    "map": "STRING",
    "rating": "FLOAT64",
    "progress_tracking": "FLOAT64",
    "auto_number": "INT64",
    "checkbox": "BOOL",
    "creation_log": "TIMESTAMP",
    "last_updated": "TIMESTAMP",
    "time_tracking": "STRING",
    "item_id": "INT64",
    "files": "STRING",
    "people": "STRING",
    "person": "STRING",
    "board_relation": "STRING",
    "mirror": "STRING",
    "location": "STRING"
}

table_name = "operations.resoluciones"
create_stmt = f"CREATE TABLE `{table_name}` (\n"
for col in columns:
    col_id = col["id"]
    col_type = col["type"]
    bq_type = type_map.get(col_type, "STRING")
    create_stmt += f"  `{col_id}` {bq_type},\n"
create_stmt = create_stmt.rstrip(",\n") + "\n);\n"
sql_path = os.path.join("output", f"board_{BOARD_ID}_create_table.sql")
with open(sql_path, "w", encoding="utf-8") as sf:
    sf.write(create_stmt)
print("JSON guardado en:", json_path)
print("Script SQL guardado en:", sql_path)
