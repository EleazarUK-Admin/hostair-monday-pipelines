import requests
import json
import datetime
from google.cloud import storage

API_URL = "https://api.monday.com/v2"
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
BOARD_ID = "8230364285"
BUCKET_NAME = "limpieza-test"
GCS_PREFIX = "llenado/"

def obtener_items_board(board_id, api_token):
    headers = {"Authorization": api_token, "Content-Type": "application/json"}
    query = f"""
    {{
      boards(ids: {board_id}) {{
        items_page {{
          items {{ id name }}
        }}
      }}
    }}
    """
    r = requests.post(API_URL, headers=headers, json={"query": query})
    r.raise_for_status()
    data = r.json()
    return data["data"]["boards"][0]["items_page"]["items"]

def obtener_valores_item(item_id, api_token):
    headers = {"Authorization": api_token, "Content-Type": "application/json"}
    query = f"""
    {{
      items(ids: {item_id}) {{
        id
        name
        column_values {{
          column {{ id title }}
          id
          type
          value
        }}
      }}
    }}
    """
    r = requests.post(API_URL, headers=headers, json={"query": query})
    r.raise_for_status()
    data = r.json()
    items_data = data["data"]["items"]
    return items_data[0] if items_data else None

def obtener_datos_tablero(board_id, api_token):
    items = obtener_items_board(board_id, api_token)
    resultados = []
    for it in items:
        item_data = obtener_valores_item(it["id"], api_token)
        if item_data:
            resultados.append(item_data)
        else:
            resultados.append({"id": it["id"], "name": it["name"], "column_values": []})
    return resultados

def subir_json_a_gcs(bucket_name, blob_name, contenido_json):
    c = storage.Client()
    bucket = c.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(contenido_json, content_type="application/json")

if __name__ == "__main__":
    try:
        datos = obtener_datos_tablero(BOARD_ID, API_TOKEN)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        destino = f"{GCS_PREFIX}datos_tablero_{ts}.json"
        contenido = json.dumps(datos, indent=2, ensure_ascii=False)
        subir_json_a_gcs(BUCKET_NAME, destino, contenido)
        print(f"Guardado en gs://{BUCKET_NAME}/{destino}")
    except Exception as e:
        print(f"Error: {e}")
