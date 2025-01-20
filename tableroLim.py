import requests
import json

API_URL = "https://api.monday.com/v2"
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
BOARD_ID = "8230364285"   # ID de tu tablero principal

def obtener_items_board(board_id, api_token):
    """
    1) Consulta a Monday para obtener la lista de items (id y name) de un tablero.
    """
    headers = {
        "Authorization": api_token,
        "Content-Type": "application/json"
    }
    query_items = f"""
    {{
      boards(ids: {board_id}) {{
        items_page {{
          items {{
            id
            name
          }}
        }}
      }}
    }}
    """
    response = requests.post(API_URL, headers=headers, json={"query": query_items})
    if response.status_code != 200:
        raise Exception(f"Error al obtener items: {response.text}")
    
    data = response.json()
    # Navegamos en data -> data["data"]["boards"][0]["items_page"]["items"]
    items = data["data"]["boards"][0]["items_page"]["items"]
    return items

def obtener_valores_item(item_id, api_token):
    """
    2) Dado un item_id, obtiene los valores de cada columna en ese item (column_values).
    """
    headers = {
        "Authorization": api_token,
        "Content-Type": "application/json"
    }
    query_values = f"""
    {{
      items (ids: {item_id}) {{
        id
        name
        column_values {{
          column {{
            id
            title
          }}
          id
          type
          value
        }}
      }}
    }}
    """
    response = requests.post(API_URL, headers=headers, json={"query": query_values})
    if response.status_code != 200:
        raise Exception(f"Error al obtener valores del item {item_id}: {response.text}")
    
    data = response.json()
    # data["data"]["items"] es una lista de items, en este caso debería tener 1 (el item buscado)
    items_data = data["data"]["items"]
    return items_data[0] if items_data else None

def obtener_datos_tablero(board_id, api_token):
    """
    3) Orquestador: 
       - Obtiene la lista de todos los items del tablero (id, name).
       - Para cada item, obtiene los column_values.
       - Retorna una lista con la data combinada.
    """
    todos_los_items = obtener_items_board(board_id, api_token)
    resultados = []

    for item in todos_los_items:
        item_id = item["id"]
        item_name = item["name"]
        
        # Para cada item, obtenemos sus column_values
        info_item = obtener_valores_item(item_id, api_token)

        if info_item:
            resultados.append(info_item)
        else:
            # En caso de que no haya column_values, igual guardamos la info mínima
            resultados.append({
                "id": item_id,
                "name": item_name,
                "column_values": []
            })
    
    return resultados

if __name__ == "__main__":
    try:
        # 4) Llamamos a la función para un board_id dado
        datos_combinados = obtener_datos_tablero(BOARD_ID, API_TOKEN)

        # Guardamos en un archivo JSON
        with open("chido.json", "w", encoding="utf-8") as archivo_json:
            json.dump(datos_combinados, archivo_json, indent=2, ensure_ascii=False)

        print("Datos guardados exitosamente en 'datos_tablero.json'")
    except Exception as e:
        print(f"Error: {e}")
