import requests
import json
import os
import openpyxl

API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE5Njk2MzQyMCwiYWFpIjoxMSwidWlkIjozMzY5MTA2MywiaWFkIjoiMjAyMi0xMS0xOVQwOToxMjoyMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTIxMzE3ODcsInJnbiI6InVzZTEifQ.ZdHFWNMZULEp188h9gSnPT8oLSmu3vHE3RMzXru4UwA"
API_URL = "https://api.monday.com/v2"

# Lista con la información de cada board que quieres procesar.
# table_info = [
#     {"board_id": 3169137106, "board_name": "compras_pagos", "table": "compras_pagos"},
#     {"board_id": 7269476761, "board_name": "caja_chica_chilpancingo", "table": "caja_chica_chilpancingo"},
#     {"board_id": 7072610932, "board_name": "agente_soporte_campo", "table": "agente_soporte_campo"},
#     {"board_id": 5914627798, "board_name": "soporte_operativo", "table": "soporte_operativo"},
#     {"board_id": 5355817123, "board_name": "acciones", "table": "acciones"},
#     {"board_id": 7019548313, "board_name": "anuncios_penalizados", "table": "anuncios_penalizados"},
#     {"board_id": 6908297780, "board_name": "resoluciones", "table": "resoluciones"},
#     {"board_id": 2663242816, "board_name": "solicitudes_mantenimiento", "table": "solicitudes_mantenimiento"},
#     {"board_id": 4460406422, "board_name": "Limpiezas", "table": "prod_monday_limpiezas"}
# ]
table_info = [
    {"board_id": 3270960828, "board_name": "monday_properties_main", "table": "properties.monday_properties_main"},
    {"board_id": 4045465610, "board_name": "monday_properties_general_information", "table": "properties.monday_properties_general_information"},
    {"board_id": 4045377615, "board_name": "monday_properties_equipment", "table": "properties.monday_properties_equipment"},
    {"board_id": 4045430146, "board_name": "monday_properties_amenities", "table": "properties.monday_properties_amenities"},
    {"board_id": 4045413655, "board_name": "monday_properties_accesories", "table": "properties.monday_properties_accesories"},
    {"board_id": 4045551102, "board_name": "monday_properties_services", "table": "properties.monday_properties_services"},
    {"board_id": 4045514460, "board_name": "monday_properties_presentation_standars", "table": "properties.monday_properties_presentation_standars"},
    {"board_id": 6311489730, "board_name": "monday_properties_checkin_guide", "table": "properties.monday_properties_checkin_guide"},
]
# Mapeo de tipos Monday -> tipos BigQuery
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

def get_board_columns(board_id):
  
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
    variables = {"boardIds": [str(board_id)]}
    headers = {
        "Authorization": API_TOKEN,
        "Content-Type": "application/json"
    }

    response = requests.post(API_URL, json={"query": query, "variables": variables}, headers=headers)
    data = response.json()

    # Manejo de errores
    if "errors" in data:
        raise Exception(f"Error al consultar board {board_id}: {data['errors']}")

    boards_data = data.get("data", {}).get("boards", [])
    if not boards_data:
        raise Exception(f"No se encontró el board con ID {board_id}.")

    board_info = boards_data[0]
    return board_info

def generate_create_table_sql(table_name, columns):
    """
    Genera la sentencia CREATE TABLE para BigQuery, 
    usando el dict type_map como referencia de tipos.
    """
    create_stmt = f"CREATE TABLE `{table_name}` (\n"
    for col in columns:
        col_id = col["id"]
        col_type = col["type"]
        bq_type = type_map.get(col_type, "STRING")  # por defecto: STRING si no está en el dict
        create_stmt += f"  `{col_id}` {bq_type},\n"
    # Remueve la última coma y cierra
    create_stmt = create_stmt.rstrip(",\n") + "\n);\n"
    return create_stmt

def main():
    # Crea la carpeta de salida
    os.makedirs("output2", exist_ok=True)

    # Creamos un único Workbook de Excel
    wb = openpyxl.Workbook()
    # Por defecto se crea una hoja llamada "Sheet". La usaremos para el primer board o la eliminamos.
    # En este caso la podemos eliminar y crear hojas nuevas a conveniencia:
    default_sheet = wb.active
    wb.remove(default_sheet)

    # Iteramos sobre cada board en table_info
    for info in table_info:
        board_id = info["board_id"]
        board_name = info["board_name"]
        table_name = info["table"]

        print("="*60)
        print(f"Procesando board_id={board_id} ({board_name}) -> tabla: {table_name}")

        # a) Obtener las columnas del board
        board_info = get_board_columns(board_id)
        columns = board_info.get("columns", [])

        # b) Guardar un archivo JSON con la info completa del board
        json_path = os.path.join("output", f"board_{board_id}_info.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(board_info, f, ensure_ascii=False, indent=2)
        print("JSON guardado en:", json_path)

        # c) Generar la sentencia CREATE TABLE
        # Ajusta si deseas un dataset distinto (ej: "operations.")
        full_table_name = f"operations.{table_name}"
        create_table_sql = generate_create_table_sql(full_table_name, columns)

        # d) Guardar el .sql
        sql_path = os.path.join("output2", f"board_{board_id}_create_table.sql")
        with open(sql_path, "w", encoding="utf-8") as sf:
            sf.write(create_table_sql)
        print("Script SQL guardado en:", sql_path)

        # e) Crear una HOJA en el Excel para este board
        #    Asegúrate de no superar 31 caracteres (límite de Excel). 
        #    Por si acaso, le recortamos el nombre.
        sheet_title = (board_name[:25] + f"_{board_id}")[:31]
        sheet = wb.create_sheet(title=sheet_title)

        # Encabezados en la hoja
        sheet.append(["Columna SQL (id)", "Columna real (title)"])

        # Llenar filas con las columnas
        for col in columns:
            sheet.append([
                col["id"],     # Ejemplo: "text99"
                col["title"]   # Ejemplo: "Nombre del cliente"
            ])

        print(f"Hoja de Excel creada para board {board_id} - {board_name}")

    # Al terminar el ciclo, guardamos el workbook con todas las hojas
    excel_path = os.path.join("output2", "boards_columns.xlsx")
    wb.save(excel_path)
    print("\nExcel con hojas separadas guardado en:", excel_path)


if __name__ == "__main__":
    main()
