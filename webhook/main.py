from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import storage
import json
from datetime import datetime
import os

app = FastAPI()

BUCKET_NAME = "limpieza-test"

def upload_to_bucket(bucket_name, destination_path, data):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=4),
        content_type="application/json"
    )

def move_file_in_bucket(bucket_name, source_path, destination_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_path)
    if not source_blob.exists():
        raise HTTPException(status_code=404, detail="El archivo no existe en la ruta especificada.")
    new_blob = bucket.copy_blob(source_blob, bucket, destination_path)
    source_blob.delete()
    return new_blob.name

@app.post("/webhook/limpiezas/")
async def webhook_handler(request: Request):
    data = await request.json()

    if 'challenge' in data:
        return JSONResponse(content={"challenge": data['challenge']})

    print("Datos recibidos en el webhook:", data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"webhook_limpieza_{timestamp}.json"

    # Ruta final dentro de la carpeta 'por_procesar'
    destination_path = f"por_procesar/{filename}"
    upload_to_bucket(BUCKET_NAME, destination_path, data)

    return {"status": "success", "filename": filename, "location": "por_procesar"}

@app.post("/move-file/")
async def move_file(file_name: str, origen: str, destino: str):
    """
    file_name: Nombre del archivo JSON (ejemplo: webhook_limpieza_20230101_123456_789012.json)
    origen: Puede ser 'por_procesar' o 'procesando'
    destino: Puede ser 'procesando' o 'procesados'
    """
    source_path = f"{origen}/{file_name}"
    destination_path = f"{destino}/{file_name}"
    new_location = move_file_in_bucket(BUCKET_NAME, source_path, destination_path)

    return {"status": "success", "new_location": new_location}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
