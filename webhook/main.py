from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import storage, bigquery
import json
from datetime import datetime
import os

app = FastAPI()

def get_bucket_for_board(board_id):
    client = bigquery.Client()
    query = """
    SELECT bucket
    FROM `project_settings.board_settings`
    WHERE board_id = @board_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("board_id", "INT64", board_id)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    rows = list(query_job)
    if not rows:
        raise HTTPException(status_code=404, detail="boardId no encontrado en board_settings.")
    bucket = rows[0].bucket
    if not bucket:
        raise HTTPException(status_code=400, detail="No se definió bucket para el boardId.")
    return bucket

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

@app.post("/webhook/soporte-operativo/")
async def webhook_handler(request: Request):
    data = await request.json()
    if "challenge" in data:
        return JSONResponse(content={"challenge": data["challenge"]})
    event = data.get("event")

    board_id = event.get("boardId")
    if board_id is None:
        raise HTTPException(status_code=400, detail="boardId no proporcionado en el payload.")
    bucket_name = get_bucket_for_board(board_id)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"webhook_{timestamp}.json"
    destination_path = f"por_procesar/{filename}"
    upload_to_bucket(bucket_name, destination_path, data)
    return {"status": "success", "filename": filename, "location": "por_procesar"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)


#gcloud builds submit --tag gcr.io/hostair-test-data/monday_webhook6 .
#gcloud run deploy monday-webhook --image=gcr.io/hostair-test-data/monday_webhook6 --platform=managed --allow-unauthenticated
