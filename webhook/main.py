from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from google.cloud import storage
import json
from datetime import datetime
import os

app = FastAPI()

BUCKET_NAME = "webhooktestu"

def upload_to_bucket(bucket_name, filename, data):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(data, ensure_ascii=False, indent=4), content_type="application/json")
    print(f"Archivo subido al bucket: {filename}")

@app.post("/webhook/limpiezas/")
async def webhook_handler(request: Request):
    data = await request.json()
    
    if 'challenge' in data:
        return JSONResponse(content={"challenge": data['challenge']})
    
    print("Datos recibidos en el webhook:", data)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")  
    filename = f"webhook_data_{timestamp}.json"
    
    upload_to_bucket(BUCKET_NAME, filename, data)
    
    return {"status": "success", "filename": filename}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
