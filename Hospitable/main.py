import os
from fastapi import FastAPI, Request, HTTPException
import json
from datetime import datetime

from google.cloud import storage
from google.cloud import bigquery

app = FastAPI()

BUCKET_NAME = "hospitable_p"
PROJECT_ID = "hostair-test-data"
DATASET_ID = "properties"
TABLE_ID = "properties_hospitable_test"

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
        raise HTTPException(
            status_code=404,
            detail="El archivo no existe en la ruta especificada."
        )
    new_blob = bucket.copy_blob(source_blob, bucket, destination_path)
    source_blob.delete()
    return new_blob.name

def insert_into_bigquery(payload):
    bq_client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    data = payload.get("data", {})
    property_id = data.get("id")
    name = data.get("name")
    picture = data.get("picture")

    address_info = data.get("address", {})
    address = {
        "number": address_info.get("number"),
        "street": address_info.get("street"),
        "city": address_info.get("city"),
        "state": address_info.get("state"),
        "country": address_info.get("country"),
        "postcode": address_info.get("postcode"),
        "coordinates": {
            "latitude": address_info.get("coordinates", {}).get("latitude"),
            "longitude": address_info.get("coordinates", {}).get("longitude")
        },
        "display": address_info.get("display")
    }

    timezone = data.get("timezone")
    listed = data.get("listed")
    amenities = data.get("amenities", [])
    description = data.get("description")
    summary = data.get("summary")
    check_in = data.get("check-in")
    check_out = data.get("check-out")
    currency = data.get("currency")

    capacity_info = data.get("capacity", {})
    capacity = {
        "max": capacity_info.get("max"),
        "bedrooms": capacity_info.get("bedrooms"),
        "beds": capacity_info.get("beds"),
        "bathrooms": capacity_info.get("bathrooms")
    }

    room_details = data.get("room_details", [])
    house_rules_info = data.get("house_rules", {})
    house_rules = {
        "pets_allowed": house_rules_info.get("pets_allowed"),
        "smoking_allowed": house_rules_info.get("smoking_allowed"),
        "events_allowed": house_rules_info.get("events_allowed")
    }

    listings = data.get("listings", [])

    tags = data.get("tags", [])
    property_type = data.get("property_type")
    room_type = data.get("room_type")
    calendar_restricted = data.get("calendar_restricted")

    parent_child_info = data.get("parent_child")
    if parent_child_info:
        parent_child = {
            "type": parent_child_info.get("type"),
            "parent": parent_child_info.get("parent"),
            "children": parent_child_info.get("children", []),
            "siblings": parent_child_info.get("siblings", [])
        }
    else:
        parent_child = None

    details_info = data.get("details", {})
    details = {
        "space_overview": details_info.get("space_overview"),
        "guest_access": details_info.get("guest_access"),
        "house_manual": details_info.get("house_manual"),
        "other_details": details_info.get("other_details"),
        "additional_rules": details_info.get("additional_rules"),
        "neighborhood_description": details_info.get("neighborhood_description"),
        "getting_around": details_info.get("getting_around"),
        "wifi_name": details_info.get("wifi_name"),
        "wifi_password": details_info.get("wifi_password")
    }

    action = payload.get("action")
    triggers = payload.get("triggers")

    created_str = payload.get("created")
    created_ts = None
    if created_str:
        try:
            # Ajustar string ISO para que datetime.fromisoformat() lo acepte correctamente
            created_str = created_str.replace("Z", "+00:00")
            created_ts = datetime.fromisoformat(created_str)
        except ValueError:
            created_ts = None

    version = payload.get("version")
    raw_payload = json.dumps(payload, ensure_ascii=False)

    row_to_insert = {
        "id": property_id,
        "name": name,
        "picture": picture,
        "address": address,
        "timezone": timezone,
        "listed": listed,
        "amenities": amenities,
        "description": description,
        "summary": summary,
        "check_in": check_in,
        "check_out": check_out,
        "currency": currency,
        "capacity": capacity,
        "room_details": room_details,
        "house_rules": house_rules,
        "listings": listings,
        "tags": tags,
        "property_type": property_type,
        "room_type": room_type,
        "calendar_restricted": calendar_restricted,
        "parent_child": parent_child,
        "details": details,
        "action": action,
        "triggers": triggers,
        "created": created_ts,
        "version": version,
        "raw_payload": raw_payload
    }

    errors = bq_client.insert_rows_json(table_ref, [row_to_insert])
    if errors:
        print("Error al insertar en BigQuery:", errors)
        raise HTTPException(
            status_code=500,
            detail=f"Error al insertar en BigQuery: {errors}"
        )

@app.post("/webhook/hospitable/")
async def hospitable_webhook_handler(request: Request):
    data = await request.json()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"hospitable_property_{timestamp}.json"
    destination_path = f"por_procesar/{filename}"

    # Sube JSON a GCS
    upload_to_bucket(BUCKET_NAME, destination_path, data)

    # Inserta en BigQuery
    try:
        insert_into_bigquery(data)
    except Exception as e:
        print(f"Ocurrió un error insertando en BigQuery: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "status": "success",
        "filename": filename,
        "location": "por_procesar"
    }

@app.post("/move-file/")
async def move_file(file_name: str, origen: str, destino: str):
    source_path = f"{origen}/{file_name}"
    destination_path = f"{destino}/{file_name}"
    new_location = move_file_in_bucket(BUCKET_NAME, source_path, destination_path)
    return {
        "status": "success",
        "new_location": new_location
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)

#gcloud builds submit --tag gcr.io/hostair-test-data/hospitable_webhook61 .
#gcloud run deploy hospitable-webhook-prop --image=gcr.io/hostair-test-data/hospitable_webhook61 --platform=managed --allow-unauthenticated
