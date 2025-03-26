import json
from google.cloud import storage
from google.cloud import bigquery

# Configuraciones
BUCKET_NAME = "hospitable_p"
PREFIX_POR_PROCESAR = "por_procesar/"
PREFIX_PROCESADO = "procesado/"

PROJECT_ID = "hostair-test-data"
DATASET_ID = "properties"
TABLE_ID = "properties_hospitable_test"

def main():
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=PREFIX_POR_PROCESAR)

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        print(f"Procesando blob: {blob.name}")
        json_string = blob.download_as_text()

        try:
            record = json.loads(json_string)
        except Exception as e:
            print(f"ERROR parseando JSON en {blob.name}: {e}")
            continue

        # Extraer campos principales
        data = record.get("data", {})
        property_id = data.get("id")
        name = data.get("name")
        picture = data.get("picture")
        listed = data.get("listed", False)
        timezone = data.get("timezone")
        description = data.get("description")
        summary = data.get("summary")
        check_in = data.get("checkin")
        check_out = data.get("checkout")
        currency = data.get("currency")
        property_type = data.get("property_type")
        room_type = data.get("room_type")
        calendar_restricted = data.get("calendar_restricted", False)

        action = record.get("action")
        triggers_list = record.get("triggers", [])
        triggers_str = ",".join(triggers_list)
        created = record.get("created")  # Ejemplo: "2025-02-21T21:04:42Z"
        version = record.get("version")
        raw_payload = json_string  # El JSON completo

        # Address
        address_raw = data.get("address", {})
        # coordinates puede ser None o dict
        coords = address_raw.get("coordinates", {})
        address_dict = {
            "number": address_raw.get("number"),
            "street": address_raw.get("street"),
            "city": address_raw.get("city"),
            "state": address_raw.get("state"),
            "country": address_raw.get("country"),
            "postcode": address_raw.get("postcode"),
            "coordinates": {
                "latitude": float(coords["latitude"]) if coords.get("latitude") else None,
                "longitude": float(coords["longitude"]) if coords.get("longitude") else None
            } if coords else None,
            "display": address_raw.get("display"),
        }

        # Capacity
        cap_raw = data.get("capacity", {})
        capacity_dict = {
            "max": cap_raw.get("max"),
            "bedrooms": cap_raw.get("bedrooms"),
            "beds": cap_raw.get("beds"),
            "bathrooms": cap_raw.get("bathrooms"),
        }

        # Amenities (array simple)
        amenities = data.get("amenities", [])

        # House rules
        hr_raw = data.get("house_rules", {})
        house_rules_dict = {
            "pets_allowed": hr_raw.get("pets_allowed"),
            "smoking_allowed": hr_raw.get("smoking_allowed"),
            "events_allowed": hr_raw.get("events_allowed"),
        }

        # Details
        det_raw = data.get("details", {})
        details_dict = {
            "space_overview": det_raw.get("space_overview"),
            "guest_access": det_raw.get("guest_access"),
            "house_manual": det_raw.get("house_manual"),
            "other_details": det_raw.get("other_details"),
            "additional_rules": det_raw.get("additional_rules"),
            "neighborhood_description": det_raw.get("neighborhood_description"),
            "getting_around": det_raw.get("getting_around"),
            "wifi_name": det_raw.get("wifi_name"),
            "wifi_password": det_raw.get("wifi_password"),
        }

        # Parent_child
        pc_raw = data.get("parent_child")
        if pc_raw is not None:
            parent_child_dict = {
                "type": pc_raw.get("type"),
                "parent": pc_raw.get("parent"),
                "children": pc_raw.get("children", []),
                "siblings": pc_raw.get("siblings", []),
            }
        else:
            parent_child_dict = None

        # Listings: array de structs
        listings_raw = data.get("listings", [])
        listings_array = []
        for lst in listings_raw:
            listings_array.append({
                "platform": lst.get("platform"),
                "platform_id": lst.get("platform_id"),
                "platform_name": None,
                "platform_email": None,
            })

        # Tags (array simple)
        tags = data.get("tags", [])

        # room_details: en el JSON vienen sub-arrays con "beds", 
        # tu tabla define un ARRAY<STRUCT<type STRING, quantity INT64>> simple. 
        # Hacemos flatten:
        room_details_flat = []
        rd_raw = data.get("room_details", [])
        for room in rd_raw:
            beds_in_room = room.get("beds", [])
            for b in beds_in_room:
                room_details_flat.append({
                    "type": b.get("type"),
                    "quantity": b.get("quantity", 0)
                })

        # ------------------------------------------
        # Construimos parámetros con TIPOS en formato SQL
        # ------------------------------------------

        # 1) address (STRUCT)
        address_param = bigquery.ScalarQueryParameter(
            "address",
            "STRUCT< \
                number STRING, \
                street STRING, \
                city STRING, \
                state STRING, \
                country STRING, \
                postcode STRING, \
                coordinates STRUCT<latitude FLOAT64, longitude FLOAT64>, \
                display STRING \
            >",
            address_dict
        )

        # 2) capacity (STRUCT)
        capacity_param = bigquery.ScalarQueryParameter(
            "capacity",
            "STRUCT< \
                max INT64, \
                bedrooms INT64, \
                beds INT64, \
                bathrooms INT64 \
            >",
            capacity_dict
        )

        # 3) house_rules (STRUCT)
        house_rules_param = bigquery.ScalarQueryParameter(
            "house_rules",
            "STRUCT< \
                pets_allowed BOOL, \
                smoking_allowed BOOL, \
                events_allowed BOOL \
            >",
            house_rules_dict
        )

        # 4) details (STRUCT)
        details_param = bigquery.ScalarQueryParameter(
            "details",
            "STRUCT< \
                space_overview STRING, \
                guest_access STRING, \
                house_manual STRING, \
                other_details STRING, \
                additional_rules STRING, \
                neighborhood_description STRING, \
                getting_around STRING, \
                wifi_name STRING, \
                wifi_password STRING \
            >",
            details_dict
        )

        # 5) parent_child (STRUCT), puede ser None
        #    OJO: si es None, BigQuery interpreta que el valor es NULL en ese param.
        parent_child_param = bigquery.ScalarQueryParameter(
            "parent_child",
            "STRUCT< \
                type STRING, \
                parent STRING, \
                children ARRAY<STRING>, \
                siblings ARRAY<STRING> \
            >",
            parent_child_dict
        )

        # 6) listings (ARRAY<STRUCT<...>>)
        listings_param = bigquery.ArrayQueryParameter(
            "listings",
            "STRUCT< \
                platform STRING, \
                platform_id STRING, \
                platform_name STRING, \
                platform_email STRING \
            >",
            listings_array
        )

        # 7) room_details (ARRAY<STRUCT<type STRING, quantity INT64>>)
        room_details_param = bigquery.ArrayQueryParameter(
            "room_details",
            "STRUCT<type STRING, quantity INT64>",
            room_details_flat
        )

        # Preparamos el MERGE
        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS T
        USING (
            SELECT
                @id AS id,
                @name AS name,
                @picture AS picture,
                @address AS address,
                @timezone AS timezone,
                @listed AS listed,
                @amenities AS amenities,
                @description AS description,
                @summary AS summary,
                @check_in AS check_in,
                @check_out AS check_out,
                @currency AS currency,
                @capacity AS capacity,
                @room_details AS room_details,
                @house_rules AS house_rules,
                @listings AS listings,
                @tags AS tags,
                @property_type AS property_type,
                @room_type AS room_type,
                @calendar_restricted AS calendar_restricted,
                @parent_child AS parent_child,
                @details AS details,
                @action AS action,
                @triggers AS triggers,
                @created AS created,
                @version AS version,
                @raw_payload AS raw_payload
        ) AS S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET
            name = S.name,
            picture = S.picture,
            address = S.address,
            timezone = S.timezone,
            listed = S.listed,
            amenities = S.amenities,
            description = S.description,
            summary = S.summary,
            check_in = S.check_in,
            check_out = S.check_out,
            currency = S.currency,
            capacity = S.capacity,
            room_details = S.room_details,
            house_rules = S.house_rules,
            listings = S.listings,
            tags = S.tags,
            property_type = S.property_type,
            room_type = S.room_type,
            calendar_restricted = S.calendar_restricted,
            parent_child = S.parent_child,
            details = S.details,
            action = S.action,
            triggers = S.triggers,
            created = S.created,
            version = S.version,
            raw_payload = S.raw_payload
        WHEN NOT MATCHED THEN
          INSERT (
            id,
            name,
            picture,
            address,
            timezone,
            listed,
            amenities,
            description,
            summary,
            check_in,
            check_out,
            currency,
            capacity,
            room_details,
            house_rules,
            listings,
            tags,
            property_type,
            room_type,
            calendar_restricted,
            parent_child,
            details,
            action,
            triggers,
            created,
            version,
            raw_payload
          )
          VALUES(
            S.id,
            S.name,
            S.picture,
            S.address,
            S.timezone,
            S.listed,
            S.amenities,
            S.description,
            S.summary,
            S.check_in,
            S.check_out,
            S.currency,
            S.capacity,
            S.room_details,
            S.house_rules,
            S.listings,
            S.tags,
            S.property_type,
            S.room_type,
            S.calendar_restricted,
            S.parent_child,
            S.details,
            S.action,
            S.triggers,
            S.created,
            S.version,
            S.raw_payload
          )
        """

        # Definimos los parámetros (los campos escalar y array simple se pueden pasar normal)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", property_id),
                bigquery.ScalarQueryParameter("name", "STRING", name),
                bigquery.ScalarQueryParameter("picture", "STRING", picture),
                address_param,
                bigquery.ScalarQueryParameter("timezone", "STRING", timezone),
                bigquery.ScalarQueryParameter("listed", "BOOL", listed),
                bigquery.ArrayQueryParameter("amenities", "STRING", amenities),
                bigquery.ScalarQueryParameter("description", "STRING", description),
                bigquery.ScalarQueryParameter("summary", "STRING", summary),
                bigquery.ScalarQueryParameter("check_in", "STRING", check_in),
                bigquery.ScalarQueryParameter("check_out", "STRING", check_out),
                bigquery.ScalarQueryParameter("currency", "STRING", currency),
                capacity_param,
                room_details_param,
                house_rules_param,
                listings_param,
                bigquery.ArrayQueryParameter("tags", "STRING", tags),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_type),
                bigquery.ScalarQueryParameter("room_type", "STRING", room_type),
                bigquery.ScalarQueryParameter("calendar_restricted", "BOOL", calendar_restricted),
                parent_child_param,
                details_param,
                bigquery.ScalarQueryParameter("action", "STRING", action),
                bigquery.ScalarQueryParameter("triggers", "STRING", triggers_str),
                bigquery.ScalarQueryParameter("created", "TIMESTAMP", created),
                bigquery.ScalarQueryParameter("version", "STRING", version),
                bigquery.ScalarQueryParameter("raw_payload", "STRING", raw_payload),
            ]
        )

        # Ejecutamos el MERGE
        try:
            query_job = bq_client.query(merge_query, job_config=job_config)
            query_job.result()
            print(f"Propiedad {property_id} procesada correctamente en BigQuery.")

            # Mover el blob a la carpeta "procesado"
            new_blob_name = blob.name.replace(PREFIX_POR_PROCESAR, PREFIX_PROCESADO)
            bucket.rename_blob(blob, new_blob_name)
            print(f"Blob movido a: {new_blob_name}")
        except Exception as e:
            print(f"Error al hacer MERGE para {blob.name}: {e}")
            # Podrías moverlo a una carpeta de errores o reintentar
            continue

if __name__ == "__main__":
    main()
