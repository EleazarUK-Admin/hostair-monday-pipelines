from google.cloud import bigquery

RESERVATION_SCHEMA = [
    # Campos básicos
    bigquery.SchemaField("id",             "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("code",           "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("platform",       "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("platform_id",    "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("booking_date",   "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("arrival_date",   "DATETIME",  mode="NULLABLE"),
    bigquery.SchemaField("departure_date", "DATETIME",  mode="NULLABLE"),
    bigquery.SchemaField("check_in",       "DATETIME",  mode="NULLABLE"),
    bigquery.SchemaField("check_out",      "DATETIME",  mode="NULLABLE"),
    bigquery.SchemaField("nights",         "INT64",   mode="NULLABLE"),

    # Campo anidado: reservation_status
    bigquery.SchemaField("reservation_status", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("current",  "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("category",     "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sub_category", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("history",  "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("category",     "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sub_category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timestamp",    "DATETIME", mode="NULLABLE"),
        ]),
    ]),

    # Conversación y logging
    bigquery.SchemaField("conversation_id", "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("last_message_at", "TIMESTAMP", mode="NULLABLE"),

    # Invitados
    bigquery.SchemaField("guests", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("total",        "INT64", mode="NULLABLE"),
        bigquery.SchemaField("adult_count",  "INT64", mode="NULLABLE"),
        bigquery.SchemaField("child_count",  "INT64", mode="NULLABLE"),
        bigquery.SchemaField("infant_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("pet_count",    "INT64", mode="NULLABLE"),
    ]),

    # Campos financieros
    bigquery.SchemaField("financials", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("guest",    "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("accommodation",       "RECORD", mode="NULLABLE", fields=[
                bigquery.SchemaField("amount",    "INT64", mode="NULLABLE"),
                bigquery.SchemaField("formatted","STRING", mode="NULLABLE"),
                bigquery.SchemaField("label",    "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("average_nightly_rate","RECORD", mode="NULLABLE", fields=[
                bigquery.SchemaField("amount",    "INT64", mode="NULLABLE"),
                bigquery.SchemaField("formatted","STRING", mode="NULLABLE"),
                bigquery.SchemaField("label",    "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("fees", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("amount",    "INT64", mode="NULLABLE"),
                bigquery.SchemaField("formatted","STRING", mode="NULLABLE"),
                bigquery.SchemaField("label",    "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("discounts",  "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("amount",    "INT64", mode="NULLABLE"),
                bigquery.SchemaField("formatted","STRING", mode="NULLABLE"),
                bigquery.SchemaField("label",    "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("taxes",      "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("amount",    "INT64", mode="NULLABLE"),
                bigquery.SchemaField("formatted","STRING", mode="NULLABLE"),
                bigquery.SchemaField("label",    "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("adjustments","RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("amount",    "INT64", mode="NULLABLE"),
                bigquery.SchemaField("formatted","STRING", mode="NULLABLE"),
                bigquery.SchemaField("label",    "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("payments",   "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("amount",    "INT64", mode="NULLABLE"),
                bigquery.SchemaField("formatted","STRING", mode="NULLABLE"),
                bigquery.SchemaField("label",    "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("total_price","RECORD", mode="NULLABLE", fields=[
                bigquery.SchemaField("amount",    "INT64", mode="NULLABLE"),
                bigquery.SchemaField("formatted","STRING", mode="NULLABLE"),
                bigquery.SchemaField("label",    "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            ]),
        ]),
        bigquery.SchemaField("host", "RECORD", mode="NULLABLE", fields=[
            # (análogo a guest: accommodation, guest_fees, host_fees, taxes, revenue…)
        ]),
    ]),

    # Propiedades asociadas
    bigquery.SchemaField("properties", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("id",           "STRING", mode="NULLABLE"),
        bigquery.SchemaField("name",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("public_name",  "STRING", mode="NULLABLE"),
        bigquery.SchemaField("picture",      "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("number",      "STRING", mode="NULLABLE"),
            bigquery.SchemaField("street",      "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city",        "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state",       "STRING", mode="NULLABLE"),
            bigquery.SchemaField("postcode",    "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country",     "STRING", mode="NULLABLE"),
            bigquery.SchemaField("coordinates","RECORD",mode="NULLABLE",fields=[
                bigquery.SchemaField("latitude", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("longitude","STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("display",     "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("timezone",           "STRING", mode="NULLABLE"),
        bigquery.SchemaField("listed",             "BOOL",   mode="NULLABLE"),
        bigquery.SchemaField("currency",           "STRING", mode="NULLABLE"),
        bigquery.SchemaField("summary",            "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description",        "STRING", mode="NULLABLE"),
        bigquery.SchemaField("checkin",            "STRING", mode="NULLABLE"),
        bigquery.SchemaField("checkout",           "STRING", mode="NULLABLE"),
        bigquery.SchemaField("amenities", "STRING",    mode="REPEATED"),
        bigquery.SchemaField("capacity", "RECORD",   mode="NULLABLE", fields=[
            bigquery.SchemaField("max",      "INT64", mode="NULLABLE"),
            bigquery.SchemaField("bedrooms", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("beds",     "INT64", mode="NULLABLE"),
            bigquery.SchemaField("bathrooms","INT64", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("room_details","RECORD",mode="REPEATED",fields=[
            bigquery.SchemaField("beds","RECORD",mode="REPEATED",fields=[
                bigquery.SchemaField("type",     "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quantity", "INT64",  mode="NULLABLE"),
            ])
        ]),
        bigquery.SchemaField("property_type","STRING", mode="NULLABLE"),
        bigquery.SchemaField("room_type",   "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tags",       "STRING", mode="REPEATED"),
        bigquery.SchemaField("house_rules","RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("pets_allowed",    "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("smoking_allowed", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("events_allowed",  "BOOL", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("listings","RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("platform",      "STRING", mode="NULLABLE"),
            bigquery.SchemaField("platform_id",   "STRING", mode="NULLABLE"),
            bigquery.SchemaField("platform_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("platform_email","STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("calendar_restricted","BOOL",mode="NULLABLE"),
        bigquery.SchemaField("parent_child",        "STRING",mode="NULLABLE"),
    ]),

    # Estado final
    bigquery.SchemaField("status",         "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status_history","RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("category","STRING",mode="NULLABLE"),
        bigquery.SchemaField("timestamp","DATETIME",mode="NULLABLE"),
    ]),
]
