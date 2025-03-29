import functions_framework
import json
import requests
from google.cloud import bigquery, storage
from datetime import datetime, date, timedelta
import calendar

@functions_framework.http
def process_event(request):
    # Parsear el payload recibido
    try:
        payload = request.get_json()
    except Exception as e:
        print(f"❌ Error parsing JSON: {e}")
        return f"Invalid JSON: {e}", 400

    # Se intenta extraer el campo date para formar el nombre del archivo
    try:
        date_field = payload.get("date")
        if isinstance(date_field, str):
            date_obj = datetime.strptime(date_field, '%Y-%m-%dT%H:%M:%SZ')
        else:
            date_obj = datetime.utcfromtimestamp(float(date_field))
        date_str = date_obj.strftime('%Y-%m-%d')
    except Exception:
        date_str = "no-date"

    # Guardar el payload en Storage para evitar duplicados
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket("reservation-transactions-recorder")

        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        action = payload["trigger"]
        if action == "reservation.created":
            base_name = payload.get("reservation_code", "reservation")
        elif action == "reservation.checkin":
            base_name = "checkin"
        elif action == "reservation.checkout":
            base_name = "checkout"
        else:
            base_name = "unknown"

        base_path = f"{year}/{month}/{day}/{action}/{base_name}.json"

        file_path = base_path
        counter = 1
        while bucket.blob(file_path).exists():
            file_path = f"{year}/{month}/{day}/{action}/{base_name}_{counter}.json"
            counter += 1

        blob = bucket.blob(file_path)
        blob.upload_from_string(json.dumps(payload), content_type="application/json")

    except Exception as e:
        print(f"❌ Error saving JSON to storage: {e} - {payload.get('trigger')} {payload.get('reservation_code')}")
        return f"Error saving JSON to storage: {e}", 500

    trigger = payload.get("trigger")
    if not trigger:
        return "Missing trigger in payload", 400

    if trigger == "reservation.created":
        return process_reservation_created(payload)
    elif trigger == "reservation.checkin":
        return process_checkin(payload)
    elif trigger == "reservation.checkout":
        return process_checkout(payload)
    elif trigger == "monthly.accrual":
        return process_monthly_accrual(payload)
    else:
        print(f"❓ trigger {trigger} without function")
        return f"Trigger {trigger} without function", 200

def process_reservation_created(payload):
    # Validación de campos requeridos
    required_fields = [
        "trigger", "reservation_code", "nights", "check_in_date",
        "checkout_date", "platform", "payout", "cleaning_fee", "property",
        "date", "created_by", "hospitable_uuid", "currency"
    ]
    missing_fields = [field for field in required_fields if field not in payload]
    if missing_fields:
        msg = f"Missing fields: {', '.join(missing_fields)}"
        print(f"❌ {msg} - {payload.get('reservation_code')}")
        return msg, 400

    # Conversión directa de payout y cleaning_fee a float
    try:
        total = round(float(payload["payout"]), 2)
        cleaning_fee = round(float(payload["cleaning_fee"]), 2)
    except (ValueError, TypeError) as e:
        msg = f"Invalid currency format: {e}"
        print(f"❌ {msg} - {payload.get('reservation_code')}")
        return msg, 400

    try:
        bq_client = bigquery.Client()
        property_name = payload["property"]
        if not property_name:
            return "Missing property field in payload", 400

        query = """
            SELECT accounting_id, mode, hospitable_uuid as query_hospitable_uuid
            FROM `hostair-test-data.properties.properties`
            WHERE property_name = @property_name
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_name", "STRING", property_name)
            ]
        )
        result = bq_client.query(query, job_config=job_config).result()
        row = next(result, None)
        if row:
            cost_center_id = row.accounting_id
            mode = row.mode
        else:
            cost_center_id = payload["hospitable_uuid"]
            mode = "H"
        cost_center_name = property_name
    except Exception as e:
        print(f"❌ Error querying property data: {e}")
        return f"Error querying property data: {e}", 500

    if mode == "C":
        print(f"❌ No processing required for property mode {mode}")
        return f"No processing required for property mode {mode}", 200

    neto = round(total / 1.16, 2)
    iva = round(total - neto, 2)
    total = round(total, 2)

    check_in = datetime.fromisoformat(payload["check_in_date"]).date().isoformat()
    checkout = datetime.fromisoformat(payload["checkout_date"]).date().isoformat()
    date_range_str = f"{check_in} - {checkout}"
    hospitable_uuid = payload["hospitable_uuid"]

    date_str = payload.get("date")
    if "T" in date_str:
        date_str = date_str.split("T")[0]

    base_fields = {
        "date": date_str,
        "cost_center_id": cost_center_id,
        "cost_center_name": cost_center_name,
        "third_party_id": "",
        "third_party_name": "",
        "classification_id": "",
        "classification_name": "",
        "reference": payload["reservation_code"],
        "invoice_uuid": None,
        "branch": "",
        "created_by": payload["created_by"]
    }

    entries = []
    # 1. Huespedes Airbnb (105.04)
    entries.append({
        **base_fields,
        "account_id": "105.04",
        "account_name": "Huespedes Airbnb",
        "debit": total,
        "credit": 0.00,
        "description": f"Reserva {date_range_str}: {hospitable_uuid}"
    })
    # 2. IVA pendiente de trasladar (209.01)
    entries.append({
        **base_fields,
        "account_id": "209.01",
        "account_name": "IVA pendiente de trasladar",
        "debit": 0.00,
        "credit": iva,
        "description": f"IVA Reserva {date_range_str}"
    })
    # 3. Hospedaje por Devengar (206.01)
    hospedaje_credit = round(total - iva - cleaning_fee, 2)
    if cleaning_fee == 0:
        hospedaje_desc = f"Hospedaje (sin tarifa de limpieza) {date_range_str}: {hospitable_uuid}"
    else:
        hospedaje_desc = f"Hospedaje {date_range_str}: {hospitable_uuid}"
    entries.append({
        **base_fields,
        "account_id": "206.01",
        "account_name": "Hospedaje por Devengar",
        "debit": 0.00,
        "credit": hospedaje_credit,
        "description": hospedaje_desc
    })
    # 4. Limpieza por devengar (206.02)
    entries.append({
        **base_fields,
        "account_id": "206.02",
        "account_name": "Limpieza por devengar",
        "debit": 0.00,
        "credit": cleaning_fee,
        "description": f"Limpieza {date_range_str}"
    })

    webhook_payload = {"entries": entries}
    webhook_url = "https://journal-entry-recorder-test-1005213489665.us-central1.run.app"

    try:
        response = requests.post(webhook_url, json=webhook_payload)
        if response.status_code != 200:
            print(f"❌ [Created] Failed to send webhook: {response.text}")
            return f"[Created] Failed to send webhook: {response.text}", response.status_code
    except Exception as e:
        print(f"❌ [Created] Error sending webhook: {e}")
        return f"[Created] Error sending webhook: {e}", 500

    print(f"✅ [Created] {payload['trigger']} {payload['reservation_code']}")
    return "Journal entry created successfully", 200

def process_checkin(payload):
    """
    Procesa el evento reservation.checkin:
      - Consulta en BigQuery las reservas cuyo check_in sea hoy.
      - Para cada reserva, consulta en la tabla journal (account_id '206.01') la info necesaria.
      - Calcula el crédito (prorrateado si check_in y checkout están en meses distintos),
      - Arma dos asientos (Hospedaje por Devengar e Ingresos por hospedaje)
      - Envía el webhook con el payload {"entries": entries} al URL del journal.
    """
    client = bigquery.Client()
    today = date.today()
    today_str = today.isoformat()  # Formato "YYYY-MM-DD"

    # Consulta las reservas con check_in igual a hoy
    query_reservations = f"""
        SELECT reservation_code, check_in, check_out, nights
        FROM `hostair-test-data.operations.reservations`
        WHERE check_in = '{today_str}'
    """
    reservas = list(client.query(query_reservations).result())
    
    if not reservas:
        print("No se encontraron reservas para check-in hoy.")
        return "No se encontraron reservas para check-in", 200

    webhook_url = "https://journal-entry-recorder-test-1005213489665.us-central1.run.app"
    
    for reserva in reservas:
        reservation_code = reserva.reservation_code
        check_in_date = reserva.check_in    # Se asume tipo DATE
        check_out_date = reserva.check_out
        total_nights = reserva.nights

        # Asegurarse de que las fechas sean del tipo date
        if isinstance(check_in_date, str):
            check_in_date = datetime.strptime(check_in_date, "%Y-%m-%d").date()
        if isinstance(check_out_date, str):
            check_out_date = datetime.strptime(check_out_date, "%Y-%m-%d").date()

        # Consulta en la tabla journal para obtener la información necesaria, incluyendo débito
        query_journal = f"""
            SELECT credit, debit, cost_center_id, cost_center_name, description
            FROM `hostair-test-data.finance.journal_entries_test`
            WHERE reference = '{reservation_code}'
              AND account_id = '206.01'
        """
        journal_entries = list(client.query(query_journal).result())
        if not journal_entries:
            print(f"⚠️ Check-in sin registro previo para la reserva: {reservation_code}")
            continue

        # Se suman todos los créditos y se restan los débitos para obtener el crédito neto
        total_credit = sum(entry.credit for entry in journal_entries)
        total_debit = sum(entry.debit for entry in journal_entries)
        net_credit = total_credit - total_debit

        # Tomar datos de centro de costo y descripción del primer registro (asumiendo que son iguales en todos)
        cost_center_id = journal_entries[0].cost_center_id
        cost_center_name = journal_entries[0].cost_center_name
        journal_description = journal_entries[0].description

        # Calcular crédito a aplicar usando net_credit
        if check_in_date.month == check_out_date.month:
            accrued_credit = net_credit
            asiento_description = "Devengo de " + journal_description
        else:
            last_day = calendar.monthrange(check_in_date.year, check_in_date.month)[1]
            last_day_date = date(check_in_date.year, check_in_date.month, last_day)
            nights_accrued = (last_day_date - check_in_date).days + 1
            accrued_credit = (net_credit / total_nights) * nights_accrued
            asiento_description = f"Devengo de {nights_accrued} noches " + journal_description
        accrued_credit = round(accrued_credit, 2)

        # Armar los dos asientos contables
        asiento1 = {
            "date": check_in_date.isoformat(),  # YYYY-MM-DD
            "cost_center_id": cost_center_id,
            "cost_center_name": cost_center_name,
            "third_party_id": "",
            "third_party_name": "",
            "classification_id": "",
            "classification_name": "",
            "reference": reservation_code,
            "invoice_uuid": None,
            "branch": "",
            "created_by": "Check-in event",
            "account_id": "206.01",
            "account_name": "Hospedaje por Devengar",
            "debit": float(accrued_credit),
            "credit": 0.0,
            "description": asiento_description
        }
        
        asiento2 = {
            "date": check_in_date.isoformat(),
            "cost_center_id": cost_center_id,
            "cost_center_name": cost_center_name,
            "third_party_id": "",
            "third_party_name": "",
            "classification_id": "",
            "classification_name": "",
            "reference": reservation_code,
            "invoice_uuid": None,
            "branch": "",
            "created_by": "Check-in event",
            "account_id": "401.01",
            "account_name": "Ingresos por hospedaje",
            "debit": 0.0,
            "credit": float(accrued_credit),
            "description": asiento_description
        }
        
        entries = [asiento1, asiento2]
        webhook_payload = {"entries": entries}
        
        try:
            response = requests.post(webhook_url, json=webhook_payload)
            if response.status_code != 200:
                print(f"❌ Error al enviar webhook para la reserva {reservation_code}: {response.status_code}")
            else:
                print(f"🛏️ hospedaje {reservation_code} devengado")
        except Exception as e:
            print(f"❌ Excepción al enviar webhook para la reserva {reservation_code}: {str(e)}")
    
    print(f"✅ [Checkin] Proceso de check-in completado")
    return "Check-in processed", 200


def process_checkout(payload):
    """
    Procesa el evento reservation.checkout:
      - Consulta en BigQuery las reservas cuyo check_out sea hoy.
      - Para cada reserva, consulta en la tabla journal (account_id '206.02') la info necesaria.
      - Se utiliza directamente el crédito obtenido para armar los asientos contables sin cálculos adicionales.
      - Arma dos asientos:
            * Asiento 1: "Limpieza por Devengar" (account_id "206.02") con el débito igual al crédito obtenido.
            * Asiento 2: "Ingreso por Limpieza" (account_id "401.05") con el crédito igual al crédito obtenido.
          Ambos asientos tienen la descripción "Limpieza devengada de (checkin - checkout)" y el campo created_by es "checkout event".
      - Envía el webhook con el payload {"entries": entries} al URL del journal.
    """
    client = bigquery.Client()
    today = date.today()
    today_str = today.isoformat()  # Formato "YYYY-MM-DD"
    
    # Consulta las reservas con check_out igual a hoy
    query_reservations = f"""
        SELECT reservation_code, check_in, check_out, nights
        FROM `hostair-test-data.operations.reservations`
        WHERE check_out = '{today_str}'
    """
    reservas = list(client.query(query_reservations).result())
    
    if not reservas:
        print("No se encontraron reservas para check-out hoy.")
        return "No se encontraron reservas para check-out", 200
    
    webhook_url = "https://journal-entry-recorder-test-1005213489665.us-central1.run.app"
    
    for reserva in reservas:
        reservation_code = reserva.reservation_code
        check_in_date = reserva.check_in    # Se asume tipo DATE o string con formato "YYYY-MM-DD"
        check_out_date = reserva.check_out
        
        # Convertir a objeto date si se reciben como string
        if isinstance(check_in_date, str):
            check_in_date = datetime.strptime(check_in_date, "%Y-%m-%d").date()
        if isinstance(check_out_date, str):
            check_out_date = datetime.strptime(check_out_date, "%Y-%m-%d").date()
        
        # Consulta en la tabla journal para obtener la información necesaria
        query_journal = f"""
            SELECT credit, cost_center_id, cost_center_name, description
            FROM `hostair-test-data.finance.journal_entries_test`
            WHERE reference = '{reservation_code}'
              AND account_id = '206.02'
        """
        journal_entries = list(client.query(query_journal).result())
        if not journal_entries:
            print(f"⚠️ Check-out sin registro previo para la reserva: {reservation_code}")
            continue
        
        journal = journal_entries[0]
        credit = journal.credit
        cost_center_id = journal.cost_center_id
        cost_center_name = journal.cost_center_name
        journal_description = journal.description
        
        # Usar el crédito obtenido directamente (sin cálculos adicionales)
        cleaning_credit = round(float(credit), 2)
        asiento_description = f"Devengo de "+ journal_description
        
        # Armar los dos asientos contables
        asiento1 = {
            "date": check_out_date.isoformat(),  # Se usa la fecha de check_out
            "cost_center_id": str(cost_center_id),
            "cost_center_name": str(cost_center_name),
            "third_party_id": "",
            "third_party_name": "",
            "classification_id": "",
            "classification_name": "",
            "reference": str(reservation_code),
            "invoice_uuid": None,
            "branch": "",
            "created_by": "Checkout event",
            "account_id": "206.02",
            "account_name": "Limpieza por Devengar",
            "debit": cleaning_credit,
            "credit": 0.0,
            "description": asiento_description
        }
        
        asiento2 = {
            "date": check_out_date.isoformat(),
            "cost_center_id": str(cost_center_id),
            "cost_center_name": str(cost_center_name),
            "third_party_id": "",
            "third_party_name": "",
            "classification_id": "",
            "classification_name": "",
            "reference": str(reservation_code),
            "invoice_uuid": None,
            "branch": "",
            "created_by": "Checkout event",
            "account_id": "401.05",
            "account_name": "Ingreso por Limpieza",
            "debit": 0.0,
            "credit": cleaning_credit,
            "description": asiento_description
        }
        
        entries = [asiento1, asiento2]
        webhook_payload = {"entries": entries}
        
        try:
            response = requests.post(webhook_url, json=webhook_payload)
            if response.status_code != 200:
                print(f"❌ Error al enviar webhook para la reserva {reservation_code}: {response.status_code}")
            else:
                print(f"🫧 limpieza {reservation_code} devengada")
        except Exception as e:
            print(f"❌ Excepción al enviar webhook para la reserva {reservation_code}: {str(e)}")
    
    print("✅ [Checkout] Proceso de checkout completado")
    return "Checkout processed", 200

def process_monthly_accrual(payload):
    """
    Procesa el evento monthly.accrual:
      - Se consulta en BigQuery las reservas cuyo check_in sea anterior a hoy y cuyo check_out sea mayor o igual a hoy.
      - Para cada reserva se calcula cuántas noches corresponden al mes actual y se prorratea el crédito obtenido de la consulta en journal.
      - Se arma el payload con dos asientos contables:
            * Asiento 1: "Hospedaje por Devengar" (account_id "206.01") con débito igual al accrued_credit y fecha de hoy.
            * Asiento 2: "Ingresos por hospedaje" (account_id "401.01") con crédito igual al accrued_credit y fecha de check_in.
      - Si no se encuentra registro en journal para una reserva, se imprime un aviso.
    """
    client = bigquery.Client()
    today = date.today()
    today_str = today.isoformat()  # Formato YYYY-MM-DD

    # Consulta las reservas que iniciaron en meses anteriores y que tienen noches en el mes actual
    query_reservations = f"""
        SELECT reservation_code, check_in, check_out, nights
        FROM `hostair-test-data.operations.reservations`
        WHERE check_in < '{today_str}'
          AND check_out >= '{today_str}'
    """
    reservas = list(client.query(query_reservations).result())
    if not reservas:
        print("No se encontraron reservas para devengo mensual.")
        return "No se encontraron reservas para devengo mensual", 200

    webhook_url = "https://journal-entry-recorder-test-1005213489665.us-central1.run.app"

    # Determinar los límites del mes actual
    current_month_start = date(today.year, today.month, 1)
    last_day = calendar.monthrange(today.year, today.month)[1]
    current_month_end = date(today.year, today.month, last_day)

    for reserva in reservas:
        reservation_code = reserva.reservation_code
        check_in_date = reserva.check_in
        check_out_date = reserva.check_out
        total_nights = reserva.nights

        # Convertir a objeto date si vienen como string
        if isinstance(check_in_date, str):
            check_in_date = datetime.strptime(check_in_date, "%Y-%m-%d").date()
        if isinstance(check_out_date, str):
            check_out_date = datetime.strptime(check_out_date, "%Y-%m-%d").date()

        # Consulta en la tabla journal para obtener el crédito y datos de cost center
        query_journal = f"""
            SELECT credit, cost_center_id, cost_center_name, description
            FROM `hostair-test-data.finance.journal_entries_test`
            WHERE reference = '{reservation_code}'
              AND account_id = '206.01'
        """
        journal_entries = list(client.query(query_journal).result())
        if not journal_entries:
            print(f"⚠️ Devengo mensual sin registro previo para la reserva: {reservation_code}")
            continue

        journal = journal_entries[0]
        credit = journal.credit
        cost_center_id = journal.cost_center_id
        cost_center_name = journal.cost_center_name
        journal_description = journal.description

        # Calcular cuántas noches del mes actual se cobran para esta reserva
        reservation_start_in_current = max(check_in_date, current_month_start)
        # Se utiliza check_out_date ya que es exclusiva; para el cálculo se suma un día al límite del mes
        reservation_end_in_current = min(check_out_date, current_month_end + timedelta(days=1))
        nights_current = (reservation_end_in_current - reservation_start_in_current).days

        if nights_current <= 0:
            print(f"No hay noches en el mes actual para la reserva: {reservation_code}")
            continue

        # Calcular el devengo prorrateado
        accrued_credit = round((credit / total_nights) * nights_current, 2)

        # Armar la descripción del asiento
        asiento_description = f"Devengo de {nights_current} noches reserva ({check_in_date.isoformat()} - {check_out_date.isoformat()})"

        # Asiento 1: Hospedaje por Devengar (débito) con fecha de hoy
        asiento1 = {
            "date": today_str,
            "cost_center_id": cost_center_id,
            "cost_center_name": cost_center_name,
            "third_party_id": "",
            "third_party_name": "",
            "classification_id": "",
            "classification_name": "",
            "reference": reservation_code,
            "invoice_uuid": None,
            "branch": "",
            "created_by": "Monthly accrual event",
            "account_id": "206.01",
            "account_name": "Hospedaje por Devengar",
            "debit": float(accrued_credit),
            "credit": 0.0,
            "description": asiento_description
        }
        
        # Asiento 2: Ingresos por hospedaje (crédito) con fecha de check_in
        asiento2 = {
            "date": check_in_date.isoformat(),
            "cost_center_id": cost_center_id,
            "cost_center_name": cost_center_name,
            "third_party_id": "",
            "third_party_name": "",
            "classification_id": "",
            "classification_name": "",
            "reference": reservation_code,
            "invoice_uuid": None,
            "branch": "",
            "created_by": "Monthly accrual event",
            "account_id": "401.01",
            "account_name": "Ingresos por hospedaje",
            "debit": 0.0,
            "credit": float(accrued_credit),
            "description": asiento_description
        }
        
        entries = [asiento1, asiento2]
        webhook_payload = {"entries": entries}
        
        try:
            response = requests.post(webhook_url, json=webhook_payload)
            if response.status_code != 200:
                print(f"❌ Error al enviar webhook para la reserva {reservation_code}: {response.text}")
            else:
                print(f"🗓️ hospedaje {reservation_code} de inicio de mes devengado")
        except Exception as e:
            print(f"❌ Excepción al enviar webhook para la reserva {reservation_code}: {str(e)}")

    print("✅ [Monthly Accrual] Proceso de devengo mensual completado")
    return "Monthly accrual processed", 200
