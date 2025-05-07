# main.py

from fastapi.responses import JSONResponse
from flask import Flask, request, jsonify
from google.cloud import bigquery
from datetime import datetime

app = Flask(__name__)
client = bigquery.Client()  # usa las credenciales de servicio por defecto

BQ_TABLE = "hostair-test-data.project_settings.logs_webhook_columna"

@app.route('/webhook', methods=['POST'])
def webhook():
    if 'challenge' in payload:
        return jsonify({'challenge': payload['challenge']})
    payload    = request.get_json(force=True)
    event_type = payload['event_type']           # "column_created" o "column_changed"
    board_id   = payload['board_id']
    column_id  = payload['column_id']
    changes    = payload.get('changes', {})      # dict de {campo: {old, new}}
    data_type  = payload.get('data_type')        # sólo para created
    ts         = datetime.utcnow().isoformat()

    rows = []
    if event_type == 'column_created':
        rows.append({
            'board_id':        board_id,
            'column_id':       column_id,
            'change_type':     'created',
            'changed_field':   'data_type',
            'old_value':       None,
            'new_value':       data_type,
            'data_type':       data_type,
            'event_timestamp': ts
        })
    elif event_type == 'column_changed':
        for field, vals in changes.items():
            rows.append({
                'board_id':        board_id,
                'column_id':       column_id,
                'change_type':     'changed',
                'changed_field':   field,
                'old_value':       vals['old'],
                'new_value':       vals['new'],
                'data_type':       None,
                'event_timestamp': ts
            })

    client.insert_rows_json(BQ_TABLE, rows)
    return jsonify({'status': 'success'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

#gcloud builds submit --tag gcr.io/hostair-test-data/monday_webhook_column3 .
#gcloud run deploy monday-webhook-column --image=gcr.io/hostair-test-data/monday_webhook_column3 --platform=managed --allow-unauthenticated
