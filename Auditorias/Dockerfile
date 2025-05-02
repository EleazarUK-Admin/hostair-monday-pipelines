FROM python:3.9-slim
WORKDIR /app
# Copia el archivo de dependencias y lo instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copia el resto de tus archivos de Python
COPY . .
# Define el script principal a ejecutar (por ejemplo, main.py)
CMD ["python", "main.py"]


# gcloud builds submit --tag gcr.io/hostair-test-data/auditoria_monday:latest .
# gcloud run jobs update auditoria-monday-job --image gcr.io/hostair-test-data/auditoria_monday:latest --region us-central1
# gcloud run jobs create auditoria-monday-job --image gcr.io/hostair-test-data/auditoria_monday:latest --region us-central1
# gcloud run jobs execute auditoria-monday-job --region us-central1

