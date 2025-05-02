FROM python:3.9-slim
WORKDIR /app
# Copia el archivo de dependencias y lo instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copia el resto de tus archivos de Python
COPY . .
# Define el script principal a ejecutar (por ejemplo, main.py)
CMD ["python", "main.py"]


# gcloud builds submit --tag gcr.io/hostair-test-data/ingesta_monday:latest
# gcloud run jobs create ingesta-monday-job --image gcr.io/hostair-test-data/ingesta_monday:latest --region us-central1
# gcloud run jobs execute ingesta-monday-job --region us-central1

