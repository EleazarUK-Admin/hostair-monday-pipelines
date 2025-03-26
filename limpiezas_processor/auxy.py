import re
from google.cloud import storage, bigquery

def modificar_query(query):
    """
    Busca en la sentencia SQL la asignación al campo `duration` y
    cambia el valor numérico por su representación en string.
    
    Ejemplo:
      UPDATE `support.soporte_operativo` SET `duration` = 105 WHERE `id` = 8421967169;
    se transformará en:
      UPDATE `support.soporte_operativo` SET `duration` = '105' WHERE `id` = 8421967169;
    """
    patron = r"(SET\s+`duration`\s*=\s*)(\d+)"
    query_modificada = re.sub(patron, r"\1'\2'", query)
    return query_modificada

def ejecutar_query_bigquery(query):
    """
    Ejecuta la sentencia SQL en BigQuery.
    """
    client = bigquery.Client()
    try:
        query_job = client.query(query)
        # Espera a que la consulta termine de ejecutarse
        query_job.result()
        print("Query ejecutado correctamente.")
    except Exception as e:
        print(f"Error ejecutando query: {e}")

def procesar_archivos_sql(bucket_name, prefix="sql_fallidos/"):
    """
    Se conecta al bucket de GCS, lista los archivos .sql dentro del prefijo indicado,
    y para cada sentencia SQL que contenga la asignación a `duration`, la modifica
    y la ejecuta en BigQuery.
    """
    # Inicializa el cliente de Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Lista los blobs que tengan el prefijo de la "carpeta" sql_fallidos
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if not blob.name.endswith(".sql"):
            continue  # Solo procesamos archivos .sql
        
        print(f"Procesando archivo: {blob.name}")
        contenido = blob.download_as_text()
        
        # Se asume que las sentencias están separadas por ';'
        sentencias = contenido.split(";")
        for sentencia in sentencias:
            sentencia = sentencia.strip()
            if not sentencia:
                continue  # Omitir líneas vacías
            
            # Verifica que la sentencia contenga la asignación a `duration`
            if "`duration`" in sentencia and "=" in sentencia:
                sentencia_modificada = modificar_query(sentencia)
                print("Sentencia original:")
                print(sentencia)
                print("Sentencia modificada:")
                print(sentencia_modificada)
                
                # Ejecuta la sentencia modificada en BigQuery
                ejecutar_query_bigquery(sentencia_modificada)
            else:
                print("Se omite sentencia (no contiene 'set duration'):")
                print(sentencia)
        print("-" * 40)

if __name__ == "__main__":
    # Reemplaza "soporte_operativo" por el nombre real de tu bucket de GCS
    BUCKET_NAME = "soporte_operativo"
    procesar_archivos_sql(BUCKET_NAME)
