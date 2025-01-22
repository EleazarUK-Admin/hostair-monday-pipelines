# Tablero Limpiezas - Integración con Monday y BigQuery

Este repositorio contiene varios componentes en Python para integrar la información de Monday.com con Google Cloud Storage y BigQuery. El flujo principal consiste en recibir webhooks desde Monday, generar archivos JSON y SQL en un bucket de GCS, procesarlos y finalmente ejecutar sentencias en BigQuery.

## Estructura de Carpetas
```
.
|-- limpiezas_processor/
|   |-- Dockerfile
|   |-- processor.py
|   |-- requirements.txt
|
|-- Llenado/
|   |-- llenado.py
|   |-- requirements.txt
|   |-- tableroLim.py
|
|-- sql_processor/
|   |-- Dockerfile
|   |-- main.py
|   |-- requirements.txt
|
|-- webhook/
|   |-- Dockerfile
|   |-- main.py
|   |-- requirements.txt
|
|-- .gitignore
|-- README.md
```

## Descripción de Componentes

### 1. **webhook/**

Contiene el código de un servicio Cloud Run (llamado `limpiezas-webhook`) que:
- Recibe las llamadas (webhooks) provenientes de Monday.
- Guarda los archivos `.json` en el bucket `limpieza-test`, dentro de la carpeta `por_procesar/`.
- Los archivos tienen un formato de nombre como: `webhook_limpieza_YYYYMMDD_HHMMSS_XXXXXX.json`.

### 2. **limpiezas_processor/**

Incluye código en Python para un Cloud Run Job (`limpieza-job`) que:
- Lee los `.json` en la carpeta `por_procesar/` del bucket.
- Convierte cada `.json` en un archivo `.sql` y lo guarda en `sql_por_procesar/`.
- Mueve el `.json` original a la carpeta `procesando/`.

### 3. **sql_processor/**

Contiene otro Cloud Run Job que:
- Toma los archivos `.sql` de la carpeta `sql_por_procesar/`.
- Ejecuta esos scripts en BigQuery (tabla `housekeeping.test_limpieza`).
- Si la ejecución es exitosa:
  - Mueve el `.sql` a `sql_ejecutados/`.
  - Mueve el correspondiente `.json` de `procesando/` a `procesados/`.

### 4. **Llenado/**

Scripts de Python que se ejecutan manualmente (no están en Cloud Run):
- **tableroLim.py**: Consulta la información completa de un tablero de Monday, generando un archivo `.json` que se sube al bucket en la carpeta `llenado/`.
- **llenado.py**: Toma ese `.json` desde `llenado/` y genera el INSERT correspondiente en un archivo `.sql`, que luego se sube a `sql_por_procesar/`.
- Finalmente, el archivo `.sql` será tomado por el `sql_processor` para ejecutarse en BigQuery.

## Flujo General

1. **Monday → Webhook**
   - Monday dispara un webhook cuando se crea/actualiza un ítem.
   - El servicio en `webhook/` (Cloud Run `limpiezas-webhook`) recibe la notificación y crea un archivo JSON en `gs://limpieza-test/por_procesar/`.

2. **Procesar JSON a SQL**
   - El Cloud Run Job en `limpiezas_processor/` lee los JSON de `por_procesar/`, genera un `.sql` en `sql_por_procesar/` y mueve el `.json` a `procesando/`.

3. **Ejecutar SQL en BigQuery**
   - El Cloud Run Job en `sql_processor/` toma el `.sql` de `sql_por_procesar/`, ejecuta la sentencia INSERT (o la que corresponda) en BigQuery (`housekeeping.test_limpieza`).
   - Luego mueve el `.sql` a `sql_ejecutados/` y el `.json` de `procesando/` a `procesados/`.

4. **Scripts Manuales de Llenado**
   - Para traer la información completa del tablero de Monday a BigQuery:
     1. Ejecutar `tableroLim.py` para consultar todo el tablero y generar un archivo JSON en `llenado/`.
     2. Ejecutar `llenado.py` para convertir ese JSON en un `.sql` (subiéndolo a `sql_por_procesar/` en GCS), de modo que el `sql_processor` lo ejecute.

## Ejecución Local de los Scripts en **Llenado/**

1. Crear un entorno virtual:
   ```bash
   python -m venv venv
   ```

2. Activar el entorno virtual (en Windows CMD):
   ```bash
   .\venv\Scripts\activate
   ```

3. Instalar dependencias:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. Configurar credenciales de Google Cloud:
   ```bash
   gcloud auth application-default login
   ```

5. Ejecutar los scripts:
   - Generar JSON consultando el tablero:
     ```bash
     python tableroLim.py
     ```
     Esto creará o subirá un archivo `.json` con la información completa a la carpeta `llenado/` del bucket.
   - Generar el `.sql` que inserta en BigQuery:
     ```bash
     python llenado.py
     ```
     Esto buscará el `.json` en `llenado/`, generará el SQL necesario y lo subirá a `sql_por_procesar/`.

   La carpeta `sql_processor/` y su Cloud Run Job se encargan del resto (ejecutar las sentencias en BigQuery).

## Despliegue en Cloud Run / Cloud Run Jobs

Los componentes `webhook`, `limpiezas_processor` y `sql_processor` están diseñados para desplegarse en Cloud Run / Cloud Run Jobs con contenedores Docker:

- **webhook**: Es un servicio Cloud Run que expone un endpoint HTTP para recibir Webhooks de Monday.
- **limpiezas_processor** y **sql_processor**: Son Cloud Run Jobs que se configuran para correr en intervalos (p.ej., cada minuto) usando Cloud Scheduler.

Cada carpeta contiene un `Dockerfile` y su propio `requirements.txt`.

