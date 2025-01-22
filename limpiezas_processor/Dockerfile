# Usamos una imagen base de Python
FROM python:3.9

# Creamos un directorio de trabajo
WORKDIR /app

# Copiamos los archivos de requerimientos
COPY requirements.txt .

# Instalamos las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el contenido de la carpeta actual (donde esté tu main.py)
COPY . .

# Comando por defecto: ejecutar tu script
CMD ["python", "processor.py"]
