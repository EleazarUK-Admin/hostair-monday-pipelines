import os
import subprocess

# Lista todos los archivos .py en el directorio actual, omitiendo este script controlador
python_files = [archivo for archivo in os.listdir('.') if archivo.endswith('.py') and archivo != 'main.py']

for archivo in python_files:
    print(f"Ejecutando {archivo}...")
    # Asegúrate de que 'python' sea el comando correcto (en algunos entornos puede ser 'python3')
    subprocess.run(['python', archivo])
