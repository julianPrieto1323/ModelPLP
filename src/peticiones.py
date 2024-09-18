import pandas as pd
import os
import requests
import shutil
import requests
import zipfile

def loadSynthea(save_path):
    '''
    Descarga el dataset Synthea del repositorio https://github.com/OHDSI/EunomiaDatasets y lo guarda en la carpeta que se pasa por parámetro
    Además, se queda solo con los archivos necesarios y elimina todos los archivos extras del repo.
    '''

    repo_url = "https://github.com/OHDSI/EunomiaDatasets"
    folder_path = "datasets/Synthea27Nj"
    # Extraer la parte necesaria de la URL del repositorio
    repo_name = repo_url.split("/")[-1]  # Nombre del repositorio
    user = repo_url.split("/")[-2]  # Nombre del usuario u organización
    
    # Crear la URL para descargar el repositorio como ZIP
    zip_url = f"https://github.com/{user}/{repo_name}/archive/refs/heads/main.zip"
    
    # Descargar el ZIP de todo el repositorio
    print(f"Descargando ZIP desde {zip_url}...")
    response = requests.get(zip_url)
    
    if response.status_code == 200:
        # Crear un directorio temporal para descargar y descomprimir el ZIP
        temp_dir = os.path.join(save_path, 'temp_repo')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Guardar el ZIP descargado en un archivo temporal
        zip_path = os.path.join(temp_dir, 'repo.zip')
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        print(f'Zip path {zip_path}')
        # Verificar si el archivo descargado es un archivo ZIP válido
        if zipfile.is_zipfile(zip_path):
            print(f"ZIP descargado correctamente. Descomprimiendo en {temp_dir}...")
            
            # Abrir y descomprimir el ZIP
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Verificar si el directorio de destino existe, si no, crearlo
            if not os.path.exists(save_path):
                os.makedirs(save_path)
                print(f"Directorio de destino {save_path} creado.")
            
            # Ruta a la carpeta extraída que se va a mover
            extracted_folder = os.path.join(temp_dir, f"{repo_name}-main", folder_path)
            
            # Verificar si la carpeta que queremos mover existe
            if os.path.exists(extracted_folder):
                dest_folder = os.path.join(save_path, os.path.basename(folder_path))
                
                # Mover la carpeta extraída a la ubicación deseada
                shutil.move(extracted_folder, dest_folder)
                print(f"Carpeta {os.path.basename(folder_path)} movida a {dest_folder}")
                # Abrir y descomprimir el ZIP
                with zipfile.ZipFile(dest_folder+'/Synthea27Nj_5.4.zip', 'r') as zip_ref:
                    zip_ref.extractall(dest_folder)
            else:
                print(f"No se encontró la carpeta {folder_path} en el archivo ZIP extraído.")
            
            # Eliminar el directorio temporal y el ZIP
            shutil.rmtree(temp_dir)
            print(f"Carpetas temporales y archivo ZIP eliminados.")
        else:
            print("El archivo descargado no es un ZIP válido.")
            os.remove(zip_path)
    else:
        print(f"Error al descargar el archivo ZIP: {response.status_code}")

