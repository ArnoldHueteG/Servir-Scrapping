from google.cloud import storage, bigquery
import os
import json
from bs4 import BeautifulSoup
import requests

import pandas as pd
from datetime import datetime

import os
import tempfile
import json
import shutil
import time

PROJECT_ID = "xenon-world-399922"
TABLE_NAME = "Servir.InformesLegales"
BUCKET_NAME = 'peru-servir'  # Google Cloud Storage bucket name
FOLDER_IF_PATH = 'informes-legales/2023/informe/'  # Google Cloud Storage folder path for Informes
FOLDER_OF_PATH = 'informers-legales/2023/oficio/'  # Google Cloud Storage folder path for Oficios

# Function to upload PDF to Google Cloud Storage
def upload_to_cloud_storage(pdf_path, folder_if_path, bucket_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    destination_blob_name = os.path.join(folder_if_path, os.path.basename(pdf_path))
    blob.upload_from_filename(pdf_path)

def save_json_to_bq_partitioned(json_rows, table_id, schema_path):
    # # Upload metadata to Bigquery
    project_id = table_id.split(".")[0]
    with open(schema_path, "r") as f:
        schema_fields = json.load(f)
    bq_client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema_fields,
        create_disposition="CREATE_IF_NEEDED"
    )
    job = bq_client.load_table_from_json(destination=table_id, json_rows=json_rows, job_config=job_config)

def download_year(year, save_files=False, debug=False):
    if year > 2019:
        url = f'https://www.servir.gob.pe/rectoria/informes-legales/listado-de-informes-legales/informes-tecnicos-{year}/' 
    else:
        url = f'https://www.servir.gob.pe/rectoria/informes-legales/listado-de-informes-legales/informes-{year}/' 
    print(url)
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        html_content = response.text  # The HTML content is stored in response.text
    else:
        print(f"Failed to fetch HTML content. Status code: {response.status_code}")
        exit()

    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Initialize an empty list to store the extracted data
    data_list = []

    # Temporary directory to store PDF files
    # temp_dir = tempfile.mkdtemp()
    temp_dir = "temp"

    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    tbody = soup.find('tbody')
    rows = tbody.find_all('tr')

    # Iterate over all <tr> elements within the <tbody>
    for row_num, row in enumerate(rows):
        informe = row.find('a').get_text()
        if debug:
            print(f"{informe}")
        informe_url = row.find('a')['href']
        try:
            institucion = row.find('b', string='Institución:').find_next_sibling(string=True).strip()
        except:
            institucion = ""
        try:
            asunto = row.find('b', string=lambda s: s.startswith('Asunto:') if s else False).find_next_sibling(string=True).strip()
        except:
            try:
                asunto = row.find('b', string=lambda s: s.startswith('Asunto:') if s else False)
                asunto = asunto[len("Asunto:"):].strip()
            except:
                asunto = ""
        try:
            fecha = row.find('b', string=lambda s: s.startswith('Fecha:')).find_next_sibling(string=True).strip()
            fecha = datetime.strptime(fecha, '%d/%m/%Y').strftime('%Y-%m-%d')
        except:
            fecha = data_list[row_num-1]["fecha"]
        try:
            oficio = row.find('b', string='Oficio N°:').find_next('a').get_text()
            oficio_url = row.find('b', string='Oficio N°:').find_next('a')["href"]
        except:
            oficio = ""
            oficio_url = ""
        # Append the extracted data to the list
        data_list.append({
            "fecha": fecha,
            "informe": informe,
            "informe_url": informe_url,
            "informe_gcs": "", #should be at least "" because the field is required
            "institucion": institucion,
            "asunto": asunto,
            "oficio": oficio,
            "oficio_url": oficio_url,
            "oficio_gcs": "" #should be at least "" because the field is required
            }
            )

    # Print the DataFrame
    print(len(data_list))

    save_json_to_bq_partitioned(json_rows = data_list, 
                                table_id = f"{PROJECT_ID}.{TABLE_NAME}", 
                                schema_path = "servir_informe_legales.json")

    # Iterate through each file in the temporary directory
    if save_files:
        for dc in data_list:
            filename = dc["informe_url"].split("/")[-1]
            pdf_filepath = os.path.join(temp_dir, filename)
            pdf_gcspath = os.path.join(FOLDER_IF_PATH, filename)
            while True:
                pdf_response = requests.get(dc["informe_url"])
                if len(pdf_response.content) > 1000:
                    with open(pdf_filepath, 'wb') as pdf_file:
                        pdf_file.write(pdf_response.content)
                    time.sleep(1)
                    break
                else:
                    print("sleeping 10 second")
                    time.sleep(10)
            
            blob = bucket.blob(pdf_gcspath)
            blob.upload_from_filename(pdf_filepath)
            os.remove(pdf_filepath)