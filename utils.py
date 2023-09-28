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
    import pandas as pd 
    import concurrent.futures
    import time

    df = pd.DataFrame(json_rows)
    df_group = df.groupby("fecha")
    with open(schema_path, "r") as f:
        schema_fields = json.load(f)
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        futures = []
        for fecha, df in df_group:
            futures.append(executor.submit(load_data_to_bigquery,
                                           df.to_dict(orient="records"), 
                                           f"{table_id}${fecha.replace('-','')}", 
                                           schema_fields))
        start_time = time.monotonic()
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error loading data to BigQuery: {e}")
        end_time = time.monotonic()
        duration = end_time - start_time
        print(f"Loaded all data in {duration:.2f} seconds")

def load_data_to_bigquery(json_rows, table_id, schema):
    project_id=table_id.split(".")[0]
    bq_client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha",
        ),
    )
    job = bq_client.load_table_from_json(destination=table_id, 
                                         json_rows=json_rows, 
                                         job_config=job_config)
    job.result()

def get_last_date_and_year():
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    SELECT MAX(fecha) AS last_date
    FROM `{PROJECT_ID}.{TABLE_NAME}`
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    last_date = None
    for row in result:
        last_date = row.last_date
    if last_date:
        last_year = last_date.year
        return last_date, last_year
    else:
        return None, None

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
    data_list = []
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('tbody')
    # while True:
    #     tables = soup.findall('tbody')
    #     if table[0]:
    #         break
    #     else:
    #         time.sleep(5)
    for table in tables:
        rows = table.find_all('tr')

        # Iterate over all <tr> elements within the <tbody>
        for row_num, row in enumerate(rows):
            informe = row.find('a').get_text()
            if debug:
                print(f"{informe}")
            informe_url = row.find('a')['href']
            try:
                institucion = row.find('b', string=lambda s: s.startswith('Institución:')).find_next_sibling(string=True).strip()
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
    print(len(data_list))

    save_json_to_bq_partitioned(json_rows = data_list, 
                                table_id = f"{PROJECT_ID}.{TABLE_NAME}", 
                                schema_path = "servir_informe_legales.json")
    if save_files:
        download_files_from_list(data_list)

def download_files_from_list(ls_files):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    # temp_dir = tempfile.mkdtemp()
    temp_dir = "temp"

    for dc in ls_files:
        # Informe PDFs
        informe_filename = dc["informe_url"].split("/")[-1]
        informe_pdf_filepath = os.path.join(temp_dir, informe_filename)
        informe_pdf_gcspath = os.path.join(FOLDER_IF_PATH, informe_filename)
        while True:
            pdf_response = requests.get(dc["informe_url"])
            if len(pdf_response.content) > 1000:
                with open(informe_pdf_filepath, 'wb') as pdf_file:
                    pdf_file.write(pdf_response.content)
                time.sleep(1)
                break
            else:
                print("sleeping 10 second")
                time.sleep(10)
        
        blob = bucket.blob(informe_pdf_gcspath)
        blob.upload_from_filename(informe_pdf_filepath)
        os.remove(informe_pdf_filepath)

        # Oficio PDFs
        if dc["oficio_url"] == "":
            continue
        oficio_filename = dc["oficio_url"].split("/")[-1]
        oficio_pdf_filepath = os.path.join(temp_dir, oficio_filename)
        oficio_pdf_gcspath = os.path.join(FOLDER_IF_PATH, oficio_filename)
        while True:
            pdf_response = requests.get(dc["oficio_url"])
            if len(pdf_response.content) > 1000:
                with open(oficio_pdf_filepath, 'wb') as pdf_file:
                    pdf_file.write(pdf_response.content)
                time.sleep(1)
                break
            else:
                print("sleeping 10 second")
                time.sleep(10)

        blob = bucket.blob(oficio_pdf_gcspath)
        blob.upload_from_filename(oficio_pdf_filepath)
        os.remove(oficio_pdf_filepath)

def download_since_last_date(save_files=False, debug=False):

    last_date, last_year = get_last_date_and_year() # read in bigquery

    if last_date and last_year:
        print("Last Date in BigQuery:", last_date)
        print("Year of the Last Date:", last_year)
    else:
        print("Failed to retrieve last date and year.")

    ls_data = download_year(last_year, save_files=False, debug=True)

    ls_filtered = []
    for dc in ls_data:
        if dc["Fecha"] >= last_date:
            ls_filtered.append(dc)
    save_json_to_bq_partitioned(json_rows = ls_filtered, 
                                table_id = f"{PROJECT_ID}.{TABLE_NAME}", 
                                schema_path = "servir_informe_legales.json")
    if save_files:
        download_files_from_list(ls_filtered)
