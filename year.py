import requests
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import storage, bigquery
import os
import tempfile
from pdfminer.pdfparser import  PDFParser
from pdfminer.pdfdocument import PDFDocument
import json

# Function to extract metadata from a PDF
def extract_metadata(pdf_path):
    with open(pdf_path, 'rb') as pdf_file:
        parser = PDFParser(pdf_file)
        pdf_document = PDFDocument(parser)
        metadata = pdf_document.info
        return metadata

# Function to upload PDF to Google Cloud Storage
def upload_to_cloud_storage(pdf_path, folder_if_path, bucket_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    destination_blob_name = os.path.join(folder_if_path, os.path.basename(pdf_path))
    blob.upload_from_filename(pdf_path)

# Make an HTTP request to fetch the HTML content
url = 'https://www.servir.gob.pe/rectoria/informes-legales/listado-de-informes-legales/informes-tecnicos-2023/'  # URL of the webpage containing the table
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    html_content = response.text  # The HTML content is stored in response.text
else:
    print(f"Failed to fetch HTML content. Status code: {response.status_code}")
    exit()

# Initialize Google Cloud Storage client
storage_client = storage.Client()
bucket_name = 'peru-servir'  # Google Cloud Storage bucket name
bucket = storage_client.bucket(bucket_name)
folder_if_path = 'informes-legales/2023/informe/'  # Google Cloud Storage folder path for Informes
folder_of_path = 'informers-legales/2023/oficio/'  # Google Cloud Storage folder path for Oficios

# Initialize an empty list to store the extracted data
data_list = []

# Initialize an empty list to store the metadata
metadata_list = []

# Temporary directory to store PDF files
temp_dir = tempfile.mkdtemp()

# Parse the HTML content
soup = BeautifulSoup(html_content, 'html.parser')

# Find the <tbody> element containing the <tr> elements
tbody = soup.find('tbody')

# Find all <tr> elements within the <tbody>
rows = tbody.find_all('tr')

# Iterate over all <tr> elements within the <tbody>
for row in rows:
    # Extract data from the current <tr> element
    informe_url = row.find('a')['href']
    link_text = row.find('a')
    informe = link_text.get_text()

    institution_text = row.find('b', string='Institución:').find_next_sibling(string=True).strip()
    asunto_text = row.find('b', string='Asunto:').find_next_sibling(string=True).strip()
    fecha = row.find('b', string='Fecha:').find_next_sibling(string=True).strip()

    # Download PDF and upload to Google Cloud Storage
    if informe_url.endswith(".pdf"):
        pdf_response = requests.get(informe_url)
        if pdf_response.status_code == 200:
            pdf_filename = os.path.join(temp_dir, informe_url.split("/")[-1])
            with open(pdf_filename, 'wb') as pdf_file:
                pdf_file.write(pdf_response.content)

            # Upload the PDF to Google Cloud Storage
            blob = bucket.blob(informe_url.split("/")[-1])
            blob.upload_from_filename(pdf_filename, folder_if_path)

            print(f"Uploaded PDF to Cloud Storage: {informe_url}")

            # Extract metadata from PDF
            metadata = extract_metadata(pdf_filename)
            metadata_list.append(metadata)

        else:
            print(f"Failed to download PDF: {informe_url}")

    # If 'Oficio N°' element exists, extract and download the PDF
    oficio_b_element = row.find('b', string='Oficio N°:')
    if oficio_b_element:
        a_element = oficio_b_element.find_next('a')
        if a_element:
            oficio_url = a_element['href']
        text_oficio = a_element.get_text()
        oficio_filename = os.path.join(temp_dir, oficio_url.split("/")[-1])
        oficio_response = requests.get(oficio_url)
        if oficio_response.status_code == 200:
            with open(oficio_filename, 'wb') as oficio_file:
                oficio_file.write(oficio_response.content)

            # Upload the PDF to Google Cloud Storage in the 'oficios' folder
            blob = bucket.blob(os.path.join(oficio_url.split("/")[-1]))
            blob.upload_from_filename(oficio_filename, folder_of_path)

            print(f"Uploaded PDF from Oficio: {oficio_url}")

            # Extract metadata from PDF
            metadata = extract_metadata(oficio_filename)
            metadata_list.append(metadata)

        else:
            print(f"Failed to download PDF from Oficio: {oficio_url}")

    # Append the extracted data to the list
    data_list.append([informe_url, informe, institution_text, asunto_text, fecha, text_oficio, url])

PROJECT_ID = "xenon-world-399922"
TABLE_NAME = "Servir.InformesLegales"

# Upload metadata
table_id = f"{PROJECT_ID}.{TABLE_NAME}"
schema_path = "servir_informe_legales.json"
with open(schema_path, "r") as f:
    schema_fields = json.load(f)
bq_client = bigquery.Client(project=PROJECT_ID)
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=schema_fields,
    create_disposition="CREATE_IF_NEEDED"
)
job = bq_client.load_table_from_json(destination=table_id, json_rows=metadata_list, job_config=job_config)

# Clean up the temporary directory
os.rmdir(temp_dir)

# Define the column names for the DataFrame
columns = ['Link', 'Informe', 'Institución', 'Asunto', 'Fecha', 'Oficio', 'Oficio Url']

# Create a Pandas DataFrame from the extracted data
df = pd.DataFrame(columns=columns)

# Iterate over the list of data and create DataFrames to each row
dfs = []
for row_data in data_list:
    if len(row_data) == len(columns):
        row_df = pd.DataFrame([row_data], columns=columns)
        dfs.append(row_df)

# Concatenate the DataFrames
if dfs:
    df = pd.concat(dfs, ignore_index=True)

# Print the DataFrame
print(df)
print("done")

