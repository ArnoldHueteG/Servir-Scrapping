from google.cloud import bigquery

PROJECT_ID = "xenon-world-399922"
TABLE_NAME = ""
ls_data = []

table_id = f"{PROJECT_ID}.{TABLE_NAME}"
schema_path = "servir_informe_legales.json"
with open(schema_path,"r") as f:
    schema_fields = json.load(f)
bq_client = bigquery.Client(project=PROJECT_ID)
job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema_fields,
            create_disposition="CREATE_IF_NEEDED"
        )
job = bq_client.load_table_from_json(destination=table_id, json_rows=ls_data, job_config=job_config)