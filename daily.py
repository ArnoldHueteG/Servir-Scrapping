from utils import upload_to_cloud_storage, save_json_to_bq_partitioned, download_year, get_last_date_and_year

project_id = "xenon-world-399922"
table_id = "xenon-world-399922.Servir.InformesLegales"

last_date, last_year = get_last_date_and_year(project_id, table_id) # read in bigquery

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

json_rows = ls_filtered
schema_path = "servir_informe_legales.json"
save_json_to_bq_partitioned(ls_filtered, table_id, schema_path)


