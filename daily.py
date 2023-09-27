from upload_pdf import upload_to_cloud_storage


last_date = get_last_date() # read in bigquery
ls_data = download_year(year(last_date), save_files=False, debug=True)

ls_filtered = []
for dc in ls_data:
    if dc["Fecha"] >= last_date:
        ls_filtered.append(dc)

save_to_bigquery(ls_filtered)

if save_files=True:
    get_files(ls_filtered)