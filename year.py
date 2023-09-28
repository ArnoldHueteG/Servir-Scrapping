from utils import upload_to_cloud_storage, save_json_to_bq_partitioned,download_year

# for year in range(2023, 2009, -1):
#     print(year)
#     download_year(year, save_files=False, debug=False)
download_year(2023, save_files=True, debug=True)


