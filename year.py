from utils import upload_to_cloud_storage, save_json_to_bq_partitioned,download_year

# Make an HTTP request to fetch the HTML content
download_year(2013, save_files=False, debug=True)

    # for filename in os.listdir(temp_dir):
    #     if filename.endswith(".pdf"):
    #         pdf_filepath = os.path.join(temp_dir, filename)

    #         # Upload the PDF to Google Cloud Storage
    #         blob = bucket.blob(os.path.join(FOLDER_IF_PATH, filename))
    #         blob.upload_from_filename(pdf_filepath)

    #         print(f"Uploaded PDF to Cloud Storage: {filename}")

# # Clean up the temporary directory
# shutil.rmtree(temp_dir)

# # Define the column names for the DataFrame
# columns = ['Informe', 'Link', 'Institución', 'Asunto', 'Fecha', 'Oficio', 'Oficio Url']

# # Create a Pandas DataFrame from the extracted data
# df = pd.DataFrame(columns=columns)

# # Iterate over the list of data and create DataFrames to each row
# dfs = []
# for row_data in data_list:
#     if len(row_data) == len(columns):
#         row_df = pd.DataFrame([row_data], columns=columns)
#         dfs.append(row_df)

# # Concatenate the DataFrames
# if dfs:
#     df = pd.concat(dfs, ignore_index=True)



