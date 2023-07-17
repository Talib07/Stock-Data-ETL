from google.cloud import bigquery
import os
import pandas as pd

def load_bigquery(dataframe, dataset, symbol):
    directory = os.path.dirname(os.path.abspath(__file__))
    key_file_path = os.path.join(directory,'key.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file_path

    # Set the project ID and location
    project_id = 'stock-etl-391614'
    dataset_id = dataset

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Check if the dataset already exists
    dataset_ref = client.dataset(dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists.")
    except:
        # Create a new dataset
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' created.")

    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = dataframe.toPandas()

    # Store tables in BigQuery
    table_name = f"{symbol}_stocks"
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.LoadJobConfig()

    # Write the DataFrame to BigQuery
    job = client.load_table_from_dataframe(pandas_df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Table '{table_name}' created in BigQuery.")

