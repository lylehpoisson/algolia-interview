import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

start_date = pendulum.datetime(2019, 4, 1, tz='UTC')
end_date = pendulum.datetime(2019, 4, 8, tz='UTC')


@dag(start_date=start_date,
     end_date=end_date,
     schedule='0 2 * * *',
     catchup=True,
     max_active_runs=1)
def algolia_etl():
    """
    ETL pipeline for processing incoming Shopify data.

    This DAG performs the following steps:
    1. Creates the destination table in the PostgreSQL database if it doesn't currently exist.
    2. Extracts data from an S3 bucket for a given day.
    3. Transforms the data by filtering and modifying columns.
    4. Loads the transformed data into the destination table.

    The DAG skips processing if the data file for a specific day is not found in the S3 bucket.
    """

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        sql='sql/create_table.sql',
        conn_id="algolia_warehouse",)

    @task
    def extract(full_date: str):
        """
        Attempts to download a specific CSV file from a public S3 bucket based on
        a given date string.

        This function retrieves a CSV file with a name format YYYY-MM-DD.csv from the AWS S3 bucket
        'alg-data-public'. If the file exists, it is downloaded to the directory specified
        by the Airflow variable 'raw_data_path'. If the file does not exist, the function
        logs a message, skips downstream tasks by raising an AirflowSkipException, and does not
        proceed to any further actions.

        Args:
            full_date (str): A string representing the date of the file to download. This string
                            should be formatted as 'YYYY-MM-DD' to match the expected filename.

        Raises:
            AirflowSkipException: If the file corresponding to `full_date` does not exist in the
                                S3 bucket.
            ClientError: If any other S3 client error occurs during the operation.

        Example:
            # Attempts to download '2019-04-01.csv' from 'alg-data-public' bucket.
            extract('2019-04-01')
        """
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config
        from botocore.exceptions import ClientError

        # Create the local directory if it doesn't already exist
        raw_data_path = Variable.get(
            "raw_data_path", default_var="./alg-data-public")
        os.makedirs(raw_data_path, exist_ok=True)

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        bucket_name = 'alg-data-public'
        key = f'{full_date}.csv'
        local_path = f'{raw_data_path}/{key}'
        try:
            s3.head_object(Bucket=bucket_name, Key=key)
            s3.download_file(Bucket=bucket_name, Key=key, Filename=local_path)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # File not found, log a message and skip downstream tasks
                print(f"File for day {full_date} not found, skipping downstream tasks.")
                raise AirflowSkipException(
                    f"File for day {full_date} not found.")
            else:
                raise

    @task
    def transform(full_date: str):
        """
        Processes a CSV file by reading it, cleaning data, and applying transformations
        to prepare it for database load. Makes small changes for postgres compatability,
        removes rows with empty 'application_id' values, and creates the 'has_specific_prefix'
        column based on the contents of the 'index_prefix' column.

        Args:
            full_date (str): The date string corresponding to the filename. It should be formatted
                            as 'YYYY-MM-DD', which is used to construct the file path for reading
                            and writing CSV files.

        Example:
            # Processes '2019-04-01.csv' from raw data and outputs to filtered data.
            transform('2019-04-01')

        Note:
            The function assumes the presence of specific columns ('application_id', 'index_prefix',
            'nbr_metafields', 'nbrs_pinned_items') in the input data. It also assumes that the
            'nbrs_pinned_items' column is formatted as an array in string format enclosed in brackets.
        """
        import pandas as pd

        raw_data_path = Variable.get(
            "raw_data_path", default_var="./alg-data-public")
        filtered_data_path = Variable.get(
            "filtered_data_path", default_var="./filtered_csvs")
        raw_file_name = f'{raw_data_path}/{full_date}.csv'
        df = pd.read_csv(raw_file_name)
        # This helps handle null values
        df['nbr_metafields'] = df['nbr_metafields'].astype('Int64')
        # We need to change [] to {} for the list column to make it Postgres compatible
        df['nbrs_pinned_items'] = df['nbrs_pinned_items'].apply(
            lambda x: '{' + x.strip('[]') + '}')

        df.dropna(subset=['application_id'], inplace=True)
        df['has_specific_prefix'] = df['index_prefix'] == 'shopify_'

        # Create these directories if they don't already exist
        os.makedirs(filtered_data_path, exist_ok=True)

        out_file_name = f'{filtered_data_path}/filtered_{full_date}.csv'
        df.to_csv(out_file_name, index=False)

    @task
    def load(full_date: str):
        """
        Loads transformed data from a CSV file into a PostgreSQL database using a temporary table
        that mirrors the structure of the main table. The function reads the CSV file, creates
        a temporary table, copies data from the CSV to the temporary table, and then inserts the
        contents of the temporary table into the main table.

        Args:
            full_date (str): A date string in the format 'YYYY-MM-DD', used to identify the CSV
                            file that contains the data to be loaded.

        Example:
            # Loads data from 'filtered_2019-04-01.csv' into a PostgreSQL database.
            load('2019-04-01')

        Notes:
            - This function assumes the presence of a file named 'load_csv.sql' which contains
            SQL commands to be executed after data insertion. The path './dags/sql/load_csv.sql'
            must be accessible.
            - This function also assumes that database connection configurations are properly set
            in the Airflow environment.
        """
        import pandas as pd

        filtered_data_path = Variable.get(
            "filtered_data_path", default_var="./filtered_csvs")
        filtered_csv_path = f'{filtered_data_path}/filtered_{full_date}.csv'

        df = pd.read_csv(filtered_csv_path)
        df_records = df.to_dict(orient='records')
        underscore_date = full_date.replace('-', '_')
        # the joins and splits are to ensure the unit tests check content and not formatting
        create_temp_table_sql = f"""
                CREATE TEMP TABLE temp_shopify_table_{underscore_date}
                (LIKE client_ingests.shopify_data);
                """
        insert_to_temp_tbl_sql = f"""
                INSERT INTO temp_shopify_table_{underscore_date} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))})
                """

        create_temp_table_sql = ' '.join(create_temp_table_sql.split())
        insert_to_temp_tbl_sql = ' '.join(insert_to_temp_tbl_sql.split())

        with open('./dags/sql/load_csv.sql', 'r') as load_csv_sql_file:
            insert_data_sql = load_csv_sql_file.read().format(
                formatted_date=underscore_date)
        hook = PostgresHook(postgres_conn_id='algolia_warehouse')
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_temp_table_sql)
                cursor.executemany(insert_to_temp_tbl_sql, [
                                   tuple(row.values()) for row in df_records])
                cursor.execute(insert_data_sql)
            conn.commit()

    create_step = create_table
    full_date = '{{ ds }}'
    extract_step = extract(full_date)
    transform_step = transform(full_date)
    load_step = load(full_date)
    create_step >> extract_step >> transform_step >> load_step


etl_instance = algolia_etl()
