import os
import pendulum
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable


start_date = pendulum.datetime(2019, 4, 1, tz='UTC')
end_date = pendulum.datetime(2019, 4, 7, tz='UTC')


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
        Extracts the CSV data file for a specific day from an S3 bucket.

        Parameters:
            day (str): The day of the month (formatted as '01', '02', etc.) to extract the data for.

        Raises:
            AirflowSkipException: If the data file for the given day is not found in the S3 bucket, 
            this exception is raised to skip downstream tasks.
        """
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config
        from botocore.exceptions import ClientError

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
                print(f"File for day {full_date} not found, skipping downstream tasks.")  # nopep8
                raise AirflowSkipException(
                    f"File for day {full_date} not found.")
            else:
                raise

    @task
    def transform(full_date: str):
        import pandas as pd
        raw_data_path = Variable.get(
            "raw_data_path", default_var="./alg-data-public")
        filtered_data_path = Variable.get(
            "filtered_data_path", default_var="./filtered_csvs")
        raw_file_name = f'{raw_data_path}/{full_date}.csv'
        df = pd.read_csv(raw_file_name)
        df.dropna(subset=['application_id'], inplace=True)
        df['has_specific_prefix'] = df['index_prefix'] == 'shopify_'
        df['nbr_metafields'] = df['nbr_metafields'].astype('Int64')
        # We need to change [] to {} for the list column to make it Postgres compatible
        df['nbrs_pinned_items'] = df['nbrs_pinned_items'].apply(
            lambda x: '{' + x.strip('[]') + '}')
        os.makedirs(filtered_data_path, exist_ok=True)
        out_file_name = f'{filtered_data_path}/filtered_{full_date}.csv'
        df.to_csv(out_file_name, index=False)

    @task
    def load(full_date: str):
        import pandas as pd
        filtered_data_path = Variable.get(
            "filtered_data_path", default_var="./filtered_csvs")
        filtered_csv_path = f'{filtered_data_path}/filtered_{full_date}.csv'
        df = pd.read_csv(filtered_csv_path)
        df_records = df.to_dict(orient='records')
        underscore_date = full_date.replace('-', '_')
        create_temp_table_sql = f'CREATE TEMP TABLE temp_shopify_table_{underscore_date} (LIKE shopify_data);'  # nopep8
        insert_to_temp_tbl_sql = ' '.join(f"""
                INSERT INTO temp_shopify_table_{underscore_date} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))})
                """.split())
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
