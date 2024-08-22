import pytest
import re

from airflow.models import TaskInstance, DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.utils import timezone
from airflow.utils.session import create_session
from unittest.mock import patch, MagicMock


# Import your DAG
from dags.algolia_etl import algolia_etl


@pytest.fixture(scope="module")
def dag():
    return algolia_etl()


def create_dagrun(dag):
    execution_date = timezone.datetime(2019, 4, 1, tzinfo=dag.timezone)
    with create_session() as session:
        existing_dagrun = session.query(DagRun).filter(
            DagRun.dag_id == dag.dag_id,
            DagRun.execution_date == execution_date
        ).first()

        if existing_dagrun:
            return existing_dagrun

        dagrun = DagRun(
            dag_id=dag.dag_id,
            execution_date=execution_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
            run_type=DagRunType.MANUAL,
            conf=None,
            external_trigger=False
        )
        session.add(dagrun)
        session.commit()
        return dagrun


def normalize_sql(sql):
    """Remove extra whitespace and normalize SQL for comparison."""
    return re.sub(r'\s+', ' ', sql).strip()


@patch('airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.execute')
@patch('airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.render_template_fields')
def test_create_table(mock_render_template_fields, mock_execute, dag):
    """Test the create table task to ensure SQLExecuteQueryOperator's execute is called."""
    mock_render_template_fields.return_value = None  # Skip rendering

    # Setting __name__ attribute to avoid AttributeError
    mock_execute.__name__ = 'execute'

    create_table = dag.get_task('create_table')
    dagrun = create_dagrun(dag)
    ti = TaskInstance(task=create_table, run_id=dagrun.run_id)
    ti.set_state(State.NONE)
    ti.run(ignore_ti_state=True)

    mock_execute.assert_called_once()


@patch('boto3.client')
@patch('airflow.models.Variable.get')
def test_extract(mock_get, mock_boto, dag):
    """Test the extract task."""
    mock_get.return_value = "./alg-data-public"
    mock_s3 = mock_boto.return_value
    mock_s3.download_file = MagicMock()

    extract = dag.get_task('extract')
    ti = TaskInstance(task=extract, execution_date=dag.start_date)
    ti.set_state(State.NONE)
    ti.run(ignore_ti_state=True, ignore_all_deps=True)

    mock_s3.download_file.assert_called_once_with(
        Bucket='alg-data-public', Key='2019-04-01.csv', Filename='./alg-data-public/2019-04-01.csv'
    )


@patch('pandas.read_csv')
@patch('pandas.DataFrame.to_csv')
@patch('os.makedirs')
def test_transform(mock_makedirs, mock_to_csv, mock_read_csv, dag):
    """Test the transform task to ensure read_csv and to_csv are called."""

    # Mocking read_csv to return a simple DataFrame when called
    mock_df = MagicMock()
    mock_read_csv.return_value = mock_df

    transform = dag.get_task('transform')
    ti = TaskInstance(task=transform, execution_date=dag.start_date)
    ti.set_state(State.NONE)
    ti.run(ignore_ti_state=True, ignore_all_deps=True)

    # Ensure that read_csv is called once
    mock_read_csv.assert_called_once()

    # Ensure that to_csv is called once on the DataFrame returned by read_csv
    mock_df.to_csv.assert_called_once()


@patch('pandas.read_csv')
@patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn')
@patch('builtins.open', new_callable=MagicMock)
@patch('airflow.models.Variable.get')
def test_load(mock_variable_get, mock_open, mock_get_conn, mock_read_csv, dag):
    """Unit test for the load task in the DAG."""

    # Mock the return value of the Variable.get to set the path
    mock_variable_get.return_value = './filtered_csvs'

    # Mock the DataFrame returned by read_csv
    mock_df = MagicMock()
    mock_df.columns = ['column1', 'column2']
    mock_df.to_dict.return_value = [{'column1': 'value1', 'column2': 'value2'}]
    mock_read_csv.return_value = mock_df

    # Mock the SQL file content
    mock_open.return_value.__enter__.return_value.read.return_value = "INSERT INTO final_table SELECT * FROM temp_shopify_table_{formatted_date};"

    # Mock the connection and cursor
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    # Retrieve the task from the DAG
    load_task = dag.get_task('load')

    # Set up and run the TaskInstance
    ti = TaskInstance(task=load_task, execution_date=dag.start_date)
    ti.xcom_push = MagicMock()  # Mock xcom_push if the task uses XComs
    ti.run(ignore_ti_state=True, ignore_all_deps=True)

    # Ensure the Variable.get was called correctly
    mock_variable_get.assert_called_once_with(
        "filtered_data_path", default_var="./filtered_csvs")

    # Ensure the correct CSV file path was used
    mock_read_csv.assert_called_once_with(
        './filtered_csvs/filtered_2019-04-01.csv')

    # Ensure the SQL file was read correctly
    mock_open.assert_called_once_with('./dags/sql/load_csv.sql', 'r')

    # Prepare expected SQL strings and normalize them
    expected_create_temp_sql = normalize_sql(
        "CREATE TEMP TABLE temp_shopify_table_2019_04_01 (LIKE shopify_data);")
    expected_insert_to_temp_tbl_sql = normalize_sql(
        "INSERT INTO temp_shopify_table_2019_04_01 (column1, column2) VALUES (%s, %s)")
    expected_insert_data_sql = normalize_sql(
        "INSERT INTO final_table SELECT * FROM temp_shopify_table_2019_04_01;")

    # Retrieve actual SQL strings and normalize them
    actual_create_temp_sql = normalize_sql(
        mock_cursor.execute.call_args_list[0][0][0])
    actual_insert_to_temp_tbl_sql = normalize_sql(
        mock_cursor.executemany.call_args_list[0][0][0])
    actual_insert_data_sql = normalize_sql(
        mock_cursor.execute.call_args_list[1][0][0])

    # Ensure the correct SQL commands were executed
    assert actual_create_temp_sql == expected_create_temp_sql, f"Expected {expected_create_temp_sql}, but got {actual_create_temp_sql}"  # nopep8
    assert actual_insert_to_temp_tbl_sql == expected_insert_to_temp_tbl_sql, f"Expected {expected_insert_to_temp_tbl_sql}, but got {actual_insert_to_temp_tbl_sql}"  # nopep8
    assert actual_insert_data_sql == expected_insert_data_sql, f"Expected {expected_insert_data_sql}, but got {actual_insert_data_sql}"  # nopep8

    # Ensure that executemany is called with the correct data
    mock_cursor.executemany.assert_called_once_with(
        expected_insert_to_temp_tbl_sql, [('value1', 'value2')]
    )
