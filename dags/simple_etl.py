import datetime
import os.path
from unittest.mock import patch

from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.context import Context

import etl.s3_etl as e
from etl.local_mocks import list_objects, read_json, to_parquet


def notify_failure(context):
    slack_msg = f"""
        :red_circle: Task Failed.
        *Dag*: {context['task_instance'].dag_id}
        *Task*: {context['task_instance'].task_id}
        *Execution Time*: {context['logical_date']}
        *Log Url*: {context['task_instance'].log_url}
    """
    # alert = SlackWebhookOperator(
    #     task_id='slack_alert',
    #     http_conn_id='slack_connection',
    #     message=slack_msg,
    #     username='airflow'
    # )
    # return alert.execute(context=context)
    print(slack_msg)


def wrapped_get_unprocessed_files(data_interval_start, data_interval_end, bucket):
    """
    The wrapped method is created to make the code runnable in a local Docker container with a Moto server.
    """
    with patch('awswrangler.s3.list_objects', side_effect=list_objects):
        return e.get_unprocessed_files(data_interval_start, data_interval_end, bucket)


def wrapped_convert_files(task_instance):
    """
    The wrapped method is created to make the code runnable in a local Docker container with a Moto server.
    """
    with patch('awswrangler.s3.read_json', side_effect=read_json):
        with patch('awswrangler.s3.to_parquet', side_effect=to_parquet):
            return e.convert_files(task_instance)


def wrapped_write_to_postgres(task_instance):
    """
    The wrapped method is created to make the code runnable in a local Docker container with a Moto server.
    """
    with patch('awswrangler.s3.read_json', side_effect=read_json):
        return e.write_to_postgres(task_instance)


class SQLOperator(BaseOperator):
    ui_color = '#44b5e2'
    template_fields = ('query',)
    template_ext = ('.sql',)
    template_fields_renderers = {'query': 'sql'}

    def __init__(self, *, query, **kwargs):
        self.query = query
        if 'task_id' not in kwargs:
            kwargs['task_id'] = os.path.basename(query).split('.')[0]
        super().__init__(**kwargs)

    def execute(self, context: Context):
        e.execute_sql(self.query)


with DAG(
    "simple_etl",
    timetable=CronDataIntervalTimetable('0 0 * * *', timezone='UTC'),
    default_args={'on_failure_callback': notify_failure},
    start_date=datetime.datetime(year=2025, month=4, day=1),
    catchup=False,
    user_defined_macros={
        'temp_table_name': e.TEMP_TABLE_NAME,
        'main_table': e.MAIN_TABLE_NAME,
    }
) as dag:
    PythonOperator(
        task_id='start_mock_s3',
        python_callable=e.start_mock_s3,
        op_kwargs={'bucket': e.BUCKET_NAME},
    ) >> PythonOperator(
        task_id='get_unprocessed_files',
        python_callable=wrapped_get_unprocessed_files,
        op_kwargs={'bucket': e.BUCKET_NAME},
        retries=2,
        retry_delay=datetime.timedelta(seconds=300),
    ) >> PythonOperator(
        task_id='convert_files',
        python_callable=wrapped_convert_files,
        retries=2,
        retry_delay=datetime.timedelta(seconds=300),
    ) >> PythonOperator(
        task_id='write_to_postgres',
        python_callable=wrapped_write_to_postgres,
        retries=2,
        retry_delay=datetime.timedelta(seconds=300),
    ) >> BranchPythonOperator(
        task_id='check_upsert_required',
        python_callable=e.check_upsert_required
    ) >> SQLOperator(
        query='etl/upsert_company_info.sql'
    )
