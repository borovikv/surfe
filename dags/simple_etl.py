import datetime
from unittest.mock import patch

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from etl.local_mocks import list_objects, read_json
import etl.s3_etl as e


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


BUCKET_NAME = 'a-bucket'


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
        return e.convert_files(task_instance)


with DAG(
        "simple_etl",
        timetable=CronDataIntervalTimetable('0 0 * * *', timezone='UTC'),
        default_args={'on_failure_callback': notify_failure},
        start_date=datetime.datetime(year=2025, month=4, day=1),
        catchup=False,
) as dag:
    PythonOperator(
        task_id='start_mock_s3',
        python_callable=e.start_mock_s3,
        op_kwargs={'bucket': BUCKET_NAME},
    ) >> PythonOperator(
        task_id="get_unprocessed_files",
        python_callable=wrapped_get_unprocessed_files,
        op_kwargs={'bucket': BUCKET_NAME},
        retries=2,
        retry_delay=datetime.timedelta(seconds=300),
    ) >> PythonOperator(
        task_id="convert_files",
        python_callable=wrapped_convert_files,
        retries=2,
        retry_delay=datetime.timedelta(seconds=300),
    )
