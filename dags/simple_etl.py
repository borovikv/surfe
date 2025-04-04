import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from etl.s3_etl import get_unprocessed_files


def notify_failure(context):
    slack_msg = f"""
        :red_circle: Task Failed.
        *Task*: {context['task_instance'].task_id}
        *Dag*: {context['task_instance'].dag_id}
        *Execution Time*: {context['execution_date']}
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


with DAG(
        "simple_etl",
        timetable=CronDataIntervalTimetable('0 0 * * *', timezone='UTC'),
        default_args={'on_failure_callback': notify_failure},
        start_date=datetime.datetime(2025, 4, 1),
        catchup=False,
) as dag:
    PythonOperator(
        task_id="get_unprocessed_files",
        python_callable=get_unprocessed_files,
        op_kwargs={'bucket': 'a-bucket'},
        retries=2,
        retry_delay=datetime.timedelta(seconds=300),
    )
