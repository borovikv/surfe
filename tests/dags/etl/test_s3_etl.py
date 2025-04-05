import datetime
import io
from unittest.mock import MagicMock

import awswrangler.s3
import pandas as pd

import dags.etl.s3_etl as subject
from dags.etl.utils import abs_path

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)


def test_convert_to_parquet(bucket):
    name = 'part-00152-37fec780-2ce1-4ccb-b095-72ca81e8094e.c001'
    path = f's3://{bucket}/company/202504/partition_by_column=UK/{name}.json.gz'
    with open(abs_path(f'input/{name}.json.gz'), 'br') as f:
        awswrangler.s3.upload(f, path=path)
    subject.convert_to_parquet(input_path=path)

    result = awswrangler.s3.read_parquet(f's3://a-bucket/results/company/202504/partition_by_column=UK/{name}.parquet')
    assert result.shape == (463, 163)


def test_get_unprocessed_files(bucket):
    paths = [
        f's3://{bucket}/company/202504/partition_by_column=UK/abra.json.gz',
        f's3://{bucket}/company/202504/partition_by_column=FR/cadabra.json.gz',
        f's3://{bucket}/company/202504/partition_by_column=UK/hocus.json.gz',
        f's3://{bucket}/company/202504/partition_by_column=IT/pocus.json.gz'
    ]
    for path in paths:
        awswrangler.s3.upload(io.BytesIO('anything'.encode('utf-8')), path=path)

    output_paths = [
        f's3://{bucket}/results/company/202504/partition_by_column=UK/abra.json.gz',
        f's3://{bucket}/results/company/202504/partition_by_column=UK/hocus.json.gz',
    ]
    for path in output_paths:
        awswrangler.s3.upload(io.BytesIO('anything'.encode('utf-8')), path=path)

    result = subject.get_unprocessed_files(
        data_interval_start=datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1),
        data_interval_end=datetime.datetime.now(datetime.UTC),
        bucket=bucket
    )

    expected = [
        f's3://{bucket}/company/202504/partition_by_column=FR/cadabra.json.gz',
        f's3://{bucket}/company/202504/partition_by_column=IT/pocus.json.gz'
    ]

    assert result == expected


def test_base_file_name():
    path = 's3://bucket/company/202504/partition_by_column=FR/cadabra.json.gz'
    result = subject.base_file_name(path, start=2, ext='json.gz')
    assert result == 'bucket/company/202504/partition_by_column=FR/cadabra.'
    result = subject.base_file_name(path, start=3, ext='json.gz')
    assert result == 'company/202504/partition_by_column=FR/cadabra.'


def test_write_to_postgres(bucket):
    df = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob'],
        'age': [25, 30]
    })
    s3_path = f's3://{bucket}/some/path/name.json'
    awswrangler.s3.to_json(df, s3_path, lines=True, orient='records')

    subject.write_to_postgres(MagicMock(xcom_pull=lambda *_: [s3_path]))

    df_from_db = pd.read_sql_table('temp_company_info', con=subject.get_engine('postgres'))

    pd.testing.assert_frame_equal(
        df_from_db.sort_values('id').reset_index(drop=True),
        df.sort_values('id').reset_index(drop=True)
    )
