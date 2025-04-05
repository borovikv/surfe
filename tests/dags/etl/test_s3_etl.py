import datetime
import io
from unittest.mock import MagicMock

import awswrangler.s3
import jinja2
import pandas as pd
from sqlalchemy import inspect

import dags.etl.s3_etl as subject
from dags.etl.utils import abs_path


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


def test_execute_sql_with_upsert_company_info(bucket):
    name = 'part-00152-37fec780-2ce1-4ccb-b095-72ca81e8094e.c001'
    s3_path = f's3://{bucket}/company/202504/partition_by_column=UK/{name}.json.gz'
    file_path = abs_path(f'input/{name}.json.gz')
    with open(file_path, 'br') as f:
        awswrangler.s3.upload(f, path=s3_path)
    subject.write_to_postgres(MagicMock(xcom_pull=lambda *_: [s3_path]))

    subject.execute_sql(query='drop table if exists test_table;')
    engine = subject.get_engine('postgres')

    main_table = 'test_table'
    assert main_table not in inspect(engine).get_table_names(schema='public')

    with open(abs_path('dags/etl/upsert_company_info.sql')) as f:
        query = jinja2.Template(f.read()).render(temp_table_name=subject.TEMP_TABLE_NAME, main_table=main_table)

    subject.execute_sql(query=query)
    assert main_table in inspect(engine).get_table_names(schema='public')

    df_from_db = pd.read_sql_table(main_table, con=engine)
    assert df_from_db.shape == pd.read_json(file_path, lines=True).shape
