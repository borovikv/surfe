import glob
import logging
import os.path
import re

import awswrangler.s3
import boto3
import sqlalchemy


def start_mock_s3(bucket, data_interval_start):
    client = boto3.client('s3', endpoint_url='http://moto:3000')
    client.create_bucket(Bucket=bucket)
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    for p in glob.glob(os.path.join(base_path, 'input/*.json.gz')):
        name = os.path.basename(p)
        client.upload_file(Filename=p, Bucket=bucket, Key=f"company/{data_interval_start.strftime("%Y%m")}/{name}")


def get_unprocessed_files(data_interval_start, data_interval_end, bucket):
    input_path = f's3://{bucket}/company/{data_interval_start.strftime("%Y%m")}/'
    output_path = f's3://{bucket}/results/company/{data_interval_start.strftime("%Y%m")}/'
    time_window = dict(
        last_modified_begin=data_interval_start,
        # Uncomment when running in a real environment.
        # This limits the number of files processed per task run,
        # which can be important during backfilling.
        # last_modified_end=data_interval_end,
    )

    output_files = awswrangler.s3.list_objects(path=output_path, **time_window)
    processed_files = [base_file_name(str(x), start=4, ext='parquet') + 'json.gz' for x in output_files]
    input_files = awswrangler.s3.list_objects(
        path=input_path,
        ignore_suffix=processed_files,
        suffix='.json.gz',
        **time_window,
    )

    return input_files


def base_file_name(x: str, start: int, ext: str):
    return x.split('/', maxsplit=start)[-1][:-len(ext)]


def convert_files(task_instance):
    result = []
    for path in task_instance.xcom_pull('get_unprocessed_files'):
        result.append(convert_to_parquet(path))
    return result


def convert_to_parquet(input_path: str):
    logging.info(f'Start processing {input_path}')
    df = awswrangler.s3.read_json(input_path, lines=True)
    path_components = re.match(r'^s3://(?P<bucket>[^/]+)/(?P<file_name>.+)\.json\.gz$', input_path).groupdict()

    output_path = 's3://{bucket}/results/{file_name}.parquet'.format(**path_components)
    logging.info(f'Writing DataFrame {df.shape} to {output_path}')
    awswrangler.s3.to_parquet(df, path=output_path)
    return output_path


def write_to_postgres(task_instance):
    files = task_instance.xcom_pull('get_unprocessed_files')
    if not files:
        logging.warning('Nothing to save')
        return

    df = awswrangler.s3.read_json(files, lines=True)
    if df.empty:
        logging.warning('Nothing to save')
        return

    dtype = {}
    for c in df.columns:
        first_valid_idx = df[c].first_valid_index()
        if first_valid_idx is not None and isinstance(df.at[first_valid_idx, c], (dict, list)):
            dtype[c] = sqlalchemy.types.JSON

    db_name = 'postgres'
    table_name = 'temp_company_info'
    engine = get_engine(db_name)
    df.to_sql(name=table_name, con=engine, index=False, if_exists='replace', dtype=dtype)

    logging.info(f"Data was written successfully to '{table_name}' table in '{db_name}' database!")


def get_engine(db_name):
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = 5432
    connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = sqlalchemy.create_engine(connection_string)
    return engine


def upsert(engine):
    # Perform bulk upsert using SQL
    table_name = 'result_company_info'
    bulk_upsert_query = f"""
        INSERT INTO {table_name} (id, name, age)
        SELECT id, name, age FROM temp_table
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            age = EXCLUDED.age;
    """
    with engine.connect() as connection:
        connection.execute(sqlalchemy.text(bulk_upsert_query))
    logging.info("Bulk upsert completed successfully!")
