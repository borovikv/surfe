import re
from datetime import timedelta

import awswrangler.s3


def convert_to_parquet(input_path: str):
    df = awswrangler.s3.read_json(input_path, lines=True)
    path_components = re.match(r'^s3://(?P<bucket>[^/]+)/(?P<file_name>.+)\.json\.gz$', input_path).groupdict()
    output_path = 's3://{bucket}/results/{file_name}.parquet'.format(**path_components)
    awswrangler.s3.to_parquet(df, path=output_path)


def get_unprocessed_files(data_interval_start, data_interval_end, bucket):
    path = f's3://{bucket}/company/{data_interval_start.strftime("%Y%m")}/'
    output_path = f's3://{bucket}/results/company/{data_interval_start.strftime("%Y%m")}/'
    time_window = dict(
        last_modified_begin=data_interval_start,
        last_modified_end=data_interval_end
    )
    output_files = awswrangler.s3.list_objects(output_path, **time_window)
    processed_files = [base_file_name(x, start=4, ext='parquet') + 'json.gz' for x in output_files]
    input_files = awswrangler.s3.list_objects(path, **time_window, ignore_suffix=processed_files)

    return input_files


def base_file_name(x: str, start: int, ext: str):
    return x.split('/', maxsplit=start)[-1][:-len(ext)]
