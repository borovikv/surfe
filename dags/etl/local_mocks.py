import datetime
from io import BytesIO
from typing import Any, Literal, Callable
from urllib.parse import urlparse

import awswrangler.s3
import boto3
import pandas as pd
from awswrangler.typing import RaySettings


def list_objects(
        path: str,
        suffix: str | list[str] | None = None,
        ignore_suffix: str | list[str] | None = None,
        last_modified_begin: datetime.datetime | None = None,
        last_modified_end: datetime.datetime | None = None,
        ignore_empty: bool = False,
        chunked: bool = False,
        s3_additional_kwargs: dict[str, Any] | None = None,
):
    """
    Replacement for awswrangler.s3.list_objects so it can work with moto server
    """
    s3_client = s3()
    # On top of user provided ignore_suffix input, add "/"
    ignore_suffix_acc = set("/")
    if isinstance(ignore_suffix, str):
        ignore_suffix_acc.add(ignore_suffix)
    elif isinstance(ignore_suffix, list):
        ignore_suffix_acc.update(ignore_suffix)

    result_iterator = awswrangler.s3._list._list_objects(
        path=path,
        suffix=suffix,
        ignore_suffix=list(ignore_suffix_acc),
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_empty=ignore_empty,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if chunked:
        return result_iterator
    return [path for paths in result_iterator for path in paths]


def s3():
    return boto3.client('s3', endpoint_url='http://moto:3000')


def read_json(
        path: str | list[str],
        path_suffix: str | list[str] | None = None,
        path_ignore_suffix: str | list[str] | None = None,
        version_id: str | dict[str, str] | None = None,
        ignore_empty: bool = True,
        orient: str = "columns",
        use_threads: bool | int = True,
        last_modified_begin: datetime.datetime | None = None,
        last_modified_end: datetime.datetime | None = None,
        s3_additional_kwargs: dict[str, Any] | None = None,
        dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
        chunksize: int | None = None,
        dataset: bool = False,
        partition_filter: Callable[[dict[str, str]], bool] | None = None,
        ray_args: RaySettings | None = None,
        **pandas_kwargs: Any,
):
    if dtype_backend != "numpy_nullable":
        pandas_kwargs["dtype_backend"] = dtype_backend

    s3_client = s3()

    if (dataset is True) and ("lines" not in pandas_kwargs):
        pandas_kwargs["lines"] = True
    pandas_kwargs["orient"] = orient
    ignore_index: bool = orient not in ("split", "index", "columns")

    return awswrangler.s3._read_text._read_text_format(
        read_format="json",
        path=path,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        version_id=version_id,
        ignore_empty=ignore_empty,
        use_threads=use_threads,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        dataset=dataset,
        partition_filter=partition_filter,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_index=ignore_index,
        ray_args=ray_args,
        **pandas_kwargs,
    )


def to_parquet(df, path):
    s3_client = s3()
    with BytesIO() as buf:
        df.to_parquet(buf)
        s3_client.put_object(**split_s3_path(path), Body=buf.getvalue())


def split_s3_path(s3_path):
    parsed_url = urlparse(s3_path)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')
    return dict(Bucket=bucket, Key=key)
